//! The passive notice a thread gets when someone else starts or moves one of
//! its held children.
//!
//! A thread keeps a picture of its own queue (`cairn:~/queue`: "6089 is 9th").
//! When the operator presses Start now on one of its children, or drags one to
//! a new place in the fleet's order, or another thread does either under the
//! operator's grant, that picture is wrong and nothing in the thread's own
//! record says why. So the act queues ONE passive line on the child's
//! coordinating job (`wakes::coordinating_job_for_child`, the rule every
//! child-sourced signal uses), naming who acted, what they did, and where the
//! child stood before:
//!
//! - `The operator started cairn/6089 now (was 9th in execution's queue, skipping the fleet cap).`
//! - `The operator moved cairn/6089 to 3rd in the fleet's order (was 9th).`
//!
//! The line is a keyed system direct: its frozen message renders inline in the
//! recipient's next turn, so the thread learns it without reading anything,
//! and it is passive, so it never wakes the thread by itself. Its message id is
//! the act's record: a Start now's id derives from the execution (an execution
//! starts once), so a retried or repeated delivery never adds a second line.
//!
//! No notice goes out when the acting thread IS the owning thread (it knows),
//! nor for a sweep's ordinary release, which is not an act on the thread's
//! record. A closed thread has no coordinating job, so it takes none.
//! Delivery is best effort: a failure is logged and never fails the act.

use cairn_db::turso::params;

use super::capacity::{CapacityHoldExt, FleetOrdered};
use super::census::ordinal;
use super::sweep::CapSkipAuthority;
use crate::messages::queued::DeliveryUrgency;
use crate::models::CapacityHold;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt, TrackedConnection};

/// The push-key prefix of a queue notice: `queue:{message id}`.
pub(crate) const QUEUE_NOTICE_PREFIX: &str = "queue";

/// Where a held execution stood before an act on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Place {
    /// Its 1-based place in its thread's queue, as `cairn:~/queue` lists it.
    Thread { name: String, place: usize },
    /// Its 1-based place in the fleet's order, for work with no thread.
    Fleet { place: usize },
    /// In no queue: it waited only on its own `after:` gates.
    Gates,
}

impl Place {
    fn phrase(&self) -> String {
        match self {
            Place::Thread { name, place } => format!("was {} in {name}'s queue", ordinal(*place)),
            Place::Fleet { place } => format!("was {} in the fleet's order", ordinal(*place)),
            Place::Gates => "was waiting on its after: gates".to_string(),
        }
    }
}

/// A held execution about to be started now, as its owner will be told.
#[derive(Debug, Clone)]
pub(crate) struct StartSubject {
    issue_id: String,
    /// `cairn/6089`.
    label: String,
    place: Place,
}

/// The issue, label, and thread of an execution.
const SUBJECT_SQL: &str = "SELECT i.id, i.number, p.key, i.parent_thread_id, t.name
   FROM executions e
   JOIN issues i ON i.id = e.issue_id
   JOIN projects p ON p.id = i.project_id
   LEFT JOIN threads t ON t.id = i.parent_thread_id
  WHERE e.id = ?1";

fn issue_label(project_key: &str, number: i64) -> String {
    format!(
        "{}/{number}",
        cairn_common::uri::canonical_project(project_key)
    )
}

/// Where `execution_id` stands before a Start now: its place in its thread's
/// queue, or in the fleet's order when it has no thread. Read under the
/// admission lock the release holds, so it is the line the start skips.
/// `None` for an execution with no issue, or when the read fails.
pub(crate) async fn place_before_start(
    orch: &Orchestrator,
    db: &LocalDb,
    execution_id: &str,
) -> Option<StartSubject> {
    let id = execution_id.to_string();
    let read = db
        .read(move |conn| {
            let id = id.clone();
            Box::pin(async move { subject_conn(conn, &id).await })
        })
        .await;
    let (issue_id, label, thread_place) = match read {
        Ok(Some(found)) => found,
        Ok(None) => return None,
        Err(error) => {
            log::warn!("queue notice: could not read {execution_id}'s place: {error}");
            return None;
        }
    };
    let place = match thread_place {
        Some(place) => place,
        None => match super::capacity::fleet_order(orch).await {
            Ok(order) => fleet_place(&order, execution_id),
            Err(error) => {
                log::warn!("queue notice: could not read the fleet's order: {error}");
                Place::Gates
            }
        },
    };
    Some(StartSubject {
        issue_id,
        label,
        place,
    })
}

/// The execution's issue and label, and its thread place when it has a thread
/// (`None` there means "no thread: ask the fleet").
async fn subject_conn(
    conn: &TrackedConnection,
    execution_id: &str,
) -> DbResult<Option<(String, String, Option<Place>)>> {
    let mut rows = conn.query(SUBJECT_SQL, params![execution_id]).await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let issue_id = row.text(0)?;
    let label = issue_label(&row.text(2)?, row.i64(1)?);
    let thread = row.opt_text(3)?.zip(row.opt_text(4)?);
    drop(rows);
    let place = match thread {
        Some((thread_id, name)) => {
            let queue = super::reorder::thread_queue_conn(conn, &thread_id).await?;
            Some(
                match queue
                    .iter()
                    .position(|queued| queued.execution_id == execution_id)
                {
                    Some(index) => Place::Thread {
                        name,
                        place: index + 1,
                    },
                    None => Place::Gates,
                },
            )
        }
        None => None,
    };
    Ok(Some((issue_id, label, place)))
}

fn fleet_place(order: &[FleetOrdered], execution_id: &str) -> Place {
    order
        .iter()
        .position(|entry| entry.queued.execution_id == execution_id)
        .map_or(Place::Gates, |index| Place::Fleet { place: index + 1 })
}

/// The line a Start now leaves its owner.
pub(crate) fn start_line(
    subject: &StartSubject,
    by: &CapSkipAuthority,
    skipped: &[CapacityHold],
) -> String {
    let skipped: Vec<String> = skipped
        .iter()
        .map(|hold| hold.skipped_label().to_string())
        .collect();
    let detail = format!(
        "{}{}",
        subject.place.phrase(),
        super::census::skipped_phrase(&skipped)
    );
    match by {
        CapSkipAuthority::Operator => {
            format!("The operator started {} now ({detail}).", subject.label)
        }
        CapSkipAuthority::Granted { thread, .. } => format!(
            "{thread} started {} now under the operator's grant ({detail}).",
            subject.label
        ),
    }
}

/// After a Start now commits: tell the child's owner, unless the owner is the
/// thread that started it.
pub(crate) async fn started(
    orch: &Orchestrator,
    db: &LocalDb,
    execution_id: &str,
    subject: StartSubject,
    by: &CapSkipAuthority,
    skipped: &[CapacityHold],
) {
    let actor = match by {
        CapSkipAuthority::Operator => None,
        CapSkipAuthority::Granted { thread, .. } => Some(thread.as_str()),
    };
    let content = start_line(&subject, by, skipped);
    let message_id = super::sweep::wake_message_id("queue-start", execution_id);
    deliver(orch, db, &subject.issue_id, actor, &message_id, &content).await;
}

/// One fleet move that changed a child's place, as its owner will be told.
#[derive(Debug, Clone)]
pub(crate) struct MovedSubject {
    pub db: std::sync::Arc<LocalDb>,
    pub issue_id: String,
    /// `cairn/6089`.
    pub label: String,
    /// Its 1-based place in the fleet's order before the move.
    pub from: usize,
    /// Where it landed.
    pub to: usize,
}

impl MovedSubject {
    /// The subject of moving `entry` from `from`, when it has an issue.
    pub(crate) fn of(entry: &FleetOrdered, from: usize, to: usize) -> Option<Self> {
        let issue_id = entry.queued.issue_id.clone()?;
        let label = match (&entry.queued.project_key, entry.queued.issue_number) {
            (Some(project), Some(number)) => issue_label(project, number),
            _ => entry.queued.execution_id.clone(),
        };
        Some(Self {
            db: entry.db.clone(),
            issue_id,
            label,
            from,
            to,
        })
    }
}

/// The line a fleet move leaves its owner.
pub(crate) fn move_line(subject: &MovedSubject, mover: &str) -> String {
    let mover = if mover == "the operator" {
        "The operator"
    } else {
        mover
    };
    format!(
        "{mover} moved {} to {} in the fleet's order (was {}).",
        subject.label,
        ordinal(subject.to),
        ordinal(subject.from)
    )
}

/// After a fleet move commits: tell the moved child's owner, unless the owner
/// is the thread that moved it. `actor` is the moving thread's name.
pub(crate) async fn moved(
    orch: &Orchestrator,
    subject: &MovedSubject,
    mover: &str,
    actor: Option<&str>,
) {
    let content = move_line(subject, mover);
    let message_id = uuid::Uuid::new_v4().to_string();
    deliver(
        orch,
        &subject.db,
        &subject.issue_id,
        actor,
        &message_id,
        &content,
    )
    .await;
}

/// The name of the thread whose session `job_id` is, if it is one.
const JOB_THREAD_NAME_SQL: &str =
    "SELECT t.name FROM jobs j JOIN threads t ON t.id = j.thread_id WHERE j.id = ?1";

/// Queue `content` as one passive system direct on `issue_id`'s coordinating
/// job, under message id `message_id`, in one transaction. Nothing when the
/// message already exists, the child has no live owner, the owner is the
/// acting thread, or the owner has never run.
async fn deliver(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
    actor: Option<&str>,
    message_id: &str,
    content: &str,
) {
    let (issue, actor, message, body) = (
        issue_id.to_string(),
        actor.map(str::to_string),
        message_id.to_string(),
        content.to_string(),
    );
    let delivered = db
        .write(move |conn| {
            let (issue, actor, message, body) =
                (issue.clone(), actor.clone(), message.clone(), body.clone());
            Box::pin(
                async move { deliver_conn(conn, &issue, actor.as_deref(), &message, &body).await },
            )
        })
        .await;
    match delivered {
        Ok(Some(recipient)) => {
            log::info!("queue notice for {issue_id} queued on {recipient}: {content}");
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );
        }
        Ok(None) => {}
        Err(error) => log::warn!("queue notice for {issue_id} failed: {error}"),
    }
}

async fn deliver_conn(
    conn: &TrackedConnection,
    issue_id: &str,
    actor: Option<&str>,
    message_id: &str,
    content: &str,
) -> DbResult<Option<String>> {
    if super::sweep::wake_already_sent_conn(conn, message_id).await? {
        return Ok(None);
    }
    let Some(recipient) =
        crate::orchestrator::wakes::coordinating_job_for_child(conn, issue_id).await?
    else {
        return Ok(None);
    };
    if let Some(actor) = actor {
        let owner = crate::storage::query_opt_text_conn(
            conn,
            JOB_THREAD_NAME_SQL,
            params![recipient.as_str()],
        )
        .await?;
        if owner.as_deref() == Some(actor) {
            return Ok(None);
        }
    }
    let Some(run) = super::sweep::recipient_run_conn(conn, &recipient).await? else {
        return Ok(None);
    };
    let key = format!("{QUEUE_NOTICE_PREFIX}:{message_id}");
    cairn_attention::direct::insert_system_direct_push_with_message_id_conn(
        conn,
        &recipient,
        &run,
        content,
        DeliveryUrgency::Passive,
        Some(&key),
        message_id,
    )
    .await?;
    Ok(Some(recipient))
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::storage::{LocalDb, RowExt};

    /// Every queue notice queued on `recipient`, oldest first: (wake, line).
    pub(crate) async fn notices(db: &LocalDb, recipient: &str) -> Vec<(String, String)> {
        let recipient = recipient.to_string();
        db.read(move |conn| {
            let recipient = recipient.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT a.wake, m.content FROM attention_pushes a
                           JOIN messages m ON m.id = a.fingerprint
                          WHERE a.recipient = ?1 AND a.key LIKE 'queue:%'
                          ORDER BY a.created_at, m.content",
                        cairn_db::turso::params![recipient.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?));
                }
                Ok(out)
            })
        })
        .await
        .unwrap()
    }

    /// A live session job `job` for thread `thread`, with run `run`.
    pub(crate) async fn thread_session(db: &LocalDb, thread: &str, job: &str, run: &str) {
        db.execute_script(format!(
            "INSERT INTO jobs(id, project_id, node_name, status, thread_id, uri_segment,
                              created_at, updated_at)
               VALUES('{job}', 'p', 'session', 'running', '{thread}', 'thread', 1, 1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
               VALUES('{run}', 'p', '{job}', 'live', 1, 1);"
        ))
        .await
        .unwrap();
    }
}
