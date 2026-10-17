//! What the operator's verdict tells the work it was asked about.
//!
//! A verdict reaches two audiences, in the same transaction that records it,
//! so neither can be lost or doubled:
//!
//! - the proposing thread gets one system direct message, keyed
//!   `tenet-verdict:<id>`: the verdict, the chosen text, the rejected
//!   alternatives, the comment, and what it gated. A decline steers the
//!   thread awake; an approval rides along quietly with its next turn.
//! - every issue an approved tenet releases gets a comment naming the choice,
//!   so the planner that starts on release reads what the operator decided
//!   from the issue it works. The comment's id is derived from the tenet and
//!   the issue, so it is written once per pair however many paths reach it:
//!   here for the gates that exist when the verdict is given, and in the gate
//!   sweep, before release, for a gate that names an already-approved tenet
//!   (a later filing of the same plan, a re-gate, or a filing racing the
//!   answer).

use cairn_db::turso::{params, Connection};

use super::read::answerer_label;
use super::{tenet_uri, TenetRow, TenetStatus, TENET_GATE_FACT};
use crate::messages::queued::DeliveryUrgency;
use crate::storage::{DbResult, RowExt};

/// What delivering a verdict wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Delivered {
    /// The proposing thread's session job, when the message reached it and
    /// its urgency wants an idle session roused.
    pub wake: Option<String>,
    /// How many gated issues got the approval comment.
    pub comments: usize,
}

/// The id of a tenet's verdict message: a UUID derived from the tenet, so the
/// message row itself records that the verdict was delivered.
fn verdict_message_id(tenet_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(verdict_key(tenet_id).as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes)
        .into_uuid()
        .to_string()
}

fn verdict_key(tenet_id: &str) -> String {
    format!("tenet-verdict:{tenet_id}")
}

/// The id of tenet `tenet_id`'s decision comment on `issue_id`: a UUID derived
/// from the pair, carrying the issue's routing scope like any minted comment
/// id, so the comment row itself records that the decision was delivered.
fn decision_comment_id(tenet_id: &str, issue_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("tenet-decision:{tenet_id}:{issue_id}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    let uuid = uuid::Builder::from_random_bytes(bytes)
        .into_uuid()
        .to_string();
    match cairn_common::ids::parse_route_scope(issue_id) {
        Ok(cairn_common::ids::RouteScope::Team(team)) => format!("{team}~{uuid}"),
        _ => uuid,
    }
}

/// Write approved tenet `tenet`'s decision comment on `issue_id` unless it is
/// already there, inside the caller's transaction. Returns whether it wrote.
/// A tenet without an approval in force has no decision to deliver.
pub(crate) async fn ensure_decision_comment_conn(
    conn: &Connection,
    tenet: &TenetRow,
    issue_id: &str,
    now: i64,
) -> DbResult<bool> {
    if !matches!(
        tenet.status,
        TenetStatus::Approved | TenetStatus::Superseded
    ) {
        return Ok(false);
    }
    let id = decision_comment_id(&tenet.id, issue_id);
    if crate::storage::query_opt_text_conn(
        conn,
        "SELECT id FROM comments WHERE id = ?1",
        params![id.as_str()],
    )
    .await?
    .is_some()
    {
        return Ok(false);
    }
    let label = answerer_label(tenet.answer_source, tenet.answered_by.as_ref())
        .unwrap_or_else(|| "unattributed".to_string());
    crate::issues::comments::insert_conn(
        conn,
        &id,
        issue_id,
        &approval_comment(tenet, &label),
        &crate::models::CommentSource::User.to_string(),
        now,
    )
    .await?;
    Ok(true)
}

/// [`ensure_decision_comment_conn`] for the tenet a `tenet_verdict` clause
/// names, in the database that holds the gated issue. Returns whether it
/// wrote; a tenet this database cannot see delivers nothing.
pub(crate) async fn ensure_decision_comment(
    db: &crate::storage::LocalDb,
    clause: &crate::routes::TriggerClause,
    issue_id: &str,
    now: i64,
) -> Result<bool, String> {
    let Some(Ok((project, number))) = clause
        .get("tenet")
        .and_then(serde_json::Value::as_str)
        .map(crate::execution::gates::vocabulary::parse_tenet_uri)
    else {
        return Ok(false);
    };
    let issue_id = issue_id.to_string();
    db.write(move |conn| {
        let project = project.clone();
        let issue_id = issue_id.clone();
        Box::pin(async move {
            match super::lookup_conn(conn, &project, number).await? {
                super::TenetLookup::Found(tenet) => {
                    ensure_decision_comment_conn(conn, &tenet, &issue_id, now).await
                }
                _ => Ok(false),
            }
        })
    })
    .await
    .map_err(|error| format!("Failed to deliver the tenet's decision: {error}"))
}

/// One execution held on the tenet, by issue.
struct Gated {
    issue_id: String,
    issue_uri: String,
    exec_seq: Option<i64>,
}

async fn gated_conn(conn: &Connection, uri: &str) -> DbResult<Vec<Gated>> {
    let mut rows = conn
        .query(
            "SELECT e.issue_id, e.seq FROM execution_gates g
               JOIN executions e ON e.id = g.execution_id
              WHERE json_extract(g.predicate_json, '$.fact') = ?1
                AND json_extract(g.predicate_json, '$.tenet') = ?2
                AND e.issue_id IS NOT NULL
              ORDER BY e.started_at, e.seq",
            params![TENET_GATE_FACT, uri],
        )
        .await?;
    let mut found = Vec::new();
    while let Some(row) = rows.next().await? {
        found.push((row.text(0)?, row.opt_i64(1)?));
    }
    drop(rows);
    let mut gated = Vec::with_capacity(found.len());
    for (issue_id, exec_seq) in found {
        let issue_uri = crate::issues::relations::issue_uri_for_id(conn, &issue_id).await?;
        gated.push(Gated {
            issue_id,
            issue_uri,
            exec_seq,
        });
    }
    Ok(gated)
}

fn list(items: &[&str]) -> String {
    if items.is_empty() {
        "none".to_string()
    } else {
        items.join("; ")
    }
}

/// The approval comment a gated issue gets.
fn approval_comment(tenet: &TenetRow, label: &str) -> String {
    format!(
        "Tenet #{} approved by the operator ({label}): {}. Rejected: {}. Comment: {}.",
        tenet.number,
        tenet.text().unwrap_or("?"),
        list(&tenet.rejected()),
        tenet.comment.as_deref().unwrap_or("none"),
    )
}

fn verdict_message(
    tenet: &TenetRow,
    uri: &str,
    label: &str,
    superseded: Option<i64>,
    gated: &[Gated],
) -> String {
    let number = tenet.number;
    let approved = tenet.status != TenetStatus::Declined;
    let verdict = match (tenet.verdict.as_deref(), tenet.selected_option) {
        (Some("your_call"), _) => {
            "approved it as your call, adopting your recommendation, option 1".to_string()
        }
        (_, Some(index)) => format!("approved option {}", index + 1),
        _ => "declined it: none of the options".to_string(),
    };
    let mut lines = vec![
        format!("The operator ({label}) answered tenet #{number} ({uri}): {verdict}."),
        format!("Question: {}", tenet.question),
    ];
    if let Some(text) = tenet.text() {
        lines.push(format!("Chosen: {text}"));
    }
    lines.push(format!("Rejected: {}", list(&tenet.rejected())));
    lines.push(format!(
        "Comment: {}",
        tenet.comment.as_deref().unwrap_or("none")
    ));
    if let Some(previous) = superseded {
        lines.push(format!(
            "It supersedes tenet #{previous}, which is no longer in force."
        ));
    }
    if gated.is_empty() {
        lines.push("It gated no filing.".to_string());
    } else {
        let gates: Vec<String> = gated
            .iter()
            .map(|gate| match gate.exec_seq {
                Some(seq) => format!("{}/executions/{seq}", gate.issue_uri),
                None => gate.issue_uri.clone(),
            })
            .collect();
        lines.push(if approved {
            format!(
                "It gated {}: each starts once its other gates are met, and its issue carries \
                 this choice as a comment.",
                gates.join(", ")
            )
        } else {
            format!(
                "It gated {}: each stays held, and its foreclosure names your exits.",
                gates.join(", ")
            )
        });
    }
    lines.join("\n")
}

/// Deliver an answered tenet's verdict inside the transaction that recorded
/// it. A proposing thread that cannot be addressed (closed, or with no run to
/// address yet) does not fail the verdict: the operator's answer stands, and
/// the thread reads it from the tenet when it next runs.
pub(super) async fn deliver_conn(
    conn: &Connection,
    tenet: &TenetRow,
    superseded: Option<i64>,
    now: i64,
) -> DbResult<Delivered> {
    let mut delivered = Delivered::default();
    let mut rows = conn
        .query(
            "SELECT key FROM projects WHERE id = ?1",
            params![tenet.project_id.as_str()],
        )
        .await?;
    let Some(project_key) = rows.next().await?.map(|row| row.text(0)).transpose()? else {
        return Ok(delivered);
    };
    drop(rows);
    let uri = tenet_uri(&project_key, tenet.number);
    let label = answerer_label(tenet.answer_source, tenet.answered_by.as_ref())
        .unwrap_or_else(|| "unattributed".to_string());
    let gated = gated_conn(conn, &uri).await?;

    for gate in &gated {
        if ensure_decision_comment_conn(conn, tenet, &gate.issue_id, now).await? {
            delivered.comments += 1;
        }
    }

    let message_id = verdict_message_id(&tenet.id);
    if crate::storage::query_opt_text_conn(
        conn,
        "SELECT id FROM messages WHERE id = ?1",
        params![message_id.as_str()],
    )
    .await?
    .is_some()
    {
        return Ok(delivered);
    }
    let active = crate::threads::thread_status_conn(conn, &tenet.proposing_thread_id)
        .await?
        .is_some_and(|status| status.is_active());
    if !active {
        log::info!(
            "tenet {uri} was answered, but its proposing thread is closed; the verdict waits on the tenet"
        );
        return Ok(delivered);
    }
    let session =
        crate::threads::ensure_thread_session_conn(conn, &tenet.proposing_thread_id, None).await?;
    let Some(run) = crate::storage::query_opt_text_conn(
        conn,
        "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, id DESC LIMIT 1",
        params![session.as_str()],
    )
    .await?
    else {
        log::warn!(
            "tenet {uri} was answered, but its proposing thread's session has no run to address; the verdict waits on the tenet"
        );
        return Ok(delivered);
    };
    let urgency = if tenet.status == TenetStatus::Declined {
        DeliveryUrgency::Steer
    } else {
        DeliveryUrgency::Passive
    };
    cairn_attention::direct::insert_system_direct_push_with_message_id_conn(
        conn,
        &session,
        &run,
        &verdict_message(tenet, &uri, &label, superseded, &gated),
        urgency,
        Some(&verdict_key(&tenet.id)),
        &message_id,
    )
    .await?;
    if urgency.wakes_idle() {
        delivered.wake = Some(session);
    }
    Ok(delivered)
}
