//! Evaluation events: the snapshot of one conclusion, and its lifecycle.
//!
//! An event is inserted in the same write as the conclusion it snapshots, so a
//! conclusion and its event commit together. Its dedupe key names the
//! conclusion, so concluding the same thing twice inserts one event.

use cairn_db::turso::{params, Connection};

use super::{cap_chars, stable_id, BACKOFF_SECS, EVENT_TEXT_CHARS, MAX_ATTEMPTS};
use crate::storage::{DbResult, LocalDb, RowExt};
use crate::transitions::Resolution;

pub const KIND_ISSUE: &str = "issue_resolution";
pub const KIND_ERA: &str = "era";

/// An event about to be inserted.
#[derive(Debug, Clone, PartialEq)]
pub struct NewEvent {
    pub project_id: String,
    pub source_uri: String,
    pub source_kind: &'static str,
    pub dedupe_key: String,
    pub owner_device_id: String,
    pub text: String,
    pub created_at: i64,
}

impl NewEvent {
    pub fn id(&self) -> String {
        stable_id("tenet-evidence-event", &self.dedupe_key)
    }
}

/// Insert `event` unless its conclusion already has one. Reports whether this
/// call inserted it.
pub async fn insert_event_conn(conn: &Connection, event: &NewEvent) -> DbResult<bool> {
    let changed = conn
        .execute(
            "INSERT OR IGNORE INTO tenet_evidence_events
                 (id, project_id, source_uri, source_kind, dedupe_key, owner_device_id,
                  text, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.id(),
                event.project_id.as_str(),
                event.source_uri.as_str(),
                event.source_kind,
                event.dedupe_key.as_str(),
                event.owner_device_id.as_str(),
                event.text.as_str(),
                event.created_at
            ],
        )
        .await?;
    Ok(changed > 0)
}

fn resolution_name(resolution: Resolution) -> &'static str {
    match resolution {
        Resolution::Merged => "merged",
        Resolution::Closed => "closed",
    }
}

/// The event for an issue's terminal resolution, read inside the resolving
/// write. `None` when the issue row is gone.
///
/// The dedupe key carries `resolution_generation`, which `unresolve` bumps:
/// resolution instants cannot tell two resolutions apart, the counter can.
pub async fn issue_event_conn(
    conn: &Connection,
    issue_id: &str,
    resolution: Resolution,
    device_id: &str,
    now: i64,
) -> DbResult<Option<NewEvent>> {
    let mut rows = conn
        .query(
            "SELECT i.project_id, p.key, i.number, i.title, i.description,
                    i.resolution_generation
             FROM issues i JOIN projects p ON p.id = i.project_id
             WHERE i.id = ?1",
            params![issue_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let project_id = row.text(0)?;
    let key = row.text(1)?;
    let number = row.i64(2)?;
    let title = row.text(3)?;
    let description = row.opt_text(4)?.unwrap_or_default();
    let generation = row.i64(5)?;
    drop(rows);

    let mut pr_rows = conn
        .query(
            "SELECT title, body FROM merge_requests WHERE issue_id = ?1
             ORDER BY opened_at DESC LIMIT 1",
            params![issue_id],
        )
        .await?;
    let pr = match pr_rows.next().await? {
        Some(row) => Some((row.opt_text(0)?, row.opt_text(1)?)),
        None => None,
    };
    drop(pr_rows);

    let source_uri = cairn_common::uri::build_issue_uri(&key, number as i32);
    let resolution = resolution_name(resolution);
    let header = format!("Issue {source_uri} \u{2014} {title}\nResolution: {resolution}");
    let pull_request = pr
        .map(|(pr_title, pr_body)| {
            format!(
                "\n\nPull request: {}\n{}",
                pr_title.unwrap_or_default(),
                pr_body.unwrap_or_default()
            )
        })
        .unwrap_or_default();
    Ok(Some(NewEvent {
        project_id,
        source_uri,
        source_kind: KIND_ISSUE,
        dedupe_key: format!("issue:{issue_id}:{resolution}:{generation}"),
        owner_device_id: device_id.to_string(),
        text: issue_snapshot(&header, &description, &pull_request),
        created_at: now,
    }))
}

/// How much of the snapshot an issue's description keeps even when its pull
/// request is long.
const DESCRIPTION_RESERVE_CHARS: usize = 1000;

/// An issue's snapshot within [`EVENT_TEXT_CHARS`]. The description is what
/// was proposed and the pull request is what was done, so the pull request
/// keeps its place when the description alone would fill the budget: a plan
/// that said one thing and a merge that did another must reach the judge as
/// both.
fn issue_snapshot(header: &str, description: &str, pull_request: &str) -> String {
    let description = format!("\n\n{description}");
    let budget = EVENT_TEXT_CHARS.saturating_sub(header.chars().count());
    let reserve = description
        .chars()
        .count()
        .min(DESCRIPTION_RESERVE_CHARS)
        .min(budget);
    let pull_request = cap_chars(pull_request.to_string(), budget - reserve);
    let description = cap_chars(description, budget - pull_request.chars().count());
    cap_chars(
        format!("{header}{description}{pull_request}"),
        EVENT_TEXT_CHARS,
    )
}

/// Snapshot an issue's terminal resolution as an event, inside the resolving
/// write.
pub async fn insert_issue_event_conn(
    conn: &Connection,
    issue_id: &str,
    resolution: Resolution,
    device_id: &str,
    now: i64,
) -> DbResult<bool> {
    match issue_event_conn(conn, issue_id, resolution, device_id, now).await? {
        Some(event) => insert_event_conn(conn, &event).await,
        None => Ok(false),
    }
}

/// The event for an era whose generated summary just landed, read from the
/// database holding the era. `None` when the era's job has no home address.
pub async fn era_event_conn(
    conn: &Connection,
    job_id: &str,
    seq: i32,
    summary: &str,
    device_id: &str,
    now: i64,
) -> DbResult<Option<NewEvent>> {
    let mut rows = conn
        .query("SELECT project_id FROM jobs WHERE id = ?1", params![job_id])
        .await?;
    let project_id = match rows.next().await? {
        Some(row) => row.opt_text(0)?,
        None => None,
    };
    drop(rows);
    let Some(project_id) = project_id else {
        log::debug!("era {job_id}#{seq} has no project; no tenet evidence event");
        return Ok(None);
    };
    let Some(home) = cairn_db::lineage::home_uri_for_job_conn(conn, job_id).await? else {
        log::debug!("era {job_id}#{seq} has no home address; no tenet evidence event");
        return Ok(None);
    };
    let source_uri = format!("{home}/eras/{seq}");

    let mut child_rows = conn
        .query(
            "SELECT child_issue_uri, child_title, final_status
             FROM thread_era_children
             WHERE job_id = ?1 AND era_seq = ?2
             ORDER BY child_issue_id ASC",
            params![job_id, seq],
        )
        .await?;
    let mut children = String::new();
    while let Some(row) = child_rows.next().await? {
        children.push_str(&format!(
            "\n- {} ({}) {}",
            row.text(1)?,
            row.text(2)?,
            row.text(0)?
        ));
    }
    drop(child_rows);

    let text = format!("Thread era {source_uri}\nSummary: {summary}\nChildren:{children}");
    Ok(Some(NewEvent {
        project_id,
        source_uri,
        source_kind: KIND_ERA,
        dedupe_key: format!("era:{job_id}:{seq}"),
        owner_device_id: device_id.to_string(),
        text: cap_chars(text, EVENT_TEXT_CHARS),
        created_at: now,
    }))
}

/// An event this machine owes an evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingEvent {
    pub id: String,
    pub project_id: String,
    pub source_uri: String,
    pub owner_device_id: String,
    pub text: String,
    pub attempts: i64,
}

/// The pending events `device` owns whose retry time has come, oldest first.
pub async fn due_events(
    db: &LocalDb,
    device: &str,
    now: i64,
    limit: usize,
) -> DbResult<Vec<PendingEvent>> {
    db.query_all(
        "SELECT id, project_id, source_uri, owner_device_id, text, attempts
         FROM tenet_evidence_events
         WHERE owner_device_id = ?1 AND state = 'pending' AND next_attempt_at <= ?2
         ORDER BY created_at, id
         LIMIT ?3",
        params![device, now, limit as i64],
        |row| {
            Ok(PendingEvent {
                id: row.text(0)?,
                project_id: row.text(1)?,
                source_uri: row.text(2)?,
                owner_device_id: row.text(3)?,
                text: row.text(4)?,
                attempts: row.i64(5)?,
            })
        },
    )
    .await
}

/// Settle an event that needed no judgement.
pub async fn mark_skipped(db: &LocalDb, event: &PendingEvent, now: i64) -> DbResult<bool> {
    let changed = db
        .execute(
            "UPDATE tenet_evidence_events SET state = 'skipped', evaluated_at = ?1
             WHERE id = ?2 AND state = 'pending' AND owner_device_id = ?3",
            params![now, event.id.as_str(), event.owner_device_id.as_str()],
        )
        .await?;
    Ok(changed > 0)
}

/// Record one failed attempt: back off, or give up after [`MAX_ATTEMPTS`].
pub async fn mark_attempt_failed(
    db: &LocalDb,
    event: &PendingEvent,
    now: i64,
    error: &str,
) -> DbResult<bool> {
    let attempts = event.attempts + 1;
    let (state, next) = if attempts >= MAX_ATTEMPTS {
        ("failed", now)
    } else {
        let backoff = BACKOFF_SECS[(attempts as usize - 1).min(BACKOFF_SECS.len() - 1)];
        ("pending", now + backoff)
    };
    let changed = db
        .execute(
            "UPDATE tenet_evidence_events
             SET attempts = ?1, state = ?2, next_attempt_at = ?3, last_error = ?4,
                 evaluated_at = CASE WHEN ?2 = 'failed' THEN ?5 ELSE evaluated_at END
             WHERE id = ?6 AND state = 'pending' AND owner_device_id = ?7",
            params![
                attempts,
                state,
                next,
                error,
                now,
                event.id.as_str(),
                event.owner_device_id.as_str()
            ],
        )
        .await?;
    Ok(changed > 0)
}
