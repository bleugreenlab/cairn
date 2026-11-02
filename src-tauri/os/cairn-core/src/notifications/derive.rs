//! One derivation per kind. Each reads its source rows and decides, from the
//! row alone, whether the cause is open now or when it resolved.
//!
//! Two rules recur because the live data needs them. A question or permission
//! is open only on its job's current turn, the rule the sidebar's awaiting-input
//! state already uses: a run that crashed or exited leaves its unanswered prompt
//! behind without withdrawing it. And nothing is open on an issue that already
//! merged or closed: superseded executions leave PR rows `open` behind them, and
//! the issue's own resolution is what says the work is over.

use super::{
    HoldRegistry, NotificationAction, NotificationKind, NotificationState, OperatorNotification,
    PermissionActionRequest, ProjectRef,
};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_common::uri;
use cairn_db::turso::{params, Row};

/// The longest summary line, in characters.
const SUMMARY_CHARS: usize = 160;

/// Where a job lives, for its address and its label. Selected beside any
/// statement that has a job aliased `j` and [`PLACE_JOINS`] in its FROM.
const PLACE_COLUMNS: &str = "j.id, j.node_name, j.uri_segment, parent.uri_segment, e.seq,
    i.number, i.status, i.updated_at, th.name";
const PLACE_JOINS: &str = "LEFT JOIN jobs parent ON parent.id = j.parent_job_id
    LEFT JOIN executions e ON e.id = j.execution_id
    LEFT JOIN issues i ON i.id = j.issue_id
    LEFT JOIN threads th ON th.id = COALESCE(j.thread_id, parent.thread_id)";

struct JobPlace {
    job_id: String,
    node_name: Option<String>,
    segment: Option<String>,
    parent_segment: Option<String>,
    exec_seq: Option<i64>,
    issue_number: Option<i64>,
    issue_status: Option<String>,
    issue_updated_at: Option<i64>,
    thread_name: Option<String>,
}

impl JobPlace {
    fn read(row: &Row, at: usize) -> Result<Self, DbError> {
        Ok(Self {
            job_id: row.text(at)?,
            node_name: row.opt_text(at + 1)?,
            segment: row.opt_text(at + 2)?,
            parent_segment: row.opt_text(at + 3)?,
            exec_seq: row.opt_i64(at + 4)?,
            issue_number: row.opt_i64(at + 5)?,
            issue_status: row.opt_text(at + 6)?,
            issue_updated_at: row.opt_i64(at + 7)?,
            thread_name: row.opt_text(at + 8)?,
        })
    }

    /// When the job's issue merged or closed, if it did.
    fn issue_resolved_at(&self) -> Option<i64> {
        issue_resolved_at(self.issue_status.as_deref(), self.issue_updated_at)
    }

    fn node_address(&self) -> Option<(i32, i32, &str)> {
        Some((
            self.issue_number? as i32,
            self.exec_seq? as i32,
            self.segment.as_deref()?,
        ))
    }

    fn uri(&self, project_key: &str) -> String {
        if let Some((number, seq, segment)) = self.node_address() {
            return uri::build_job_base_uri(
                project_key,
                number,
                seq,
                segment,
                self.parent_segment.as_deref(),
            );
        }
        match (&self.thread_name, &self.parent_segment, &self.segment) {
            (Some(thread), Some(_), Some(segment)) => {
                uri::build_thread_task_uri(project_key, thread, segment)
            }
            (Some(thread), _, _) => uri::build_thread_uri(project_key, thread),
            _ => uri::build_project_uri(project_key),
        }
    }

    /// The address of the job's named artifact, or the job's own address.
    fn artifact_uri(&self, project_key: &str, output_name: Option<&str>) -> String {
        match (self.node_address(), output_name) {
            (Some((number, seq, segment)), Some(name)) => match self.parent_segment.as_deref() {
                Some(parent) => uri::build_task_artifact_uri_named(
                    project_key,
                    number,
                    seq,
                    parent,
                    segment,
                    Some(name),
                ),
                None => uri::build_node_artifact_uri_named(
                    project_key,
                    number,
                    seq,
                    segment,
                    Some(name),
                ),
            },
            _ => self.uri(project_key),
        }
    }

    /// "#5508 builder", "thread design", "review in thread design".
    fn label(&self) -> String {
        let who = self
            .node_name
            .as_deref()
            .or(self.segment.as_deref())
            .unwrap_or("agent");
        match (self.issue_number, &self.thread_name) {
            (Some(number), _) => format!("#{number} {who}"),
            (None, Some(thread)) if self.parent_segment.is_some() => {
                format!("{who} in thread {thread}")
            }
            (None, Some(thread)) => format!("thread {thread}"),
            (None, None) => who.to_string(),
        }
    }
}

fn issue_resolved_at(status: Option<&str>, updated_at: Option<i64>) -> Option<i64> {
    match status {
        Some("merged" | "closed") => updated_at,
        _ => None,
    }
}

fn truncate(text: &str) -> String {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if line.chars().count() <= SUMMARY_CHARS {
        return line;
    }
    let mut cut: String = line.chars().take(SUMMARY_CHARS - 1).collect();
    cut.push('…');
    cut
}

fn entry(
    project: &ProjectRef,
    key: String,
    kind: NotificationKind,
    at: i64,
    summary: String,
    source_uri: String,
    resolved_at: Option<i64>,
    action: Option<NotificationAction>,
) -> OperatorNotification {
    let open = resolved_at.is_none();
    OperatorNotification {
        key,
        project_id: project.id.clone(),
        project_key: project.key.clone(),
        kind,
        at,
        summary: truncate(&summary),
        source_uri,
        state: if open {
            NotificationState::Open
        } else {
            NotificationState::Resolved
        },
        state_at: resolved_at,
        // A resolved entry has nothing left to act on.
        action: if open { action } else { None },
    }
}

fn db_error(error: DbError) -> String {
    error.to_string()
}

/// Rows a kind reads: those open now, and those resolved inside the window.
///
/// Two statements rather than `open OR resolved_at >= ?2`: the halves have
/// different access paths, and Turso plans an `OR` across them as one scan
/// (see the database-migrations skill). `select` ends in a `WHERE` clause
/// binding the project as `?1`; `recent` binds the window's lower bound as `?2`.
async fn open_and_recent<T: Send + 'static>(
    db: &LocalDb,
    select: &str,
    open: &str,
    recent: &str,
    project_id: &str,
    since: i64,
    map: fn(&Row) -> Result<T, DbError>,
) -> Result<Vec<T>, String> {
    let mut rows = db
        .query_all(
            format!("{select} AND {open}"),
            params![project_id.to_string()],
            map,
        )
        .await
        .map_err(db_error)?;
    rows.extend(
        db.query_all(
            format!("{select} AND {recent}"),
            params![project_id.to_string(), since],
            map,
        )
        .await
        .map_err(db_error)?,
    );
    Ok(rows)
}

/// Every database-backed kind, for one project, and the earliest moment the
/// answer changes with no write (see [`agent_failures`]). `since` bounds how
/// far back resolved entries are read.
pub(super) async fn from_sources(
    db: &LocalDb,
    project: &ProjectRef,
    since: i64,
    now: i64,
) -> Result<(Vec<OperatorNotification>, Option<i64>), String> {
    let mut entries = Vec::new();
    entries.extend(questions(db, project, since).await?);
    entries.extend(permissions(db, project, since).await?);
    entries.extend(tenets(db, project, since).await?);
    entries.extend(pull_requests(db, project, since).await?);
    let (failures, next_change_at) = agent_failures(db, project, since, now).await?;
    entries.extend(failures);
    entries.extend(awaiting_review(db, project).await?);
    Ok((entries, next_change_at))
}

/// The first question's text from a stored `questions` payload.
fn first_question(questions: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(questions).ok()?;
    let list = value
        .as_array()
        .or_else(|| value.get("questions").and_then(serde_json::Value::as_array))?;
    let first = list.first()?;
    first
        .get("question")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Open while unanswered and unwithdrawn on its job's current turn, on a job
/// that is still live and a turn that was not cancelled; resolved when
/// answered or withdrawn. A crashed run alone does not close a question: its
/// yielded turn can still be answered and resumed.
pub(super) async fn questions(
    db: &LocalDb,
    project: &ProjectRef,
    since: i64,
) -> Result<Vec<OperatorNotification>, String> {
    let sql = format!(
        "SELECT p.id, p.run_id, p.questions, p.created_at, p.answered_at, p.withdrawn_at,
                {open_prompt},
                {PLACE_COLUMNS}
           FROM prompts p
           JOIN runs r ON r.id = p.run_id
           JOIN jobs j ON j.id = COALESCE(p.job_id, r.job_id)
           LEFT JOIN turns pt ON pt.id = p.turn_id
           {PLACE_JOINS}
          WHERE j.project_id = ?1",
        // One definition of an open question, shared with the execution
        // queue's fleet cap, which frees exactly these jobs' slots.
        open_prompt = cairn_db::open_prompt_sql!()
    );
    let rows = open_and_recent(
        db,
        &sql,
        "p.response IS NULL AND p.withdrawn_at IS NULL",
        "COALESCE(p.answered_at, p.withdrawn_at) >= ?2",
        &project.id,
        since,
        |row| {
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.text(2)?,
                row.i64(3)?,
                row.opt_i64(4)?.or(row.opt_i64(5)?),
                row.opt_i64(6)?.unwrap_or(0) != 0,
                JobPlace::read(row, 7)?,
            ))
        },
    )
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(id, run_id, questions, created_at, closed_at, current, place)| {
                let resolved_at = match closed_at {
                    Some(at) => Some(at),
                    // Unanswered: open only on the current turn of a live issue.
                    None if current => place.issue_resolved_at(),
                    None => return None,
                };
                let asked = first_question(&questions)
                    .map(|question| format!(": {question}"))
                    .unwrap_or_default();
                Some(entry(
                    project,
                    format!("question:{id}"),
                    NotificationKind::Question,
                    created_at,
                    format!("{} asks{asked}", place.label()),
                    place.uri(&project.key),
                    resolved_at,
                    Some(NotificationAction::Question {
                        prompt_id: id,
                        run_id,
                        questions,
                    }),
                ))
            },
        )
        .collect())
}

/// Open while `pending`; resolved when answered or withdrawn.
pub(super) async fn permissions(
    db: &LocalDb,
    project: &ProjectRef,
    since: i64,
) -> Result<Vec<OperatorNotification>, String> {
    let sql = format!(
        "SELECT pr.id, pr.run_id, pr.tool_use_id, pr.tool_name, pr.tool_input, pr.created_at,
                pr.status, pr.responded_at, {PLACE_COLUMNS}
           FROM permission_requests pr
           JOIN runs r ON r.id = pr.run_id
           JOIN jobs j ON j.id = COALESCE(pr.job_id, r.job_id)
           {PLACE_JOINS}
          WHERE j.project_id = ?1"
    );
    let rows = open_and_recent(
        db,
        &sql,
        cairn_db::pending_permission_sql!(),
        "pr.status <> 'pending' AND pr.responded_at >= ?2",
        &project.id,
        since,
        |row| {
            Ok((
                PermissionActionRequest {
                    id: row.text(0)?,
                    run_id: row.text(1)?,
                    tool_use_id: row.text(2)?,
                    tool_name: row.text(3)?,
                    tool_input: serde_json::from_str(&row.text(4)?)
                        .unwrap_or(serde_json::Value::Null),
                    created_at: row.i64(5)?,
                },
                row.text(6)?,
                row.opt_i64(7)?,
                JobPlace::read(row, 8)?,
            ))
        },
    )
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(request, status, responded_at, place)| {
            let resolved_at = if status == "pending" {
                place.issue_resolved_at()
            } else {
                Some(responded_at?)
            };
            Some(entry(
                project,
                format!("permission:{}", request.id),
                NotificationKind::Permission,
                request.created_at,
                format!(
                    "{} needs permission for {}",
                    place.label(),
                    request.tool_name
                ),
                place.uri(&project.key),
                resolved_at,
                Some(NotificationAction::Permission { request }),
            ))
        })
        .collect())
}

/// Open while `proposed`; resolved at the verdict, or at the withdrawal or
/// supersession (`updated_at`) for a tenet that never got one.
pub(super) async fn tenets(
    db: &LocalDb,
    project: &ProjectRef,
    since: i64,
) -> Result<Vec<OperatorNotification>, String> {
    let rows = open_and_recent(
        db,
        "SELECT number, statement, status, created_at, COALESCE(answered_at, updated_at)
           FROM tenets
          WHERE project_id = ?1",
        "status = 'proposed'",
        "status <> 'proposed' AND COALESCE(answered_at, updated_at) >= ?2",
        &project.id,
        since,
        |row| {
            Ok((
                row.i64(0)?,
                row.text(1)?,
                row.text(2)?,
                row.i64(3)?,
                row.i64(4)?,
            ))
        },
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|(number, statement, status, created_at, closed_at)| {
            let tenet_uri = crate::tenets::tenet_uri(&project.key, number);
            entry(
                project,
                format!("tenet:{}:{number}", project.id),
                NotificationKind::Tenet,
                created_at,
                format!("Tenet {number} awaits your verdict: {statement}"),
                tenet_uri.clone(),
                (status != "proposed").then_some(closed_at),
                Some(NotificationAction::Tenet { number, tenet_uri }),
            )
        })
        .collect())
}

fn pr_condition(checks_status: Option<&str>, mergeable: Option<&str>) -> String {
    let mut parts = Vec::new();
    match checks_status.map(str::to_ascii_lowercase).as_deref() {
        Some("success") => parts.push("checks passing"),
        Some("failure" | "error") => parts.push("checks failing"),
        Some("pending") => parts.push("checks running"),
        _ => {}
    }
    if mergeable.is_some_and(|state| state.eq_ignore_ascii_case("conflicting")) {
        parts.push("conflicts with base");
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(", "))
    }
}

/// A PR row still `open`, and one merged or closed inside the window.
const PR_OPEN: &str = "mr.status = 'open'";
const PR_RECENT: &str = "mr.status <> 'open' AND COALESCE(mr.merged_at, mr.closed_at) >= ?2";

/// Open while `open` on an issue that has not merged or closed; resolved at
/// the PR's merge or close, or at the issue's.
pub(super) async fn pull_requests(
    db: &LocalDb,
    project: &ProjectRef,
    since: i64,
) -> Result<Vec<OperatorNotification>, String> {
    let sql = format!(
        "SELECT mr.id, mr.job_id, mr.title, mr.status, mr.merge_method, mr.opened_at,
                COALESCE(mr.merged_at, mr.closed_at), mr.checks_status, mr.github_mergeable,
                (SELECT a.output_name FROM artifacts a WHERE a.job_id = mr.job_id
                  ORDER BY a.created_at DESC, a.version DESC LIMIT 1),
                ar.uri_segment, ae.seq, pi.number, pi.status, pi.updated_at,
                {PLACE_COLUMNS}
           FROM merge_requests mr
           LEFT JOIN jobs j ON j.id = mr.job_id
           {PLACE_JOINS}
           LEFT JOIN action_runs ar ON ar.id = mr.job_id
           LEFT JOIN executions ae ON ae.id = ar.execution_id
           LEFT JOIN issues pi ON pi.id = mr.issue_id
          WHERE mr.project_id = ?1"
    );
    let rows = open_and_recent(db, &sql, PR_OPEN, PR_RECENT, &project.id, since, |row| {
        let place = match row.opt_text(15)? {
            Some(_) => Some(JobPlace::read(row, 15)?),
            None => None,
        };
        Ok((
            (
                row.text(0)?,
                row.text(1)?,
                row.text(2)?,
                row.text(3)?,
                row.text(4)?,
                row.i64(5)?,
                row.opt_i64(6)?,
            ),
            (row.opt_text(7)?, row.opt_text(8)?, row.opt_text(9)?),
            (
                row.opt_text(10)?,
                row.opt_i64(11)?,
                row.opt_i64(12)?,
                row.opt_text(13)?,
                row.opt_i64(14)?,
            ),
            place,
        ))
    })
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(
                (id, owner, title, status, merge_method, opened_at, closed_at),
                (checks_status, mergeable, output_name),
                (action_segment, action_seq, number, issue_status, issue_updated_at),
                place,
            )| {
                let resolved_at = if status == "open" {
                    issue_resolved_at(issue_status.as_deref(), issue_updated_at)
                } else {
                    Some(closed_at?)
                };
                let source_uri = match (&place, action_segment, action_seq, number) {
                    (Some(place), ..) => place.artifact_uri(&project.key, output_name.as_deref()),
                    (None, Some(segment), Some(seq), Some(number)) => {
                        uri::build_node_uri(&project.key, number as i32, seq as i32, &segment)
                    }
                    (None, _, _, Some(number)) => uri::build_issue_uri(&project.key, number as i32),
                    _ => uri::build_project_uri(&project.key),
                };
                let on = number.map(|n| format!("#{n} ")).unwrap_or_default();
                Some(entry(
                    project,
                    format!("pr:{id}"),
                    NotificationKind::PrReady,
                    opened_at,
                    format!(
                        "{on}PR ready to merge: {title}{}",
                        pr_condition(checks_status.as_deref(), mergeable.as_deref())
                    ),
                    source_uri,
                    resolved_at,
                    Some(NotificationAction::Merge {
                        action_run_id: owner,
                        merge_method,
                    }),
                ))
            },
        )
        .collect())
}

/// A failed turn: its id, run, state, end time, the start of the turn after
/// it (if any), and where its job lives.
type FailedTurn = (String, Option<String>, String, i64, Option<i64>, JobPlace);

fn read_failed_turn(row: &Row) -> Result<FailedTurn, DbError> {
    Ok((
        row.text(0)?,
        row.opt_text(1)?,
        row.text(2)?,
        row.i64(3)?,
        row.opt_i64(4)?,
        JobPlace::read(row, 5)?,
    ))
}

/// A top-level job whose turn failed, or was cut short by a crash nothing
/// recovered from. Open while that turn is still the job's current one on an
/// issue that has not merged or closed; resolved by the next turn on the job,
/// or by the issue's resolution.
///
/// A crash Cairn recovers from on its own (the provider-silence watchdog, the
/// digest reseed) starts a successor turn only after several more writes, any
/// of which can trigger a rebuild in between. So a failed turn becomes an entry
/// only once it has stood for [`super::AGENT_FAILURE_SETTLE_SECS`] with no
/// successor. Returns the earliest moment a held-back turn settles, which the
/// projection schedules its own rebuild for.
pub(super) async fn agent_failures(
    db: &LocalDb,
    project: &ProjectRef,
    since: i64,
    now: i64,
) -> Result<(Vec<OperatorNotification>, Option<i64>), String> {
    const FAILED_TURN: &str =
        "(t.state = 'failed' OR (t.state = 'interrupted' AND t.end_reason = 'crash'))";
    let open_sql = format!(
        "SELECT t.id, t.run_id, t.state, COALESCE(t.ended_at, t.updated_at), NULL,
                {PLACE_COLUMNS}
           FROM jobs j
           JOIN turns t ON t.id = j.current_turn_id
           {PLACE_JOINS}
          WHERE j.project_id = ?1 AND j.parent_job_id IS NULL AND {FAILED_TURN}"
    );
    // Recent failures only, through the turn-time index: older ones have no
    // resolution inside the window, and an open one is found above.
    let recent_sql = format!(
        "SELECT t.id, t.run_id, t.state, COALESCE(t.ended_at, t.updated_at),
                (SELECT MIN(n.created_at) FROM turns n
                  WHERE n.job_id = t.job_id AND n.created_at > t.created_at),
                {PLACE_COLUMNS}
           FROM turns t
           JOIN jobs j ON j.id = t.job_id
           {PLACE_JOINS}
          WHERE t.created_at >= ?2
            AND j.project_id = ?1 AND j.parent_job_id IS NULL AND {FAILED_TURN}"
    );
    let mut rows = db
        .query_all(open_sql, params![project.id.clone()], read_failed_turn)
        .await
        .map_err(db_error)?;
    rows.extend(
        db.query_all(
            recent_sql,
            params![project.id.clone(), since],
            read_failed_turn,
        )
        .await
        .map_err(db_error)?,
    );
    let mut settles_at: Option<i64> = None;
    let entries = rows
        .into_iter()
        .filter_map(|(turn_id, _run_id, state, ended_at, next_turn_at, place)| {
            let resolved_at = next_turn_at.or(place.issue_resolved_at());
            // Too fresh to call: a recovery may be about to start its successor.
            // Report when it will settle, so the projection rebuilds then.
            if resolved_at.is_none() && ended_at > now - super::AGENT_FAILURE_SETTLE_SECS {
                let at = ended_at + super::AGENT_FAILURE_SETTLE_SECS;
                settles_at = Some(settles_at.map_or(at, |earliest| earliest.min(at)));
                return None;
            }
            let what = if state == "failed" {
                "failed"
            } else {
                "crashed"
            };
            // Per turn: a warm run hosts many turns, and a dismissal closes one
            // failure, not every later one in the same run.
            Some(entry(
                project,
                format!("agent_failed:{turn_id}"),
                NotificationKind::AgentFailed,
                ended_at,
                format!("{} {what}", place.label()),
                place.uri(&project.key),
                resolved_at,
                None,
            ))
        })
        .collect();
    Ok((entries, settles_at))
}

/// A node blocked on the operator's review of an artifact that is not a PR
/// (PRs are [`pull_requests`]). Open only: the confirmation that clears it
/// records no time of its own, so once reviewed the entry leaves the list.
pub(super) async fn awaiting_review(
    db: &LocalDb,
    project: &ProjectRef,
) -> Result<Vec<OperatorNotification>, String> {
    let sql = format!(
        "SELECT a.id, a.output_name, a.created_at, {PLACE_COLUMNS}
           FROM jobs j
           {PLACE_JOINS}
           JOIN artifacts a ON a.id = (
                SELECT a2.id FROM artifacts a2 WHERE a2.job_id = j.id
                 ORDER BY a2.created_at DESC, a2.version DESC LIMIT 1)
          WHERE j.project_id = ?1 AND j.status = 'blocked' AND a.confirmed = 0
            AND NOT EXISTS (SELECT 1 FROM merge_requests mr WHERE mr.job_id = j.id)"
    );
    let rows = db
        .query_all(sql, params![project.id.clone()], |row| {
            Ok((
                row.text(0)?,
                row.opt_text(1)?,
                row.i64(2)?,
                JobPlace::read(row, 3)?,
            ))
        })
        .await
        .map_err(db_error)?;
    Ok(rows
        .into_iter()
        .filter(|(.., place)| place.issue_resolved_at().is_none())
        .map(|(artifact_id, output_name, created_at, place)| {
            let what = output_name.as_deref().unwrap_or("output");
            entry(
                project,
                format!("review:{artifact_id}"),
                NotificationKind::AwaitingReview,
                created_at,
                format!("{}: {what} awaits your review", place.label()),
                place.artifact_uri(&project.key, output_name.as_deref()),
                None,
                None,
            )
        })
        .collect())
}

/// What the registered hold sources report for the project.
pub(super) fn holds(registry: &HoldRegistry, project: &ProjectRef) -> Vec<OperatorNotification> {
    registry
        .holds_for(&project.id)
        .into_iter()
        .map(|(source, hold)| {
            entry(
                project,
                format!("hold:{source}:{}", hold.id),
                NotificationKind::Hold,
                hold.opened_at,
                hold.reason.clone(),
                hold.source_uri
                    .clone()
                    .unwrap_or_else(|| uri::build_project_uri(&project.key)),
                hold.released_at,
                Some(NotificationAction::Hold {
                    source: source.to_string(),
                    hold_id: hold.id,
                    reason: hold.reason,
                    expected_release: hold.expected_release,
                    override_label: hold.override_label,
                }),
            )
        })
        .collect()
}
