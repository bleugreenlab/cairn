//! Answering the tool calls an interrupted turn left open.
//!
//! A turn can end while the model still owes itself results: an operator stopped
//! it, or the host died under it. The turn's assistant events are the durable
//! record of every call it made and its `tool_result` events of every answer, so
//! the calls still owed are read from the transcript rather than from a table of
//! in-flight work. Each is answered with a synthetic `tool_result` carrying
//! `{"synthetic":true,"reason":...}` in `raw`.
//!
//! The two causes differ in what they may claim. A stop happens beside a live
//! carrier, so it answers run and write calls and waits, bounded, for a write
//! still applying items. After a restart no carrier exists, so every open call is
//! answered at once, and the answer states what is known about the call's effect
//! in `raw.effect`. A call an active `agent_waits` row speaks for is left to
//! `durable_suspend::reconcile`, which is what keeps each call answered once.

use crate::agent_process::stream::TranscriptEvent;
use crate::mcp::handlers::run::run_batch_lost_to_restart_text;
use crate::mcp::handlers::tool_use_correlation::answered_or_actively_owned_ids;
use crate::mcp::handlers::write::replay;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbResult, LocalDb, RowExt};
use crate::transcripts::stream_store::{self, EventInsert};
use cairn_common::ids;
use cairn_session::agent_process::stream::{ToolUseInfo, TranscriptJson};
use std::collections::HashSet;

/// Why a turn's open calls are being answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InterruptCause {
    /// An operator stopped the turn while its host stayed alive.
    UserStop,
    /// The host serving the turn died; the boot sweep settled it.
    HostRestart,
}

/// What a restart's answer knows about a call's effect, recorded as
/// `raw.effect` so tests and the UI need not read the prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallEffect {
    /// The call changed nothing.
    None,
    /// The call may or may not have taken effect.
    Unknown,
    /// Some of the call took effect, and the answer lists what is known.
    Partial,
    /// The call took effect before its result was lost.
    Landed,
}

impl CallEffect {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Unknown => "unknown",
            Self::Partial => "partial",
            Self::Landed => "landed",
        }
    }
}

#[derive(Debug, Clone)]
struct PendingToolResult {
    tool_use_id: String,
    tool_name: String,
    session_id: Option<String>,
    parent_tool_use_id: Option<String>,
    result: String,
    is_error: bool,
    reason: &'static str,
    effect: Option<CallEffect>,
    /// The report to seal a still-pending ledger row with, so a redelivery of
    /// this call replays the answer instead of stalling on the pending marker.
    seal_report: Option<String>,
}

fn normalized_tool_name(name: &str) -> &str {
    name.rsplit("__").next().unwrap_or(name)
}

fn is_run_tool_call(name: &str, input: &serde_json::Value) -> bool {
    let normalized = normalized_tool_name(name).to_ascii_lowercase();
    normalized == "run"
        || normalized == "bash"
        || name == "Bash"
        || input.get("command").is_some_and(|value| value.is_string())
}

fn is_write_tool_call(name: &str) -> bool {
    normalized_tool_name(name).eq_ignore_ascii_case("write")
}

const POST_EFFECT_STOP_ANNOTATION: &str =
    "The user stopped the turn after this write took effect. The applied report below is authoritative. Do not retry this write.";

const DURING_WRITE_STOP_ANNOTATION: &str =
    "The user stopped the turn during this write. Items under applied took effect; do not resend them. Items marked not applied did not run. Read the target of any item marked not confirmed before retrying.";

/// How long a stop waits for a resource write that is applying items to stop
/// at its next item boundary and seal its report.
const WRITE_STOP_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

pub(crate) const USER_STOP_TOOL_RESULT: &str = "Run interrupted by user stop.";

fn annotate_write_report(report: &str, key: &str, annotation: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(report) else {
        return format!("{annotation}\n{report}");
    };
    let Some(object) = value.as_object_mut() else {
        return format!("{annotation}\n{report}");
    };
    object.insert(
        key.to_string(),
        serde_json::Value::String(annotation.to_string()),
    );
    serde_json::to_string(&value).unwrap_or_else(|_| report.to_string())
}

fn annotate_stopped_write_report(report: &str) -> String {
    annotate_write_report(
        report,
        "interrupted_during_write",
        DURING_WRITE_STOP_ANNOTATION,
    )
}

async fn write_ledger_row(
    db: &LocalDb,
    run_id: &str,
    tool_use_id: &str,
) -> DbResult<Option<LedgerRow>> {
    let run_id = run_id.to_string();
    let tool_use_id = tool_use_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        let tool_use_id = tool_use_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT report, finalized FROM write_replay_ledger
                     WHERE run_id = ?1 AND tool_use_id = ?2",
                    (run_id.as_str(), tool_use_id.as_str()),
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => Some(LedgerRow {
                    report: row.text(0)?,
                    finalized: row.i64(1)? != 0,
                }),
                None => None,
            })
        })
    })
    .await
}

/// The result a stop gives a `write` call that has no result yet, or `None`
/// when the write never claimed its ledger row (nothing took effect).
///
/// A resource write still applying items was asked to stop before its next
/// item; wait, bounded, for it to seal its row. A sealed row is reported as
/// it stands. A row still pending when the wait runs out (or one no carrier
/// is applying, such as a blocking group's checkpoint) is reported from its
/// last checkpoint, with every other item `not confirmed`: the carrier may
/// still be running, so nothing is ever called `not applied` from here.
async fn stopped_write_result(
    orch: &Orchestrator,
    db: &LocalDb,
    run_id: &str,
    tool_use_id: &str,
    input: &serde_json::Value,
) -> DbResult<Option<String>> {
    let deadline = tokio::time::Instant::now() + WRITE_STOP_WAIT;
    loop {
        let Some(LedgerRow { report: row, .. }) = write_ledger_row(db, run_id, tool_use_id).await?
        else {
            return Ok(None);
        };
        if !replay::is_pending(&row) {
            return Ok(Some(if replay::report_was_stopped(&row) {
                annotate_stopped_write_report(&row)
            } else {
                annotate_write_report(
                    &row,
                    "interrupted_after_effect",
                    POST_EFFECT_STOP_ANNOTATION,
                )
            }));
        }
        if !orch.write_stops.is_applying(run_id, tool_use_id)
            || tokio::time::Instant::now() >= deadline
        {
            return Ok(Some(annotate_stopped_write_report(
                &replay::unsettled_write_report(&row, input),
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// A write's row in the replay ledger, as evidence of how far it got.
#[derive(Debug, Clone)]
pub(crate) struct LedgerRow {
    pub(crate) report: String,
    /// False only for a sealed commit whose delivery was interrupted before its
    /// row settled. A pending resource claim reads as finalized here: it is told
    /// apart by [`replay::is_pending`].
    pub(crate) finalized: bool,
}

/// Every clause of a restart answer but a run's follows this one. A run's own
/// wording opens with what action runs classify, so it has one source.
const RESTART_OPENING: &str =
    "Cairn restarted while this call was in flight, so its result was lost.";

const RESTART_READ: &str = "A read changes nothing. Re-run it if you still need the answer.";

const RESTART_WRITE_NEVER_STARTED: &str = "None of its items ran. Re-send it if you still need it.";

const RESTART_WRITE_FILES_UNKNOWN: &str = "Its file edits may or may not have been committed. Look for its commit message in `cairn:@/vcs/log` and read the files before re-sending it.";

const RESTART_WRITE_PARTIAL: &str = "Items under applied took effect, so do not resend them. Items marked not applied did not run. Read the target of every item marked not confirmed before retrying it.";

const RESTART_WRITE_POST_SEAL_UNFINISHED: &str = "Its file edits were committed, but the work that follows the commit, such as publishing the branch or opening its pull request, may not have finished. Do not re-send the file edits. Check the branch and its pull request before retrying the rest.";

const RESTART_WRITE_LANDED: &str = "It took effect before its result reached you. The report below is authoritative. Do not retry this write.";

const RESTART_OUTCOME_UNKNOWN: &str =
    "Its outcome is unknown. Check what it touched before retrying it.";

/// What a restart says about one open call.
#[derive(Debug, Clone)]
pub(crate) struct RestartAnswer {
    pub(crate) text: String,
    pub(crate) effect: CallEffect,
    pub(crate) seal_report: Option<String>,
}

fn restart_answer_saying(detail: &str, effect: CallEffect) -> RestartAnswer {
    RestartAnswer {
        text: format!("{RESTART_OPENING} {detail}"),
        effect,
        seal_report: None,
    }
}

fn restart_answer_with_report(
    detail: &str,
    report: &str,
    effect: CallEffect,
    seal_report: Option<String>,
) -> RestartAnswer {
    RestartAnswer {
        text: format!("{RESTART_OPENING} {detail}\n{report}"),
        effect,
        seal_report,
    }
}

fn write_carries_file_targets(input: &serde_json::Value) -> bool {
    use cairn_common::change_validation::{classify_target, TargetKind};
    input
        .get("changes")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|changes| {
            changes.iter().any(|change| {
                change
                    .get("target")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|target| matches!(classify_target(target), TargetKind::File))
            })
        })
}

/// What a restart can say about a `write`, from its replay ledger row.
///
/// A resource-only batch claims its row before any dispatch, so no row means no
/// item ran. A batch with file targets records its row only after publication,
/// so a commit can land before the row exists and no row proves nothing. The
/// resource claim needs the delivery to have named its own call, which
/// byte-identical open twins cannot do: `has_identical_open_twin` marks that case.
fn restart_write_answer(
    input: &serde_json::Value,
    ledger: Option<&LedgerRow>,
    has_identical_open_twin: bool,
) -> RestartAnswer {
    let Some(row) = ledger else {
        return if has_identical_open_twin {
            restart_answer_saying(RESTART_OUTCOME_UNKNOWN, CallEffect::Unknown)
        } else if write_carries_file_targets(input) {
            restart_answer_saying(RESTART_WRITE_FILES_UNKNOWN, CallEffect::Unknown)
        } else {
            restart_answer_saying(RESTART_WRITE_NEVER_STARTED, CallEffect::None)
        };
    };
    if replay::is_pending(&row.report) {
        let report = replay::restart_write_report(&row.report, input);
        return restart_answer_with_report(
            RESTART_WRITE_PARTIAL,
            &report,
            CallEffect::Partial,
            Some(report.clone()),
        );
    }
    if !row.finalized {
        return restart_answer_with_report(
            RESTART_WRITE_POST_SEAL_UNFINISHED,
            &row.report,
            CallEffect::Partial,
            None,
        );
    }
    if replay::report_was_stopped(&row.report) {
        return restart_answer_with_report(
            RESTART_WRITE_PARTIAL,
            &row.report,
            CallEffect::Partial,
            None,
        );
    }
    restart_answer_with_report(RESTART_WRITE_LANDED, &row.report, CallEffect::Landed, None)
}

/// What a restart says about the open call `name(input)`. `ledger` and
/// `has_identical_open_twin` are consulted only for a `write`.
pub(crate) fn restart_answer(
    name: &str,
    input: &serde_json::Value,
    ledger: Option<&LedgerRow>,
    has_identical_open_twin: bool,
) -> RestartAnswer {
    match normalized_tool_name(name).to_ascii_lowercase().as_str() {
        "read" => restart_answer_saying(RESTART_READ, CallEffect::None),
        "write" => restart_write_answer(input, ledger, has_identical_open_twin),
        "run" => {
            let commits = input
                .get("commit_msg")
                .and_then(serde_json::Value::as_str)
                .is_some();
            RestartAnswer {
                text: run_batch_lost_to_restart_text(commits),
                effect: if commits {
                    CallEffect::Unknown
                } else {
                    CallEffect::None
                },
                seal_report: None,
            }
        }
        _ => restart_answer_saying(RESTART_OUTCOME_UNKNOWN, CallEffect::Unknown),
    }
}

#[derive(Debug, Clone)]
struct RecordedCall {
    tool: ToolUseInfo,
    session_id: Option<String>,
    parent_tool_use_id: Option<String>,
}

/// A turn's calls in transcript order, and the ids its own events answered.
async fn recorded_calls(
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
) -> DbResult<(Vec<RecordedCall>, HashSet<String>)> {
    let run_id = run_id.to_string();
    let turn_id = turn_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        let turn_id = turn_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data
                     FROM events
                     WHERE run_id = ?1
                       AND turn_id = ?2
                       AND event_type IN ('assistant', 'tool_result')
                     ORDER BY sequence ASC, rowid ASC",
                    (run_id.as_str(), turn_id.as_str()),
                )
                .await?;
            let mut calls = Vec::new();
            let mut completed = HashSet::new();
            while let Some(row) = rows.next().await? {
                let data = row.text(0)?;
                let Ok(event) = serde_json::from_str::<TranscriptEvent>(&data) else {
                    continue;
                };
                match event.event_type.as_str() {
                    "assistant" => {
                        for tool in event.tool_uses.unwrap_or_default() {
                            calls.push(RecordedCall {
                                tool,
                                session_id: event.session_id.clone(),
                                parent_tool_use_id: event.parent_tool_use_id.clone(),
                            });
                        }
                    }
                    "tool_result" => {
                        if let Some(tool_use_id) = event.tool_use_id {
                            completed.insert(tool_use_id);
                        }
                    }
                    _ => {}
                }
            }
            Ok((calls, completed))
        })
    })
    .await
}

async fn user_stop_results(
    orch: &Orchestrator,
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
) -> Result<Vec<PendingToolResult>, String> {
    let (calls, completed) = recorded_calls(db, run_id, turn_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut candidates = Vec::new();
    let mut writes = Vec::new();
    for call in calls {
        if is_run_tool_call(&call.tool.name, &call.tool.input) {
            candidates.push(PendingToolResult {
                tool_use_id: call.tool.id,
                tool_name: call.tool.name,
                session_id: call.session_id,
                parent_tool_use_id: call.parent_tool_use_id,
                result: USER_STOP_TOOL_RESULT.to_string(),
                is_error: true,
                reason: "user_stop",
                effect: None,
                seal_report: None,
            });
        } else if is_write_tool_call(&call.tool.name) {
            writes.push(call);
        }
    }
    // Outside the transcript read: a write still applying items is waited for.
    for call in writes {
        if completed.contains(&call.tool.id) {
            continue;
        }
        let result = stopped_write_result(
            orch,
            &orch.db.local,
            run_id,
            &call.tool.id,
            &call.tool.input,
        )
        .await
        .map_err(|e| e.to_string())?;
        if let Some(result) = result {
            candidates.push(PendingToolResult {
                tool_use_id: call.tool.id,
                tool_name: call.tool.name,
                session_id: call.session_id,
                parent_tool_use_id: call.parent_tool_use_id,
                result,
                is_error: false,
                reason: "user_stop_after_write_effect",
                effect: None,
                seal_report: None,
            });
        }
    }
    Ok(candidates
        .into_iter()
        .filter(|candidate| !completed.contains(&candidate.tool_use_id))
        .collect())
}

/// Every call of the turn nothing has answered and no active wait speaks for.
///
/// Answers are gathered per run, not per turn: a recorded tool result does not
/// always carry its turn, and a false restart answer laid over a real result
/// would be worse than leaving the call open. A provider can repeat a tool use
/// across assistant events, so ids are also deduplicated here.
async fn host_restart_results(
    orch: &Orchestrator,
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
) -> Result<Vec<PendingToolResult>, String> {
    let (calls, _) = recorded_calls(db, run_id, turn_id)
        .await
        .map_err(|e| e.to_string())?;
    let closed = answered_or_actively_owned_ids(db, run_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut seen = HashSet::new();
    let open: Vec<RecordedCall> = calls
        .into_iter()
        .filter(|call| !closed.contains(&call.tool.id) && seen.insert(call.tool.id.clone()))
        .collect();
    let mut results = Vec::with_capacity(open.len());
    for call in &open {
        let tool = &call.tool;
        let is_write = is_write_tool_call(&tool.name);
        let ledger = if is_write {
            write_ledger_row(&orch.db.local, run_id, &tool.id)
                .await
                .map_err(|e| e.to_string())?
        } else {
            None
        };
        let has_identical_open_twin = is_write
            && open
                .iter()
                .filter(|other| {
                    is_write_tool_call(&other.tool.name) && other.tool.input == tool.input
                })
                .count()
                > 1;
        let answer = restart_answer(
            &tool.name,
            &tool.input,
            ledger.as_ref(),
            has_identical_open_twin,
        );
        results.push(PendingToolResult {
            tool_use_id: tool.id.clone(),
            tool_name: tool.name.clone(),
            session_id: call.session_id.clone(),
            parent_tool_use_id: call.parent_tool_use_id.clone(),
            result: answer.text,
            // A write that landed succeeded; only the delivery of its result failed.
            is_error: answer.effect != CallEffect::Landed,
            reason: HOST_RESTART_REASON,
            effect: Some(answer.effect),
            seal_report: answer.seal_report,
        });
    }
    Ok(results)
}

/// `raw.reason` of a result the restart settlement wrote.
const HOST_RESTART_REASON: &str = "host_restart";

fn pending_tool_results(
    orch: &Orchestrator,
    run_id: &str,
    turn_id: &str,
    cause: InterruptCause,
) -> Result<Vec<PendingToolResult>, String> {
    let run_id = run_id.to_string();
    let turn_id = turn_id.to_string();
    run_db_blocking({
        let orch = orch.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&orch.db, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            match cause {
                InterruptCause::UserStop => user_stop_results(&orch, &db, &run_id, &turn_id).await,
                InterruptCause::HostRestart => {
                    host_restart_results(&orch, &db, &run_id, &turn_id).await
                }
            }
        }
    })
}

/// The ids of the tool calls a turn still owes results for, as a user stop
/// reads them.
#[cfg(test)]
pub(crate) fn pending_tool_result_ids(
    orch: &Orchestrator,
    run_id: &str,
    turn_id: &str,
) -> Result<Vec<String>, String> {
    pending_tool_results(orch, run_id, turn_id, InterruptCause::UserStop)
        .map(|pending| pending.into_iter().map(|call| call.tool_use_id).collect())
}

/// Answer every open call of `turn_id` as `cause` dictates, returning how many
/// results were written.
///
/// A restart answer needs no idempotency key. The boot sweep reports a turn only
/// for the transition it wrote itself, so a later boot never settles it again, no
/// process remains that could answer the call, and a call answered by any means
/// is already excluded when the candidates are read.
pub(crate) fn settle_unanswered_calls(
    orch: &Orchestrator,
    run_id: &str,
    turn_id: &str,
    cause: InterruptCause,
) -> Result<usize, String> {
    let pending = pending_tool_results(orch, run_id, turn_id, cause)?;
    if pending.is_empty() {
        return Ok(0);
    }

    let owning = run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let now = chrono::Utc::now().timestamp() as i32;
    let mut inserted = 0;
    for pending_result in pending {
        let event_id = ids::mint_child(run_id);
        let mut raw = serde_json::json!({ "synthetic": true, "reason": pending_result.reason });
        if let Some(effect) = pending_result.effect {
            raw["effect"] = serde_json::Value::String(effect.as_str().to_string());
        }
        let event = TranscriptEvent {
            event_type: "tool_result".to_string(),
            session_id: pending_result.session_id.clone(),
            parent_tool_use_id: pending_result.parent_tool_use_id.clone(),
            content: None,
            thinking: None,
            tool_name: Some(pending_result.tool_name.clone()),
            tool_input: None,
            tool_uses: None,
            tool_use_id: Some(pending_result.tool_use_id.clone()),
            tool_result: Some(pending_result.result.clone()),
            is_error: pending_result.is_error,
            thinking_ms: None,
            queued_message_id: None,
            raw: Some(raw),
        };
        let data = event.observed().to_event_json();
        let event_insert = EventInsert {
            id: event_id.clone(),
            run_id: run_id.to_string(),
            session_id: pending_result.session_id.clone(),
            timestamp: now,
            event_type: "tool_result".to_string(),
            data: data.clone(),
            parent_tool_use_id: pending_result.parent_tool_use_id.clone(),
            created_at: now,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            thinking_tokens: None,
            turn_id: Some(turn_id.to_string()),
            cost_usd: None,
        };
        if stream_store::insert_event(owning.clone(), event_insert)? {
            inserted += 1;
            let scope = crate::notify::event_run_scope(orch.db.local.clone(), run_id);
            let _ = orch.services.emitter.emit(
                "db-change",
                crate::notify::event_db_change_scoped(
                    run_id,
                    pending_result.session_id.as_deref(),
                    &scope,
                    "insert",
                ),
            );
        }
        if let Some(report) = pending_result.seal_report {
            let local = orch.db.local.clone();
            let (run_id, tool_use_id) = (run_id.to_string(), pending_result.tool_use_id);
            run_db_blocking(move || async move {
                replay::seal_if_pending(&local, &run_id, &tool_use_id, &report).await;
                Ok::<_, String>(())
            })?;
        }
    }
    Ok(inserted)
}

/// A quoted answer past this many characters is cut, so one write with a huge
/// report cannot swamp the prompt that resumes its turn.
const RESTART_NOTE_QUOTE_LIMIT: usize = 4_000;
const RESTART_NOTE_SUBJECT_LIMIT: usize = 120;

fn truncated_chars(text: &str, limit: usize) -> Option<String> {
    text.char_indices()
        .nth(limit)
        .map(|(end, _)| text[..end].to_string())
}

/// What a call acted on, for naming it in a sentence: the first target, path or
/// command in its input.
fn call_subject(input: &serde_json::Value) -> Option<String> {
    use serde_json::Value;
    let first_of = |key: &str, fields: &[&str]| -> Option<(String, usize)> {
        let items = input.get(key)?.as_array()?;
        let first = items.first()?;
        let named = first.as_str().or_else(|| {
            fields
                .iter()
                .find_map(|field| first.get(*field).and_then(Value::as_str))
        })?;
        Some((named.to_string(), items.len()))
    };
    let (subject, count) = first_of("changes", &["target"])
        .or_else(|| first_of("paths", &[]))
        .or_else(|| first_of("commands", &["command", "target", "description"]))
        .or_else(|| {
            ["command", "path", "file_path", "target", "url"]
                .iter()
                .find_map(|key| input.get(*key).and_then(Value::as_str))
                .map(|named| (named.to_string(), 1))
        })?;
    let subject = match truncated_chars(&subject, RESTART_NOTE_SUBJECT_LIMIT) {
        Some(cut) => format!("{cut}…"),
        None => subject,
    };
    Some(if count > 1 {
        format!("{subject} and {} more", count - 1)
    } else {
        subject
    })
}

/// The list a resume prompt carries: each call the restart settlement answered
/// on this turn, named, with the settlement's own text. `None` when it answered
/// none.
///
/// This prompt is what reaches the model: a native resume reads the provider's
/// own session file, not the transcript event the settlement wrote.
pub(crate) async fn restart_answered_calls_note(
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
) -> DbResult<Option<String>> {
    let run_id = run_id.to_string();
    let turn_id = turn_id.to_string();
    let events: Vec<TranscriptEvent> = db
        .read(|conn| {
            let run_id = run_id.clone();
            let turn_id = turn_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data
                         FROM events
                         WHERE run_id = ?1
                           AND turn_id = ?2
                           AND event_type IN ('assistant', 'tool_result')
                         ORDER BY sequence ASC, rowid ASC",
                        (run_id.as_str(), turn_id.as_str()),
                    )
                    .await?;
                let mut events = Vec::new();
                while let Some(row) = rows.next().await? {
                    if let Ok(event) = serde_json::from_str::<TranscriptEvent>(&row.text(0)?) {
                        events.push(event);
                    }
                }
                Ok(events)
            })
        })
        .await?;
    let input_of = |id: &str| {
        events
            .iter()
            .flat_map(|event| event.tool_uses.iter().flatten())
            .find(|tool| tool.id == id)
            .map(|tool| &tool.input)
    };
    let lines: Vec<String> = events
        .iter()
        .filter(|event| {
            event
                .raw
                .as_ref()
                .and_then(|raw| raw.get("reason"))
                .and_then(serde_json::Value::as_str)
                == Some(HOST_RESTART_REASON)
        })
        .filter_map(|event| {
            let id = event.tool_use_id.as_deref()?;
            let text = event.tool_result.as_deref()?;
            let name = normalized_tool_name(event.tool_name.as_deref().unwrap_or("tool"));
            let named = match input_of(id).and_then(call_subject) {
                Some(subject) => format!("`{name}` on `{subject}`"),
                None => format!("`{name}`"),
            };
            let quoted = match truncated_chars(text, RESTART_NOTE_QUOTE_LIMIT) {
                Some(cut) => format!("{cut}… (cut short here)"),
                None => text.to_string(),
            };
            Some(format!("- {named}: {quoted}"))
        })
        .collect();
    Ok((!lines.is_empty()).then(|| {
        format!(
            "The calls that were in flight, and what is known about each:\n{}",
            lines.join("\n")
        )
    }))
}
