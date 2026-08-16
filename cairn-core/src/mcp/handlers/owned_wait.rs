//! The `waitFor` condition vocabulary: duration, terminal exit, terminal output
//! phrase, node check lanes settling. Suspension itself -- the `agent_waits` row, the park, the single
//! synthetic result and single continuation -- belongs to
//! [`super::durable_suspend`], which this shares with long-running `run` batches.

use super::durable_suspend::{self, Condition, Record};
use super::run::{ChecksWaitEvent, TerminalWaitEvent, WaitDuration, WaitFor};
use super::tool_use_correlation::{claim_tool_use_id, Claim};
use crate::execution::checks_settlement::{
    node_checks_settlement, resolve_checks_target, ChecksSnapshot, Settlement, VERDICTLESS_DWELL_MS,
};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::{sync::Arc, time::Duration};

const INLINE_BUDGET: Duration = Duration::from_secs(45);
const MAX_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// What the agent reads for a `waitFor` that outlived the inline budget.
///
/// This is the whole agent-visible surface of a durable wait, so it says what
/// actually happened -- the call is continuing -- and never implies anyone
/// declined it. `crate::mcp::handlers::suspension_markers` pins that property.
pub(crate) const WAIT_SUSPENDED_MARKER: &str =
    "Wait suspended; the same run call will resume when its condition fires.";

/// What a wait is told when nothing in its turn's transcript is the call it came
/// from. Correlation is the only identity an MCP-hosted agent has, so this is a
/// dead end for the suspension rather than a degraded mode.
const WAIT_CALL_UNRESOLVED: &str = "Could not correlate waitFor with its originating run tool call, so it could not be suspended and nothing is waiting on it now. Reissue it from the active assistant turn if the work still matters.";

/// What a wait is told when several open calls of its turn are indistinguishable
/// from one another. Answering one of them would risk answering another, and a
/// clean refusal is strictly better than a wrong answer.
const WAIT_CALL_AMBIGUOUS: &str = "Could not correlate waitFor with its originating run tool call: this turn has several identical open waits, and nothing distinguishes them, so answering one would risk answering another. This wait could not be suspended; reissue it as the only wait of its turn if the work still matters.";

/// Claim the `run` tool call this wait came from, by finding it in the current
/// turn's transcript.
///
/// This is the identity, not a fallback. The MCP `tools/call` transport carries
/// no provider tool-use id, so `cairn-cmd` sends `tool_use_id: None` for every
/// tool; only the Cairn-native tool loop (`backends::http_loop`), which dispatches
/// tools itself and therefore knows the id, populates it. For every MCP-hosted
/// agent this lookup is the sole bridge from a callback back to the provider tool
/// call whose result the resolved wait must complete. Blocking task/question
/// appends correlate the same way.
///
/// The match is semantic: the recorded item's `waitFor` is parsed back into a
/// `WaitFor` and compared as a value. Comparing raw JSON against a re-serialized
/// expectation made identity depend on incidental encoding — an omitted optional,
/// key order, any field added later — under which no terminal exit wait could
/// ever correlate (CAIRN-3115).
///
/// It CLAIMS rather than resolving by recency, because a resolved wait writes a
/// synthetic tool result to whatever id it is handed. Two identical waits in one
/// assistant event both match on contents, and taking the newest would answer one
/// of them with the other's result; the exclusive claim narrows to unanswered,
/// unclaimed invocations and refuses a tie instead (CAIRN-3232).
async fn claim_wait_tool_use_id(
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
    wait: &WaitFor,
) -> Claim {
    claim_tool_use_id(db, run_id, turn_id, |name, input| {
        if name != "run" && !name.ends_with("__run") {
            return false;
        }
        let Some(commands) = input.get("commands").and_then(|value| value.as_array()) else {
            return false;
        };
        // A wait item is always the sole item in its batch, but it may carry
        // keys of its own (`description`), so identity rests on the parsed wait
        // rather than on the item's key count.
        let [item] = commands.as_slice() else {
            return false;
        };
        item.get("waitFor")
            .and_then(|value| serde_json::from_value::<WaitFor>(value.clone()).ok())
            .is_some_and(|recorded| &recorded == wait)
    })
    .await
}

pub(crate) async fn handle_owned_wait(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    wait: &WaitFor,
) -> String {
    let (ctx, db) = match super::run_context::lookup_run_routed(&orch.db, request).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let turn_id = match orch.process_state.get_current_turn_id(&ctx.run_id) {
        Some(value) => value,
        None => return "waitFor requires an active turn".into(),
    };
    // The transport's own id when there is one. Otherwise the record stays
    // unbound until the durable path below actually needs a call to answer —
    // see [`bind_call`].
    let tool_use_id = request.tool_use_id.clone().unwrap_or_default();
    let created = chrono::Utc::now().timestamp_millis();
    let (condition, deadline) = match normalize(orch, request, &ctx.job_id, wait, created).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let session_id = match durable_suspend::run_session(&db, &ctx.run_id).await {
        Ok(Some(value)) => value,
        Ok(None) => return "waitFor requires an active session".into(),
        Err(error) => return error,
    };
    let record = Record {
        id: cairn_common::ids::mint_child(&ctx.run_id),
        job_id: ctx.job_id,
        run_id: ctx.run_id,
        session_id,
        turn_id,
        tool_use_id,
        condition,
        deadline,
        created,
    };

    // The fast path remains wholly inside the live predecessor turn. Only a
    // budget expiry establishes durable suspension; the slow trigger reuses the
    // absolute deadline or level-triggered terminal state.
    match tokio::time::timeout(
        INLINE_BUDGET,
        trigger(orch.clone(), db.clone(), record.clone()),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => error,
        Err(_) => {
            // Durable path. Suspension is established BEFORE the trigger is
            // armed; see `durable_suspend` for why that order is load-bearing.
            // A refusal is already a complete, agent-facing sentence: it says
            // what happened to this wait and what to do next, so it is returned
            // as the call's own result rather than wrapped in another prefix.
            let record = match bind_call(&db, record, wait).await {
                Ok(record) => record,
                Err(refusal) => return refusal,
            };
            let handoff = match durable_suspend::suspend(orch, &db, &record).await {
                Ok(handoff) => handoff,
                Err(error) => return error,
            };
            let (owned_orch, owned_db, owned_record) = (orch.clone(), db.clone(), record.clone());
            tokio::spawn(async move {
                if !handoff.parked().await {
                    log::warn!(
                        "owned wait {} was never parked; leaving it for startup reconciliation",
                        owned_record.id
                    );
                    return;
                }
                let result =
                    match trigger(owned_orch.clone(), owned_db.clone(), owned_record.clone()).await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            serde_json::json!({"outcome":"error","error":error}).to_string()
                        }
                    };
                if let Err(error) =
                    durable_suspend::resolve(&owned_orch, &owned_db, &owned_record, &result, false)
                        .await
                {
                    log::warn!("owned wait resolution failed: {error}");
                }
            });
            WAIT_SUSPENDED_MARKER.into()
        }
    }
}

/// Bind a wait to the provider call it must answer, on the way to suspending it.
///
/// Correlation happens HERE and not at handler entry, because binding a call is
/// only meaningful for a suspension: a wait that finishes inside its inline
/// budget returns its result as the tool call's own return value and never
/// writes a synthetic one. Claiming eagerly would refuse a pair of identical
/// short waits that would both have finished inline perfectly well, and would
/// make every wait pay for a transcript lookup it usually does not need.
async fn bind_call(db: &LocalDb, record: Record, wait: &WaitFor) -> Result<Record, String> {
    if !record.tool_use_id.is_empty() {
        return Ok(record);
    }
    match claim_wait_tool_use_id(db, &record.run_id, &record.turn_id, wait).await {
        Claim::One(tool_use_id) => Ok(Record {
            tool_use_id,
            ..record
        }),
        Claim::None => {
            log::warn!(
                "wait for run {} found no unanswered tool call of its own to suspend on",
                record.run_id
            );
            Err(WAIT_CALL_UNRESOLVED.to_string())
        }
        Claim::Ambiguous(count) => {
            log::warn!(
                "wait for run {} matches {count} indistinguishable open tool calls, so it cannot claim one without risking another call's answer",
                record.run_id
            );
            Err(WAIT_CALL_AMBIGUOUS.to_string())
        }
    }
}

async fn normalize(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    caller_job_id: &str,
    wait: &WaitFor,
    now: i64,
) -> Result<(Condition, Option<i64>), String> {
    match wait {
        WaitFor::Duration { duration } => {
            let ms = parse_duration(duration)?;
            Ok((Condition::Duration, Some(now + ms as i64)))
        }
        WaitFor::Terminal {
            reference,
            on,
            phrase,
            ..
        } => {
            let uri = if reference.starts_with("cairn:~/") {
                let home = super::run_context::lookup_home_uri_routed(&orch.db, request).await?;
                format!("{}{}", home.trim_end_matches('/'), &reference[7..])
            } else {
                reference.clone()
            };
            let slug = uri
                .rsplit_once("/terminal/")
                .map(|v| v.1)
                .filter(|v| !v.is_empty() && !v.contains('/'))
                .ok_or("terminal ref must be a canonical terminal URI")?
                .to_string();
            match on {
                TerminalWaitEvent::Exit if phrase.is_some() => {
                    return Err("terminal exit wait does not accept phrase".into())
                }
                TerminalWaitEvent::Output
                    if phrase.as_deref().map(str::trim).unwrap_or("").is_empty() =>
                {
                    return Err("terminal output wait requires phrase".into())
                }
                _ => {}
            }
            Ok((
                Condition::Terminal {
                    uri,
                    slug,
                    on: on.clone(),
                    phrase: phrase.clone(),
                },
                None,
            ))
        }
        WaitFor::Checks {
            reference,
            on,
            suite,
            ..
        } => {
            let suite = checks_suite(on, suite.as_deref())?;
            let home = super::run_context::lookup_home_uri_routed(&orch.db, request)
                .await
                .ok();
            let target = resolve_checks_target(orch, reference, home.as_deref()).await?;
            // Your own lanes are the one thing this cannot answer. Turn-end
            // checks are armed by the END of a turn, so a node waiting on its
            // own would be waiting for a wave its own live turn is what
            // prevents — a guaranteed hang, and the wake surface is the shape
            // that actually serves the intent.
            if target.job_id == caller_job_id {
                return Err(format!(
                    "a node cannot wait on its own check lanes: the turn-end wave is armed by this turn ENDING, so the wait would never fire. \
                     End your turn with a wake instead: write({{changes:[{{target:\"cairn:~/wakes\",mode:\"append\",payload:{{subscribe:{{kind:\"checks\",ref:\"{}\"}}}}}}]}})",
                    target.uri
                ));
            }
            // Arm-time validation, so a target that can never settle is refused
            // now rather than parking a turn on it. Past this point the poller
            // treats the same errors as transients, because a momentarily
            // unreadable repository says nothing about the lanes.
            node_checks_settlement(orch, &target.job_id, suite.as_deref()).await?;
            Ok((
                Condition::Checks {
                    uri: target.uri,
                    job_id: target.job_id,
                    suite,
                },
                None,
            ))
        }
    }
}
/// The lane set a checks wait watches, from the `on`/`suite` pair.
///
/// The two keys are one decision, so a mismatched pair is refused rather than
/// silently resolved: watching the whole node when a suite was named, or one
/// suite when none was, is a different wait than the one that was asked for.
fn checks_suite(on: &ChecksWaitEvent, suite: Option<&str>) -> Result<Option<String>, String> {
    match (on, suite.map(str::trim).filter(|s| !s.is_empty())) {
        (ChecksWaitEvent::Settled, Some(_)) => Err(
            "checks settled wait watches the whole node and does not accept suite; \
             use on:\"verdict\" to watch one suite"
                .into(),
        ),
        (ChecksWaitEvent::Verdict, None) => Err("checks verdict wait requires suite, e.g. \
             {kind:\"checks\",ref:\"…/checks\",on:\"verdict\",suite:\"rust-tests\"}"
            .into()),
        (_, suite) => Ok(suite.map(ToString::to_string)),
    }
}

fn parse_duration(v: &WaitDuration) -> Result<u64, String> {
    let ms = match v {
        WaitDuration::Milliseconds(v) => *v,
        WaitDuration::Human(v) => {
            let p = v
                .find(|c: char| !c.is_ascii_digit())
                .ok_or("duration needs ms, s, m, h, or d")?;
            let n: u64 = v[..p].parse().map_err(|_| "invalid duration")?;
            let f = match &v[p..] {
                "ms" => 1,
                "s" => 1000,
                "m" => 60000,
                "h" => 3600000,
                "d" => 86400000,
                _ => return Err("duration needs ms, s, m, h, or d".into()),
            };
            n.checked_mul(f).ok_or("duration too large")?
        }
    };
    if ms == 0 || ms > MAX_MS {
        return Err("duration must be between 1ms and 7d".into());
    }
    Ok(ms)
}

/// Await one waitFor condition. Level-triggered by design, so a host restart can
/// simply re-arm it -- which is exactly what startup reconciliation does.
pub(super) async fn trigger(
    orch: Orchestrator,
    db: Arc<LocalDb>,
    r: Record,
) -> Result<String, String> {
    match &r.condition {
        Condition::Duration => {
            let deadline = r.deadline.unwrap();
            let left = deadline - chrono::Utc::now().timestamp_millis();
            if left > 0 {
                tokio::time::sleep(Duration::from_millis(left as u64)).await
            }
            Ok(serde_json::json!({"outcome":"elapsed","elapsedMs":chrono::Utc::now().timestamp_millis()-r.created,"deadlineMs":deadline}).to_string())
        }
        Condition::Terminal {
            uri,
            slug,
            on,
            phrase,
        } => loop {
            if let Some(v) =
                terminal(&orch, &db, &r.job_id, uri, slug, on, phrase.as_deref()).await?
            {
                return Ok(v);
            }
            tokio::time::sleep(Duration::from_millis(100)).await
        },
        Condition::Checks { uri, job_id, suite } => {
            checks(&orch, uri, job_id, suite.as_deref(), r.created).await
        }
        // A run batch has no pollable condition: its trigger is the awaited
        // executor result, which only the suspending host holds.
        Condition::RunBatch { .. } | Condition::McpContinuation { .. } => {
            Err("this durable suspension has no waitFor condition to await".into())
        }
    }
}

/// Poll a node's check lanes until they stop moving.
///
/// Two poll rates rather than one, because the snapshot's cost is not uniform. A
/// wave in flight publishes an immutable sealed-tree snapshot that the status
/// read reuses; with no wave, the same read re-resolves tree hashes and the
/// cumulative diff through jj, which under repository load is orders of
/// magnitude more expensive. A node still working can be waited on for a long
/// time, and hammering its repository to learn that it is still working is the
/// wrong trade.
async fn checks(
    orch: &Orchestrator,
    uri: &str,
    job_id: &str,
    suite: Option<&str>,
    created: i64,
) -> Result<String, String> {
    const IN_FLIGHT_POLL: Duration = Duration::from_secs(2);
    const QUIET_POLL: Duration = Duration::from_secs(10);
    /// How long the target may stay unreadable before the wait gives up on it.
    ///
    /// A momentarily unreadable repository says nothing about the lanes, so a
    /// snapshot error is a transient. A node that has been ARCHIVED or deleted
    /// out from under an armed wait produces the same error forever, though, and
    /// tolerating it forever is the one thing this condition must not do.
    const UNAVAILABLE_GRACE_MS: i64 = 10 * 60 * 1000;

    // When the verdictless reading first appeared. `None` whenever the node is
    // moving or settled on real verdicts, so the dwell restarts from scratch
    // every time the reading is contradicted.
    let mut verdictless_since: Option<i64> = None;
    let mut unavailable_since: Option<i64> = None;
    loop {
        let snapshot = match node_checks_settlement(orch, job_id, suite).await {
            Ok(snapshot) => {
                unavailable_since = None;
                snapshot
            }
            // Armed against a target that resolved, so a read that fails now is
            // a transient (a busy repository, a replica reopening) -- until it
            // has been failing long enough to be the target's permanent state.
            Err(error) => {
                let now = chrono::Utc::now().timestamp_millis();
                let since = *unavailable_since.get_or_insert(now);
                if now - since >= UNAVAILABLE_GRACE_MS {
                    return Err(format!(
                        "{uri} has been unreadable for {} minutes, so its lanes cannot be observed \
                         and this wait would never fire: {error}",
                        UNAVAILABLE_GRACE_MS / 60_000
                    ));
                }
                log::debug!("checks wait on {uri}: snapshot unavailable ({error}); retrying");
                tokio::time::sleep(QUIET_POLL).await;
                continue;
            }
        };
        let now = chrono::Utc::now().timestamp_millis();
        match &snapshot.settlement {
            Settlement::Settled { verdictless } if verdictless.is_empty() => {
                return Ok(settled_result(uri, suite, &snapshot, now - created))
            }
            Settlement::Settled { .. } => {
                let since = *verdictless_since.get_or_insert(now);
                if now - since >= VERDICTLESS_DWELL_MS {
                    return Ok(settled_result(uri, suite, &snapshot, now - created));
                }
            }
            Settlement::Moving(_) => verdictless_since = None,
        }
        let in_flight =
            orch.turn_end_checks_in_flight(job_id) || orch.write_checks_in_flight(job_id);
        // A dwell in progress polls at the fast rate regardless: it is a claim
        // about a specific instant, and confirming it slowly would stretch the
        // window it exists to close.
        let interval = if in_flight || verdictless_since.is_some() {
            IN_FLIGHT_POLL
        } else {
            QUIET_POLL
        };
        tokio::time::sleep(interval).await;
    }
}

/// The settled answer, shaped so an orchestrator can branch on one field and
/// still read what happened without a follow-up call.
fn settled_result(
    uri: &str,
    suite: Option<&str>,
    snapshot: &ChecksSnapshot,
    elapsed_ms: i64,
) -> String {
    let verdictless = match &snapshot.settlement {
        Settlement::Settled { verdictless } => verdictless.clone(),
        Settlement::Moving(_) => Vec::new(),
    };
    let mut value = serde_json::json!({
        "outcome": "settled",
        "checks": uri,
        "verdict": snapshot.verdict(),
        "summary": snapshot.tally(),
        "lanes": snapshot.lane_lines(),
        "elapsedMs": elapsed_ms,
    });
    if let Some(suite) = suite {
        value["suite"] = serde_json::json!(suite);
    }
    if !verdictless.is_empty() {
        value["verdictless"] = serde_json::json!(verdictless);
        let reason = snapshot.terminal_reason.as_deref().unwrap_or(
            "legacy check state has no durable attempt reason; the restart-protection dwell elapsed",
        );
        value["terminalReason"] = serde_json::json!(reason);
        value["note"] = serde_json::json!(format!(
            "These lanes stopped without producing a verdict for this tree. \
             Terminal reason: {reason}. Nothing is going to run them; read {uri} \
             for the node's own account."
        ));
    }
    value.to_string()
}
async fn terminal(
    orch: &Orchestrator,
    db: &LocalDb,
    job: &str,
    uri: &str,
    slug: &str,
    on: &TerminalWaitEvent,
    phrase: Option<&str>,
) -> Result<Option<String>, String> {
    let (j, s) = (job.to_string(), slug.to_string());
    let row=db.read(|c|{let (j,s)=(j.clone(),s.clone());Box::pin(async move{let mut r=c.query("SELECT session_id,status,exit_code,output_tail FROM job_terminals WHERE job_id=?1 AND slug=?2",params![j,s]).await?;r.next().await?.map(|x|Ok((x.text(0)?,x.text(1)?,x.opt_i64(2)?,x.opt_text(3)?))).transpose()})}).await.map_err(|e|e.to_string())?;
    let Some((sid, status, code, tail)) = row else {
        return Err(format!("Terminal not found: {uri}"));
    };
    let exited = status == "exited";
    if matches!(on, TerminalWaitEvent::Exit) && exited {
        return Ok(Some(
            serde_json::json!({"outcome":"exited","terminal":uri,"exitCode":code,"excerpt":tail})
                .to_string(),
        ));
    }
    if matches!(on, TerminalWaitEvent::Output) {
        let live = orch
            .pty_state
            .sessions
            .lock()
            .ok()
            .and_then(|m| m.get(&sid).cloned())
            .and_then(|s| s.lock().ok()?.output_buffer.clone())
            .and_then(|b| {
                let bytes: Vec<u8> = b.lock().ok()?.iter().copied().collect();
                Some(String::from_utf8_lossy(&bytes).to_string())
            })
            .or(tail.clone())
            .unwrap_or_default();
        if let Some(p) = phrase {
            if let Some(ex) = crate::services::scan_for_phrase("", &live, p).matched_excerpt {
                return Ok(Some(serde_json::json!({"outcome":"matched","terminal":uri,"phrase":p,"exitCode":code,"excerpt":ex}).to_string()));
            }
        }
        if exited {
            return Ok(Some(serde_json::json!({"outcome":"terminal_exited","terminal":uri,"phrase":phrase,"exitCode":code,"excerpt":tail}).to_string()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    async fn test_orchestrator() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open(root.join("test.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i','p',1,'T','active',1,1)",
            "INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-1','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,created_at,updated_at) VALUES ('run-1','i','p','job-1','live',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,created_at,updated_at) VALUES ('old-turn','session-1','run-1','job-1',1,'completed',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,created_at,updated_at) VALUES ('current-turn','session-1','run-1','job-1',2,'active',2,2)",
        ] {
            local.execute(sql, ()).await.unwrap();
        }
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build()
    }

    async fn insert_assistant_event(
        db: &LocalDb,
        id: &str,
        run_id: &str,
        turn_id: &str,
        sequence: i64,
        data: serde_json::Value,
    ) {
        let id = id.to_string();
        let run_id = run_id.to_string();
        let turn_id = turn_id.to_string();
        let data = data.to_string();
        db.write(|conn| {
            let (id, run_id, turn_id, data) = (id.clone(), run_id.clone(), turn_id.clone(), data.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id,run_id,turn_id,sequence,timestamp,event_type,data,created_at) VALUES(?1,?2,?3,?4,1,'assistant',?5,1)",
                    params![id, run_id, turn_id, sequence, data],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// The predicate must accept a run item exactly as the model emits it: with
    /// optional fields omitted rather than sent as null, and with any incidental
    /// key the run schema permits on a wait item (`description`).
    ///
    /// Every fixture here is literal model-shaped JSON, never
    /// `serde_json::to_value(&wait)`. Building the expectation from the same
    /// serializer the predicate uses is what let CAIRN-3115 ship: a serialized
    /// `phrase: null` can never equal an input that omitted the key, so no
    /// terminal exit wait could correlate, while the test asserting otherwise
    /// stayed green.
    #[tokio::test]
    async fn correlates_wait_input_exactly_as_the_model_emits_it() {
        let cases = [
            (
                "terminal exit wait, phrase omitted",
                serde_json::json!({
                    "waitFor":{"kind":"terminal","ref":"cairn:~/terminal/tests","on":"exit"}
                }),
            ),
            (
                "terminal output wait carrying a phrase",
                serde_json::json!({
                    "waitFor":{"kind":"terminal","ref":"cairn:~/terminal/dev","on":"output","phrase":"ready"}
                }),
            ),
            (
                "duration wait",
                serde_json::json!({"waitFor":{"duration":"3m"}}),
            ),
            (
                "checks settled wait, suite omitted",
                serde_json::json!({
                    "waitFor":{"kind":"checks","ref":"cairn://p/cairn/3427/1/builder/checks","on":"settled"}
                }),
            ),
            (
                "checks verdict wait carrying a suite",
                serde_json::json!({
                    "waitFor":{"kind":"checks","ref":"cairn:~/checks","on":"verdict","suite":"rust-tests"}
                }),
            ),
            (
                "wait item that also carries a description",
                serde_json::json!({
                    "waitFor":{"kind":"terminal","ref":"cairn:~/terminal/tests","on":"exit"},
                    "description":"wait for the suite to finish"
                }),
            ),
        ];
        for (label, item) in cases {
            let orch = test_orchestrator().await;
            // What the handler itself holds: the wait parsed out of that input.
            let wait: WaitFor = serde_json::from_value(item["waitFor"].clone()).unwrap();
            insert_assistant_event(
                &orch.db.local,
                "event-1",
                "run-1",
                "current-turn",
                1,
                serde_json::json!({"toolUses":[{
                    "toolUseId":"provider-run-id",
                    "name":"mcp__cairn__run",
                    "input":{"commands":[item]}
                }]}),
            )
            .await;

            assert_eq!(
                claim_wait_tool_use_id(&orch.db.local, "run-1", "current-turn", &wait).await,
                Claim::One("provider-run-id".into()),
                "{label} must correlate with its originating run tool call"
            );
        }
    }

    /// Correlation stays an identity, not a category: a wait item in the current
    /// turn that names a different terminal is not this callback's origin.
    #[tokio::test]
    async fn a_different_wait_in_the_same_turn_does_not_correlate() {
        let orch = test_orchestrator().await;
        insert_assistant_event(
            &orch.db.local,
            "event-1",
            "run-1",
            "current-turn",
            1,
            serde_json::json!({"toolUses":[{
                "toolUseId":"provider-run-id",
                "name":"mcp__cairn__run",
                "input":{"commands":[{"waitFor":{"kind":"terminal","ref":"cairn:~/terminal/tests","on":"exit"}}]}
            }]}),
        )
        .await;
        let other: WaitFor = serde_json::from_value(
            serde_json::json!({"kind":"terminal","ref":"cairn:~/terminal/other","on":"exit"}),
        )
        .unwrap();

        assert_eq!(
            claim_wait_tool_use_id(&orch.db.local, "run-1", "current-turn", &other).await,
            Claim::None
        );
    }

    #[tokio::test]
    async fn uncorrelatable_callback_does_not_fabricate_or_insert_wait_identity() {
        // `agent_waits` is provided by the standard migrations that
        // `test_orchestrator` runs; no manual table creation is needed.
        let orch = test_orchestrator().await;
        let wait = WaitFor::Duration {
            duration: WaitDuration::Human("3m".into()),
        };
        assert_eq!(
            claim_wait_tool_use_id(&orch.db.local, "run-1", "current-turn", &wait).await,
            Claim::None
        );
        let count: i64 = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT COUNT(*) FROM agent_waits", ()).await?;
                    rows.next().await?.unwrap().i64(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn identical_prior_turn_wait_is_ignored_until_current_event_arrives() {
        let orch = test_orchestrator().await;
        let wait = WaitFor::Duration {
            duration: WaitDuration::Human("3m".into()),
        };
        let event = |id: &str| {
            serde_json::json!({"toolUses":[{
                "id":id,
                "name":"run",
                "input":{"commands":[{"waitFor":{"duration":"3m"}}]}
            }]})
        };
        insert_assistant_event(
            &orch.db.local,
            "old-event",
            "run-1",
            "old-turn",
            1,
            event("old-provider-id"),
        )
        .await;

        let db = orch.db.local.clone();
        let current_event = event("current-provider-id");
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            insert_assistant_event(
                &db,
                "current-event",
                "run-1",
                "current-turn",
                2,
                current_event,
            )
            .await;
        });

        assert_eq!(
            claim_wait_tool_use_id(&orch.db.local, "run-1", "current-turn", &wait).await,
            Claim::One("current-provider-id".into())
        );
    }

    fn record(condition: Condition, deadline: Option<i64>) -> Record {
        Record {
            id: "wait-1".into(),
            job_id: "job-1".into(),
            run_id: "run-1".into(),
            session_id: "session-1".into(),
            turn_id: "turn-1".into(),
            tool_use_id: "tool-1".into(),
            condition,
            deadline,
            created: chrono::Utc::now().timestamp_millis(),
        }
    }
    #[test]
    fn duration_bounds() {
        assert_eq!(
            parse_duration(&WaitDuration::Human("3m".into())).unwrap(),
            180000
        );
        assert!(parse_duration(&WaitDuration::Human("0s".into())).is_err());
        assert!(parse_duration(&WaitDuration::Human("8d".into())).is_err());
    }

    #[tokio::test]
    async fn elapsed_duration_returns_without_creating_durable_state() {
        let orch = test_orchestrator().await;
        let now = chrono::Utc::now().timestamp_millis();
        let result = trigger(
            orch.clone(),
            orch.db.local.clone(),
            record(Condition::Duration, Some(now)),
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&result).unwrap()["outcome"],
            "elapsed"
        );
    }

    #[tokio::test]
    async fn output_wait_fires_on_exit_before_phrase() {
        let orch = test_orchestrator().await;
        orch.db
            .local
            .execute(
                "INSERT INTO job_terminals (id, job_id, session_id, command, status, exit_code, created_at, exited_at, slug, output_tail)
                 VALUES ('t1','job-1','sess-t1','sleep 1','exited',3,1,50,'tests','boom crash exit')",
                (),
            )
            .await
            .unwrap();
        let result = trigger(
            orch.clone(),
            orch.db.local.clone(),
            record(
                Condition::Terminal {
                    uri: "cairn://p/prj/1/1/builder/terminal/tests".into(),
                    slug: "tests".into(),
                    on: TerminalWaitEvent::Output,
                    phrase: Some("ready".into()),
                },
                None,
            ),
        )
        .await
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            value["outcome"], "terminal_exited",
            "an output/phrase wait must resolve when the terminal exits before the phrase"
        );
    }

    #[test]
    fn a_checks_wait_refuses_a_mismatched_on_and_suite_pair() {
        assert_eq!(
            checks_suite(&ChecksWaitEvent::Settled, None).unwrap(),
            None,
            "a settled wait watches every lane"
        );
        assert_eq!(
            checks_suite(&ChecksWaitEvent::Verdict, Some("rust-tests")).unwrap(),
            Some("rust-tests".to_string())
        );
        assert!(checks_suite(&ChecksWaitEvent::Settled, Some("rust-tests"))
            .unwrap_err()
            .contains("does not accept suite"));
        for empty in [None, Some(""), Some("   ")] {
            assert!(
                checks_suite(&ChecksWaitEvent::Verdict, empty)
                    .unwrap_err()
                    .contains("requires suite"),
                "a verdict wait naming no suite watches nothing in particular: {empty:?}"
            );
        }
    }

    /// The resume is the whole agent-visible surface of a settled wait, so it
    /// has to carry the branchable answer AND enough of the lanes that reading
    /// the resource is a choice rather than a second required call.
    #[test]
    fn a_settled_resume_carries_the_verdict_the_lanes_and_the_gaps() {
        use crate::execution::checks_settlement::{classify, ChecksSnapshot};
        use crate::execution::checks_status::{NodeCheckState, NodeCheckStatus};
        use crate::messages::delivery::HeadTurn;

        let lane = |name: &str, state: NodeCheckState| NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: name.to_string(),
            state,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: None,
            duration_ms: None,
            ran_at: None,
            passed: None,
            failed: None,
            skipped: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
        };
        let statuses = vec![
            lane("rust-lint", NodeCheckState::Pending),
            lane("rust-tests", NodeCheckState::Passed),
        ];
        let snapshot = ChecksSnapshot {
            settlement: classify(&statuses, HeadTurn::Idle, false),
            statuses,
            terminal_reason: Some("issue merged before submission".to_string()),
        };
        let uri = "cairn://p/cairn/3427/1/builder/checks";
        let value: serde_json::Value =
            serde_json::from_str(&settled_result(uri, None, &snapshot, 1234)).unwrap();

        assert_eq!(value["outcome"], "settled");
        assert_eq!(value["checks"], uri);
        assert_eq!(value["verdict"], "incomplete");
        assert_eq!(value["elapsedMs"], 1234);
        assert_eq!(value["verdictless"][0], "rust-lint");
        assert_eq!(value["terminalReason"], "issue merged before submission");
        assert!(value["note"]
            .as_str()
            .unwrap()
            .contains("without producing a verdict"));
        let lanes = value["lanes"].as_array().unwrap();
        assert_eq!(lanes.len(), 2);
        assert!(lanes[0].as_str().unwrap().contains("[no verdict]"));

        // A clean settle says nothing about gaps it does not have -- an empty
        // `verdictless` key would still read as a caveat worth chasing.
        let statuses = vec![lane("rust-tests", NodeCheckState::Passed)];
        let clean = ChecksSnapshot {
            settlement: classify(&statuses, HeadTurn::Idle, false),
            statuses,
            terminal_reason: None,
        };
        let value: serde_json::Value =
            serde_json::from_str(&settled_result(uri, Some("rust-tests"), &clean, 5)).unwrap();
        assert_eq!(value["verdict"], "passed");
        assert_eq!(value["suite"], "rust-tests");
        assert!(value.get("verdictless").is_none());
        assert!(value.get("note").is_none());
    }

    /// The wait's half of CAIRN-3153. An exit wait reads its answer off the
    /// terminal row, so it carries whatever status finalization recorded — which,
    /// now that an agent terminal's command *is* its lifetime process, is the
    /// command's own exit code rather than a shell's.
    #[tokio::test]
    async fn exit_wait_resolves_with_the_terminals_recorded_status() {
        let orch = test_orchestrator().await;
        orch.db
            .local
            .execute(
                "INSERT INTO job_terminals (id, job_id, session_id, command, status, exit_code, created_at, exited_at, slug, output_tail)
                 VALUES ('t1','job-1','sess-t1','bun test','exited',7,1,50,'tests','1 test failed')",
                (),
            )
            .await
            .unwrap();

        let result = trigger(
            orch.clone(),
            orch.db.local.clone(),
            record(
                Condition::Terminal {
                    uri: "cairn://p/prj/1/1/builder/terminal/tests".into(),
                    slug: "tests".into(),
                    on: TerminalWaitEvent::Exit,
                    phrase: None,
                },
                None,
            ),
        )
        .await
        .unwrap();

        let value: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(value["outcome"], "exited");
        assert_eq!(
            value["exitCode"], 7,
            "the wait must carry the command's real exit code"
        );
        assert_eq!(value["excerpt"], "1 test failed");
    }

    /// The upgrade window, and the reason an exit wait armed under the old
    /// semantics is not stranded by this change. Startup re-arms every pending
    /// wait, and a terminal orphaned by that same restart is `recovering`: the
    /// wait must not resolve falsely then (nothing has exited), and must resolve
    /// as soon as recovery settles the terminal — whether by re-spawning the
    /// command as its lifetime process and letting it exit, or by proving the
    /// owning executor gone and marking the row exited. The condition is
    /// level-triggered against the row, so it needs nothing the crashed host held.
    #[tokio::test]
    async fn exit_wait_survives_the_recovery_window_then_resolves_when_it_settles() {
        let orch = test_orchestrator().await;
        // A terminal shaped exactly like a pre-upgrade agent command terminal that
        // a host restart orphaned: a command, no operator title, recovering.
        orch.db
            .local
            .execute(
                "INSERT INTO job_terminals (id, job_id, session_id, command, title, status, created_at, slug)
                 VALUES ('t1','job-1','sess-t1','bun test',NULL,'recovering',1,'tests')",
                (),
            )
            .await
            .unwrap();
        let armed = record(
            Condition::Terminal {
                uri: "cairn://p/prj/1/1/builder/terminal/tests".into(),
                slug: "tests".into(),
                on: TerminalWaitEvent::Exit,
                phrase: None,
            },
            None,
        );

        let premature = tokio::time::timeout(
            Duration::from_millis(300),
            trigger(orch.clone(), orch.db.local.clone(), armed.clone()),
        )
        .await;
        assert!(
            premature.is_err(),
            "a recovering terminal has not exited; the wait must keep waiting"
        );

        orch.db
            .local
            .execute(
                "UPDATE job_terminals SET status='exited', exit_code=7, exited_at=50 WHERE id='t1'",
                (),
            )
            .await
            .unwrap();

        let result = trigger(orch.clone(), orch.db.local.clone(), armed)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(value["outcome"], "exited");
        assert_eq!(value["exitCode"], 7);
    }

    #[tokio::test]
    async fn missing_terminal_fails_before_any_durable_wait_exists() {
        let orch = test_orchestrator().await;
        let error = trigger(
            orch.clone(),
            orch.db.local.clone(),
            record(
                Condition::Terminal {
                    uri: "cairn://p/prj/1/1/builder/terminal/missing".into(),
                    slug: "missing".into(),
                    on: TerminalWaitEvent::Exit,
                    phrase: None,
                },
                None,
            ),
        )
        .await
        .unwrap_err();
        assert!(error.contains("Terminal not found"));
    }
}
