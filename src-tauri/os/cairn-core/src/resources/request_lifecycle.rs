//! `cairn://p/{project}/requests/{id}`: one run batch's lifecycle, by any id
//! it was called by.
//!
//! A batch's life is written across tables keyed by different ids: the
//! request ledger (`run_batch_requests`, [`crate::mcp::handlers::run::ledger`])
//! by request id, tool call id and run id; the wait a parked call took
//! (`agent_waits`) by wait id and tool call id; the detached batch
//! (`run_batch_detached`) and adopted results (`run_batch_adopted_outcomes`)
//! by request id; the placement journal (`execution_queue`, in the runner's
//! own store) by request, entry and attempt id; and the run itself (`runs`).
//! This resolves whichever id it is given to the request it names, gathers
//! every row the request has, and reads one lifecycle from them. Every stage
//! names the table it came from, and nothing is read from the log. Every
//! lookup leads with an indexed column.
//!
//! The end is decided from rows alone ([`derive_end`]). A wait's end is never
//! a batch's end: a batch whose wait was abandoned while it runs reads as
//! still running, with the wait's resolution quoted.

use std::collections::{BTreeMap, BTreeSet};

use cairn_db::turso::params;
use serde::Serialize;

use crate::mcp::handlers::run::detached;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Every id kind this resolves, with the shape each takes.
const ID_KINDS: &[(&str, &str)] = &[
    (
        "run-batch request id",
        "a UUID such as 472b219b-3852-4a74-867a-df41db86dfa2",
    ),
    (
        "tool call id",
        "the call's tool-use id, such as toolu_01Ab… or exec-fde657a4…",
    ),
    (
        "run id",
        "the agent run's UUID; lists every batch the run made",
    ),
    (
        "wait id",
        "the id of the `agent_waits` row a parked call took",
    ),
    ("placement entry id", "an `execution_queue` entry id"),
    ("placement attempt id", "an `execution_queue` attempt id"),
    ("check request id", "the UUID a check filing returned"),
    (
        "check lane request or attempt id",
        "a `check_lane_adoptions` request or attempt id",
    ),
    (
        "check observation",
        "an observation's UUID, or the 24-hex handle its page shows",
    ),
];

/// The lifecycle address of a request.
pub(crate) fn lifecycle_uri(project: &str, request_id: &str) -> String {
    format!(
        "cairn://p/{}/requests/{request_id}",
        cairn_uri::canonical_project(project)
    )
}

/// The lifecycle address of a request whose owner is known by its node home.
/// `None` when the home names no project, which leaves the link out rather
/// than guessing it.
pub(crate) fn lifecycle_uri_for_home(home: &str, request_id: &str) -> Option<String> {
    let resource = cairn_uri::parse_uri(home)?;
    Some(lifecycle_uri(resource.project_key()?, request_id))
}

/// What waits for room on one machine, with what closes its room: the
/// "Parked against this machine" section of its page. `None` when nothing
/// waits there.
pub(crate) async fn parked_section(orch: &Orchestrator, executor_id: &str) -> Option<String> {
    let rows = orch.fleet.parked_against(executor_id);
    if rows.is_empty() {
        return None;
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut out = format!(
        "\n## Parked against this machine ({})\n\nRequests waiting for room here that this machine has not been offered, so its own queue does not count them. Each names the closures of this machine's room that hold it, judged by its own need as admission judges it.\n\n",
        rows.len()
    );
    for row in rows {
        let crate::fleet::ParkedRequest {
            request_id,
            project_id,
            since_unix_ms: since,
            holds,
        } = row;
        let held = if holds.is_empty() {
            "no closure holds it; it waits for room to free".to_string()
        } else {
            holds
                .iter()
                .map(|closure| {
                    format!(
                        "held ({}): {}",
                        format!("{:?}", closure.reason).to_lowercase(),
                        crate::fleet::queue::closure_detail(closure)
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        };
        let key = orch
            .db
            .local
            .query_opt_text("SELECT key FROM projects WHERE id=?1", (project_id,))
            .await
            .ok()
            .flatten();
        let request = match key {
            Some(key) => format!("[{request_id}]({})", lifecycle_uri(&key, &request_id)),
            None => format!("`{request_id}`"),
        };
        let since = since as i64;
        out.push_str(&format!(
            "- request {request}, waiting {} (since {}): {held}\n",
            span(now_ms - since),
            clock(since)
        ));
    }
    Some(out)
}

/// Read one request's lifecycle as Markdown or (`json`) as one JSON object.
pub(crate) async fn read_request(
    orch: &Orchestrator,
    project: &str,
    id: &str,
    json: bool,
) -> String {
    let db = orch.db.for_project(project).await;
    let project_id = match crate::mcp::handlers::run_context::project_id_by_key(&db, project).await
    {
        Ok(project_id) => project_id,
        Err(error) => return error,
    };
    let local = &orch.db.local;
    let checks_db = crate::execution::checks::requests::request_db(orch, &project_id)
        .await
        .ok();
    let resolution = match resolve(&db, local, checks_db.as_deref(), &project_id, id).await {
        Ok(resolution) => resolution,
        Err(error) => return format!("Unable to resolve `{id}`: {error}"),
    };
    let requests = resolution.request_ids();
    match requests.as_slice() {
        [] => match &resolution.check_request {
            Some(check) => check_request_only(project, check, json),
            None => unknown(id, json),
        },
        [request_id] => {
            let wait_hint = resolution.waits.get(request_id).map(String::as_str);
            let facts = match gather(&db, local, request_id, wait_hint).await {
                Ok(facts) => facts,
                Err(error) => return format!("Unable to read request {request_id}: {error}"),
            };
            let context = Context::live(orch, &db, request_id, &facts).await;
            let lifecycle = build(
                project,
                request_id,
                resolution.kinds_for(request_id),
                resolution.check_request.clone(),
                facts,
                &context,
            );
            if json {
                serde_json::to_string_pretty(&lifecycle).unwrap_or_else(|error| error.to_string())
            } else {
                render(&lifecycle)
            }
        }
        many => {
            let rows = match summaries(&db, many).await {
                Ok(rows) => rows,
                Err(error) => return format!("Unable to list the requests `{id}` names: {error}"),
            };
            let run = run_row(&db, id).await.ok().flatten();
            disambiguation(project, id, &resolution, &rows, run.as_ref(), json)
        }
    }
}

// --- Resolution -----------------------------------------------------------

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct CheckRequestRef {
    #[serde(skip)]
    id: String,
    /// Its address within the project: `check-requests/{number}`.
    number: u64,
    suite: String,
    state: String,
    fleet_request_id: Option<String>,
}

#[derive(Debug, Default)]
struct Resolution {
    /// Each request the id names, with the id kind that reached it, in the
    /// order they were first found.
    matches: Vec<(String, &'static str)>,
    /// The wait a request was found through, read back by its key.
    waits: BTreeMap<String, String>,
    check_request: Option<CheckRequestRef>,
}

impl Resolution {
    fn add(&mut self, request_id: String, kind: &'static str) {
        if !self
            .matches
            .iter()
            .any(|(id, seen)| *id == request_id && *seen == kind)
        {
            self.matches.push((request_id, kind));
        }
    }

    fn request_ids(&self) -> Vec<String> {
        let mut seen = BTreeSet::new();
        self.matches
            .iter()
            .filter(|(id, _)| seen.insert(id.clone()))
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn kinds_for(&self, request_id: &str) -> Vec<&'static str> {
        self.matches
            .iter()
            .filter(|(id, _)| id == request_id)
            .map(|(_, kind)| *kind)
            .collect()
    }
}

/// The request a run-batch wait's condition carries.
fn run_batch_request_of(condition_json: &str) -> Option<String> {
    let condition: serde_json::Value = serde_json::from_str(condition_json).ok()?;
    if condition.get("kind").and_then(|kind| kind.as_str()) != Some("run_batch") {
        return None;
    }
    condition
        .get("request_id")
        .and_then(|id| id.as_str())
        .map(str::to_owned)
}

async fn texts(db: &LocalDb, sql: &'static str, id: &str) -> Result<Vec<String>, String> {
    db.query_all(sql, params![id.to_owned()], |row| row.text(0))
        .await
        .map_err(|error| error.to_string())
}

/// `(wait id, condition)` pairs.
async fn waits(db: &LocalDb, sql: &'static str, id: &str) -> Result<Vec<(String, String)>, String> {
    db.query_all(sql, params![id.to_owned()], |row| {
        Ok((row.text(0)?, row.text(1)?))
    })
    .await
    .map_err(|error| error.to_string())
}

async fn resolve(
    db: &LocalDb,
    local: &LocalDb,
    checks_db: Option<&LocalDb>,
    project_id: &str,
    id: &str,
) -> Result<Resolution, String> {
    const REQUEST: &str = "run-batch request id";
    let mut out = Resolution::default();
    for (sql, kind) in [
        (
            "SELECT request_id FROM run_batch_requests WHERE request_id=?1",
            REQUEST,
        ),
        (
            "SELECT request_id FROM run_batch_detached WHERE request_id=?1",
            REQUEST,
        ),
        (
            "SELECT request_id FROM run_batch_requests WHERE tool_use_id=?1",
            "tool call id",
        ),
        (
            "SELECT request_id FROM run_batch_requests WHERE run_id=?1 ORDER BY received_ms",
            "run id",
        ),
        (
            "SELECT request_id FROM run_batch_adopted_outcomes WHERE request_id=?1",
            REQUEST,
        ),
        (
            "SELECT request_id FROM run_batch_adopted_outcomes WHERE attempt_id=?1",
            "adopted attempt id",
        ),
    ] {
        for found in texts(db, sql, id).await? {
            out.add(found, kind);
        }
    }
    for (sql, kind) in [
        (
            "SELECT id, condition_json FROM agent_waits WHERE tool_use_id=?1",
            "tool call id",
        ),
        (
            "SELECT id, condition_json FROM agent_waits WHERE id=?1",
            "wait id",
        ),
        (
            "SELECT id, condition_json FROM agent_waits WHERE run_id=?1",
            "run id",
        ),
        (
            "SELECT id, condition_json FROM agent_waits WHERE json_extract(condition_json, '$.request_id')=?1",
            REQUEST,
        ),
    ] {
        for (wait_id, condition) in waits(db, sql, id).await? {
            if let Some(found) = run_batch_request_of(&condition) {
                out.waits.entry(found.clone()).or_insert(wait_id);
                out.add(found, kind);
            }
        }
    }
    for (sql, kind) in [
        (
            "SELECT request_id FROM execution_queue WHERE entry_id=?1",
            "placement entry id",
        ),
        (
            "SELECT request_id FROM execution_queue WHERE attempt_id=?1",
            "placement attempt id",
        ),
        (
            "SELECT request_id FROM check_lane_adoptions WHERE request_id=?1",
            "check lane request id",
        ),
        (
            "SELECT request_id FROM check_lane_adoptions WHERE attempt_id=?1",
            "check lane attempt id",
        ),
        (
            "SELECT request_id FROM check_lane_adoptions WHERE observation_id=?1",
            "check observation id",
        ),
    ] {
        for found in texts(local, sql, id).await? {
            out.add(found, kind);
        }
    }
    // The 24-hex handle an observation page shows is not the UUID the lane
    // records; both resolve.
    let observation = db
        .query_opt_text(
            "SELECT id FROM check_result_observations WHERE project_id=?1 AND public_handle=?2",
            (project_id.to_owned(), id.to_owned()),
        )
        .await
        .map_err(|error| error.to_string())?;
    if let Some(observation) = observation {
        for found in texts(
            local,
            "SELECT request_id FROM check_lane_adoptions WHERE observation_id=?1",
            &observation,
        )
        .await?
        {
            out.add(found, "check observation handle");
        }
    }
    if out.matches.is_empty() {
        // A request only the placement journal knows: a check lane, or a
        // batch whose rows were never written.
        for found in texts(
            local,
            "SELECT request_id FROM execution_queue INDEXED BY idx_execution_queue_request WHERE request_id=?1 LIMIT 1",
            id,
        )
        .await?
        {
            out.add(found, REQUEST);
        }
    }
    if let Some(checks_db) = checks_db {
        let row = checks_db
            .query_opt(
                "SELECT id, suite, state, request_id, number FROM check_requests WHERE id=?1",
                params![id.to_owned()],
                |row| {
                    Ok(CheckRequestRef {
                        id: row.text(0)?,
                        suite: row.text(1)?,
                        state: row.text(2)?,
                        fleet_request_id: row.opt_text(3)?,
                        number: u64::try_from(row.opt_i64(4)?.unwrap_or_default())
                            .unwrap_or_default(),
                    })
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        if let Some(check) = row {
            if let Some(fleet) = check.fleet_request_id.clone() {
                out.add(fleet, "check request id");
            }
            out.check_request = Some(check);
        }
    }
    Ok(out)
}

// --- Facts ----------------------------------------------------------------

/// The request's ledger row, as the run handler wrote it.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct LedgerRow {
    tool_use_id: Option<String>,
    job_id: String,
    run_id: String,
    session_id: Option<String>,
    turn_id: Option<String>,
    commits: bool,
    received_ms: i64,
    incarnation: Option<String>,
    label: Option<String>,
    item_count: Option<i64>,
    sequential: Option<bool>,
    declared_timeout_ms: Option<i64>,
    verdict_branch: Option<String>,
    cwd: Option<String>,
    selector_json: Option<String>,
    attempt_id: Option<String>,
    executor_id: Option<String>,
    machine: Option<String>,
    generation: Option<i64>,
    cell_id: Option<String>,
    pid: Option<i64>,
    started_ms: Option<i64>,
    peak_cpu_milli: Option<i64>,
    peak_resident_bytes: Option<i64>,
    last_progress_ms: Option<i64>,
    output_bytes: Option<i64>,
    ended_ms: Option<i64>,
    end: Option<String>,
    end_detail: Option<String>,
    /// Its number within its job: the `batches/{n}` handle.
    number: Option<u64>,
}

const LEDGER_COLUMNS: &str =
    "tool_use_id, job_id, run_id, session_id, turn_id, commits, received_ms, \
     incarnation, label, item_count, sequential, declared_timeout_ms, verdict_branch, cwd, \
     selector_json, attempt_id, executor_id, machine, generation, cell_id, pid, started_ms, \
     peak_cpu_milli, peak_resident_bytes, last_progress_ms, output_bytes, ended_ms, \"end\", \
     end_detail, number";

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DetachedRow {
    job_id: String,
    run_id: String,
    label: Option<String>,
    commits: bool,
    state: String,
    verdict: Option<String>,
    reason: String,
    lost_evidence: Option<String>,
    created_ms: i64,
    settled_ms: Option<i64>,
    notified_ms: Option<i64>,
    waiting_on: Option<String>,
    holds_branch: bool,
    output: Option<String>,
    commit: String,
    snapshot: Option<String>,
    late_report: Option<String>,
}

impl From<detached::DetachedBatch> for DetachedRow {
    fn from(row: detached::DetachedBatch) -> Self {
        Self {
            holds_branch: row.holds_branch,
            commit: detached::commit_phrase(&row),
            snapshot: detached::snapshot_phrase(&row),
            late_report: row
                .late_report
                .as_ref()
                .map(|late| detached::late_notice_sentence(&row, late)),
            job_id: row.job_id,
            run_id: row.run_id,
            label: row.label,
            commits: row.commits,
            state: row.state.as_str().to_owned(),
            verdict: row.verdict,
            reason: row.reason,
            lost_evidence: row.lost_evidence,
            created_ms: row.created_ms,
            settled_ms: row.settled_ms,
            notified_ms: row.notified_ms,
            waiting_on: row.waiting_on,
            output: row.output,
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct WaitRow {
    id: String,
    job_id: String,
    run_id: String,
    session_id: String,
    predecessor_turn_id: String,
    tool_use_id: String,
    state: String,
    resolution_json: Option<String>,
    successor_turn_id: Option<String>,
    created_ms: i64,
    resolved_ms: Option<i64>,
    late_cancel: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct AdoptedRow {
    attempt_id: String,
    machine: String,
    state: String,
    received_ms: i64,
    settled_ms: Option<i64>,
}

/// One placement journal event.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct JournalRow {
    entry_id: String,
    attempt_id: String,
    runner_incarnation: String,
    state: String,
    executor_id: Option<String>,
    machine: Option<String>,
    generation: Option<i64>,
    decline_reason: Option<String>,
    detail: Option<String>,
    at_ms: i64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RunRow {
    id: String,
    status: String,
    exit_reason: Option<String>,
    /// `runs.exited_at` is unix seconds; this is milliseconds.
    exited_ms: Option<i64>,
}

/// One check lane attempt, from `check_lane_adoptions` and its waiters.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct LaneRow {
    attempt_id: String,
    check_names: String,
    consumer: String,
    consumer_ref: String,
    executor_id: String,
    machine: String,
    granted_at_ms: i64,
    state: String,
    stopped_at_ms: Option<i64>,
    evidence: Option<String>,
    observation_id: Option<String>,
    observation_handle: Option<String>,
    observation_uri: Option<String>,
    waiters: Vec<String>,
}

#[derive(Debug, Default)]
struct Facts {
    lanes: Vec<LaneRow>,
    ledger: Option<LedgerRow>,
    detached: Option<detached::DetachedBatch>,
    wait: Option<WaitRow>,
    adopted: Vec<AdoptedRow>,
    journal: Vec<JournalRow>,
    run: Option<RunRow>,
    /// The first runner start after the request was received.
    restart_ms: Option<i64>,
}

async fn ledger_row(db: &LocalDb, request_id: &str) -> Result<Option<LedgerRow>, String> {
    db.query_opt(
        format!("SELECT {LEDGER_COLUMNS} FROM run_batch_requests WHERE request_id=?1"),
        params![request_id.to_owned()],
        |row| {
            Ok(LedgerRow {
                tool_use_id: row.opt_text(0)?,
                job_id: row.text(1)?,
                run_id: row.text(2)?,
                session_id: row.opt_text(3)?,
                turn_id: row.opt_text(4)?,
                commits: row.i64(5)? != 0,
                received_ms: row.i64(6)?,
                incarnation: row.opt_text(7)?,
                label: row.opt_text(8)?,
                item_count: row.opt_i64(9)?,
                sequential: row.opt_i64(10)?.map(|value| value != 0),
                declared_timeout_ms: row.opt_i64(11)?,
                verdict_branch: row.opt_text(12)?,
                cwd: row.opt_text(13)?,
                selector_json: row.opt_text(14)?,
                attempt_id: row.opt_text(15)?,
                executor_id: row.opt_text(16)?,
                machine: row.opt_text(17)?,
                generation: row.opt_i64(18)?,
                cell_id: row.opt_text(19)?,
                pid: row.opt_i64(20)?,
                started_ms: row.opt_i64(21)?,
                peak_cpu_milli: row.opt_i64(22)?,
                peak_resident_bytes: row.opt_i64(23)?,
                last_progress_ms: row.opt_i64(24)?,
                output_bytes: row.opt_i64(25)?,
                ended_ms: row.opt_i64(26)?,
                end: row.opt_text(27)?,
                end_detail: row.opt_text(28)?,
                number: row.opt_i64(29)?.and_then(|n| u64::try_from(n).ok()),
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

async fn run_row(db: &LocalDb, run_id: &str) -> Result<Option<RunRow>, String> {
    db.query_opt(
        "SELECT id, status, exit_reason, exited_at FROM runs WHERE id=?1",
        params![run_id.to_owned()],
        |row| {
            Ok(RunRow {
                id: row.text(0)?,
                status: row.text(1)?,
                exit_reason: row.opt_text(2)?,
                exited_ms: row.opt_i64(3)?.map(|seconds| seconds * 1000),
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

const WAIT_COLUMNS: &str = "id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, \
     state, resolution_json, successor_turn_id, created_at, resolved_at, condition_json";

fn wait_of(row: &cairn_db::turso::Row) -> DbResult<(WaitRow, String)> {
    let condition = row.text(11)?;
    let late_cancel = serde_json::from_str::<serde_json::Value>(&condition)
        .ok()
        .and_then(|value| {
            value
                .get("settlement")?
                .get("late_cancel")?
                .as_str()
                .map(str::to_owned)
        });
    Ok((
        WaitRow {
            id: row.text(0)?,
            job_id: row.text(1)?,
            run_id: row.text(2)?,
            session_id: row.text(3)?,
            predecessor_turn_id: row.text(4)?,
            tool_use_id: row.text(5)?,
            state: row.text(6)?,
            resolution_json: row.opt_text(7)?,
            successor_turn_id: row.opt_text(8)?,
            created_ms: row.i64(9)?,
            resolved_ms: row.opt_i64(10)?,
            late_cancel,
        },
        condition,
    ))
}

/// The wait a parked call took for this request: by the wait id the resolver
/// found it through, else through the ledger's `(run_id, tool_use_id)`, which
/// `agent_waits` indexes uniquely, else among its run's waits.
async fn wait_row(
    db: &LocalDb,
    request_id: &str,
    hint: Option<&str>,
    run_id: Option<&str>,
    tool_use_id: Option<&str>,
) -> Result<Option<WaitRow>, String> {
    let rows = match (hint, run_id, tool_use_id) {
        (Some(wait_id), _, _) => {
            db.query_all(
                format!("SELECT {WAIT_COLUMNS} FROM agent_waits WHERE id=?1"),
                params![wait_id.to_owned()],
                wait_of,
            )
            .await
        }
        (None, Some(run_id), Some(tool_use_id)) => {
            db.query_all(
                format!(
                    "SELECT {WAIT_COLUMNS} FROM agent_waits WHERE run_id=?1 AND tool_use_id=?2"
                ),
                params![run_id.to_owned(), tool_use_id.to_owned()],
                wait_of,
            )
            .await
        }
        (None, _, _) => {
            db.query_all(
                format!("SELECT {WAIT_COLUMNS} FROM agent_waits WHERE json_extract(condition_json, '$.request_id')=?1"),
                params![request_id.to_owned()],
                wait_of,
            )
            .await
        }
    }
    .map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .find(|(_, condition)| run_batch_request_of(condition).as_deref() == Some(request_id))
        .map(|(wait, _)| wait))
}

async fn gather(
    db: &LocalDb,
    local: &LocalDb,
    request_id: &str,
    wait_hint: Option<&str>,
) -> Result<Facts, String> {
    let ledger = ledger_row(db, request_id).await?;
    let detached = detached::load(db, request_id).await?;
    let run_id = ledger
        .as_ref()
        .map(|row| row.run_id.clone())
        .or_else(|| detached.as_ref().map(|row| row.run_id.clone()));
    let tool_use_id = ledger.as_ref().and_then(|row| row.tool_use_id.clone());
    let wait = wait_row(
        db,
        request_id,
        wait_hint,
        run_id.as_deref(),
        tool_use_id.as_deref(),
    )
    .await?;
    let run_id = run_id.or_else(|| wait.as_ref().map(|wait| wait.run_id.clone()));
    let adopted = db
        .query_all(
            "SELECT attempt_id, machine, state, received_ms, settled_ms FROM run_batch_adopted_outcomes WHERE request_id=?1",
            params![request_id.to_owned()],
            |row| {
                Ok(AdoptedRow {
                    attempt_id: row.text(0)?,
                    machine: row.text(1)?,
                    state: row.text(2)?,
                    received_ms: row.i64(3)?,
                    settled_ms: row.opt_i64(4)?,
                })
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let journal = local
        .query_all(
            "SELECT entry_id, attempt_id, runner_incarnation, state, executor_id, generation, \
             decline_reason, detail, created_at FROM execution_queue INDEXED BY idx_execution_queue_request \
             WHERE request_id=?1 ORDER BY created_at, sequence",
            params![request_id.to_owned()],
            |row| {
                Ok(JournalRow {
                    entry_id: row.text(0)?,
                    attempt_id: row.text(1)?,
                    runner_incarnation: row.text(2)?,
                    state: row.text(3)?,
                    executor_id: row.opt_text(4)?,
                    machine: None,
                    generation: row.opt_i64(5)?,
                    decline_reason: row.opt_text(6)?,
                    detail: row.opt_text(7)?,
                    at_ms: row.i64(8)?,
                })
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let run = match run_id.as_deref() {
        Some(run_id) => run_row(db, run_id).await?,
        None => None,
    };
    let received = ledger
        .as_ref()
        .map(|row| row.received_ms)
        .or_else(|| detached.as_ref().map(|row| row.created_ms))
        .or_else(|| wait.as_ref().map(|row| row.created_ms));
    let restart_ms = match received {
        Some(received) => local
            .query_opt_i64(
                "SELECT occurred_at_ms FROM runner_events WHERE kind='runner_started' \
                 AND occurred_at_ms > ?1 ORDER BY occurred_at_ms LIMIT 1",
                (received,),
            )
            .await
            .map_err(|error| error.to_string())?,
        None => None,
    };
    let lanes = lanes(db, local, request_id).await?;
    Ok(Facts {
        lanes,
        ledger,
        detached,
        wait,
        adopted,
        journal,
        run,
        restart_ms,
    })
}

/// The request's check lane attempts, with their waiters and the observation
/// each recorded, by both its UUID and its public handle.
async fn lanes(db: &LocalDb, local: &LocalDb, request_id: &str) -> Result<Vec<LaneRow>, String> {
    let mut lanes = local
        .query_all(
            "SELECT attempt_id, check_names, consumer, consumer_ref, executor_id, machine, granted_at_ms, \
             state, stopped_at_ms, evidence, observation_id FROM check_lane_adoptions WHERE request_id=?1 \
             ORDER BY granted_at_ms",
            params![request_id.to_owned()],
            |row| {
                Ok(LaneRow {
                    attempt_id: row.text(0)?,
                    check_names: row.text(1)?,
                    consumer: row.text(2)?,
                    consumer_ref: row.text(3)?,
                    executor_id: row.text(4)?,
                    machine: row.text(5)?,
                    granted_at_ms: row.i64(6)?,
                    state: row.text(7)?,
                    stopped_at_ms: row.opt_i64(8)?,
                    evidence: row.opt_text(9)?,
                    observation_id: row.opt_text(10)?,
                    observation_handle: None,
                    observation_uri: None,
                    waiters: Vec::new(),
                })
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if lanes.is_empty() {
        return Ok(lanes);
    }
    let waiters = local
        .query_all(
            "SELECT attempt_id, check_name, consumer, consumer_ref, state FROM check_lane_adoption_waiters WHERE request_id=?1",
            params![request_id.to_owned()],
            |row| {
                Ok((
                    row.text(0)?,
                    format!("{} for {} `{}`: {}", row.text(1)?, row.text(2)?, row.text(3)?, row.text(4)?),
                ))
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    for lane in &mut lanes {
        lane.waiters = waiters
            .iter()
            .filter(|(attempt, _)| *attempt == lane.attempt_id)
            .map(|(_, line)| line.clone())
            .collect();
        if let Some(observation) = &lane.observation_id {
            lane.observation_handle = db
                .query_opt_text(
                    "SELECT CAST(number AS TEXT) FROM check_result_observations WHERE id=?1",
                    (observation.clone(),),
                )
                .await
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(lanes)
}

// --- The lifecycle --------------------------------------------------------

/// What the read knows that is not in a table: this runner's live view.
#[derive(Debug, Default)]
struct Context {
    now_ms: i64,
    incarnation: Option<String>,
    in_flight: bool,
    live: Option<Live>,
    status: Option<String>,
    progress: Option<String>,
    node: Option<String>,
    machines: BTreeMap<String, String>,
}

impl Context {
    async fn live(orch: &Orchestrator, db: &LocalDb, request_id: &str, facts: &Facts) -> Self {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let status = facts
            .detached
            .as_ref()
            .filter(|batch| batch.state == detached::DetachedState::Running)
            .map(|batch| detached::live_state(orch, batch).sentence());
        let progress =
            detached::live_progress(orch, request_id).map(|progress| progress.describe(now_ms));
        let executing = orch.fleet.executing_command(request_id);
        let in_flight = executing.is_some()
            || orch.fleet.is_request_in_flight(request_id)
            || crate::mcp::handlers::run::detached::live_progress(orch, request_id).is_some();
        let live = executing.map(|execution| Live {
            machine: execution.machine,
            cores: execution.cpu_milli.map(|milli| milli as f64 / 1000.0),
            resident_bytes: execution.resident_bytes,
            sampled_ago_ms: execution.measured_ms.map(|at| (now_ms - at).max(0)),
        });
        let run_id = facts
            .ledger
            .as_ref()
            .map(|row| row.run_id.clone())
            .or_else(|| facts.detached.as_ref().map(|row| row.run_id.clone()))
            .or_else(|| facts.wait.as_ref().map(|row| row.run_id.clone()));
        let node = match run_id {
            Some(run_id) => {
                crate::mcp::handlers::run_context::lookup_home_uri_by_run_id(db, &run_id)
                    .await
                    .ok()
            }
            None => None,
        };
        let machines = facts
            .journal
            .iter()
            .filter_map(|row| row.executor_id.clone())
            .chain(
                facts
                    .ledger
                    .as_ref()
                    .and_then(|row| row.executor_id.clone()),
            )
            .map(|executor_id| {
                let name = orch.fleet.executor_name_for_text(&executor_id);
                (executor_id, name)
            })
            .collect();
        Self {
            now_ms,
            incarnation: orch.fleet.runner_incarnation().map(str::to_owned),
            in_flight,
            live,
            status,
            progress,
            node,
            machines,
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Live {
    machine: String,
    cores: Option<f64>,
    resident_bytes: Option<i64>,
    sampled_ago_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Ids {
    request_id: String,
    tool_use_id: Option<String>,
    wait_id: Option<String>,
    run_id: Option<String>,
    job_id: Option<String>,
    node: Option<String>,
    session_id: Option<String>,
    turn_id: Option<String>,
    entry_ids: Vec<String>,
    attempt_ids: Vec<String>,
    check_request: Option<CheckRequestRef>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Execution {
    machine: Option<String>,
    attempt_id: Option<String>,
    cell_id: Option<String>,
    pid: Option<i64>,
    generation: Option<i64>,
    started_ms: Option<i64>,
    peak_cores: Option<f64>,
    peak_resident_bytes: Option<i64>,
    last_progress_ms: Option<i64>,
    output_bytes: Option<i64>,
    source: &'static str,
}

/// How the request ended, or that it has not.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct EndFact {
    /// `delivered`, `lost`, `cancelled`, `refused`, `still_running`, `open`
    /// (no end yet, and nothing says it is lost), or `unknown`.
    kind: &'static str,
    sentence: String,
    at_ms: Option<i64>,
    source: &'static str,
    /// A row's own words, verbatim, when the end rests on them.
    quoted: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RequestLifecycle {
    request_id: String,
    matched_as: Vec<&'static str>,
    ids: Ids,
    end: EndFact,
    ledger: Option<LedgerRow>,
    detached: Option<DetachedRow>,
    wait: Option<WaitRow>,
    adopted: Vec<AdoptedRow>,
    placement: Vec<JournalRow>,
    /// From the first journal event to the first claim, or to now.
    unplaced_ms: Option<i64>,
    lanes: Vec<LaneRow>,
    execution: Option<Execution>,
    live: Option<Live>,
    progress: Option<String>,
    run: Option<RunRow>,
    actions: Vec<String>,
}

fn clock(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.format("%Y-%m-%d %H:%M:%SZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}

fn clock_of(ms: Option<i64>) -> String {
    ms.map_or_else(|| "an unrecorded time".to_string(), clock)
}

fn span(ms: i64) -> String {
    let seconds = ms.max(0) / 1000;
    match seconds {
        0..=119 => format!("{seconds}s"),
        120..=7199 => format!("{}m", seconds / 60),
        _ => format!("{}h{:02}m", seconds / 3600, (seconds % 3600) / 60),
    }
}

fn bytes(count: i64) -> String {
    const KIB: i64 = 1024;
    if count >= KIB * KIB {
        format!("{:.1} MiB", count as f64 / (KIB * KIB) as f64)
    } else if count >= KIB {
        format!("{:.1} KiB", count as f64 / KIB as f64)
    } else {
        format!("{count} B")
    }
}

fn ended(
    kind: &'static str,
    sentence: String,
    at_ms: Option<i64>,
    source: &'static str,
) -> EndFact {
    EndFact {
        kind,
        sentence,
        at_ms,
        source,
        quoted: None,
    }
}

/// The sentence for a run that has exited.
fn run_exit(run: &RunRow) -> Option<String> {
    let exited = run.exited_ms?;
    Some(format!(
        "run `{}` {}: {} at {}",
        run.id,
        run.status,
        run.exit_reason
            .as_deref()
            .unwrap_or("no exit reason recorded"),
        clock(exited)
    ))
}

/// Decide the request's end from its rows, from the most definite fact to
/// the least: an end the call wrote, then the detached row that carries a
/// batch past its call, then the call's own unfinished record, then the
/// wait, then adopted results.
fn derive_end(facts: &Facts, context: &Context) -> EndFact {
    let abandoned_wait = facts
        .wait
        .as_ref()
        .filter(|wait| wait.state == "cancelled")
        .and_then(|wait| wait.resolution_json.clone());
    let still_running = |sentence: String, source: &'static str| EndFact {
        kind: "still_running",
        sentence: match &abandoned_wait {
            Some(_) => format!(
                "{sentence}. Its wait was ended while it runs, and a wait's end is not the batch's"
            ),
            None => sentence,
        },
        at_ms: None,
        source,
        quoted: abandoned_wait.clone(),
    };
    let ledger = facts.ledger.as_ref();
    if let Some(row) = ledger {
        let at = row.ended_ms;
        let detail = row.end_detail.as_deref().unwrap_or("no reason recorded");
        match row.end.as_deref() {
            Some("answered") => {
                let size = row
                    .output_bytes
                    .map(|count| format!(" ({} of answer)", bytes(count)))
                    .unwrap_or_default();
                return ended(
                    "delivered",
                    format!("answered on its call at {}{size}", clock_of(at)),
                    at,
                    "run_batch_requests",
                );
            }
            Some("resumed") => {
                let turn = facts
                    .wait
                    .as_ref()
                    .and_then(|wait| wait.successor_turn_id.as_deref())
                    .map(|turn| format!(" as turn `{turn}`"))
                    .unwrap_or_default();
                return ended(
                    "delivered",
                    format!(
                        "its call parked, and its result resumed job `{}`{turn} at {}",
                        row.job_id,
                        clock_of(at)
                    ),
                    at,
                    "run_batch_requests",
                );
            }
            Some("refused") => {
                return ended(
                    "refused",
                    format!("refused at {}: {detail}", clock_of(at)),
                    at,
                    "run_batch_requests",
                );
            }
            Some("cancelled") => {
                return ended(
                    "cancelled",
                    format!("cancelled {detail} at {}", clock_of(at)),
                    at,
                    "run_batch_requests",
                );
            }
            _ => {}
        }
    }
    if let Some(wait) = facts
        .wait
        .as_ref()
        .filter(|wait| wait.state == "resolving" && wait.resolution_json.is_some())
    {
        return ended(
            "open",
            format!(
                "its result is being delivered to job `{}` through wait `{}`",
                wait.job_id, wait.id
            ),
            None,
            "agent_waits",
        );
    }
    if let Some(detached) = &facts.detached {
        let at = detached.settled_ms;
        return match detached.state.as_str() {
            "settled" => {
                let verdict = detached.verdict.as_deref().unwrap_or("settled");
                let sentence = match detached.notified_ms {
                    Some(notified) => format!(
                        "settled `{verdict}` at {}; its result was queued to job `{}` as a notice at {}",
                        clock_of(at),
                        detached.job_id,
                        clock(notified)
                    ),
                    None => format!(
                        "settled `{verdict}` at {}; its call or turn carried the result",
                        clock_of(at)
                    ),
                };
                ended("delivered", sentence, at, "run_batch_detached")
            }
            "lost" => EndFact {
                quoted: detached.lost_evidence.clone(),
                ..ended(
                    "lost",
                    format!("lost at {}", clock_of(at)),
                    at,
                    "run_batch_detached",
                )
            },
            "parked" => still_running(
                format!(
                    "detached and not yet started: {}",
                    detached
                        .waiting_on
                        .as_deref()
                        .unwrap_or("waiting to be placed")
                ),
                "run_batch_detached",
            ),
            _ => {
                let on = ledger
                    .and_then(|row| row.machine.clone())
                    .or_else(|| context.live.as_ref().map(|live| live.machine.clone()));
                still_running(
                    match (&context.status, on) {
                        (Some(status), _) => status.clone(),
                        (None, Some(machine)) => {
                            format!("still running on {machine}, detached from its call")
                        }
                        (None, None) => "still running, detached from its call".to_string(),
                    },
                    "run_batch_detached",
                )
            }
        };
    }
    if let Some(row) = ledger {
        match row.end.as_deref() {
            Some("dropped") => {
                let stage = row.end_detail.as_deref().unwrap_or("an unrecorded stage");
                let sentence = match facts.run.as_ref().and_then(run_exit) {
                    Some(exit) => {
                        format!("{exit}; the batch was still in {stage} and died with its call")
                    }
                    None => format!(
                        "its call was dropped at {} while the batch was in {stage}, and nothing carried it on",
                        clock_of(row.ended_ms)
                    ),
                };
                return ended("lost", sentence, row.ended_ms, "run_batch_requests, runs");
            }
            Some("detached") => {
                return ended(
                    "unknown",
                    "its call detached it, but its `run_batch_detached` row is gone".to_string(),
                    row.ended_ms,
                    "run_batch_requests",
                );
            }
            _ => {}
        }
        if context.in_flight {
            return still_running(
                "in flight on this runner; its call has not answered yet".to_string(),
                "run_batch_requests",
            );
        }
        let earlier_runner = match (&row.incarnation, &context.incarnation) {
            (Some(received_by), Some(now)) => received_by != now,
            _ => false,
        };
        if earlier_runner && facts.adopted.is_empty() && facts.wait.is_none() {
            let restarted = facts
                .restart_ms
                .map(|at| format!("; the runner restarted at {}", clock(at)))
                .unwrap_or_default();
            return ended(
                "lost",
                format!(
                    "no end recorded: the runner that received it ended with its call still open{restarted}"
                ),
                facts.restart_ms,
                "run_batch_requests, runner_events",
            );
        }
    }
    if let Some(wait) = &facts.wait {
        return match wait.state.as_str() {
            "resolved" => {
                let turn = wait
                    .successor_turn_id
                    .as_deref()
                    .map(|turn| format!(" as turn `{turn}`"))
                    .unwrap_or_default();
                ended(
                    "delivered",
                    format!(
                        "its result resumed job `{}`{turn} at {}",
                        wait.job_id,
                        clock_of(wait.resolved_ms)
                    ),
                    wait.resolved_ms,
                    "agent_waits",
                )
            }
            "cancelled" if context.in_flight => {
                still_running("still running on this runner".to_string(), "agent_waits")
            }
            "cancelled" => EndFact {
                quoted: wait.resolution_json.clone(),
                ..ended(
                    "lost",
                    format!(
                        "its wait was cancelled at {}, and no detached row carried the batch on",
                        clock_of(wait.resolved_ms)
                    ),
                    wait.resolved_ms,
                    "agent_waits",
                )
            },
            _ => ended(
                "open",
                format!("parked: its call's wait `{}` is {}", wait.id, wait.state),
                None,
                "agent_waits",
            ),
        };
    }
    if let Some(adopted) = facts.adopted.iter().find(|row| row.state == "delivered") {
        return ended(
            "delivered",
            format!(
                "its result from {} reached its job after a restart, at {}",
                adopted.machine,
                clock_of(adopted.settled_ms)
            ),
            adopted.settled_ms,
            "run_batch_adopted_outcomes",
        );
    }
    if let Some(adopted) = facts.adopted.first() {
        return ended(
            "open",
            format!(
                "its result from {} was adopted after a restart and is still `{}`",
                adopted.machine, adopted.state
            ),
            None,
            "run_batch_adopted_outcomes",
        );
    }
    if let Some(lane) = facts.lanes.last() {
        let on = &lane.machine;
        let observation = lane
            .observation_handle
            .as_deref()
            .or(lane.observation_id.as_deref())
            .map(|observation| format!(" `{observation}`"))
            .unwrap_or_default();
        let source = "check_lane_adoptions";
        return match lane.state.as_str() {
            "recorded" => ended(
                "delivered",
                format!(
                    "check lane on {on} recorded observation{observation} at {}",
                    clock_of(lane.stopped_at_ms)
                ),
                lane.stopped_at_ms,
                source,
            ),
            "lost" => EndFact {
                quoted: lane.evidence.clone(),
                ..ended(
                    "lost",
                    format!(
                        "check lane on {on} was lost at {}",
                        clock_of(lane.stopped_at_ms)
                    ),
                    lane.stopped_at_ms,
                    source,
                )
            },
            "cancelled" => ended(
                "cancelled",
                format!(
                    "check lane on {on} was cancelled at {}",
                    clock_of(lane.stopped_at_ms)
                ),
                lane.stopped_at_ms,
                source,
            ),
            state => still_running(
                format!(
                    "check lane on {on}, granted at {}, is {}",
                    clock(lane.granted_at_ms),
                    state.replace('_', " ")
                ),
                source,
            ),
        };
    }
    if context.in_flight {
        return still_running("in flight on this runner".to_string(), "fleet");
    }
    if ledger.is_some() {
        return ended(
            "open",
            "no end recorded yet".to_string(),
            None,
            "run_batch_requests",
        );
    }
    ended(
        "unknown",
        "no end is recorded: no ledger, detached, wait or adopted row names it, so it predates the request ledger or never became a run batch".to_string(),
        None,
        "none",
    )
}

fn unique(values: impl Iterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values.filter(|value| seen.insert(value.clone())).collect()
}

fn build(
    project: &str,
    request_id: &str,
    matched_as: Vec<&'static str>,
    check_request: Option<CheckRequestRef>,
    facts: Facts,
    context: &Context,
) -> RequestLifecycle {
    let end = derive_end(&facts, context);
    let machine_of = |executor_id: &String| {
        context
            .machines
            .get(executor_id)
            .cloned()
            .unwrap_or_else(|| executor_id.clone())
    };
    let mut placement = facts.journal;
    for row in &mut placement {
        row.machine = row.executor_id.as_ref().map(machine_of);
    }
    let unplaced_ms = placement.first().map(|first| {
        placement
            .iter()
            .find(|row| row.state == "claimed")
            .map_or(context.now_ms, |claim| claim.at_ms)
            - first.at_ms
    });
    let mut lanes = facts.lanes;
    for lane in &mut lanes {
        lane.observation_uri = lane.observation_handle.as_ref().map(|handle| {
            format!(
                "cairn://p/{}/check-observations/{handle}",
                cairn_uri::canonical_project(project)
            )
        });
    }
    let ledger = facts.ledger;
    let wait = facts.wait;
    let detached = facts.detached.map(DetachedRow::from);
    let ids = Ids {
        request_id: request_id.to_owned(),
        tool_use_id: ledger
            .as_ref()
            .and_then(|row| row.tool_use_id.clone())
            .or_else(|| wait.as_ref().map(|wait| wait.tool_use_id.clone())),
        wait_id: wait.as_ref().map(|wait| wait.id.clone()),
        run_id: ledger
            .as_ref()
            .map(|row| row.run_id.clone())
            .or_else(|| detached.as_ref().map(|row| row.run_id.clone()))
            .or_else(|| wait.as_ref().map(|wait| wait.run_id.clone())),
        job_id: ledger
            .as_ref()
            .map(|row| row.job_id.clone())
            .or_else(|| detached.as_ref().map(|row| row.job_id.clone()))
            .or_else(|| wait.as_ref().map(|wait| wait.job_id.clone())),
        node: context.node.clone(),
        session_id: ledger
            .as_ref()
            .and_then(|row| row.session_id.clone())
            .or_else(|| wait.as_ref().map(|wait| wait.session_id.clone())),
        turn_id: ledger
            .as_ref()
            .and_then(|row| row.turn_id.clone())
            .or_else(|| wait.as_ref().map(|wait| wait.predecessor_turn_id.clone())),
        entry_ids: unique(placement.iter().map(|row| row.entry_id.clone())),
        attempt_ids: unique(placement.iter().map(|row| row.attempt_id.clone())),
        check_request,
    };
    let execution = match &ledger {
        Some(row) if row.executor_id.is_some() => Some(Execution {
            machine: row.executor_id.as_ref().map(|id| {
                context
                    .machines
                    .get(id)
                    .cloned()
                    .or_else(|| row.machine.clone())
                    .unwrap_or_else(|| id.clone())
            }),
            attempt_id: row.attempt_id.clone(),
            cell_id: row.cell_id.clone(),
            pid: row.pid,
            generation: row.generation,
            started_ms: row.started_ms,
            peak_cores: row.peak_cpu_milli.map(|milli| milli as f64 / 1000.0),
            peak_resident_bytes: row.peak_resident_bytes,
            last_progress_ms: row.last_progress_ms,
            output_bytes: row.output_bytes,
            source: "run_batch_requests",
        }),
        _ => placement
            .iter()
            .rev()
            .find(|row| row.state == "claimed")
            .map(|claim| Execution {
                machine: claim.machine.clone(),
                attempt_id: Some(claim.attempt_id.clone()),
                cell_id: None,
                pid: None,
                generation: claim.generation,
                started_ms: Some(claim.at_ms),
                peak_cores: None,
                peak_resident_bytes: None,
                last_progress_ms: None,
                output_bytes: ledger.as_ref().and_then(|row| row.output_bytes),
                source: "execution_queue",
            }),
    };
    let mut actions = Vec::new();
    if end.kind == "still_running" {
        if let Some(node) = &context.node {
            actions.push(format!(
                "Where it is held, and its cancel: `{node}/batches/{}`",
                detached::batch_segment(ledger.as_ref().and_then(|row| row.number), request_id)
            ));
        }
    }
    if let Some(check) = &ids.check_request {
        actions.push(format!(
            "Its check request: `{}`",
            crate::execution::checks::requests::request_uri(project, check.number)
        ));
    }
    RequestLifecycle {
        request_id: request_id.to_owned(),
        matched_as,
        ids,
        end,
        ledger,
        detached,
        wait,
        adopted: facts.adopted,
        placement,
        unplaced_ms,
        lanes,
        execution,
        live: context.live.clone(),
        progress: context.progress.clone(),
        run: facts.run,
        actions,
    }
}

// --- Rendering ------------------------------------------------------------

fn machine_link(name: &str) -> String {
    format!("[{name}](cairn://executors/{name})")
}

fn push_id(lines: &mut Vec<String>, label: &str, value: Option<&str>) {
    if let Some(value) = value {
        lines.push(format!("- {label}: `{value}`"));
    }
}

fn heading(lines: &mut Vec<String>, title: &str) {
    lines.push(String::new());
    lines.push(format!("## {title}"));
    lines.push(String::new());
}

fn journal_line(row: &JournalRow) -> String {
    let on = row
        .machine
        .as_deref()
        .map(|machine| {
            let generation = row
                .generation
                .map(|generation| format!(" (link generation {generation})"))
                .unwrap_or_default();
            format!(" {}{generation}", machine_link(machine))
        })
        .unwrap_or_default();
    let held = match (&row.decline_reason, &row.detail) {
        (Some(reason), Some(detail)) => format!("; held ({reason}): {detail}"),
        (Some(reason), None) => format!("; held ({reason})"),
        (None, Some(detail)) => format!("; {detail}"),
        (None, None) => String::new(),
    };
    let verb = match row.state.as_str() {
        "offered" => "offered to",
        "claimed" => "claimed by",
        other => other,
    };
    format!(
        "- {} {verb}{on} (entry `{}`, attempt `{}`){held}",
        clock(row.at_ms),
        row.entry_id,
        row.attempt_id
    )
}

fn cores(value: Option<f64>) -> String {
    value.map_or_else(
        || "unmeasured".to_string(),
        |cores| format!("{cores:.1} cores"),
    )
}

fn render(lifecycle: &RequestLifecycle) -> String {
    let mut lines = vec![
        format!("# Request `{}`", lifecycle.request_id),
        String::new(),
        format!("Matched as: {}.", lifecycle.matched_as.join(", ")),
    ];
    heading(&mut lines, "End");
    lines.push(format!(
        "**{}**: {} (`{}`)",
        lifecycle.end.kind.replace('_', " "),
        lifecycle.end.sentence,
        lifecycle.end.source
    ));
    if let Some(quoted) = &lifecycle.end.quoted {
        lines.push(String::new());
        lines.push(format!("> {}", quoted.replace('\n', "\n> ")));
    }

    let ids = &lifecycle.ids;
    heading(&mut lines, "Ids");
    push_id(&mut lines, "Request", Some(&ids.request_id));
    push_id(&mut lines, "Tool call", ids.tool_use_id.as_deref());
    push_id(&mut lines, "Wait", ids.wait_id.as_deref());
    push_id(&mut lines, "Run", ids.run_id.as_deref());
    push_id(&mut lines, "Job", ids.job_id.as_deref());
    if let Some(node) = &ids.node {
        lines.push(format!("- Node: {node}"));
    }
    push_id(&mut lines, "Session", ids.session_id.as_deref());
    push_id(&mut lines, "Turn", ids.turn_id.as_deref());
    if !ids.entry_ids.is_empty() {
        lines.push(format!(
            "- Placement entries: `{}`",
            ids.entry_ids.join("`, `")
        ));
    }
    if !ids.attempt_ids.is_empty() {
        lines.push(format!("- Attempts: `{}`", ids.attempt_ids.join("`, `")));
    }
    if let Some(check) = &ids.check_request {
        lines.push(format!(
            "- Check request: {} ({}, {})",
            check.number, check.suite, check.state
        ));
    }

    match (&lifecycle.ledger, &lifecycle.wait, &lifecycle.detached) {
        (Some(row), _, _) => {
            heading(&mut lines, "Call (`run_batch_requests`)");
            lines.push(format!("- Received: {}", clock(row.received_ms)));
            if let Some(incarnation) = &row.incarnation {
                lines.push(format!("- Received by runner: `{incarnation}`"));
            }
            heading(&mut lines, "Batch (`run_batch_requests`)");
            if let Some(label) = &row.label {
                lines.push(format!("- Label: {label}"));
            }
            match row.item_count {
                Some(items) => lines.push(format!(
                    "- Items: {items}, {}",
                    if row.sequential == Some(true) {
                        "in order"
                    } else {
                        "in parallel"
                    }
                )),
                None => lines
                    .push("- Not recorded: the call ended before its items resolved".to_string()),
            }
            if row.item_count.is_some() {
                lines.push(format!(
                    "- Declared timeout: {}",
                    row.declared_timeout_ms
                        .map_or_else(|| "none (an item declares no bound)".to_string(), span)
                ));
            }
            lines.push(format!("- Commits: {}", row.commits));
            if let Some(branch) = &row.verdict_branch {
                lines.push(format!("- Verdict-only against: `{branch}`"));
            }
            if let Some(selector) = &row.selector_json {
                lines.push(format!("- Selector: `{selector}`"));
            }
            if let Some(cwd) = &row.cwd {
                lines.push(format!("- Cwd: `{cwd}`"));
            }
        }
        (None, Some(wait), _) => {
            heading(&mut lines, "Call (`agent_waits`)");
            lines.push(format!(
                "- Parked at {} under wait `{}` ({})",
                clock(wait.created_ms),
                wait.id,
                wait.state
            ));
            lines.push("- Its receipt predates the request ledger: when its call arrived and what the batch was were never recorded.".to_string());
        }
        (None, None, Some(detached)) => {
            heading(&mut lines, "Call (`run_batch_detached`)");
            lines.push(format!(
                "- Detached at {}: {}",
                clock(detached.created_ms),
                detached.reason
            ));
            if let Some(label) = &detached.label {
                lines.push(format!("- Label: {label}"));
            }
            lines.push(format!("- Commits: {}", detached.commits));
            lines.push(
                "- Its receipt predates the request ledger: when its call arrived was never recorded."
                    .to_string(),
            );
        }
        (None, None, None) => {}
    }

    if !lifecycle.placement.is_empty() {
        heading(&mut lines, "Placement (`execution_queue`)");
        lines.extend(lifecycle.placement.iter().map(journal_line));
        if let Some(unplaced) = lifecycle.unplaced_ms {
            let claimed = lifecycle.placement.iter().any(|row| row.state == "claimed");
            lines.push(format!(
                "- Waited {} {}",
                span(unplaced),
                if claimed {
                    "before a machine claimed it"
                } else {
                    "so far without a claim"
                }
            ));
        }
    }

    if !lifecycle.lanes.is_empty() {
        heading(&mut lines, "Check lane (`check_lane_adoptions`)");
        for lane in &lifecycle.lanes {
            let stopped = lane
                .stopped_at_ms
                .map(|at| format!(", stopped {}", clock(at)))
                .unwrap_or_default();
            lines.push(format!(
                "- Attempt `{}` on {}: {} for {} `{}`, granted {}{stopped}: {}",
                lane.attempt_id,
                machine_link(&lane.machine),
                lane.check_names,
                lane.consumer,
                lane.consumer_ref,
                clock(lane.granted_at_ms),
                lane.state
            ));
            if let Some(evidence) = &lane.evidence {
                lines.push(format!("  - Evidence: {evidence}"));
            }
            match (
                &lane.observation_id,
                &lane.observation_handle,
                &lane.observation_uri,
            ) {
                (Some(_), Some(number), Some(uri)) => {
                    lines.push(format!("  - Observation {number}: {uri}"))
                }
                (Some(id), _, _) => lines.push(format!("  - Observation: `{id}` (no number)")),
                _ => {}
            }
            for waiter in &lane.waiters {
                lines.push(format!("  - Waiter: {waiter}"));
            }
        }
    }

    if let Some(execution) = &lifecycle.execution {
        heading(&mut lines, &format!("Execution (`{}`)", execution.source));
        if let Some(machine) = &execution.machine {
            lines.push(format!("- Machine: {}", machine_link(machine)));
        }
        push_id(&mut lines, "Attempt", execution.attempt_id.as_deref());
        push_id(&mut lines, "Cell", execution.cell_id.as_deref());
        if let Some(pid) = execution.pid {
            lines.push(format!("- Root pid: {pid}"));
        }
        if let Some(generation) = execution.generation {
            lines.push(format!("- Link generation: {generation}"));
        }
        if let Some(started) = execution.started_ms {
            lines.push(format!("- Started: {}", clock(started)));
        }
        if execution.source == "run_batch_requests" {
            lines.push(format!(
                "- Peak CPU: {}; peak resident memory: {}",
                cores(execution.peak_cores),
                execution
                    .peak_resident_bytes
                    .map_or_else(|| "unmeasured".to_string(), bytes)
            ));
            if let Some(progress) = execution.last_progress_ms {
                lines.push(format!("- Last measured: {}", clock(progress)));
            }
        } else {
            lines.push(
                "- Not sampled while it ran: no pid, cell or measured use was recorded."
                    .to_string(),
            );
        }
        if let Some(output) = execution.output_bytes {
            lines.push(format!("- Answer: {}", bytes(output)));
        }
    }
    if let Some(live) = &lifecycle.live {
        lines.push(String::new());
        lines.push(format!(
            "Live on {}, sampled {} ago: {} CPU, {} resident. A live reading is never an end.",
            machine_link(&live.machine),
            live.sampled_ago_ms
                .map_or_else(|| "never".to_string(), span),
            cores(live.cores),
            live.resident_bytes
                .map_or_else(|| "unread".to_string(), bytes)
        ));
    }

    if let Some(progress) = &lifecycle.progress {
        lines.push(format!("Progress: {progress}."));
    }

    if let Some(run) = &lifecycle.run {
        heading(&mut lines, "Run (`runs`)");
        lines.push(format!(
            "- {}",
            run_exit(run).unwrap_or_else(|| format!("run `{}` {}", run.id, run.status))
        ));
    }

    if let Some(row) = &lifecycle.detached {
        heading(&mut lines, "Delivery (`run_batch_detached`)");
        let inline = row.reason == crate::mcp::handlers::run::detached::INLINE_CAPTURE;
        lines.push(if inline {
            "- The call owns its inline answer.".to_string()
        } else {
            format!("- Detached because: {}", row.reason)
        });
        match row.state.as_str() {
            "parked" | "running" => {
                if !row.commits {
                    lines.push("- Commits: none; it has no commit_msg, so it cannot commit and does not hold the branch.".to_string());
                } else if row.holds_branch {
                    lines.push("- Commits: it carries a commit_msg and holds its captured base until settlement, including while queued.".to_string());
                }
                lines.push(if inline {
                    "- Its result returns on the call while it remains connected. Do not re-run it."
                        .to_string()
                } else {
                    "- Its result reaches this job as a notice when it settles. Do not re-run it."
                        .to_string()
                });
            }
            "settled" => {
                lines.push(format!(
                    "- Verdict: {}",
                    row.verdict.as_deref().unwrap_or("finished")
                ));
                lines.push(format!("- Commits: {}", row.commit));
                if let Some(snapshot) = &row.snapshot {
                    lines.push(snapshot.clone());
                }
                heading(&mut lines, "Output");
                lines.push(row.output.as_deref().unwrap_or("(no output)").to_string());
            }
            "lost" => {
                lines.push(if row.commits {
                    "Its outcome is unknown. It carried a commit_msg; read `cairn:@/vcs/log?limit=5` before re-running it.".to_string()
                } else {
                    "Its outcome is unknown. It had no commit_msg, so it committed nothing.".to_string()
                });
            }
            _ => {}
        }
        if let Some(late) = &row.late_report {
            lines.push(late.clone());
        }
    }
    if let Some(wait) = &lifecycle.wait {
        if let Some(note) = &wait.late_cancel {
            lines.push(format!("Cancel: {note}."));
        }
        if let Some(resolution) = &wait.resolution_json {
            heading(&mut lines, "Wait result (`agent_waits`)");
            lines.push(resolution.clone());
        }
    }

    if !lifecycle.actions.is_empty() {
        heading(&mut lines, "Actions");
        lines.extend(lifecycle.actions.iter().map(|action| format!("- {action}")));
    }
    lines.join("\n")
}

fn unknown(id: &str, json: bool) -> String {
    if json {
        return serde_json::json!({
            "id": id,
            "matched": false,
            "acceptedIdKinds": ID_KINDS
                .iter()
                .map(|(kind, shape)| serde_json::json!({"kind": kind, "shape": shape}))
                .collect::<Vec<_>>(),
        })
        .to_string();
    }
    let mut lines = vec![
        format!("`{id}` names no request in this project."),
        String::new(),
        "It accepts any of these ids:".to_string(),
        String::new(),
    ];
    lines.extend(
        ID_KINDS
            .iter()
            .map(|(kind, shape)| format!("- {kind}: {shape}")),
    );
    lines.push(String::new());
    lines.push("A batch received before the request ledger existed resolves only through the rows it left: its wait, its detached row, or its placement journal.".to_string());
    lines.join("\n")
}

fn check_request_only(project: &str, check: &CheckRequestRef, json: bool) -> String {
    let uri = crate::execution::checks::requests::request_uri(project, check.number);
    if json {
        return serde_json::json!({ "checkRequest": check, "uri": uri }).to_string();
    }
    format!(
        "This is check request {} ({}, {}), which no fleet request carries yet. Its own page says where it stands: `{uri}`",
        check.number, check.suite, check.state
    )
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Summary {
    request_id: String,
    label: Option<String>,
    received_ms: Option<i64>,
    end: Option<String>,
}

async fn summaries(db: &LocalDb, request_ids: &[String]) -> Result<Vec<Summary>, String> {
    let mut out = Vec::with_capacity(request_ids.len());
    for request_id in request_ids {
        let row = ledger_row(db, request_id).await?;
        out.push(Summary {
            request_id: request_id.clone(),
            label: row.as_ref().and_then(|row| row.label.clone()),
            received_ms: row.as_ref().map(|row| row.received_ms),
            end: row.and_then(|row| row.end),
        });
    }
    Ok(out)
}

fn disambiguation(
    project: &str,
    id: &str,
    resolution: &Resolution,
    rows: &[Summary],
    run: Option<&RunRow>,
    json: bool,
) -> String {
    if json {
        return serde_json::json!({
            "id": id,
            "run": run,
            "requests": rows
                .iter()
                .map(|row| serde_json::json!({
                    "uri": lifecycle_uri(project, &row.request_id),
                    "matchedAs": resolution.kinds_for(&row.request_id),
                    "summary": row,
                }))
                .collect::<Vec<_>>(),
        })
        .to_string();
    }
    let mut lines = vec![
        format!("`{id}` names {} requests.", rows.len()),
        String::new(),
    ];
    if let Some(run) = run {
        lines.push(format!(
            "Run: {}",
            run_exit(run).unwrap_or_else(|| format!("`{}` {}", run.id, run.status))
        ));
        lines.push(String::new());
    }
    for row in rows {
        lines.push(format!(
            "- {} {}, received {}, {} (matched as {})",
            lifecycle_uri(project, &row.request_id),
            row.label.as_deref().unwrap_or("(no label recorded)"),
            row.received_ms
                .map_or_else(|| "before the ledger".to_string(), clock),
            row.end.as_deref().unwrap_or("no end recorded"),
            resolution.kinds_for(&row.request_id).join(", ")
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
#[path = "request_lifecycle_tests.rs"]
mod tests;
