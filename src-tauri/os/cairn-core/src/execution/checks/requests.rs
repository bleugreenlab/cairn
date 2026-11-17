//! Check requests: one durable row for every requested suite verdict.
//!
//! A check request is the same thing whoever asks: one suite on one revision,
//! optionally pinned to a machine or platform, with a requester. Its row in
//! `check_requests` is written when the request is accepted, before anything
//! is resolved, prepared or placed, and it is the only record of the request's
//! state: what it waits behind while queued, the machine running it, and the
//! answer it settled with. Nothing about a request lives only in memory.
//!
//! One driver moves a row through its life: `accepted` (resolving and
//! preparing its revision), `queued` (submitted to the fleet), `running` (a
//! machine claimed it), then a terminal state written exactly once under the
//! `state IN (open)` guard. A runner restart re-drives every row that was never
//! granted under the same id, and hands every granted one to the lane adoption
//! path; a granted row settles `lost` only on death evidence the runner holds
//! (its machine reported the item gone, or the machine was removed), never on a
//! restart or a timer alone.
//!
//! An agent files a request with `write cairn:~/checks` (or `cairn check run`,
//! which is that write) and waits for it with `run waitFor kind:"checks"` on
//! the request's URI. Nothing is placed, charged or held on the requester's
//! behalf except the check itself.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use crate::execution::check_placement::CheckPlacementOverride;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::executor_protocol::{CheckSelectorSource, ExecutorSelector};

use super::{ManualCheckError, ManualCheckProgressUpdate, ManualCheckRevision};

/// The states a request can still leave.
pub(crate) const OPEN_STATES: &str = "('accepted','queued','granted','running')";

const ROW_COLUMNS: &str = "id,project_id,suite,revision_spec,subject_job_id,selector_json,retry,consumer,
    requester_ref,requester_job_id,state,commit_sha,request_id,executor,diagnostic,held_by,held_at_ms,
    held_count,pinned_executor,waiting_to_learn,observation_id,outcome_json,evidence,cancel_requested_by,
    accepted_at_ms,queued_at_ms,state_since_ms,updated_ms,publication_id";

/// This runner's identity on the rows it drives, the one the team admission
/// authority leases ordinary work to.
pub(crate) fn this_runner() -> String {
    std::env::var("CAIRN_RUNNER_ID").unwrap_or_else(|_| "local-runner".into())
}

/// One `check_requests` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckRequestRow {
    pub id: String,
    pub project_id: String,
    pub suite: String,
    pub revision_spec: Option<String>,
    pub subject_job_id: Option<String>,
    pub selector_json: Option<String>,
    pub retry: bool,
    pub consumer: String,
    pub requester_ref: String,
    pub requester_job_id: Option<String>,
    pub state: String,
    pub commit_sha: Option<String>,
    pub request_id: Option<String>,
    pub executor: Option<String>,
    pub diagnostic: Option<String>,
    pub held_by: Option<String>,
    pub held_at_ms: Option<i64>,
    pub held_count: i64,
    pub pinned_executor: Option<String>,
    pub waiting_to_learn: Option<String>,
    pub observation_id: Option<String>,
    pub outcome_json: Option<String>,
    pub evidence: Option<String>,
    pub cancel_requested_by: Option<String>,
    pub accepted_at_ms: i64,
    pub queued_at_ms: Option<i64>,
    pub state_since_ms: i64,
    pub updated_ms: i64,
    pub publication_id: Option<String>,
}

impl CheckRequestRow {
    fn from_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<Self> {
        Ok(Self {
            id: row.text(0)?,
            project_id: row.text(1)?,
            suite: row.text(2)?,
            revision_spec: row.opt_text(3)?,
            subject_job_id: row.opt_text(4)?,
            selector_json: row.opt_text(5)?,
            retry: row.i64(6)? != 0,
            consumer: row.text(7)?,
            requester_ref: row.text(8)?,
            requester_job_id: row.opt_text(9)?,
            state: row.text(10)?,
            commit_sha: row.opt_text(11)?,
            request_id: row.opt_text(12)?,
            executor: row.opt_text(13)?,
            diagnostic: row.opt_text(14)?,
            held_by: row.opt_text(15)?,
            held_at_ms: row.opt_i64(16)?,
            held_count: row.i64(17)?,
            pinned_executor: row.opt_text(18)?,
            waiting_to_learn: row.opt_text(19)?,
            observation_id: row.opt_text(20)?,
            outcome_json: row.opt_text(21)?,
            evidence: row.opt_text(22)?,
            cancel_requested_by: row.opt_text(23)?,
            accepted_at_ms: row.i64(24)?,
            queued_at_ms: row.opt_i64(25)?,
            state_since_ms: row.i64(26)?,
            updated_ms: row.i64(27)?,
            publication_id: row.opt_text(28)?,
        })
    }

    pub(crate) fn is_open(&self) -> bool {
        matches!(
            self.state.as_str(),
            "accepted" | "queued" | "granted" | "running"
        )
    }

    /// The placement the request asked for, if any.
    pub(crate) fn placement(&self) -> Option<CheckPlacementOverride> {
        let stored: StoredPlacement = serde_json::from_str(self.selector_json.as_deref()?).ok()?;
        CheckPlacementOverride::new(stored.selector, stored.source).ok()
    }
}

#[derive(Serialize, Deserialize)]
struct StoredPlacement {
    selector: ExecutorSelector,
    source: CheckSelectorSource,
}

fn stored_placement(placement: &CheckPlacementOverride) -> String {
    serde_json::to_string(&StoredPlacement {
        selector: placement.selector().clone(),
        source: placement.source(),
    })
    .expect("a placement selector serializes")
}

/// A check request's answer as its readers are shown it: where a pending
/// request stands, or the answer it settled with. The serialized form is the
/// `observation` every client of `cairn check` has read since the manual
/// producer existed, with `cancelled` and `lost` added.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "state"
)]
pub(crate) enum CheckRequestAnswer {
    Queued {
        diagnostic: String,
    },
    Running {
        diagnostic: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        executor: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        since_unix_ms: Option<u64>,
        /// What a lane adopted across a restart is waiting to learn before it
        /// can settle, when nothing has been heard from its machine since.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        waiting_to_learn: Option<String>,
    },
    Parked {
        diagnostic: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        executor: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        since_unix_ms: Option<u64>,
        /// The machine's latest refusal, verbatim, and how many it has made.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        held_by: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        held_since_unix_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        refusals: Option<u64>,
    },
    Complete {
        result: Box<crate::execution::checks::ManualConfiguredCheckResult>,
    },
    ConflictsWithBase {
        branch: String,
        head: String,
        base_branch: String,
        base_commit: String,
        paths: Vec<String>,
    },
    /// The branch conflicts with its base only in declared regenerable
    /// paths: its owner's replay takes the branch's bytes and regenerates
    /// them, so no verdict is owed at this head.
    RegenerationPending {
        branch: String,
        head: String,
        base_branch: String,
        base_commit: String,
        paths: Vec<String>,
    },
    Failed {
        error: String,
    },
    /// Withdrawn or stopped by someone with the authority to: never a verdict.
    Cancelled {
        reason: String,
    },
    /// The lane it was running on is gone, on the evidence named.
    Lost {
        evidence: String,
    },
}

impl CheckRequestAnswer {
    fn settled_state(&self) -> &'static str {
        match self {
            Self::Complete { .. } => "recorded",
            Self::Cancelled { .. } => "cancelled",
            Self::Lost { .. } => "lost",
            Self::Queued { .. } | Self::Running { .. } | Self::Parked { .. } => {
                unreachable!("a pending answer is never a settlement")
            }
            Self::ConflictsWithBase { .. }
            | Self::RegenerationPending { .. }
            | Self::Failed { .. } => "failed",
        }
    }

    fn evidence(&self) -> Option<String> {
        match self {
            Self::Failed { error } => Some(error.clone()),
            Self::Cancelled { reason } => Some(reason.clone()),
            Self::Lost { evidence } => Some(evidence.clone()),
            Self::ConflictsWithBase { branch, .. } => {
                Some(format!("{branch} conflicts with its base; no verdict"))
            }
            Self::RegenerationPending { branch, .. } => Some(format!(
                "{branch} conflicts with its base only in regenerable paths; no verdict owed"
            )),
            _ => None,
        }
    }
}

fn answer_from_error(error: ManualCheckError) -> CheckRequestAnswer {
    use crate::mcp::handlers::branch::RevisionCaptureError;
    use crate::orchestrator::base_advance::preparation::PreparationRefusal;
    if let ManualCheckError::Capture(RevisionCaptureError::Preparation { refusal, .. }) = &error {
        if let PreparationRefusal::Conflict(conflict) = &**refusal {
            return CheckRequestAnswer::ConflictsWithBase {
                branch: conflict.branch.clone(),
                head: conflict.head.clone(),
                base_branch: conflict.base_branch.clone(),
                base_commit: conflict.base_commit.clone(),
                paths: conflict.paths.clone(),
            };
        }
        if let PreparationRefusal::RegenerationPending(conflict) = &**refusal {
            return CheckRequestAnswer::RegenerationPending {
                branch: conflict.branch.clone(),
                head: conflict.head.clone(),
                base_branch: conflict.base_branch.clone(),
                base_commit: conflict.base_commit.clone(),
                paths: conflict.paths.clone(),
            };
        }
    }
    CheckRequestAnswer::Failed {
        error: error.to_string(),
    }
}

/// What a requester asks for.
#[derive(Debug, Clone)]
pub(crate) struct CheckRequestSpec {
    pub project_id: String,
    pub project_key: String,
    pub suite: String,
    /// What `--branch` named, or `None` for the subject job's own head.
    pub revision: Option<String>,
    /// The job whose head (or project) the request resolves against.
    pub subject_job_id: String,
    pub placement: Option<CheckPlacementOverride>,
    pub retry: bool,
    /// The authenticated principal that asked: a node URI or `operator`.
    pub requester_ref: String,
    pub requester_job_id: Option<String>,
}

/// A filed request, as its requester is answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FiledRequest {
    pub id: String,
    pub uri: String,
    pub suite: String,
    /// An identical open request already existed and this is it.
    pub joined: bool,
}

/// The canonical address of a check request.
pub(crate) fn request_uri(project_key: &str, id: &str) -> String {
    format!("cairn://p/{project_key}/check-requests/{id}")
}

/// The exact `run` item that waits for a request to settle with its verdict.
pub(crate) fn wait_line(uri: &str) -> String {
    format!("run({{commands:[{{waitFor:{{kind:\"checks\",ref:\"{uri}\",on:\"verdict\"}}}}]}})")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The database a project's requests live in.
pub(crate) async fn request_db(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<Arc<LocalDb>, String> {
    crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn load_request(
    db: &LocalDb,
    project_id: &str,
    id: &str,
) -> Result<Option<CheckRequestRow>, String> {
    db.query_opt(
        format!("SELECT {ROW_COLUMNS} FROM check_requests WHERE project_id=?1 AND id=?2"),
        (project_id.to_string(), id.to_string()),
        CheckRequestRow::from_row,
    )
    .await
    .map_err(|error| format!("read check request {id}: {error}"))
}

async fn load_request_by_id(db: &LocalDb, id: &str) -> Result<Option<CheckRequestRow>, String> {
    db.query_opt(
        format!("SELECT {ROW_COLUMNS} FROM check_requests WHERE id=?1"),
        (id.to_string(),),
        CheckRequestRow::from_row,
    )
    .await
    .map_err(|error| format!("read check request {id}: {error}"))
}

/// The requests a node filed, open first, then the most recently settled,
/// from whichever database (private or a team replica) holds them.
pub(crate) async fn requests_of_requester(
    orch: &Orchestrator,
    requester_job_id: &str,
    settled_limit: usize,
) -> Result<Vec<CheckRequestRow>, String> {
    let mut rows = Vec::new();
    for db in orch.db.all_dbs().await {
        rows.extend(requests_of_requester_in(&db, requester_job_id, settled_limit).await?);
    }
    Ok(rows)
}

async fn requests_of_requester_in(
    db: &LocalDb,
    requester_job_id: &str,
    settled_limit: usize,
) -> Result<Vec<CheckRequestRow>, String> {
    let mut rows = db
        .query_all(
            format!(
                "SELECT {ROW_COLUMNS} FROM check_requests
                  WHERE requester_job_id=?1 AND state IN {OPEN_STATES}
                  ORDER BY accepted_at_ms"
            ),
            (requester_job_id.to_string(),),
            CheckRequestRow::from_row,
        )
        .await
        .map_err(|error| error.to_string())?;
    rows.extend(
        db.query_all(
            format!(
                "SELECT {ROW_COLUMNS} FROM check_requests
                  WHERE requester_job_id=?1 AND state NOT IN {OPEN_STATES}
                  ORDER BY updated_ms DESC LIMIT ?2"
            ),
            (requester_job_id.to_string(), settled_limit as i64),
            CheckRequestRow::from_row,
        )
        .await
        .map_err(|error| error.to_string())?,
    );
    Ok(rows)
}

/// Open requests this runner drives that are pinned to a machine, oldest
/// first, each with the database it lives in: what waits on the runner for
/// that machine, which the machine's own queue never shows.
pub(crate) async fn requests_pinned_to(
    orch: &Orchestrator,
    executor_name: &str,
) -> Result<Vec<(Arc<LocalDb>, CheckRequestRow)>, String> {
    let mut pinned = Vec::new();
    for db in orch.db.all_dbs().await {
        let rows = db
            .query_all(
                format!(
                    "SELECT {ROW_COLUMNS} FROM check_requests
                      WHERE pinned_executor=?1 AND state IN ('accepted','queued')
                        AND COALESCE(driven_by,?2)=?2
                      ORDER BY accepted_at_ms"
                ),
                (executor_name.to_string(), this_runner()),
                CheckRequestRow::from_row,
            )
            .await
            .map_err(|error| error.to_string())?;
        pinned.extend(rows.into_iter().map(|row| (db.clone(), row)));
    }
    pinned.sort_by_key(|(_, row)| row.accepted_at_ms);
    Ok(pinned)
}

// --- filing ----------------------------------------------------------------

/// File one check request: the single seam every requester calls.
///
/// The revision is fixed first ([`accepted_revision`]), so a name that
/// resolves to nothing is refused before any row exists, and the commit an
/// own-head or exact request judges is on the row from the start. A named
/// managed branch is prepared once, by the driver. An open request from the
/// same requester for the same suite, revision, placement and commit is
/// returned instead of a second one.
pub(crate) async fn file_check_request(
    orch: &Orchestrator,
    spec: CheckRequestSpec,
) -> Result<FiledRequest, String> {
    let suite = spec.suite.trim().to_string();
    if suite.is_empty() {
        return Err("configured suite name must not be empty".into());
    }
    let revision = accepted_revision(orch, &spec.subject_job_id, spec.revision.as_deref()).await?;
    let coordinate = match &revision {
        ManualCheckRevision::Captured(captured) => Coordinate::Commit(captured.commit_id.clone()),
        _ => Coordinate::Unresolved,
    };
    let db = request_db(orch, &spec.project_id).await?;
    let accepted = accept(&db, &spec, &suite, &coordinate).await?;
    if !accepted.joined {
        spawn_driver(orch, db, accepted.id.clone(), Some(revision));
    }
    Ok(FiledRequest {
        uri: request_uri(&spec.project_key, &accepted.id),
        id: accepted.id,
        suite,
        joined: accepted.joined,
    })
}

/// The revision a request judges, fixed when it is accepted: what `--branch`
/// names, or for the subject job's own head the commit that head is at now,
/// captured exactly. A request on its own head is about the head it was filed
/// at, so neither its first driver nor a re-drive after a restart judges a
/// head that moved on since.
pub(crate) async fn accepted_revision(
    orch: &Orchestrator,
    subject_job_id: &str,
    revision: Option<&str>,
) -> Result<ManualCheckRevision, String> {
    match revision {
        Some(branch) => super::classify_manual_check_revision(orch, subject_job_id, branch).await,
        None => match crate::execution::cache::resolve_job_logical_head(orch, subject_job_id).await
        {
            Ok(head) => super::classify_manual_check_revision(orch, subject_job_id, &head).await,
            // A head that cannot be read here (a thread session) is refused by
            // the driver, in the requester's own words.
            Err(_) => Ok(ManualCheckRevision::OwnHead),
        },
    }
}

/// What an accepted request is about, as far as acceptance can tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Coordinate {
    /// The commit it judges, captured at acceptance.
    Commit(String),
    /// A named managed branch, resolved when the driver prepares it.
    Unresolved,
}

pub(crate) struct Accepted {
    pub id: String,
    pub joined: bool,
}

/// The identity an open request is joined by. Two filings that differ in any
/// part of it, including the commit they are about, are two requests.
pub(crate) fn open_key(spec: &CheckRequestSpec, suite: &str, coordinate: &Coordinate) -> String {
    let coordinate = match coordinate {
        Coordinate::Commit(commit) => Some(commit.as_str()),
        Coordinate::Unresolved => None,
    };
    serde_json::json!([
        spec.project_id,
        spec.requester_ref,
        suite,
        spec.subject_job_id,
        spec.revision,
        spec.placement.as_ref().map(stored_placement),
        spec.retry,
        coordinate,
    ])
    .to_string()
}

/// Insert the request, or join the open one with the same identity. One
/// statement decides which: the unique `open_key` makes two racing filings
/// one row, and a settled row has released its key.
pub(crate) async fn accept(
    db: &LocalDb,
    spec: &CheckRequestSpec,
    suite: &str,
    coordinate: &Coordinate,
) -> Result<Accepted, String> {
    let key = open_key(spec, suite, coordinate);
    let selector_json = spec.placement.as_ref().map(stored_placement);
    let pinned = spec
        .placement
        .as_ref()
        .and_then(|placement| placement.selector().name.clone());
    let commit = match coordinate {
        Coordinate::Commit(commit) => Some(commit.clone()),
        Coordinate::Unresolved => None,
    };
    // The open row can settle between a refused insert and the read of its
    // id, which frees the key; the next insert then takes it.
    for _ in 0..3 {
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_ms();
        let inserted = db
            .execute(
                "INSERT INTO check_requests(id,project_id,suite,revision_spec,subject_job_id,selector_json,retry,
                    consumer,requester_ref,requester_job_id,priority,state,commit_sha,pinned_executor,
                    open_key,driven_by,accepted_at_ms,state_since_ms,updated_ms)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,'requested',?8,?9,'review_check','accepted',?10,?11,?12,?13,?14,?14,?14)
                 ON CONFLICT(open_key) DO NOTHING",
                cairn_db::turso::params![
                    id.as_str(),
                    spec.project_id.as_str(),
                    suite,
                    spec.revision.clone(),
                    spec.subject_job_id.as_str(),
                    selector_json.clone(),
                    i64::from(spec.retry),
                    spec.requester_ref.as_str(),
                    spec.requester_job_id.clone(),
                    commit.clone(),
                    pinned.clone(),
                    key.as_str(),
                    this_runner(),
                    now,
                ],
            )
            .await
            .map_err(|error| format!("persist check request: {error}"))?;
        if inserted > 0 {
            return Ok(Accepted { id, joined: false });
        }
        let open = db
            .query_opt_text(
                "SELECT id FROM check_requests WHERE open_key=?1",
                (key.clone(),),
            )
            .await
            .map_err(|error| format!("look for the open check request: {error}"))?;
        if let Some(id) = open {
            return Ok(Accepted { id, joined: true });
        }
    }
    Err(format!(
        "check request for {suite} was neither accepted nor joined: an identical request kept settling as this one was filed; file it again"
    ))
}

// --- the driver --------------------------------------------------------------

/// The live driver of each open request, so a restart re-arm or a cancel
/// never starts a second one beside it. Only a guard: the row is the state.
static DRIVERS: LazyLock<Mutex<HashMap<String, tokio::task::AbortHandle>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Settlements, so a wait on a request learns of one without polling.
static SETTLED: LazyLock<tokio::sync::watch::Sender<u64>> =
    LazyLock::new(|| tokio::sync::watch::channel(0).0);

/// A receiver that changes whenever any request settles.
pub(crate) fn subscribe_settlements() -> tokio::sync::watch::Receiver<u64> {
    SETTLED.subscribe()
}

fn driving(id: &str) -> bool {
    DRIVERS.lock().unwrap().contains_key(id)
}

/// A runner restart, as far as one request's driver knows: the task that was
/// driving it is gone with the process that ran it.
#[cfg(test)]
pub(crate) fn forget_driver_for_test(id: &str) {
    if let Some(driver) = DRIVERS.lock().unwrap().remove(id) {
        driver.abort();
    }
}

#[cfg(test)]
pub(crate) fn driving_for_test(id: &str) -> bool {
    driving(id)
}

enum Drive {
    /// Resolve the revision, submit, and record.
    Fresh(Option<ManualCheckRevision>),
    /// Re-bind to a lane an earlier runner granted.
    Adopt(Box<crate::storage::check_lane_adoptions::Claim>),
}

fn spawn_driver(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    id: String,
    revision: Option<ManualCheckRevision>,
) {
    spawn(orch, db, id, Drive::Fresh(revision));
}

/// Start the one driver of a request, against the database its row lives in
/// (the private one, or its project's team replica).
fn spawn(orch: &Orchestrator, db: Arc<LocalDb>, id: String, drive: Drive) {
    let mut drivers = DRIVERS.lock().unwrap();
    if drivers.contains_key(&id) {
        return;
    }
    let (start, started) = tokio::sync::oneshot::channel::<()>();
    let task_orch = orch.clone();
    let task_id = id.clone();
    let task = tokio::spawn(async move {
        let _ = started.await;
        match drive {
            Drive::Fresh(revision) => drive_fresh(&task_orch, db, &task_id, revision).await,
            Drive::Adopt(claim) => drive_adopted(&task_orch, db, &task_id, *claim).await,
        }
        DRIVERS.lock().unwrap().remove(&task_id);
    });
    drivers.insert(id, task.abort_handle());
    drop(drivers);
    let _ = start.send(());
}

async fn load_for_drive(db: &LocalDb, id: &str) -> Option<CheckRequestRow> {
    match load_request_by_id(db, id).await {
        Ok(Some(row)) => Some(row),
        Ok(None) => {
            log::warn!("check request {id} has no row; nothing to drive");
            None
        }
        Err(error) => {
            log::error!("{error}; check request {id} is left for the next re-arm");
            None
        }
    }
}

/// The revision a re-driven request judges: the commit it was accepted at or
/// already resolved to, so a head that moved on while the runner was down is
/// never judged in its place.
async fn revision_for_redrive(
    orch: &Orchestrator,
    row: &CheckRequestRow,
    job_id: &str,
) -> Result<ManualCheckRevision, String> {
    match (row.commit_sha.as_deref(), row.revision_spec.as_deref()) {
        (Some(commit), _) => super::classify_manual_check_revision(orch, job_id, commit).await,
        (None, Some(spec)) => super::classify_manual_check_revision(orch, job_id, spec).await,
        (None, None) => Ok(ManualCheckRevision::OwnHead),
    }
}

/// Settle a driver's request with what its lane produced, unless the lane was
/// left running on its machine because this runner is shutting down: then the
/// request stays open and `running`, and the next runner adopts the lane and
/// settles it with what that lane produces.
async fn conclude(
    orch: &Orchestrator,
    db: &LocalDb,
    id: &str,
    row: CheckRequestRow,
    answer: CheckRequestAnswer,
) {
    let row = load_request_by_id(db, id)
        .await
        .ok()
        .flatten()
        .unwrap_or(row);
    if let CheckRequestAnswer::Complete { result } = &answer {
        if let Some(left) = &result.left_running {
            leave_running(db, &row, left).await;
            return;
        }
    }
    settle(orch, db, &row, answer).await;
}

async fn leave_running(
    db: &LocalDb,
    row: &CheckRequestRow,
    left: &crate::fleet::LeftRunningAttempt,
) {
    let waiting = format!(
        "left running on {} when its runner stopped at {}; the next runner adopts the lane and settles with what it produces",
        left.machine,
        clock(left.stopped_at_ms as i64),
    );
    let written = db
        .execute(
            format!(
                "UPDATE check_requests SET state='running',executor=COALESCE(executor,?2),
                    request_id=COALESCE(request_id,?3),attempt_id=?4,waiting_to_learn=?5,updated_ms=?6
                  WHERE id=?1 AND state IN {OPEN_STATES}"
            ),
            cairn_db::turso::params![
                row.id.as_str(),
                left.machine.as_str(),
                left.request_id.as_str(),
                left.attempt_id.as_str(),
                waiting,
                now_ms()
            ],
        )
        .await;
    if let Err(error) = written {
        log::error!(
            "check request {} was left running on {} but could not say so: {error}",
            row.id,
            left.machine
        );
    }
}

async fn drive_fresh(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    id: &str,
    revision: Option<ManualCheckRevision>,
) {
    let Some(row) = load_for_drive(&db, id).await else {
        return;
    };
    if !row.is_open() {
        return;
    }
    if let Some(by) = &row.cancel_requested_by {
        settle(orch, &db, &row, cancelled_answer(&row, by)).await;
        return;
    }
    let Some(job_id) = row.subject_job_id.clone() else {
        settle(
            orch,
            &db,
            &row,
            CheckRequestAnswer::Failed {
                error: "this request names no job to resolve its revision against".into(),
            },
        )
        .await;
        return;
    };
    let revision = match revision {
        Some(revision) => revision,
        None => match revision_for_redrive(orch, &row, &job_id).await {
            Ok(revision) => revision,
            Err(error) => {
                settle(orch, &db, &row, CheckRequestAnswer::Failed { error }).await;
                return;
            }
        },
    };
    let (progress, writer) = progress_writer(db.clone(), id.to_string());
    let result = super::run_manual_configured_check_with_progress(
        orch,
        &job_id,
        super::ManualCheckRequest {
            check_name: &row.suite,
            revision,
            placement: row.placement(),
            retry: row.retry,
            check_request: Some(id),
        },
        progress,
    )
    .await;
    // Every progress write lands before the settlement, so a late `running`
    // never overwrites a terminal state (the guard refuses it anyway).
    let _ = writer.await;
    let answer = match result {
        Ok(result) => CheckRequestAnswer::Complete {
            result: Box::new(result),
        },
        Err(error) => answer_from_error(error),
    };
    conclude(orch, &db, id, row, answer).await;
}

/// Re-bind a request to the lane an earlier runner granted it, and settle with
/// what that lane produces. The fleet decides loss: a machine whose first
/// report omits the item, or that is removed, settles the claim `lost`, and
/// the request carries that evidence. Until then the row stays `running`.
async fn drive_adopted(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    id: &str,
    claim: crate::storage::check_lane_adoptions::Claim,
) {
    let Some(row) = load_for_drive(&db, id).await else {
        return;
    };
    if !row.is_open() {
        return;
    }
    let lane_db = request_db(orch, &claim.project_id)
        .await
        .unwrap_or_else(|_| db.clone());
    let result = super::execute_adopted_check_item(orch, lane_db, &claim, 0).await;
    let answer = match result {
        Ok(result) => {
            // A reloaded lane's recorder leaves delivery to its adopter: this
            // request's waiter is delivered here, which is what lets the
            // machine release the result it retained.
            if let Some(observation) = &result.observation_id {
                deliver_adopted(orch, &claim, &row, observation).await;
            }
            CheckRequestAnswer::Complete {
                result: Box::new(result),
            }
        }
        Err(error) => {
            let settled = crate::storage::check_lane_adoptions::lookup(
                &orch.db.local,
                &claim.request_id,
                &claim.attempt_id,
            )
            .await
            .ok()
            .flatten();
            match settled.as_ref().map(|claim| claim.state.as_str()) {
                Some("cancelled") => CheckRequestAnswer::Cancelled { reason: error },
                Some("lost") => CheckRequestAnswer::Lost {
                    evidence: lost_evidence(&claim, &error),
                },
                _ => CheckRequestAnswer::Failed { error },
            }
        }
    };
    conclude(orch, &db, id, row, answer).await;
}

async fn deliver_adopted(
    orch: &Orchestrator,
    claim: &crate::storage::check_lane_adoptions::Claim,
    row: &CheckRequestRow,
    observation: &str,
) {
    let identity = (claim.request_id.clone(), claim.attempt_id.clone());
    for attempt in 0..5u32 {
        match orch
            .fleet
            .deliver_check_consumer(&identity, &row.suite, "manual", &row.id, observation)
            .await
        {
            Ok(()) => return,
            Err(error) => {
                log::warn!(
                    "check request {} could not deliver its adopted observation (attempt {}): {error}",
                    row.id,
                    attempt + 1
                );
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
}

/// A lost lane's evidence, naming its machine and its last sign of life.
fn lost_evidence(claim: &crate::storage::check_lane_adoptions::Claim, evidence: &str) -> String {
    let last = if claim.updated_ms > 0 {
        claim.updated_ms
    } else {
        claim.granted_at_ms
    };
    format!(
        "lost on {} (request {} attempt {}); last sign of life {}: {evidence}",
        claim.machine,
        claim.request_id,
        claim.attempt_id,
        stamp(last),
    )
}

fn stamp(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.format("%Y-%m-%d %H:%M:%SZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}

fn clock(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.format("%H:%M:%SZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}

/// One writer per request, in order, so progress writes never race each other
/// or the settlement that follows them.
fn progress_writer(
    db: Arc<LocalDb>,
    id: String,
) -> (
    super::manual::ManualCheckProgress,
    tokio::task::JoinHandle<()>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ManualCheckProgressUpdate>();
    let writer = tokio::spawn(async move {
        while let Some(update) = rx.recv().await {
            if let Err(error) = record_progress(&db, &id, &update).await {
                log::warn!("check request {id}: progress not recorded: {error}");
            }
        }
    });
    (
        Arc::new(move |update| {
            let _ = tx.send(update);
        }),
        writer,
    )
}

/// The row state a board update means: preparing is still `accepted`, waiting
/// in the fleet's queue is `queued`, a claimed lane is `running`. A bare board
/// state (`pending`, a verdict word) changes nothing.
fn progress_state(update: &ManualCheckProgressUpdate) -> Option<&'static str> {
    match update.state.as_str() {
        "parked" => Some("queued"),
        "running" => Some("running"),
        _ => None,
    }
}

pub(crate) async fn record_progress(
    db: &LocalDb,
    id: &str,
    update: &ManualCheckProgressUpdate,
) -> Result<(), String> {
    let now = now_ms();
    let state = progress_state(update);
    let since = update.since_unix_ms.map(|ms| ms as i64).unwrap_or(now);
    db.execute(
        format!(
            "UPDATE check_requests SET
                state_since_ms=CASE WHEN ?2 IS NOT NULL AND state<>?2 THEN ?3 ELSE state_since_ms END,
                state=COALESCE(?2,state),
                queued_at_ms=CASE WHEN ?2='queued' THEN COALESCE(queued_at_ms,?3) ELSE queued_at_ms END,
                executor=CASE WHEN ?2='running' THEN COALESCE(?4,executor) WHEN ?2='queued' THEN NULL ELSE executor END,
                diagnostic=?5,
                request_id=COALESCE(?6,request_id),
                held_count=held_count+CASE WHEN ?7 IS NOT NULL AND ?7 IS NOT held_by THEN 1 ELSE 0 END,
                held_at_ms=CASE WHEN ?7 IS NOT NULL AND ?7 IS NOT held_by THEN ?8 ELSE held_at_ms END,
                held_by=CASE WHEN ?2='running' THEN NULL ELSE COALESCE(?7,held_by) END,
                waiting_to_learn=CASE WHEN ?2='running' THEN NULL ELSE waiting_to_learn END,
                updated_ms=?8
              WHERE id=?1 AND state IN {OPEN_STATES}"
        ),
        cairn_db::turso::params![
            id,
            state,
            since,
            update.executor.clone(),
            update.diagnostic.as_str(),
            update.request_id.clone(),
            update.held_by.clone(),
            now,
        ],
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Write a request's terminal answer once, then wake everything waiting on it.
async fn settle(
    orch: &Orchestrator,
    db: &LocalDb,
    row: &CheckRequestRow,
    answer: CheckRequestAnswer,
) {
    // A cancel stands unless the lane recorded a verdict anyway: what the
    // check said is the truth about the commit, whoever stopped waiting.
    let answer = match (&row.cancel_requested_by, &answer) {
        (Some(_), CheckRequestAnswer::Complete { result })
            if result.observation_id.is_some() && result.no_verdict.is_none() =>
        {
            answer
        }
        (Some(by), _) => cancelled_answer(row, by),
        _ => answer,
    };
    let observation_id = match &answer {
        CheckRequestAnswer::Complete { result } => result.observation_id.clone(),
        _ => None,
    };
    let state = answer.settled_state();
    let evidence = answer.evidence();
    let outcome = serde_json::to_string(&answer).expect("a check request answer serializes");
    for attempt in 0..5u32 {
        let written = db
            .execute(
                format!(
                    "UPDATE check_requests SET state=?2,outcome_json=?3,evidence=?4,open_key=NULL,
                        observation_id=COALESCE(observation_id,
                            (SELECT id FROM check_result_observations WHERE project_id=?6 AND id=?5)),
                        waiting_to_learn=NULL,state_since_ms=?7,updated_ms=?7
                      WHERE id=?1 AND state IN {OPEN_STATES}"
                ),
                cairn_db::turso::params![
                    row.id.as_str(),
                    state,
                    outcome.as_str(),
                    evidence.clone(),
                    observation_id.clone(),
                    row.project_id.as_str(),
                    now_ms(),
                ],
            )
            .await;
        match written {
            Ok(_) => break,
            Err(error) => {
                log::error!(
                    "check request {} could not record its {state} answer (attempt {}): {error}",
                    row.id,
                    attempt + 1
                );
                tokio::time::sleep(std::time::Duration::from_millis(200 << attempt)).await;
            }
        }
    }
    SETTLED.send_modify(|generation| *generation += 1);
    let _ = orch;
}

fn cancelled_answer(row: &CheckRequestRow, by: &str) -> CheckRequestAnswer {
    let reason = match (row.state.as_str(), row.executor.as_deref()) {
        ("running" | "granted", Some(machine)) => format!(
            "cancelled by {by} while running on {machine}; incomplete, not a verdict and not reusable"
        ),
        ("running" | "granted", None) => {
            format!("cancelled by {by} while running; incomplete, not a verdict and not reusable")
        }
        _ => format!("cancelled by {by} before placement; no verdict"),
    };
    CheckRequestAnswer::Cancelled { reason }
}

// --- cancel ----------------------------------------------------------------

/// Withdraw an open request on `by`'s behalf.
///
/// The cancel is written to the row first, so a restart before it takes effect
/// still honours it. A request not yet submitted settles at once; a queued or
/// running one has its physical lane withdrawn or stopped, and settles as
/// cancelled when its driver hears so, never as passed.
pub(crate) async fn cancel_check_request(
    orch: &Orchestrator,
    db: &LocalDb,
    row: &CheckRequestRow,
    by: &str,
) -> Result<String, String> {
    if !row.is_open() {
        return Err(format!(
            "Nothing was cancelled: check request `{}` already settled ({}).",
            row.id,
            row.evidence.as_deref().unwrap_or(&row.state)
        ));
    }
    db.execute(
        format!(
            "UPDATE check_requests SET cancel_requested_by=COALESCE(cancel_requested_by,?2),updated_ms=?3
              WHERE id=?1 AND state IN {OPEN_STATES}"
        ),
        cairn_db::turso::params![row.id.as_str(), by, now_ms()],
    )
    .await
    .map_err(|error| format!("record the cancel of check request {}: {error}", row.id))?;
    let current = load_request_by_id(db, &row.id)
        .await?
        .unwrap_or_else(|| row.clone());
    if current.request_id.is_none() {
        // Not yet submitted: nothing on the fleet to withdraw.
        if let Some(driver) = DRIVERS.lock().unwrap().remove(&current.id) {
            driver.abort();
        }
        let current = load_request_by_id(db, &row.id).await?.unwrap_or(current);
        if current.request_id.is_none() {
            settle(orch, db, &current, cancelled_answer(&current, by)).await;
            return Ok(format!(
                "Cancelled check request `{}` ({}) before placement.",
                current.id, current.suite
            ));
        }
        withdraw_lane(orch, current.request_id.as_deref().unwrap_or_default());
        settle(orch, db, &current, cancelled_answer(&current, by)).await;
        return Ok(format!(
            "Cancelled check request `{}` ({}); its lane was told to stop.",
            current.id, current.suite
        ));
    }
    withdraw_lane(orch, current.request_id.as_deref().unwrap_or_default());
    if !driving(&current.id) {
        settle(orch, db, &current, cancelled_answer(&current, by)).await;
    }
    Ok(format!(
        "Cancelling check request `{}` ({}): its lane was withdrawn or told to stop, and it settles as cancelled, never as a verdict.",
        current.id, current.suite
    ))
}

fn withdraw_lane(orch: &Orchestrator, request_id: &str) {
    if request_id.is_empty() {
        return;
    }
    // A pure-verdict batch's one item coalesces under its own identity.
    let item = format!("{request_id}:check-0");
    if matches!(
        orch.fleet.cancel_request(&item),
        crate::fleet::CancelOutcome::Unknown
    ) && matches!(
        orch.fleet.cancel_request(request_id),
        crate::fleet::CancelOutcome::Unknown
    ) {
        orch.fleet.cancel_reported_attempts(request_id);
    }
}

/// A stopped node's own requests end with it: each is cancelled in the name of
/// the stop, so a restart does not re-drive work its requester abandoned.
pub(crate) async fn cancel_requests_of_stopped_job(orch: &Orchestrator, job_id: &str) {
    for db in orch.db.all_dbs().await {
        let rows = db
            .query_all(
                format!(
                    "SELECT {ROW_COLUMNS} FROM check_requests WHERE requester_job_id=?1 AND state IN {OPEN_STATES}"
                ),
                (job_id.to_string(),),
                CheckRequestRow::from_row,
            )
            .await
            .unwrap_or_default();
        for row in rows {
            let by = format!("the stop of its requester {}", row.requester_ref);
            if let Err(error) = cancel_check_request(orch, &db, &row, &by).await {
                log::warn!("stop of {job_id}: {error}");
            }
        }
    }
}

// --- restart -------------------------------------------------------------------

/// Re-arm every open request after a restart, the one place requests recover.
///
/// A request never granted is re-driven under the same id from the commit it
/// already resolved to. A granted one is handed to its lane's adoption: the
/// fleet's reload kept the claim (a check request owns adoption like the main
/// cadence does), and the driver re-binds to it and settles with what the lane
/// produces, or with the fleet's death evidence. A row whose lane already
/// recorded takes that observation and runs nothing. A cancel issued before the
/// restart is honoured.
///
/// Every open database is scanned, the private one and each team replica, and
/// only the rows this runner drives are re-armed: a team replica also holds
/// requests other runners drive, and their lanes are theirs.
pub(crate) async fn rearm_at_boot(orch: &Orchestrator) {
    orch.fleet.await_restart_adoptions_loaded().await;
    for db in orch.db.all_dbs().await {
        let rows = match db
            .query_all(
                format!(
                    "SELECT {ROW_COLUMNS} FROM check_requests
                      WHERE state IN {OPEN_STATES} AND COALESCE(driven_by,?1)=?1
                      ORDER BY accepted_at_ms"
                ),
                (this_runner(),),
                CheckRequestRow::from_row,
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                log::error!("check requests cannot be re-armed at boot: {error}");
                continue;
            }
        };
        for row in rows {
            rearm(orch, &db, row).await;
        }
    }
}

async fn rearm(orch: &Orchestrator, db: &Arc<LocalDb>, row: CheckRequestRow) {
    if driving(&row.id) {
        return;
    }
    if let Some(by) = row.cancel_requested_by.clone() {
        if let Some(request_id) = &row.request_id {
            withdraw_lane(orch, request_id);
        }
        settle(orch, db, &row, cancelled_answer(&row, &by)).await;
        return;
    }
    if row.observation_id.is_some() {
        // Its lane recorded before the restart; the answer is in the ledger.
        match crate::execution::checks::recover_manual_check_result(
            orch,
            db.clone(),
            &row.project_id,
            &row.id,
        )
        .await
        {
            Ok(result) => {
                settle(
                    orch,
                    db,
                    &row,
                    CheckRequestAnswer::Complete {
                        result: Box::new(result),
                    },
                )
                .await;
                return;
            }
            Err(error) => log::warn!(
                "check request {} has a recorded observation it cannot read back ({error}); re-driving it",
                row.id
            ),
        }
    }
    let claim = match &row.request_id {
        Some(request_id) => {
            crate::storage::check_lane_adoptions::latest_for_request(&orch.db.local, request_id)
                .await
                .ok()
                .flatten()
        }
        None => None,
    };
    match claim {
        Some(claim)
            if matches!(
                claim.state.as_str(),
                "running" | "awaiting_adoption" | "joined"
            ) =>
        {
            let last = if claim.updated_ms > 0 {
                claim.updated_ms
            } else {
                claim.granted_at_ms
            };
            let waiting = format!(
                "running on {} since {}; no word from the executor since {}, waiting for it to report the item or for its link to be superseded",
                claim.machine,
                clock(claim.granted_at_ms),
                clock(last.max(row.updated_ms)),
            );
            let _ = db
                .execute(
                    format!(
                        "UPDATE check_requests SET state='running',executor=COALESCE(executor,?2),
                            waiting_to_learn=?3,attempt_id=?4,updated_ms=?5
                          WHERE id=?1 AND state IN {OPEN_STATES}"
                    ),
                    cairn_db::turso::params![
                        row.id.as_str(),
                        claim.machine.as_str(),
                        waiting,
                        claim.attempt_id.as_str(),
                        now_ms()
                    ],
                )
                .await;
            spawn(
                orch,
                db.clone(),
                row.id.clone(),
                Drive::Adopt(Box::new(claim)),
            );
        }
        Some(claim) if claim.state == "lost" => {
            let evidence = claim
                .evidence
                .clone()
                .unwrap_or_else(|| "its lane was lost".into());
            let evidence = lost_evidence(&claim, &evidence);
            settle(orch, db, &row, CheckRequestAnswer::Lost { evidence }).await;
        }
        Some(claim) if claim.state == "cancelled" => {
            let reason = claim
                .evidence
                .clone()
                .unwrap_or_else(|| format!("its lane on {} was cancelled", claim.machine));
            settle(orch, db, &row, CheckRequestAnswer::Cancelled { reason }).await;
        }
        // Never granted, or granted and recorded without this row learning
        // which observation: driving it again reuses that verdict from the
        // ledger rather than running the check a second time.
        _ => spawn_driver(orch, db.clone(), row.id.clone(), None),
    }
}

// --- reading -------------------------------------------------------------------

/// A request's answer: its settled one, or where it stands while open.
pub(crate) async fn request_answer(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    row: &CheckRequestRow,
) -> Result<CheckRequestAnswer, String> {
    if let Some(outcome) = &row.outcome_json {
        return serde_json::from_str(outcome)
            .map_err(|error| format!("decode check request {} answer: {error}", row.id));
    }
    if !row.is_open() {
        return Ok(CheckRequestAnswer::Failed {
            error: row
                .evidence
                .clone()
                .unwrap_or_else(|| format!("settled {} with no recorded answer", row.state)),
        });
    }
    // Attached by its recorder (or its coalesced publisher) before the driver
    // wrote the answer: the ledger already holds it.
    if row.observation_id.is_some() || row.publication_id.is_some() {
        if let Ok(result) = crate::execution::checks::recover_manual_check_result(
            orch,
            db,
            &row.project_id,
            &row.id,
        )
        .await
        {
            return Ok(CheckRequestAnswer::Complete {
                result: Box::new(result),
            });
        }
    }
    let diagnostic = row.diagnostic.clone();
    let since = Some(row.state_since_ms.max(0) as u64);
    Ok(match row.state.as_str() {
        "accepted" => CheckRequestAnswer::Queued {
            diagnostic: diagnostic.unwrap_or_else(|| format!("resolving {}", row.suite)),
        },
        "queued" => CheckRequestAnswer::Parked {
            diagnostic: diagnostic
                .unwrap_or_else(|| "queued, waiting for a machine to accept it".into()),
            executor: row.pinned_executor.clone(),
            since_unix_ms: row.queued_at_ms.map(|ms| ms.max(0) as u64).or(since),
            held_by: row.held_by.clone(),
            held_since_unix_ms: row.held_at_ms.map(|ms| ms.max(0) as u64),
            refusals: (row.held_count > 0).then_some(row.held_count as u64),
        },
        _ => CheckRequestAnswer::Running {
            diagnostic: diagnostic.unwrap_or_else(|| match &row.executor {
                Some(machine) => format!("running on {machine}"),
                None => "running".into(),
            }),
            executor: row.executor.clone(),
            since_unix_ms: since,
            waiting_to_learn: row.waiting_to_learn.clone(),
        },
    })
}

/// The settled answer in a few words, never `passed` for a non-verdict.
pub(crate) fn verdict_words(answer: &CheckRequestAnswer) -> String {
    use CheckRequestAnswer as A;
    match answer {
        A::Complete { result } => {
            let reference = result.observation_ref.as_deref().unwrap_or("not recorded");
            if let Some(unmet) = &result.placement_unmet {
                format!("no verdict for the requested placement: {unmet}")
            } else if let Some(no_verdict) = &result.no_verdict {
                format!("no verdict ({}): {}", no_verdict.kind, no_verdict.cause)
            } else if result.passed {
                format!("recorded passed ({}), {reference}", result.disposition)
            } else {
                format!("recorded failed ({}), {reference}", result.disposition)
            }
        }
        A::Failed { error } => format!("failed without a verdict: {error}"),
        A::Cancelled { reason } => reason.clone(),
        A::Lost { evidence } => format!("lost, no verdict: {evidence}"),
        A::ConflictsWithBase { branch, .. } => {
            format!("no verdict: {branch} conflicts with its base")
        }
        A::RegenerationPending { branch, .. } => {
            format!("no verdict owed: {branch} awaits regeneration of conflicting paths")
        }
        A::Queued { diagnostic } | A::Running { diagnostic, .. } | A::Parked { diagnostic, .. } => {
            diagnostic.clone()
        }
    }
}

/// A request's state as one short clause: `queued since 09:01Z, pinned to
/// bgsrv-ub; bgsrv-ub declined it 3 times, latest: …`.
pub(crate) fn standing(row: &CheckRequestRow) -> String {
    match row.state.as_str() {
        "accepted" => format!("resolving its revision since {}", clock(row.accepted_at_ms)),
        "queued" => {
            let mut words = format!(
                "queued since {}",
                clock(row.queued_at_ms.unwrap_or(row.state_since_ms))
            );
            if let Some(machine) = &row.pinned_executor {
                words.push_str(&format!(", pinned to {machine}"));
            }
            match &row.held_by {
                Some(held) => {
                    let refusals = match row.held_count {
                        n if n > 1 => format!("{n} refusals, "),
                        _ => String::new(),
                    };
                    words.push_str(&format!(
                        "; {refusals}latest at {}: {held}",
                        clock(row.held_at_ms.unwrap_or(row.updated_ms))
                    ));
                }
                None => {
                    if let Some(diagnostic) = &row.diagnostic {
                        words.push_str(&format!("; {diagnostic}"));
                    }
                }
            }
            words
        }
        "granted" | "running" => match (&row.waiting_to_learn, &row.executor) {
            (Some(waiting), _) => waiting.clone(),
            (None, Some(machine)) => {
                format!("running on {machine} since {}", clock(row.state_since_ms))
            }
            (None, None) => format!("running since {}", clock(row.state_since_ms)),
        },
        "recorded" => "recorded".into(),
        state => match &row.evidence {
            Some(evidence) => format!("{state}: {evidence}"),
            None => state.to_string(),
        },
    }
}

/// The JSON a request reads as with `?format=json`: its row's facts and the
/// `observation` shape `cairn check watch` renders.
pub(crate) async fn request_value(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    project_key: &str,
    row: &CheckRequestRow,
) -> Result<serde_json::Value, String> {
    let answer = request_answer(orch, db, row).await?;
    let uri = request_uri(project_key, &row.id);
    Ok(serde_json::json!({
        "ok": true,
        "id": row.id,
        "uri": uri,
        "suite": row.suite,
        "state": row.state,
        "standing": standing(row),
        "requester": row.requester_ref,
        "revision": row.revision_spec,
        "commit": row.commit_sha,
        "pinnedTo": row.pinned_executor,
        "waitFor": wait_line(&uri),
        "observation": answer,
    }))
}

#[cfg(test)]
#[path = "requests_tests.rs"]
mod tests;
