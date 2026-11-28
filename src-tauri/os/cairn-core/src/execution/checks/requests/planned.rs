//! Immutable cadence input and returned-delta custody on the canonical request row.
use super::super::{
    CheckConsumer, CheckExecMode, CheckGroupResults, CheckProducer, CheckRunCommit,
    PlannedCheckBatchItem, PlannedCheckBatchOutcome, PlannedCheckRequest,
};
use super::*;
use cairn_common::executor_protocol::{CellOwnerRef, MutationPolicy};
use cairn_common::wire_vocabulary::CellPriority;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WriteLanePhase {
    Fixer,
    Convergence,
    Verifier,
    LegacyFixer,
}

impl WriteLanePhase {
    fn mutation_policy(self) -> MutationPolicy {
        match self {
            Self::Fixer | Self::Convergence | Self::LegacyFixer => MutationPolicy::AllowDelta,
            Self::Verifier => MutationPolicy::PureVerdict,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Snapshot {
    project_id: String,
    project_key: String,
    repository: String,
    store_dir: PathBuf,
    sealed_commit: String,
    requesting_job_id: String,
    owner: CellOwnerRef,
    affinity_key: Option<String>,
    priority: CellPriority,
    item: PlannedCheckBatchItem,
    #[serde(skip)]
    run_context: Option<crate::mcp::handlers::RunContext>,
    // Only the three content transports are data. Launch credentials are
    // reacquired from the runner's environment, never journaled. Absent once
    // the request settles ([`SETTLED_CONTEXT`]): a settled request never
    // launches again, and the cell request it still rebuilds for its fold
    // publication reads ids, repository and project, never an item's env.
    #[serde(default)]
    transport: Vec<(String, String)>,
    phase: WriteLanePhase,
}

impl Snapshot {
    fn rebind(&mut self, row: &CheckRequestRow) {
        self.project_id = row.project_id.clone();
        self.owner.project_id = row.project_id.clone();
        if let Some(job) = &row.subject_job_id {
            self.requesting_job_id = job.clone();
            self.owner.job_id = Some(job.clone());
            if let Some(recording) = &mut self.item.recording_plan {
                recording.job_id = job.clone();
            }
        }
    }
    fn capture(request: &PlannedCheckRequest, phase: WriteLanePhase) -> Result<Self, String> {
        if request.on_complete.is_some()
            || request.purpose.is_some()
            || request.publication.is_some()
        {
            return Err(
                "a write cadence lane must not carry another execution or publication owner".into(),
            );
        }
        let recording = request
            .item
            .recording_plan
            .as_ref()
            .ok_or("a cadence request requires an immutable recording plan")?;
        if recording.evaluated_commit != request.sealed_commit
            || recording.input_hash != request.item.input_hash
            || recording.plan.name != request.item.name
        {
            return Err("cadence request and recording coordinates disagree".into());
        }
        Ok(Self {
            project_id: request.project_id.clone(),
            project_key: request
                .owner
                .project_key
                .clone()
                .ok_or("write request has no project key")?,
            repository: request.repository.clone(),
            store_dir: request.store_dir.clone(),
            sealed_commit: request.sealed_commit.clone(),
            requesting_job_id: request.requesting_job_id.clone(),
            owner: request.owner.clone(),
            affinity_key: request.affinity_key.clone(),
            priority: request.priority,
            item: request.item.clone(),
            run_context: request.run_context.clone(),
            phase,
            transport: request
                .item
                .env
                .iter()
                .filter(|(name, _)| {
                    super::super::CHECK_CONTENT_TRANSPORTS
                        .iter()
                        .any(|(content, _)| name == content)
                })
                .cloned()
                .collect(),
        })
    }

    fn restore(&self, orch: &Orchestrator, id: &str) -> PlannedCheckRequest {
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let env = super::super::slot_check_env(jj.shell_env());
        let mut item = self.item.clone();
        item.env = super::super::write_check_transport_env(&env, self.transport.clone());
        let producer = CheckProducer {
            consumer: CheckConsumer::Write,
            consumer_ref: id.to_string(),
            request_id: Some(id.to_string()),
        };
        if let Some(recording) = &mut item.recording_plan {
            recording.producer = producer.clone();
            recording.effective_env = item.env.clone();
        }
        PlannedCheckRequest {
            producer,
            project_id: self.project_id.clone(),
            repository: self.repository.clone(),
            store_dir: self.store_dir.clone(),
            sealed_commit: self.sealed_commit.clone(),
            requesting_job_id: self.requesting_job_id.clone(),
            owner: self.owner.clone(),
            affinity_key: self.affinity_key.clone(),
            priority: self.priority,
            env,
            item,
            run_context: self.run_context.clone(),
            status_board: None,
            on_complete: None,
            purpose: None,
            publication: None,
        }
    }

    fn wire(
        &self,
        orch: &Orchestrator,
        id: &str,
        identity: &(String, String),
    ) -> cairn_common::executor_protocol::CellRequest {
        let request = self.restore(orch, id);
        let selector = request.item.executor.clone();
        let platforms = request.item.verdict_platforms.clone();
        let avoid = request.item.avoid_executor_ids.clone();
        super::super::build_check_batch_wire(
            &request.into_batch(self.phase.mutation_policy()),
            identity.0.clone(),
            identity.1.clone(),
            selector,
            platforms,
            avoid,
        )
        .request
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Returned {
    /// Empty once every wave of the request's job is delivered
    /// ([`RETIRED_CONTEXT`]): only a wave replay reads the raw results.
    #[serde(default)]
    results: CheckGroupResults,
    identity: Option<(String, String)>,
    delta: Option<crate::fleet::MutationDelta>,
}

impl Returned {
    fn capture(outcome: &PlannedCheckBatchOutcome) -> Self {
        Self {
            results: outcome.results.clone(),
            identity: outcome
                .results
                .values()
                .filter_map(|r| r.as_ref().ok())
                .filter_map(|r| r.publication.as_ref())
                .find_map(|p| p.request_identity())
                .or_else(|| {
                    outcome
                        .request
                        .as_ref()
                        .map(|r| (r.request_id.clone(), r.attempt_id.clone()))
                }),
            delta: outcome.delta.clone(),
        }
    }
    fn outcome(
        &self,
        orch: &Orchestrator,
        id: &str,
        snapshot: &Snapshot,
    ) -> PlannedCheckBatchOutcome {
        let mut results = self.results.clone();
        if let Some(identity) = &self.identity {
            for result in results.values_mut().filter_map(|r| r.as_mut().ok()) {
                if result.publication.is_none() {
                    result.publication = Some(crate::fleet::PublicationCoordination::for_request(
                        identity.clone(),
                    ));
                }
            }
        }
        PlannedCheckBatchOutcome {
            results,
            request: self
                .identity
                .as_ref()
                .map(|identity| snapshot.wire(orch, id, identity)),
            delta: self.delta.clone(),
            store_dir: Some(snapshot.store_dir.clone()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Context {
    write_lane: Snapshot,
    #[serde(default)]
    returned: Option<Returned>,
    #[serde(default)]
    recorded: bool,
    #[serde(default)]
    fold: Option<Result<Fold, String>>,
    #[serde(default)]
    final_recording: Option<super::super::CheckAdoptionRecordingPlan>,
    #[serde(default)]
    terminal: Option<super::super::ManualConfiguredCheckResult>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Fold {
    commit: String,
    patch: String,
    paths: Vec<String>,
    landed_on_tested_base: bool,
}

/// A write request's `context_json` as its settling write leaves it.
///
/// The launch transport is the request's bulk: the wave's changed-file list
/// plus, whenever the wave plans the executor-protocol suite, the executor's
/// base-content bundle (1.7-2.8 MB on the 11-27 database), copied into every
/// suite's request. Nothing reads it once the request has settled. Everything
/// else stays, because a wave still in flight replays its settled phases after
/// a restart and reads their raw results, delta and fold.
pub(crate) const SETTLED_CONTEXT: &str = "CASE WHEN consumer='write' \
     THEN json_remove(context_json,'$.write_lane.transport') ELSE context_json END";

/// What a settled write request keeps once no wave of its job is undelivered:
/// its phase, item and final recording (what `rearm_deliveries` and a legacy
/// finish deserialize), `recorded`, and the returned identity its consumer
/// delivery names. The raw results, delta, fold and terminal answer are read
/// only by a wave replay, and a delivered wave never replays; the request's
/// answer itself is `outcome_json`.
const RETIRED_CONTEXT: &str = "json_remove(context_json,'$.write_lane.transport',\
     '$.returned.results','$.returned.delta','$.fold','$.terminal')";

/// Whether a write request's context still holds something [`RETIRED_CONTEXT`]
/// drops, so a retirement pass rewrites only rows it shrinks.
const RETIRABLE: &str = "(json_type(context_json,'$.write_lane.transport') IS NOT NULL \
     OR json_type(context_json,'$.returned.results') IS NOT NULL \
     OR json_type(context_json,'$.returned.delta') IS NOT NULL \
     OR json_type(context_json,'$.fold') IS NOT NULL \
     OR json_type(context_json,'$.terminal') IS NOT NULL)";

const SETTLED_STATES: &str = "('recorded','failed','cancelled','lost')";

/// Retire the settled write requests of `job` once it has no undelivered wave,
/// inside the transaction that delivered its last one.
pub(crate) async fn retire_settled_requests(
    conn: &crate::storage::TrackedConnection,
    job: &str,
) -> crate::storage::DbResult<u64> {
    Ok(conn
        .execute(
            &format!(
                "UPDATE check_requests SET context_json={RETIRED_CONTEXT}
                  WHERE subject_job_id=?1 AND state IN {SETTLED_STATES} AND consumer='write'
                    AND NOT EXISTS(SELECT 1 FROM write_check_waves WHERE job_id=?1 AND delivered=0)
                    AND {RETIRABLE}"
            ),
            (job.to_string(),),
        )
        .await?)
}

/// Rows one backlog write rewrites. A pre-compaction row is up to 3 MB, and
/// Turso reads a whole record to touch any column of it, so a write stays
/// near the evictor's 4 MB page.
const BACKLOG_WRITE_ROWS: usize = 2;

/// Strip the launch context from write requests that settled before their
/// settling write did it, a few rows per transaction, and from delivered
/// waves the same way ([`super::super::write_wave::compact_delivered_backlog`]).
///
/// Runs once per boot over every open database. Rows already compact are
/// rewritten by no write: the predicate reads them and matches nothing.
pub(crate) async fn compact_settled_backlog(orch: Orchestrator) {
    for db in orch.db.all_dbs().await {
        let mut rewritten = 0u64;
        let mut after = 0i64;
        loop {
            if orch.fleet.runner_stopping_since().is_some() {
                return;
            }
            let page = match db
                .query_all(
                    "SELECT rowid FROM check_requests WHERE rowid > ?1 ORDER BY rowid LIMIT 64",
                    (after,),
                    |row| row.i64(0),
                )
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    log::warn!("settled write-request compaction stopped: {error}");
                    break;
                }
            };
            let Some(last) = page.last().copied() else {
                break;
            };
            for rowids in page.chunks(BACKLOG_WRITE_ROWS) {
                match compact_settled_rows(&db, rowids).await {
                    Ok(count) => rewritten += count,
                    Err(error) => log::warn!("settled write-request compaction: {error}"),
                }
                tokio::task::yield_now().await;
            }
            after = last;
        }
        if rewritten > 0 {
            log::info!("compacted {rewritten} settled write-check requests");
        }
        super::super::write_wave::compact_delivered_backlog(&orch, &db).await;
    }
}

/// One backlog write: each settled write request among `rowids` keeps what
/// [`RETIRED_CONTEXT`] keeps when its job has no undelivered wave, and what
/// [`SETTLED_CONTEXT`] keeps while one could still replay it.
async fn compact_settled_rows(db: &LocalDb, rowids: &[i64]) -> Result<u64, String> {
    let list = rowids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    db.execute(
        format!(
            "UPDATE check_requests SET context_json=CASE
                 WHEN EXISTS(SELECT 1 FROM write_check_waves w
                              WHERE w.job_id=check_requests.subject_job_id AND w.delivered=0)
                 THEN json_remove(context_json,'$.write_lane.transport')
                 ELSE {RETIRED_CONTEXT} END
              WHERE rowid IN ({list}) AND consumer='write' AND state IN {SETTLED_STATES}
                AND {RETIRABLE}"
        ),
        (),
    )
    .await
    .map_err(|error| error.to_string())
}

fn filing_id(_project_id: &str, key: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(key.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}

async fn context(db: &LocalDb, id: &str) -> Result<Option<Context>, String> {
    let value = db
        .query_opt_text(
            "SELECT context_json FROM check_requests WHERE id=?1",
            (id.to_string(),),
        )
        .await
        .map_err(|e| e.to_string())?;
    match value {
        Some(value)
            if serde_json::from_str::<serde_json::Value>(&value)
                .ok()
                .is_some_and(|v| v.get("write_lane").is_some()) =>
        {
            serde_json::from_str(&value)
                .map(Some)
                .map_err(|e| format!("invalid durable write lane {id}: {e}"))
        }
        _ => Ok(None),
    }
}

async fn checkpoint(
    db: &LocalDb,
    id: &str,
    field: &str,
    value: &impl Serialize,
) -> Result<(), String> {
    let value = serde_json::to_string(value).map_err(|e| e.to_string())?;
    let updated = db.execute(
        "UPDATE check_requests SET context_json=json_set(context_json,?2,json(?3)),updated_ms=?4 WHERE id=?1 AND json_type(context_json,'$.write_lane')='object' AND (json_type(context_json,?2) IS NULL OR json_type(context_json,?2)='null' OR json_extract(context_json,?2)=json_extract(?3,'$') OR ?2='$.recorded')",
        cairn_db::turso::params![id,format!("$.{field}"),value,now_ms()]
    ).await.map_err(|e| format!("checkpoint write request {id}: {e}"))?;
    if updated != 1 {
        return Err(format!(
            "write request {id} has no durable execution context"
        ));
    }
    Ok(())
}

pub(crate) async fn planned_filing(
    orch: &Orchestrator,
    project_id: &str,
    key: &str,
) -> Result<(String, i64), String> {
    let db = request_db(orch, project_id).await?;
    let id = filing_id(project_id, key);
    let row = load_request(&db, project_id, &id)
        .await?
        .ok_or("write lane is not filed")?;
    Ok((id, row.accepted_at_ms))
}

pub(crate) async fn load_planned_fold(
    orch: &Orchestrator,
    project_id: &str,
    key: &str,
) -> Result<Option<Result<crate::mcp::handlers::run::PublishedSlotDelta, String>>, String> {
    let db = request_db(orch, project_id).await?;
    Ok(context(&db, &filing_id(project_id, key))
        .await?
        .and_then(|c| c.fold)
        .map(|r| {
            r.map(|f| crate::mcp::handlers::run::PublishedSlotDelta {
                commit: f.commit,
                patch: f.patch,
                paths: f.paths,
                landed_on_tested_base: f.landed_on_tested_base,
            })
        }))
}

pub(crate) async fn record_planned_fold(
    orch: &Orchestrator,
    project_id: &str,
    key: &str,
    fold: &Result<crate::mcp::handlers::run::PublishedSlotDelta, String>,
) -> Result<(), String> {
    let db = request_db(orch, project_id).await?;
    let id = filing_id(project_id, key);
    let context = context(&db, &id).await?.ok_or("write lane is not filed")?;
    if context.write_lane.phase != WriteLanePhase::Fixer {
        return Err("only the original fixer phase may fold a delta".into());
    }
    if context.returned.is_none() {
        return Err("a fold requires durable returned-delta custody".into());
    }
    if context.fold.is_some() {
        return Ok(());
    }
    let fold = fold.clone().map(|f| Fold {
        commit: f.commit,
        patch: f.patch,
        paths: f.paths,
        landed_on_tested_base: f.landed_on_tested_base,
    });
    checkpoint(&db, &id, "fold", &fold).await
}

/// File a preplanned single lane through the same acceptance/driver seam, then
/// wait for its durable raw answer. The board's final reduction is not a recorder.
pub(crate) async fn file_planned_check_request(
    orch: &Orchestrator,
    key: &str,
    request: PlannedCheckRequest,
    phase: WriteLanePhase,
) -> Result<PlannedCheckBatchOutcome, String> {
    let project_id = request.project_id.clone();
    // Request admission has its own state machine; do not embed it in each
    // phase future and, transitively, in the write-wave driver.
    let filed = Box::pin(file_check_request(
        orch,
        CheckRequestInput::Planned {
            key: key.to_string(),
            request: Box::new(request),
            phase,
        },
    ))
    .await?;
    let db = request_db(orch, &project_id).await?;
    let mut settled = subscribe_settlements();
    loop {
        let c = context(&db, &filed.id)
            .await?
            .ok_or("write request lost its immutable context")?;
        if c.recorded || (c.write_lane.phase != WriteLanePhase::Verifier && c.returned.is_some()) {
            return c
                .returned
                .as_ref()
                .ok_or_else(|| "recorded write request lost its raw answer".to_string())
                .map(|r| r.outcome(orch, &filed.id, &c.write_lane));
        }
        let row = load_request(&db, &project_id, &filed.id)
            .await?
            .ok_or("write request vanished")?;
        if !row.is_open() {
            return Err(row
                .evidence
                .unwrap_or_else(|| format!("write request settled {}", row.state)));
        }
        settled
            .changed()
            .await
            .map_err(|_| "write request settlement channel closed")?;
    }
}

pub(super) async fn file(
    orch: &Orchestrator,
    key: String,
    request: PlannedCheckRequest,
    phase: WriteLanePhase,
) -> Result<FiledRequest, String> {
    let snapshot = Snapshot::capture(&request, phase)?;
    let id = filing_id(&snapshot.project_id, &key);
    let db = request_db(orch, &snapshot.project_id).await?;
    let recording = snapshot.item.recording_plan.as_ref().unwrap();
    let spec = CheckRequestSpec {
        consumer: CheckConsumer::Write,
        cadence: Some("write"),
        priority: snapshot.priority,
        project_id: snapshot.project_id.clone(),
        project_key: snapshot.project_key.clone(),
        suite: snapshot.item.name.clone(),
        revision: Some(snapshot.sealed_commit.clone()),
        subject_job_id: snapshot.requesting_job_id.clone(),
        placement: None,
        retry: false,
        requester_ref: request.producer.consumer_ref.clone(),
        requester_job_id: Some(snapshot.requesting_job_id.clone()),
    };
    let serialized = serde_json::to_string(&Context {
        write_lane: snapshot.clone(),
        returned: None,
        recorded: false,
        fold: None,
        final_recording: None,
        terminal: None,
    })
    .map_err(|e| e.to_string())?;
    let accepted = accept_with_context(
        &db,
        &spec,
        &spec.suite,
        &Coordinate::Commit(snapshot.sealed_commit.clone()),
        Some(&id),
        Some(&serialized),
    )
    .await?;
    if accepted.id != id {
        return Err("stable write filing joined a different request".into());
    }
    let persisted = context(&db, &id)
        .await?
        .ok_or("stable write filing has no immutable snapshot")?;
    if persisted.write_lane.phase != phase
        || persisted.write_lane.sealed_commit != snapshot.sealed_commit
        || persisted.write_lane.item.command != snapshot.item.command
        || persisted.write_lane.item.input_hash != snapshot.item.input_hash
    {
        return Err("stable write phase was retried with different execution coordinates".into());
    }
    if !accepted.joined {
        db.execute(
        "UPDATE check_requests SET tree_hash=?2,input_hash=?3,pinned_executor=?4 WHERE id=?1 AND tree_hash IS NULL",
        cairn_db::turso::params![
            id.as_str(),
            recording.tree_hash.as_str(),
            recording.input_hash.as_str(),
            snapshot.item.executor.as_ref().and_then(|s| s.name.clone())
        ],
    )
    .await
    .map_err(|e| e.to_string())?;
    }
    if !driving(&id) {
        let mut live = request;
        live.producer = CheckProducer {
            consumer: CheckConsumer::Write,
            consumer_ref: id.clone(),
            request_id: Some(id.clone()),
        };
        if let Some(recording) = &mut live.item.recording_plan {
            recording.producer = live.producer.clone();
        }
        spawn(orch, db, id.clone(), Drive::Planned(Some(Box::new(live))));
    }
    Ok(FiledRequest {
        uri: request_uri(&spec.project_key, accepted.number),
        id,
        suite: spec.suite,
        joined: accepted.joined,
    })
}

/// Returns true when this row belongs to the immutable planned driver.
pub(super) async fn rearm(orch: &Orchestrator, db: &Arc<LocalDb>, row: &CheckRequestRow) -> bool {
    match context(db, &row.id).await {
        Ok(Some(_)) => {
            spawn(orch, db.clone(), row.id.clone(), Drive::Planned(None));
            true
        }
        Ok(None) => false,
        Err(error) => {
            log::error!("{error}; keeping {} unanswered", row.id);
            true
        }
    }
}

async fn claim_for(
    orch: &Orchestrator,
    row: &CheckRequestRow,
) -> Result<Option<crate::storage::check_lane_adoptions::Claim>, String> {
    // A grant and its waiter are durable together. A progress update is not the
    // grant boundary and may have been lost with the runner.
    let identity = orch.db.local.query_opt(
        "SELECT request_id,attempt_id FROM check_lane_adoption_waiters WHERE consumer='write' AND consumer_ref=?1 ORDER BY updated_ms DESC LIMIT 1",
        (row.id.clone(),), |r| Ok((r.text(0)?,r.text(1)?))
    ).await.map_err(|e|e.to_string())?;
    match identity {
        Some((request, attempt)) => {
            crate::storage::check_lane_adoptions::lookup(&orch.db.local, &request, &attempt)
                .await
                .map_err(|e| e.to_string())
        }
        None => match &row.request_id {
            Some(request) => {
                crate::storage::check_lane_adoptions::latest_for_request(&orch.db.local, request)
                    .await
                    .map_err(|e| e.to_string())
            }
            None => Ok(None),
        },
    }
}

pub(super) async fn drive(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    id: &str,
    live: Option<Box<PlannedCheckRequest>>,
) {
    loop {
        match drive_inner(orch, db.clone(), id, live.clone()).await {
            Ok(()) => return,
            Err(error) => {
                log::error!("write request {id}: {error}; durable custody retained for retry");
                let _=db.execute("UPDATE check_requests SET waiting_to_learn=?2 WHERE id=?1 AND state IN ('accepted','queued','granted','running')",
                    (id.to_string(),error)).await;
                SETTLED.send_modify(|g| *g += 1);
                if orch.fleet.runner_stopping_since().is_some() {
                    return;
                }
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
}

async fn drive_inner(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    id: &str,
    live: Option<Box<PlannedCheckRequest>>,
) -> Result<(), String> {
    let row = load_request_by_id(&db, id)
        .await?
        .ok_or("write request vanished")?;
    let mut c = context(&db, id)
        .await?
        .ok_or("write request has no snapshot")?;
    // Embedded execution payloads are not identity authorities after team rekey.
    c.write_lane.rebind(&row);
    if let Some(recording) = &mut c.final_recording {
        recording.job_id = row
            .subject_job_id
            .clone()
            .ok_or("write request lost its owning job")?;
        recording.producer = CheckProducer {
            consumer: CheckConsumer::Write,
            consumer_ref: id.to_string(),
            request_id: Some(id.to_string()),
        };
    }
    if c.recorded {
        if c.write_lane.phase == WriteLanePhase::LegacyFixer {
            return finish_legacy(orch, &db, &row, &c).await;
        }
        if let Some(mut terminal) = c.terminal.clone() {
            terminal.observation_id = row.observation_id.clone();
            settle(
                orch,
                &db,
                &row,
                CheckRequestAnswer::Complete {
                    result: Box::new(terminal.clone()),
                },
            )
            .await;
            deliver_checkpointed(orch, &row, &c, terminal.observation_id.as_deref()).await?;
        }
        return Ok(());
    }
    let mut request = live
        .map(|r| *r)
        .unwrap_or_else(|| c.write_lane.restore(orch, id));
    request.project_id = row.project_id.clone();
    request.requesting_job_id = c.write_lane.requesting_job_id.clone();
    request.owner.project_id = row.project_id.clone();
    request.owner.job_id = row.subject_job_id.clone();
    if c.returned.is_none() {
        let outcome = match claim_for(orch, &row).await? {
            Some(claim) if claim.state == "lost" || claim.state == "cancelled" => {
                let evidence = claim
                    .evidence
                    .clone()
                    .unwrap_or_else(|| "the granted lane ended".into());
                let answer = if claim.state == "lost" {
                    CheckRequestAnswer::Lost {
                        evidence: lost_evidence(&claim, &evidence),
                    }
                } else {
                    CheckRequestAnswer::Cancelled { reason: evidence }
                };
                settle(orch, &db, &row, answer).await;
                return Ok(());
            }
            Some(claim) => {
                let waiting=format!("running on {}; waiting for its retained result or explicit executor death evidence",claim.machine);
                db.execute("UPDATE check_requests SET state='running',request_id=?2,attempt_id=?3,executor=?4,waiting_to_learn=?5 WHERE id=?1",
                    cairn_db::turso::params![id,claim.request_id.as_str(),claim.attempt_id.as_str(),claim.machine.as_str(),waiting])
                    .await.map_err(|e|e.to_string())?;
                let identity = (claim.request_id.clone(), claim.attempt_id.clone());
                orch.fleet.defer_result_ack(&identity.0).retain();
                let executed = orch
                    .fleet
                    .bind_adopted(identity.clone(), &claim.executor_id)
                    .await;
                let current = crate::storage::check_lane_adoptions::lookup(
                    &orch.db.local,
                    &identity.0,
                    &identity.1,
                )
                .await
                .map_err(|e| e.to_string())?
                .ok_or("adopted grant disappeared")?;
                if matches!(current.state.as_str(), "lost" | "cancelled") {
                    let evidence = current
                        .evidence
                        .clone()
                        .unwrap_or("adopted lane ended without a result".into());
                    let answer = if current.state == "lost" {
                        CheckRequestAnswer::Lost {
                            evidence: lost_evidence(&current, &evidence),
                        }
                    } else {
                        CheckRequestAnswer::Cancelled { reason: evidence }
                    };
                    settle(orch, &db, &row, answer).await;
                    return Ok(());
                }
                let ordinal = claim
                    .check_names
                    .iter()
                    .position(|name| name == &row.suite)
                    .ok_or("granted lane lost its suite")?;
                let recording = request
                    .item
                    .recording_plan
                    .as_ref()
                    .ok_or("granted lane lacks its recording contract")?;
                let (result, delta) = match recording.result_shape {
                    super::super::CheckAdoptionResultShape::Batch => {
                        let (mut results, delta) = super::super::split_write_check_batch_outcome(
                            executed.outcome,
                            &executed.account,
                            claim.check_names.len(),
                        );
                        (results.remove(ordinal), delta)
                    }
                    super::super::CheckAdoptionResultShape::Single => (
                        super::super::check_result_from_cell_outcome(
                            executed.outcome,
                            &executed.account,
                            None,
                        ),
                        None,
                    ),
                };
                PlannedCheckBatchOutcome {
                    results: HashMap::from([(request.item.index, result)]),
                    request: Some(c.write_lane.wire(orch, id, &identity)),
                    delta,
                    store_dir: Some(request.store_dir.clone()),
                }
            }
            None => {
                if matches!(row.state.as_str(), "running" | "granted") {
                    return Err("the row reports a granted write execution but its grant evidence is unavailable; waiting to learn, not placing a replacement".into());
                }
                // Only absence of any durable grant permits execution again.
                if let Some(by) = &row.cancel_requested_by {
                    settle(orch, &db, &row, cancelled_answer(&row, by)).await;
                    return Ok(());
                }
                request.producer = CheckProducer {
                    consumer: CheckConsumer::Write,
                    consumer_ref: id.to_string(),
                    request_id: Some(id.to_string()),
                };
                submit_with_progress(orch, db.clone(), id, request.clone(), c.write_lane.phase)
                    .await?
            }
        };
        if outcome
            .results
            .values()
            .any(|r| matches!(r, Err(super::super::CheckExecutionFailure::LeftRunning(_))))
        {
            return Err(
                "the granted write lane is still running; waiting for its executor to report"
                    .into(),
            );
        }
        let returned = Returned::capture(&outcome);
        checkpoint(&db, id, "returned", &returned).await?;
        c.returned = Some(returned);
    }
    if c.write_lane.phase == WriteLanePhase::LegacyFixer {
        checkpoint(&db, id, "legacy_discarded_delta", &LEGACY_PHASE_UNKNOWN).await?;
        checkpoint(
            &db,
            id,
            "legacy_successor_tool",
            &format!("legacy-write-upgrade:{id}"),
        )
        .await?;
        c.final_recording = request.item.recording_plan.clone();
    }
    if c.write_lane.phase != WriteLanePhase::Verifier && c.final_recording.is_none() {
        SETTLED.send_modify(|g| *g += 1);
        return Ok(());
    }
    let returned = c.returned.as_ref().unwrap();
    let mut raw = returned.outcome(orch, id, &c.write_lane);
    let recording = c
        .final_recording
        .clone()
        .or_else(|| request.item.recording_plan.clone())
        .ok_or("missing write recording plan")?;
    let coordinate = crate::execution::cache::SealedCheckCoordinate::new(
        &recording.evaluated_commit,
        &recording.defined_by_commit,
        &recording.tree_hash,
        &recording.input_hash,
    );
    let result = raw
        .results
        .remove(&request.item.index)
        .ok_or("missing returned lane result")?;
    let outcomes = super::super::run_planned_checks_with_board(
        db.clone(),
        &row.project_id,
        CheckRunCommit {
            evaluated: &recording.evaluated_commit,
            defined_by: &recording.defined_by_commit,
        },
        &recording.tree_hash,
        &recording.job_id,
        &[(recording.plan.clone(), recording.input_hash.clone())],
        &[coordinate],
        &[request.item.infra_scopes.clone()],
        &recording.tool_use_id,
        &request.producer,
        CheckExecMode::Shared,
        Some(orch),
        None,
        &request.item.env,
        true,
        move |_, _, _| {
            let result = result.clone();
            async move { result }
        },
        |_| {},
    )
    .await;
    let outcome = outcomes
        .into_iter()
        .next()
        .ok_or("write recorder returned no outcome")?;
    let manual = super::super::manual::manual_configured_check_result(
        &row.suite,
        &c.write_lane.project_key,
        recording.evaluated_commit,
        recording.tree_hash,
        recording.input_hash,
        Some(&recording.plan.command),
        outcome,
    );
    // The raw delta and exact result are already durable. Now both live and
    // retained deliveries may release custody, but never before this checkpoint.
    checkpoint(&db, id, "terminal", &manual).await?;
    checkpoint(&db, id, "recorded", &true).await?;
    if c.write_lane.phase == WriteLanePhase::LegacyFixer {
        let c = context(&db, id)
            .await?
            .ok_or("legacy context disappeared")?;
        let row = load_request_by_id(&db, id)
            .await?
            .ok_or("legacy row disappeared")?;
        return finish_legacy(orch, &db, &row, &c).await;
    }
    settle(
        orch,
        &db,
        &row,
        CheckRequestAnswer::Complete {
            result: Box::new(manual.clone()),
        },
    )
    .await;
    let c = context(&db, id)
        .await?
        .ok_or("write context disappeared after recording")?;
    deliver_checkpointed(orch, &row, &c, manual.observation_id.as_deref()).await?;
    SETTLED.send_modify(|g| *g += 1);
    Ok(())
}

async fn deliver_checkpointed(
    orch: &Orchestrator,
    row: &CheckRequestRow,
    c: &Context,
    observation: Option<&str>,
) -> Result<(), String> {
    if let Some(identity) = c.returned.as_ref().and_then(|r| r.identity.as_ref()) {
        match observation {
            Some(observation)=>orch.fleet.deliver_check_consumer(identity,&row.suite,"write",&row.id,observation).await?,
            None=>orch.fleet.withdraw_check_consumer(identity,&row.suite,"write",&row.id,
                "durable write result produced no accepted observation; its full result and delta remain on the request row").await?,
        }
    }
    Ok(())
}

static FINALIZERS: LazyLock<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

/// The wave supplies a proof-carrying recording coordinate only after all folds
/// and convergence reruns. Execution coordinates in write_lane remain immutable.
pub(crate) async fn finalize_planned_check_request(
    orch: &Orchestrator,
    key: &str,
    request: &PlannedCheckRequest,
) -> Result<PlannedCheckBatchOutcome, String> {
    let db = request_db(orch, &request.project_id).await?;
    let id = filing_id(&request.project_id, key);
    let lock = FINALIZERS
        .lock()
        .await
        .entry(id.clone())
        .or_default()
        .clone();
    let _guard = lock.lock().await;
    let c = context(&db, &id)
        .await?
        .ok_or("write phase has not been filed")?;
    if c.returned.is_none() {
        let row = load_request_by_id(&db, &id)
            .await?
            .ok_or("write phase disappeared")?;
        if !row.is_open() {
            // Loss/cancellation returned no command result. Keep the wave's
            // existing terminal failure; there is nothing to record or fold.
            return Ok(PlannedCheckBatchOutcome {
                results: HashMap::new(),
                request: None,
                delta: None,
                store_dir: None,
            });
        }
        return Err("write phase has no durable result to finalize".into());
    }
    let mut recording = request
        .item
        .recording_plan
        .clone()
        .ok_or("final coordinate lacks its recording proof")?;
    if recording.plan.name != c.write_lane.item.name
        || recording.defined_by_commit
            != c.write_lane
                .item
                .recording_plan
                .as_ref()
                .unwrap()
                .defined_by_commit
    {
        return Err("final recording proof names a different suite or definition".into());
    }
    recording.producer = CheckProducer {
        consumer: CheckConsumer::Write,
        consumer_ref: id.clone(),
        request_id: Some(id.clone()),
    };
    if let Some(previous) = &c.final_recording {
        if previous.evaluated_commit != recording.evaluated_commit
            || previous.input_hash != recording.input_hash
        {
            return Err("a write phase already has a different final recording proof".into());
        }
    } else {
        checkpoint(&db, &id, "final_recording", &recording).await?;
    }
    drive_inner(orch, db.clone(), &id, None).await?;
    let c = context(&db, &id).await?.ok_or("write phase disappeared")?;
    if !c.recorded {
        return Err("write phase did not finish recording".into());
    }
    Ok(c.returned
        .as_ref()
        .unwrap()
        .outcome(orch, &id, &c.write_lane))
}

/// A later fold invalidated this phase, or the wave ended before it could
/// publish. Its raw result remains evidence, but it cannot fabricate a verdict.
pub(crate) async fn supersede_planned_check_request(
    orch: &Orchestrator,
    project_id: &str,
    key: &str,
    reason: &str,
) -> Result<(), String> {
    let db = request_db(orch, project_id).await?;
    let id = filing_id(project_id, key);
    let Some(row) = load_request_by_id(&db, &id).await? else {
        return Ok(());
    };
    if !row.is_open() {
        return Ok(());
    }
    let c = context(&db, &id)
        .await?
        .ok_or("write phase lacks its execution context")?;
    settle(
        orch,
        &db,
        &row,
        CheckRequestAnswer::Cancelled {
            reason: reason.to_string(),
        },
    )
    .await;
    if let Some(identity) = c.returned.and_then(|r| r.identity) {
        orch.fleet
            .withdraw_check_consumer(&identity, &row.suite, "write", &id, reason)
            .await?;
    }
    Ok(())
}

/// Delivery can lag the terminal row across a crash. Terminal write rows are
/// therefore revisited without placing or recording another execution.
pub(super) async fn rearm_deliveries(orch: &Orchestrator, db: &Arc<LocalDb>) {
    let rows=db.query_all(format!("SELECT {ROW_COLUMNS} FROM check_requests WHERE consumer='write' AND state IN ('recorded','cancelled') AND COALESCE(driven_by,?1)=?1"),
        (this_runner(),),CheckRequestRow::from_row).await;
    if let Ok(rows) = rows {
        for row in rows {
            if let Ok(Some(c)) = context(db, &row.id).await {
                if c.write_lane.phase == WriteLanePhase::LegacyFixer && c.recorded {
                    spawn(orch, db.clone(), row.id.clone(), Drive::Planned(None));
                    continue;
                }
                if let Err(error) =
                    deliver_checkpointed(orch, &row, &c, row.observation_id.as_deref()).await
                {
                    log::warn!("write request {} retained delivery: {error}", row.id);
                }
            }
        }
    }
}

async fn submit_with_progress(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    id: &str,
    mut request: PlannedCheckRequest,
    phase: WriteLanePhase,
) -> Result<PlannedCheckBatchOutcome, String> {
    let (progress, writer) = progress_writer(db, id.to_string());
    let recording = request
        .item
        .recording_plan
        .as_ref()
        .ok_or("write request lacks recording plan")?;
    let mut board = request.status_board.clone().unwrap_or_else(|| {
        super::super::CheckStatusBoard::new(
            &[(recording.plan.clone(), recording.input_hash.clone())],
            Arc::new(|_, _| {}),
        )
    });
    let previous = board.notify.clone();
    let request_cell = board.request.clone();
    board.notify = Arc::new(move |checks, phase| {
        progress(super::super::manual::manual_progress_update(
            &checks,
            phase.clone(),
            request_cell.lock().unwrap().clone(),
        ));
        previous(checks, phase);
    });
    request.status_board = Some(board);
    let result = match phase {
        WriteLanePhase::Verifier => super::super::submit_planned_check(orch, request).await,
        _ => super::super::submit_planned_fix(orch, request).await,
    };
    let _ = writer.await;
    result
}

/// Old granted write lanes predate wave snapshots. Their grant is still
/// authoritative: import its immutable contract, never place it a second time.
/// A legacy fixer records its old evidence but discards the unknown-phase
/// delta, then resumes as a fresh current-head wave for the same suite. Old
/// claims cannot distinguish an initial fold from non-folding convergence.
pub(super) async fn import_legacy(orch: &Orchestrator) {
    let claims = match crate::storage::check_lane_adoptions::open(&orch.db.local).await {
        Ok(claims) => claims,
        Err(error) => {
            log::error!("legacy write claims unreadable: {error}");
            return;
        }
    };
    for claim in claims.into_iter().filter(|c| c.consumer == "write") {
        if let Err(error) = import_legacy_claim(orch, &claim).await {
            log::error!(
                "legacy write grant {} retained without restart cancellation: {error}",
                claim.request_id
            );
        }
    }
}

async fn import_legacy_claim(
    orch: &Orchestrator,
    claim: &crate::storage::check_lane_adoptions::Claim,
) -> Result<(), String> {
    let db = request_db(orch, &claim.project_id).await?;
    if load_request_by_id(&db, &claim.consumer_ref)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let project_key = db
        .query_text(
            "SELECT key FROM projects WHERE id=?1",
            (claim.project_id.clone(),),
        )
        .await
        .map_err(|e| e.to_string())?
        .ok_or("legacy write project missing")?;
    let repository = db
        .query_text(
            "SELECT repo_path FROM projects WHERE id=?1",
            (claim.project_id.clone(),),
        )
        .await
        .map_err(|e| e.to_string())?
        .ok_or("legacy write repository missing")?;
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(&repository));
    // jj coordinates live in `store`, but their Git objects live in the
    // project's backing repository, including commits never checked out there.
    let contract = super::super::load_checks_contract_at_commit_detailed(
        std::path::Path::new(&repository),
        &claim.commit_sha,
    )
    .await
    .map_err(|error| error.manual_diagnostic(&claim.commit_sha))?;
    for (ordinal, name) in claim.check_names.iter().enumerate() {
        let mut recording: super::super::CheckAdoptionRecordingPlan = serde_json::from_value(
            claim
                .recording_plans
                .get(ordinal)
                .cloned()
                .ok_or("legacy recording plan missing")?,
        )
        .map_err(|e| format!("legacy recording contract cannot be adopted: {e}"))?;
        if recording.evaluated_commit != claim.commit_sha
            || claim.input_hashes.get(ordinal) != Some(&recording.input_hash)
            || recording.plan.name != *name
        {
            return Err("legacy recording contract disagrees with granted coordinates".into());
        }
        let configured = contract
            .contract
            .checks
            .get(name)
            .ok_or("legacy suite absent from immutable declaration")?
            .clone();
        let phase = if configured.fixes {
            WriteLanePhase::LegacyFixer
        } else {
            WriteLanePhase::Verifier
        };
        let key = format!("legacy:{}:{}:{name}", claim.request_id, claim.attempt_id);
        let id = filing_id(&claim.project_id, &key);
        recording.producer = CheckProducer {
            consumer: CheckConsumer::Write,
            consumer_ref: id.clone(),
            request_id: Some(id.clone()),
        };
        let request = PlannedCheckRequest {
            producer: recording.producer.clone(),
            project_id: claim.project_id.clone(),
            repository: repository.clone(),
            store_dir: store.clone(),
            sealed_commit: claim.commit_sha.clone(),
            requesting_job_id: recording.job_id.clone(),
            owner: CellOwnerRef {
                project_id: claim.project_id.clone(),
                project_key: Some(project_key.clone()),
                issue_number: None,
                job_id: Some(recording.job_id.clone()),
                execution_seq: None,
                node_kind: Some("recovered-write-check".into()),
            },
            affinity_key: None,
            priority: CellPriority::WriteCheck,
            env: Vec::new(),
            item: PlannedCheckBatchItem {
                recording_plan: Some(recording.clone()),
                index: ordinal,
                name: name.clone(),
                input_hash: recording.input_hash.clone(),
                resource_identity: super::super::CheckResourceIdentityInput::Configured {
                    name: name.clone(),
                    check: configured.clone(),
                },
                command: recording.plan.command.clone(),
                stream_id: recording.tool_use_id.clone(),
                env: Vec::new(),
                verdict_environment_names: recording.plan.verdict_environment_names.clone(),
                timeout_ms: 0,
                executor: configured.executor.clone(),
                verdict_platforms: recording.plan.verdict_platforms.clone(),
                resource_class: recording.plan.resource_class,
                infra_scopes: crate::execution::cache::InfraScopes::head_only(
                    &recording.job_id,
                    &claim.commit_sha,
                ),
                avoid_executor_ids: Vec::new(),
            },
            run_context: None,
            status_board: None,
            on_complete: None,
            purpose: None,
            publication: None,
        };
        let snapshot = Snapshot::capture(&request, phase)?;
        let json = serde_json::to_string(&Context {
            write_lane: snapshot,
            returned: None,
            recorded: false,
            fold: None,
            final_recording: None,
            terminal: None,
        })
        .map_err(|e| e.to_string())?;
        let spec = CheckRequestSpec {
            consumer: CheckConsumer::Write,
            cadence: Some("write"),
            priority: CellPriority::WriteCheck,
            project_id: claim.project_id.clone(),
            project_key: project_key.clone(),
            suite: name.clone(),
            revision: Some(claim.commit_sha.clone()),
            subject_job_id: recording.job_id.clone(),
            placement: None,
            retry: false,
            requester_ref: claim.consumer_ref.clone(),
            requester_job_id: Some(recording.job_id.clone()),
        };
        accept_with_context(
            &db,
            &spec,
            name,
            &Coordinate::Commit(claim.commit_sha.clone()),
            Some(&id),
            Some(&json),
        )
        .await?;
        db.execute("UPDATE check_requests SET request_id=?2,attempt_id=?3,state='running',executor=?4,tree_hash=?5,input_hash=?6,waiting_to_learn=?7 WHERE id=?1 AND state IN ('accepted','queued','running','granted')",
            cairn_db::turso::params![id.as_str(),claim.request_id.as_str(),claim.attempt_id.as_str(),claim.machine.as_str(),
                recording.tree_hash.as_str(),recording.input_hash.as_str(),"legacy granted write lane imported; waiting for retained result or explicit death evidence"])
            .await.map_err(|e|e.to_string())?;
        crate::storage::check_lane_adoptions::transfer_legacy_write_waiter(
            &orch.db.local,
            &crate::storage::check_lane_adoptions::Waiter {
                request_id: claim.request_id.clone(),
                attempt_id: claim.attempt_id.clone(),
                check_name: name.clone(),
                item_index: ordinal,
                consumer: "write".into(),
                consumer_ref: id.clone(),
                state: "running".into(),
                observation_id: None,
                evidence: None,
                updated_ms: now_ms(),
            },
            &claim.consumer_ref,
        )
        .await
        .map_err(|e| e.to_string())?;
        // The new waiter exists first. Refreshing the fleet's consumer view
        // cannot release custody between the legacy and durable owners.
        orch.fleet
            .withdraw_check_consumer(
                &(claim.request_id.clone(), claim.attempt_id.clone()),
                name,
                "write",
                &claim.consumer_ref,
                "legacy write consumer transferred to its durable check request",
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

const LEGACY_PHASE_UNKNOWN: &str = "granted before durable wave continuations; phase unknown";

async fn finish_legacy(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    row: &CheckRequestRow,
    c: &Context,
) -> Result<(), String> {
    let existing=db.query_opt_text("SELECT json_extract(context_json,'$.legacy_successor_wave') FROM check_requests WHERE id=?1",
        (row.id.clone(),)).await.map_err(|e|e.to_string())?;
    let successor = match existing {
        Some(wave) => Ok(wave),
        None => {
            let job = row
                .subject_job_id
                .as_deref()
                .ok_or("legacy write job disappeared")?;
            super::super::write_wave::resume_legacy_write(
                orch,
                job,
                &format!("legacy-write-upgrade:{}", row.id),
                &row.suite,
            )
            .await
        }
    };
    let reason=match successor {
        Ok(wave)=>{
            checkpoint(db,&row.id,"legacy_successor_wave",&wave).await?;
            format!("superseded: {LEGACY_PHASE_UNKNOWN}; old delta discarded unfolded; successor wave {wave}")
        },
        Err(reason) if reason.starts_with("superseded:")=>{
            checkpoint(db,&row.id,"legacy_successor_retired",&reason).await?;
            format!("{reason}; {LEGACY_PHASE_UNKNOWN}; old delta discarded unfolded")
        },
        Err(error)=>return Err(format!("legacy result is durable and its old delta will not fold; automatically filing its same-suite durable successor: {error}")),
    };
    settle(orch, db, row, CheckRequestAnswer::Cancelled { reason }).await;
    deliver_checkpointed(orch, row, c, row.observation_id.as_deref()).await
}

#[cfg(test)]
pub(crate) async fn install_granted_write_for_test(
    db: &LocalDb,
    id: &str,
    claim: &crate::storage::check_lane_adoptions::Claim,
) {
    let recording: super::super::CheckAdoptionRecordingPlan =
        serde_json::from_value(claim.recording_plans[0].clone()).unwrap();
    let plan = &recording.plan;
    let item = PlannedCheckBatchItem {
        recording_plan: Some(recording.clone()),
        index: 0,
        name: plan.name.clone(),
        input_hash: recording.input_hash.clone(),
        resource_identity: crate::execution::wire::CheckResourceIdentityInput::Checkpoint {
            name: plan.name.clone(),
            command: plan.command.clone(),
        },
        command: plan.command.clone(),
        stream_id: recording.tool_use_id.clone(),
        env: Vec::new(),
        verdict_environment_names: plan.verdict_environment_names.clone(),
        timeout_ms: 0,
        executor: None,
        verdict_platforms: plan.verdict_platforms.clone(),
        resource_class: plan.resource_class,
        infra_scopes: crate::execution::cache::InfraScopes::head_only(
            &recording.job_id,
            &claim.commit_sha,
        ),
        avoid_executor_ids: Vec::new(),
    };
    let snapshot = Snapshot {
        project_id: claim.project_id.clone(),
        project_key: "PA".into(),
        repository: "/fixture".into(),
        store_dir: PathBuf::from("/fixture"),
        sealed_commit: claim.commit_sha.clone(),
        requesting_job_id: recording.job_id.clone(),
        owner: CellOwnerRef {
            project_id: claim.project_id.clone(),
            project_key: Some("PA".into()),
            issue_number: None,
            job_id: Some(recording.job_id.clone()),
            execution_seq: None,
            node_kind: None,
        },
        affinity_key: None,
        priority: CellPriority::WriteCheck,
        item,
        run_context: None,
        transport: Vec::new(),
        phase: WriteLanePhase::Verifier,
    };
    let c = Context {
        write_lane: snapshot,
        returned: None,
        recorded: false,
        fold: None,
        final_recording: None,
        terminal: None,
    };
    db.execute(
        "UPDATE check_requests SET context_json=?2,commit_sha=?3 WHERE id=?1",
        cairn_db::turso::params![
            id,
            serde_json::to_string(&c).unwrap(),
            claim.commit_sha.clone()
        ],
    )
    .await
    .unwrap();
}
