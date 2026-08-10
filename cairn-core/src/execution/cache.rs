//! Checkpoint cache query operations.
//!
//! Caching system for command results at checkpoint nodes to avoid
//! re-executing expensive operations.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckResultIdentity {
    pub project_id: String,
    pub check_name: String,
    pub input_hash: String,
}

/// Tree listing for one node head: the latest row per check name at this sealed
/// tree. Powers the `/checks` projection and, through it, the settle-wait —
/// whatever this returns is what a lane renders and what "has this lane produced
/// a verdict?" is answered from.
///
/// Two row FAMILIES share `check_result_cache`, and `environment_fingerprint` is
/// the only thing that tells them apart:
///
/// * VERDICT rows, projected from an immutable observation. Their fingerprint is
///   the verdict environment identity that produced them — or `''` on rows
///   written before that identity existed.
/// * INFRASTRUCTURE rows, written by [`store_check_result`] purely for retry and
///   suppression diagnosis. Their fingerprint is the
///   [`infra_suppression_scope`] string, which binds the row to the one job and
///   commit that hit the failure so a stumble on one head cannot render on
///   another.
///
/// So the predicate below says: every verdict row at this tree, plus
/// infrastructure rows belonging to THIS head. It used to be spelled
/// `fingerprint = '' OR fingerprint = scope`, which stated the same thing back
/// when `''` was the only value a verdict row could carry. Once verdicts began
/// carrying a real environment identity, that spelling selected exactly the
/// NON-verdicts: every lane on every node rendered `pending` over a store full of
/// recorded green, and settle-waits called those lanes verdictless (CAIRN-3823).
/// Discriminate on the `infra:` prefix — the same discriminator
/// [`clear_infra_suppressions`] uses — never on the value a verdict happens to
/// carry today.
///
/// One tree can hold several verdicts for one check, one per environment that
/// ran it, so the newest row wins and the caller gets one row per check name.
pub(crate) fn list_check_results_for_head(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    job_id: &str,
    commit_sha: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let tree_hash = tree_hash.to_string();
    let scope = infra_suppression_scope(job_id, commit_sha);
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let tree_hash = tree_hash.clone();
            let scope = scope.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT c.project_id, c.tree_hash, c.input_hash, c.check_name, c.exit_code,
                                c.passed, c.output_tail, c.duration_ms, c.ran_at, c.target_results_json,
                                c.job_id, c.cached, c.failure_kind, c.executor_id, c.executor_device_id,
                                c.executor_connection_generation, c.executor_slot_id, c.executor_lease_epoch,
                                c.executor_started_at_unix_ms, c.executor_finished_at_unix_ms,
                                c.toolchain_fingerprint, c.infra_failure_streak, c.defined_by_commit_sha
                           FROM (
                                SELECT r.*,
                                       ROW_NUMBER() OVER (
                                           PARTITION BY r.check_name
                                           ORDER BY r.ran_at DESC, r.rowid DESC
                                       ) AS recency_rank
                                  FROM check_result_cache r
                                 WHERE r.project_id = ?1 AND r.tree_hash = ?2
                                   AND (r.environment_fingerprint NOT LIKE 'infra:%'
                                        OR r.environment_fingerprint = ?3)
                           ) c
                          WHERE c.recency_rank = 1
                          ORDER BY c.check_name ASC",
                        params![project_id.as_str(), tree_hash.as_str(), scope.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list head-scoped check result rows: {e}"))
    })
}

impl CheckResultIdentity {
    pub(crate) fn new(project_id: &str, check_name: &str, input_hash: &str) -> Self {
        Self {
            project_id: project_id.to_string(),
            check_name: check_name.to_string(),
            input_hash: input_hash.to_string(),
        }
    }
}

/// The immutable observation a run actually recorded, carried back from the write
/// so a caller reports the row it produced instead of re-deriving a key and
/// hoping a second lookup finds it.
///
/// Re-deriving is not a theoretical hazard. Legacy remote executors recorded an
/// empty environment fingerprint, which deliberately cannot match a requesting
/// machine's key. The recorder is the only reliable source of which row a run
/// wrote, including those diagnostic-only legacy rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordedCheckObservation {
    pub id: String,
    pub public_handle: String,
    /// When the source observation actually ran (Unix milliseconds). Cached aliases
    /// carry the source instant so every citation describes the same evidence.
    pub ran_at: i64,
    /// The environment identity this observation and its commit alias are keyed
    /// by. Empty for legacy remote executors, whose rows remain addressable by id
    /// for diagnosis but cannot be reused.
    pub environment_fingerprint: String,
    /// Whether this observation may suppress a later execution of the same
    /// triple. False keeps red, incomplete, or fingerprint-less verdicts out of
    /// the reuse path while still returning them to whoever asked for them.
    pub reusable: bool,
}

fn row_to_check_result(
    row: &cairn_db::turso::Row,
) -> Result<CheckResultCacheEntry, crate::storage::DbError> {
    Ok(CheckResultCacheEntry {
        project_id: row.text(0)?,
        tree_hash: row.text(1)?,
        input_hash: row.text(2)?,
        check_name: row.text(3)?,
        exit_code: row.i64(4)? as i32,
        passed: row.i64(5)? != 0,
        output_tail: row.text(6)?,
        duration_ms: row.i64(7)?,
        ran_at: row.i64(8)?,
        target_results_json: row.opt_text(9)?,
        job_id: row.opt_text(10)?,
        cached: row.opt_i64(11)?.map(|v| v != 0),
        failure_kind: row.opt_text(12)?,
        infra_failure_streak: row.opt_i64(21)?.unwrap_or(0),
        defined_by_commit_sha: row.opt_text(22)?,
        executor_id: row.opt_text(13)?,
        executor_device_id: row.opt_text(14)?,
        executor_connection_generation: row.opt_i64(15)?,
        executor_cell_id: row.opt_text(16)?,
        executor_lease_epoch: row.opt_i64(17)?,
        executor_started_at_unix_ms: row.opt_i64(18)?,
        executor_finished_at_unix_ms: row.opt_i64(19)?,
        toolchain_fingerprint: row.opt_text(20)?,
    })
}

/// Result of querying the checkpoint command cache for a job.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointCacheResult {
    command: String,
    exit_code: i32,
    commit_sha: String,
    is_valid: bool,
    ran_at: i32,
}

/// Get a cached project-declared check result by project, check name, and the
/// per-check INPUT hash (the content identity of just that check's impact-matched
/// files). Keying on the input hash — rather than the whole sealed tree — is what
/// lets a commit that touched none of a check's inputs reuse the stored verdict.
pub(crate) fn get_check_result(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
) -> Result<Option<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let check_name = check_name.to_string();
    let input_hash = input_hash.to_string();

    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let check_name = check_name.clone();
            let input_hash = input_hash.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT project_id, tree_hash, input_hash, check_name, exit_code,
                               passed, output_tail, duration_ms, ran_at, target_results_json,
                               job_id, cached, failure_kind, executor_id, executor_device_id,
                               executor_connection_generation, executor_slot_id, executor_lease_epoch,
                               executor_started_at_unix_ms, executor_finished_at_unix_ms,
                               toolchain_fingerprint, infra_failure_streak,
                               defined_by_commit_sha
                        FROM check_result_cache
                        WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                          AND passed = 1 AND failure_kind IS NULL
                        ",
                        params![
                            project_id.as_str(),
                            check_name.as_str(),
                            input_hash.as_str()
                        ],
                    )
                    .await?;

                rows.next()
                    .await?
                    .map(|row| row_to_check_result(&row))
                    .transpose()
            })
        })
        .await
        .map_err(|e| format!("Failed to load check result cache row: {e}"))
    })
}

/// Load the most recent reusable verdict admitted by the project's declared trust set.
pub(crate) struct ReusableCheckLookup<'a> {
    pub project_id: &'a str,
    pub check_name: &'a str,
    pub input_hash: &'a str,
    pub verdict_platforms: &'a [String],
    pub implementation_identity: &'a str,
    pub verdict_environment_hash: &'a str,
    pub result_schema_version: i64,
}

pub(crate) fn get_reusable_check_result(
    db: Arc<LocalDb>,
    lookup: ReusableCheckLookup<'_>,
) -> Result<Option<ReusableCheckResult>, String> {
    let ReusableCheckLookup {
        project_id,
        check_name,
        input_hash,
        verdict_platforms,
        implementation_identity,
        verdict_environment_hash,
        result_schema_version,
    } = lookup;
    if verdict_platforms.is_empty()
        || implementation_identity.is_empty()
        || verdict_environment_hash.is_empty()
        || result_schema_version <= 0
    {
        return Ok(None);
    }
    let keys = (
        project_id.to_string(),
        check_name.to_string(),
        input_hash.to_string(),
        verdict_platforms.to_vec(),
        implementation_identity.to_string(),
        verdict_environment_hash.to_string(),
    );
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let keys = keys.clone();
            Box::pin(async move {
                let placeholders = (0..keys.3.len()).map(|i| format!("?{}", i + 7))
                    .collect::<Vec<_>>().join(",");
                let sql = format!(
                    "SELECT c.project_id,c.tree_hash,c.input_hash,c.check_name,c.exit_code,
                            c.passed,c.output_tail,c.duration_ms,c.ran_at,c.target_results_json,
                            c.job_id,c.cached,c.failure_kind,c.executor_id,c.executor_device_id,
                            c.executor_connection_generation,c.executor_slot_id,c.executor_lease_epoch,
                            c.executor_started_at_unix_ms,c.executor_finished_at_unix_ms,
                            c.toolchain_fingerprint,c.infra_failure_streak,c.defined_by_commit_sha,
                            c.source_observation_id,o.environment_fingerprint
                       FROM check_result_cache c
                       JOIN check_result_observations o ON o.id=c.source_observation_id
                      WHERE c.project_id=?1 AND c.check_name=?2 AND c.input_hash=?3
                        AND c.result_schema_version=?4 AND o.runner_build_id=?5
                        AND o.verdict_environment_hash=?6
                        AND o.verdict_platform IN ({placeholders})
                        AND c.failure_kind IS NULL AND o.reusable=1
                        AND o.complete=1 AND o.failure_kind IS NULL
                        AND o.verdict IN ('passed','failed')
                        AND c.defined_by_commit_sha IS NOT NULL
                        AND o.defined_by_commit_sha IS NOT NULL
                      ORDER BY o.ran_at DESC, o.rowid DESC LIMIT 1"
                );
                let mut values: Vec<cairn_db::turso::Value> = vec![
                    keys.0.into(), keys.1.into(), keys.2.into(),
                    result_schema_version.into(), keys.4.into(), keys.5.into(),
                ];
                values.extend(keys.3.into_iter().map(Into::into));
                let mut rows = conn.query(&sql, values).await?;
                let Some(row) = rows.next().await? else { return Ok(None); };
                Ok(Some(ReusableCheckResult {
                    entry: row_to_check_result(&row)?,
                    source_observation_id: row.text(23)?,
                    environment_fingerprint: row.text(24)?,
                }))
            })
        }).await.map_err(|e| format!("Failed to load reusable check result: {e}"))
    })
}

/// Consecutive OBSERVED infrastructure failures one
/// `(project_id, check_name, input_hash)` triple may accumulate before Cairn
/// stops executing it.
///
/// An infrastructure failure is not a verdict, so [`get_check_result`] never
/// reuses its row — which means the triple re-executes on every subsequent
/// evaluation with nothing to stop it. Three attempts is enough to distinguish a
/// transient stumble (a slot that lost a race, a daemon mid-restart) from a
/// standing defect like the sccache adoption failure that struck three separate
/// times, and cheap enough that a genuinely transient failure still gets its
/// retries. It is a constant rather than a setting because an operator with a
/// broken toolchain needs the loop stopped, not a knob to tune.
///
/// **What this bounds, precisely: RETRIES taken after a failure has been
/// observed.** It is not a cap on the total number of times a command can run.
/// Evaluations that begin simultaneously, before any failure has been recorded,
/// are all admitted — at that instant nothing has come back and there is no
/// evidence to refuse them with, and refusing on suspicion would strip a healthy
/// check of its verdict and blame infrastructure for it (see
/// `concurrent_evaluations_of_a_healthy_triple_are_all_admitted`). So `K`
/// simultaneous first attempts cost `K` executions, after which the counter
/// governs and the triple converges on suppression rather than looping. Removing
/// that residue needs deduplication of identical concurrent work, not a stricter
/// counter: CAIRN-3271.
pub(crate) const OBSERVED_INFRA_FAILURE_BOUND: i64 = 3;

pub(crate) fn infra_suppression_scope(job_id: &str, commit_sha: &str) -> String {
    format!("infra:{}:{job_id}:{commit_sha}", job_id.len())
}

/// How one write moves a triple's consecutive-infrastructure-failure counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InfraStreakOp {
    /// A completed infrastructure failure. It OPENS the streak (`0` → `1`) but
    /// does not advance one that is already open: every retry past the first is
    /// counted by [`claim_check_execution`] at the moment it is RESERVED, so
    /// counting it again on completion would charge one attempt twice.
    OpenStreak,
    /// A genuine verdict — a pass, or a red the agent's own change owns. The
    /// substrate demonstrably works, so the streak and its escalation stamp go.
    Reset,
    /// A cache-hit re-stamp, which reports an older verdict onto the current tree
    /// rather than executing anything, and so must move nothing.
    Hold,
}

impl InfraStreakOp {
    /// The bound parameter form. `CASE` on a bound integer keeps the whole
    /// decision in one place in Rust rather than reconstructing it in SQL.
    fn as_param(self) -> i64 {
        match self {
            InfraStreakOp::OpenStreak => 1,
            InfraStreakOp::Reset => 0,
            InfraStreakOp::Hold => -1,
        }
    }
}

impl CheckResultCacheWrite {
    /// How this write moves the triple's counter. Derived from the payload rather
    /// than passed in by the caller, so no store site can forget to maintain it.
    pub(crate) fn infra_streak_op(&self) -> InfraStreakOp {
        if self.cached == Some(true) {
            return InfraStreakOp::Hold;
        }
        let infrastructure = !self.passed
            && self
                .failure_kind
                .as_deref()
                .and_then(crate::execution::checks::CheckFailureKind::from_stored)
                .is_some_and(crate::execution::checks::CheckFailureKind::is_infrastructure);
        if infrastructure {
            InfraStreakOp::OpenStreak
        } else {
            InfraStreakOp::Reset
        }
    }
}

/// The row of a triple that has infra-failed [`OBSERVED_INFRA_FAILURE_BOUND`] consecutive
/// times and must no longer be executed, or `None` when it may still run.
///
/// This is the read that bounds execution, and it is deliberately separate from
/// [`get_check_result`]: a hit there is a reusable VERDICT, while this is the
/// durable evidence that there is no verdict and no point looking for one.
/// Collapsing them would make a suppressed check look cached, which is the exact
/// lie the whole mechanism exists to avoid.
///
/// The whole row comes back because the caller needs two things from it: the
/// streak and the last infrastructure diagnostic (for the agent's message and the
/// operator's escalation), and every stored field, so the row can be re-stamped
/// onto the current tree and a suppressed check still appears in tree-keyed
/// listings instead of silently vanishing from the checklist.
pub(crate) fn get_suppressed_check_result(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    job_id: &str,
    commit_sha: &str,
) -> Result<Option<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let check_name = check_name.to_string();
    let input_hash = input_hash.to_string();
    let scope = infra_suppression_scope(job_id, commit_sha);

    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let check_name = check_name.clone();
            let input_hash = input_hash.clone();
            let scope = scope.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT project_id, tree_hash, input_hash, check_name, exit_code,
                               passed, output_tail, duration_ms, ran_at, target_results_json,
                               job_id, cached, failure_kind, executor_id, executor_device_id,
                               executor_connection_generation, executor_slot_id, executor_lease_epoch,
                               executor_started_at_unix_ms, executor_finished_at_unix_ms,
                               toolchain_fingerprint, infra_failure_streak,
                               defined_by_commit_sha
                        FROM check_result_cache
                        WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                          AND environment_fingerprint = ?5 AND result_schema_version = 0
                          AND infra_failure_streak >= ?4
                        ",
                        params![
                            project_id.as_str(),
                            check_name.as_str(),
                            input_hash.as_str(),
                            OBSERVED_INFRA_FAILURE_BOUND,
                            scope.as_str()
                        ],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| row_to_check_result(&row))
                    .transpose()
            })
        })
        .await
        .map_err(|e| format!("Failed to read check suppression state: {e}"))
    })
}

/// Claim the single operator escalation a suppressed triple is allowed, returning
/// `true` for the caller that won it.
///
/// The conditional `UPDATE ... WHERE infra_escalated_at IS NULL` is what makes
/// exactly-once a fact the database enforces rather than an invariant inferred
/// from the counter: two cadences can evaluate the same triple concurrently, and
/// only one of them writes the stamp.
pub(crate) fn claim_infra_escalation(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    job_id: &str,
    commit_sha: &str,
) -> Result<bool, String> {
    let project_id = project_id.to_string();
    let check_name = check_name.to_string();
    let input_hash = input_hash.to_string();
    let scope = infra_suppression_scope(job_id, commit_sha);

    run_checkpoint_cache_db(async move {
        db.write(|conn| {
            let project_id = project_id.clone();
            let check_name = check_name.clone();
            let input_hash = input_hash.clone();
            let scope = scope.clone();
            Box::pin(async move {
                let changed = conn
                    .execute(
                        "UPDATE check_result_cache SET infra_escalated_at = ?5
                         WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                           AND environment_fingerprint = ?6 AND result_schema_version = 0
                           AND infra_failure_streak >= ?4
                           AND infra_escalated_at IS NULL",
                        params![
                            project_id.as_str(),
                            check_name.as_str(),
                            input_hash.as_str(),
                            OBSERVED_INFRA_FAILURE_BOUND,
                            chrono::Utc::now().timestamp(),
                            scope.as_str()
                        ],
                    )
                    .await?;
                Ok(changed > 0)
            })
        })
        .await
        .map_err(|e| format!("Failed to claim check infrastructure escalation: {e}"))
    })
}

/// Whether a triple may execute right now — and, when it may and is already
/// infrastructure-failing, the reservation that makes it true for exactly one
/// caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckExecutionClaim {
    /// Execute. Either the triple has no infrastructure history at all, or this
    /// caller reserved one of its bounded retries.
    Clear,
    /// Do not execute: the retry budget is spent. The stored row is read where
    /// the suppression is rendered rather than carried through here, so this
    /// stays a decision and nothing more.
    Suppressed,
}

/// Reserve one bounded retry for a triple, or report it suppressed.
///
/// This is the permission to execute, and it is a single atomic statement on
/// purpose. Asking with a plain read and then counting the failure afterwards is
/// not the same question: the write cadence and the turn-end cadence can
/// evaluate one triple concurrently, so at `streak = BOUND - 1` both would
/// observe "not suppressed", both would launch, and the triple would cost
/// `BOUND + 1` executions — more with more concurrent evaluations. Reserving the
/// attempt in the same statement that decides makes the bound a fact the database
/// enforces rather than one inferred from a read that is already stale.
///
/// The reservation applies only to a triple with an OPEN streak (`>= 1`). A
/// healthy triple is not rationed: bounding executions at `streak = 0` would let
/// `BOUND` concurrent evaluations of a perfectly good check suppress it.
pub(crate) fn claim_check_execution(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    job_id: &str,
    commit_sha: &str,
) -> Result<CheckExecutionClaim, String> {
    let project_id = project_id.to_string();
    let check_name = check_name.to_string();
    let input_hash = input_hash.to_string();
    let scope = infra_suppression_scope(job_id, commit_sha);

    run_checkpoint_cache_db(async move {
        db.write(|conn| {
            let project_id = project_id.clone();
            let check_name = check_name.clone();
            let input_hash = input_hash.clone();
            let scope = scope.clone();
            Box::pin(async move {
                let changed = conn
                    .execute(
                        "UPDATE check_result_cache
                            SET infra_failure_streak = infra_failure_streak + 1
                          WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                            AND environment_fingerprint = ?5 AND result_schema_version = 0
                            AND infra_failure_streak >= 1
                            AND infra_failure_streak < ?4",
                        params![
                            project_id.as_str(),
                            check_name.as_str(),
                            input_hash.as_str(),
                            OBSERVED_INFRA_FAILURE_BOUND,
                            scope.as_str()
                        ],
                    )
                    .await?;
                if changed > 0 {
                    return Ok(CheckExecutionClaim::Clear);
                }

                // Nothing was reserved, which means one of two opposite things:
                // the triple has no open streak to ration (no row at all, or a
                // zero after a genuine verdict), or its budget is gone. Only the
                // stored counter distinguishes them.
                let mut rows = conn
                    .query(
                        "SELECT infra_failure_streak FROM check_result_cache
                          WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                            AND environment_fingerprint = ?4 AND result_schema_version = 0",
                        params![
                            project_id.as_str(),
                            check_name.as_str(),
                            input_hash.as_str(),
                            scope.as_str()
                        ],
                    )
                    .await?;
                let streak = match rows.next().await? {
                    Some(row) => row.opt_i64(0)?.unwrap_or(0),
                    None => 0,
                };
                if streak >= OBSERVED_INFRA_FAILURE_BOUND {
                    Ok(CheckExecutionClaim::Suppressed)
                } else {
                    Ok(CheckExecutionClaim::Clear)
                }
            })
        })
        .await
        .map_err(|e| format!("Failed to claim check execution: {e}"))
    })
}

/// Clear every infrastructure suppression, returning how many triples were freed.
///
/// This is the un-suppression trigger for a triple whose inputs have NOT changed.
/// A new input hash starts fresh at zero for free, because the counter lives on
/// the input-hash-keyed row; the case that needs a deliberate answer is the
/// operator who repaired the substrate while the inputs stayed byte-identical. A
/// Cairn restart is that answer: it is the same edge on which an operator's fix
/// to a toolchain, a daemon, or a wrapper actually takes effect, and it is
/// already the edge that re-arms the review cadence, so the clear and the
/// re-evaluation happen together rather than leaving the counter zeroed with
/// nothing scheduled to prove it.
pub(crate) async fn clear_infra_suppressions(db: &LocalDb) -> Result<u64, String> {
    db.execute(
        "UPDATE check_result_cache
         SET infra_failure_streak = 0, infra_escalated_at = NULL
         WHERE environment_fingerprint LIKE 'infra:%' AND result_schema_version = 0
           AND infra_failure_streak > 0",
        (),
    )
    .await
    .map_err(|e| format!("Failed to clear check infrastructure suppressions: {e}"))
}

/// Store a project-declared check result keyed by `(project_id, check_name,
/// input_hash)`. The row retains the latest visible verdict until a pass exists;
/// once a pass exists, later failed attempts cannot demote that reusable evidence.
/// A pass may refresh another pass, including cache-hit tree/job re-stamping while
/// preserving the original executor provenance. This is a latest-row store, not
/// complete attempt history.
pub fn store_check_result(db: Arc<LocalDb>, result: CheckResultCacheWrite) -> Result<(), String> {
    let streak_op = result.infra_streak_op().as_param();
    run_checkpoint_cache_db(async move {
        db.write(|conn| {
            let result = result.clone();
            Box::pin(async move {
                // Unix MILLISECONDS, the one unit this column speaks. The
                // observation projection that writes every verdict row into the
                // same table records the observation's own millisecond instant,
                // and recency rankings compare the two families directly.
                let ran_at = chrono::Utc::now().timestamp_millis();
                conn.execute(
                    "
                    INSERT INTO check_result_cache (
                        project_id, tree_hash, input_hash, check_name, exit_code, passed,
                        output_tail, duration_ms, ran_at, target_results_json, job_id, cached,
                        failure_kind, executor_id, executor_device_id,
                        executor_connection_generation, executor_slot_id, executor_lease_epoch,
                        executor_started_at_unix_ms, executor_finished_at_unix_ms,
                        toolchain_fingerprint, defined_by_commit_sha, infra_failure_streak,
                        environment_fingerprint, verdict_platform, verdict_arch, verdict_environment_hash
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                            ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?23,
                            CASE WHEN ?22 = 1 THEN 1 ELSE 0 END, ?24, ?25, ?26, ?27)
                    ON CONFLICT(project_id, check_name, input_hash, environment_fingerprint,
                                result_schema_version) DO UPDATE SET
                        tree_hash = excluded.tree_hash,
                        exit_code = excluded.exit_code,
                        passed = excluded.passed,
                        output_tail = excluded.output_tail,
                        duration_ms = excluded.duration_ms,
                        ran_at = CASE
                            WHEN excluded.cached = 1 THEN check_result_cache.ran_at
                            ELSE excluded.ran_at
                        END,
                        target_results_json = excluded.target_results_json,
                        job_id = excluded.job_id,
                        cached = excluded.cached,
                        failure_kind = excluded.failure_kind,
                        executor_id = excluded.executor_id,
                        executor_device_id = excluded.executor_device_id,
                        executor_connection_generation = excluded.executor_connection_generation,
                        executor_slot_id = excluded.executor_slot_id,
                        executor_lease_epoch = excluded.executor_lease_epoch,
                        executor_started_at_unix_ms = excluded.executor_started_at_unix_ms,
                        executor_finished_at_unix_ms = excluded.executor_finished_at_unix_ms,
                        toolchain_fingerprint = excluded.toolchain_fingerprint,
                        defined_by_commit_sha = excluded.defined_by_commit_sha,
                        infra_failure_streak = CASE
                            WHEN ?22 = 0 THEN 0
                            WHEN ?22 = 1 AND check_result_cache.infra_failure_streak < 1 THEN 1
                            ELSE check_result_cache.infra_failure_streak
                        END,
                        infra_escalated_at = CASE
                            WHEN ?22 = 0 THEN NULL
                            ELSE check_result_cache.infra_escalated_at
                        END,
                        verdict_platform = excluded.verdict_platform,
                        verdict_arch = excluded.verdict_arch,
                        verdict_environment_hash = excluded.verdict_environment_hash
                    WHERE check_result_cache.passed = 0 OR excluded.passed = 1
                    ",
                    params![
                        result.project_id.as_str(),
                        result.tree_hash.as_str(),
                        result.input_hash.as_str(),
                        result.check_name.as_str(),
                        result.exit_code as i64,
                        if result.passed { 1_i64 } else { 0_i64 },
                        result.output_tail.as_str(),
                        result.duration_ms,
                        ran_at,
                        result.target_results_json.as_deref(),
                        result.job_id.as_deref(),
                        result
                            .cached
                            .map(|cached| if cached { 1_i64 } else { 0_i64 }),
                        result.failure_kind.as_deref(),
                        result.executor_id.as_deref(),
                        result.executor_device_id.as_deref(),
                        result.executor_connection_generation,
                        result.executor_cell_id.as_deref(),
                        result.executor_lease_epoch,
                        result.executor_started_at_unix_ms,
                        result.executor_finished_at_unix_ms,
                        result.toolchain_fingerprint.as_deref(),
                        streak_op,
                        result.defined_by_commit_sha.as_deref(),
                        result.environment_fingerprint.as_str(),
                        result.verdict_platform.as_deref(), result.verdict_arch.as_deref(),
                        result.verdict_environment_hash.as_deref(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to store check result cache row: {e}"))
    })
}

/// List every cached check result for a project at one sealed tree identity,
/// ordered by check name. Powers the `/checks` projection and the PR-node
/// `### Systematic checks` section, which render all of a tree's verdicts at once.
#[cfg(test)]
pub(crate) fn list_check_results(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let tree_hash = tree_hash.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let tree_hash = tree_hash.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT project_id, tree_hash, input_hash, check_name, exit_code,
                               passed, output_tail, duration_ms, ran_at, target_results_json,
                               job_id, cached, failure_kind, executor_id, executor_device_id,
                               executor_connection_generation, executor_slot_id, executor_lease_epoch,
                               executor_started_at_unix_ms, executor_finished_at_unix_ms,
                               toolchain_fingerprint, infra_failure_streak,
                               defined_by_commit_sha
                        FROM check_result_cache
                        WHERE project_id = ?1 AND tree_hash = ?2
                        ORDER BY check_name ASC
                        ",
                        params![project_id.as_str(), tree_hash.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list check result cache rows: {e}"))
    })
}

/// List the MOST RECENT cached result per check name for a project, across every
/// sealed tree the project has ever run against. Where [`list_check_results`] is
/// keyed to one tree (the node/PR views, which show a single tree's verdicts),
/// this powers the project-settings Checks editor, which has no worktree in scope
/// and wants "how did each configured check last do".
///
/// One row per `check_name` is selected by ranking each check's rows by recency
/// (`ran_at`, tie-broken by `tree_hash`) and keeping the first. The tie-break
/// makes the pick deterministic when two trees share a `ran_at` second. Ordered
/// by check name for a stable render. Backed by
/// `idx_check_result_cache_project_check_recency`, whose column order matches the
/// partition and the ordering inside it.
///
/// This was an `NOT EXISTS` anti-join ("keep the row no newer row outranks"),
/// which is quadratic: SQLite could constrain the correlated inner scan only by
/// `project_id`, so it rescanned every project row once per project row. At the
/// 11,472 rows one real project had accumulated that is ~131M row visits, and it
/// measured 108.7 SECONDS against 45ms for a plain count of the same table
/// (CAIRN-3108). Because this runs inside the synchronous write-check planning
/// unit, that cost landed on the critical path of every source-touching commit.
/// The window form returns the identical rows in ~80ms. Keep it non-correlated.
///
/// One deliberate semantic difference: the anti-join returned BOTH rows when two
/// shared an exact (`ran_at`, `tree_hash`) — neither strictly outranks the other —
/// while `ROW_NUMBER` keeps one. Every caller keys by check name, so one row per
/// check is what they already assumed.
pub fn list_latest_check_results_for_project(
    db: Arc<LocalDb>,
    project_id: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT c.project_id, c.tree_hash, c.input_hash, c.check_name, c.exit_code,
                               c.passed, c.output_tail, c.duration_ms, c.ran_at,
                               c.target_results_json, c.job_id, c.cached, c.failure_kind, c.executor_id,
                               c.executor_device_id, c.executor_connection_generation,
                               c.executor_slot_id, c.executor_lease_epoch,
                               c.executor_started_at_unix_ms, c.executor_finished_at_unix_ms,
                               c.toolchain_fingerprint, c.infra_failure_streak,
                               c.defined_by_commit_sha
                        FROM (
                            SELECT r.*,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY r.check_name
                                       ORDER BY r.ran_at DESC, r.tree_hash DESC
                                   ) AS recency_rank
                            FROM check_result_cache r
                            WHERE r.project_id = ?1
                        ) c
                        WHERE c.recency_rank = 1
                        ORDER BY c.check_name ASC
                        ",
                        params![project_id.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list latest check result cache rows: {e}"))
    })
}

/// The most recent PASSING cached result per check name for one job.
///
/// This is the baseline selector for write-cadence check planning: "the last time
/// this check went green on THIS branch". Two properties matter and neither is
/// offered by [`list_latest_check_results_for_project`], which ranks rows across
/// the whole project regardless of job:
///
/// - **Job scoping.** A baseline tree is only a useful narrowing anchor if it sits
///   on the same branch lineage as the tree being planned. A row re-stamped by a
///   concurrently running sibling branch yields a delta that is the *symmetric
///   difference* of two unrelated trees — every file either branch touched — which
///   is routinely wider than the plain branch diff the narrowing was meant to beat.
/// - **Passing only.** Narrowing is anchored on a green verdict, so a failing row
///   must not displace the older green one it superseded. Ranking all rows and then
///   discarding a failing winner throws the branch's real baseline away and
///   collapses selection back to the full branch diff on every red run — exactly
///   when an agent is iterating and wants the tightest possible loop.
///
/// Non-correlated window form, for the reason spelled out on
/// [`list_latest_check_results_for_project`]: this runs inside the synchronous
/// write-check planning unit, on the critical path of every source-touching commit.
/// `idx_check_result_cache_job` constrains the scan to one job's rows.
pub(crate) fn list_latest_passing_check_results_for_job(
    db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let job_id = job_id.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT c.project_id, c.tree_hash, c.input_hash, c.check_name, c.exit_code,
                               c.passed, c.output_tail, c.duration_ms, c.ran_at,
                               c.target_results_json, c.job_id, c.cached, c.failure_kind, c.executor_id,
                               c.executor_device_id, c.executor_connection_generation,
                               c.executor_slot_id, c.executor_lease_epoch,
                               c.executor_started_at_unix_ms, c.executor_finished_at_unix_ms,
                               c.toolchain_fingerprint, c.infra_failure_streak,
                               c.defined_by_commit_sha
                        FROM (
                            SELECT r.*,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY r.check_name
                                       ORDER BY r.ran_at DESC, r.tree_hash DESC, r.input_hash DESC
                                   ) AS recency_rank
                            FROM check_result_cache r
                            WHERE r.job_id = ?1 AND r.passed = 1
                        ) c
                        WHERE c.recency_rank = 1
                        ORDER BY c.check_name ASC
                        ",
                        params![job_id.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list latest passing job check result cache rows: {e}"))
    })
}

/// List the most recent cached result per check name for one job, independent of
/// the current worktree/tree pointer. This is the durable fallback for node-level
/// surfaces after worktree teardown or movement.
///
/// The synchronous bridge over [`latest_check_results_for_job`], for the blocking
/// cache path; callers already on a runtime await that one directly.
pub(crate) fn list_check_results_for_job(
    db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let job_id = job_id.to_string();
    run_checkpoint_cache_db(async move { latest_check_results_for_job(&db, &job_id).await })
}

/// The most recent cached result per check name for one job — one indexed query,
/// no worktree and no VCS. Cheap enough to render inside resume assembly, which
/// is why the attention wake body reads verdicts through here rather than through
/// the full `/checks` status projection.
pub(crate) async fn latest_check_results_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT c.project_id, c.tree_hash, c.input_hash, c.check_name, c.exit_code,
                           c.passed, c.output_tail, c.duration_ms, c.ran_at,
                           c.target_results_json, c.job_id, c.cached, c.failure_kind, c.executor_id,
                           c.executor_device_id, c.executor_connection_generation,
                           c.executor_slot_id, c.executor_lease_epoch,
                           c.executor_started_at_unix_ms, c.executor_finished_at_unix_ms,
                           c.toolchain_fingerprint, c.infra_failure_streak,
                           c.defined_by_commit_sha
                    FROM check_result_cache c
                    WHERE c.job_id = ?1
                      AND NOT EXISTS (
                          SELECT 1 FROM check_result_cache newer
                          WHERE newer.job_id = c.job_id
                            AND newer.check_name = c.check_name
                            AND (newer.ran_at > c.ran_at
                                 OR (newer.ran_at = c.ran_at
                                     AND newer.tree_hash > c.tree_hash)
                                 OR (newer.ran_at = c.ran_at
                                     AND newer.tree_hash = c.tree_hash
                                     AND newer.input_hash > c.input_hash))
                      )
                    ORDER BY c.check_name ASC
                    ",
                    params![job_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(row_to_check_result(&row)?);
            }
            Ok::<_, crate::storage::DbError>(out)
        })
    })
    .await
    .map_err(|e| format!("Failed to list job check result cache rows: {e}"))
}

/// Return rows attributable to one executor connection generation. This is the
/// bulk invalidation and inspection seam; cache identity itself remains independent
/// of executor provenance.
pub fn list_check_results_for_executor_generation(
    db: Arc<LocalDb>,
    project_id: &str,
    executor_id: &str,
    generation: i64,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let executor_id = executor_id.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let executor_id = executor_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT project_id, tree_hash, input_hash, check_name, exit_code,
                                passed, output_tail, duration_ms, ran_at, target_results_json,
                                job_id, cached, failure_kind, executor_id, executor_device_id,
                                executor_connection_generation, executor_slot_id, executor_lease_epoch,
                                executor_started_at_unix_ms, executor_finished_at_unix_ms,
                                toolchain_fingerprint, infra_failure_streak,
                                defined_by_commit_sha
                         FROM check_result_cache
                         WHERE project_id = ?1 AND executor_id = ?2
                           AND executor_connection_generation = ?3
                         ORDER BY check_name, input_hash",
                        params![project_id.as_str(), executor_id.as_str(), generation],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list executor-attributed check results: {e}"))
    })
}

/// Cached result for one project-declared check at one sealed tree identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckResultCacheEntry {
    pub(crate) project_id: String,
    pub(crate) tree_hash: String,
    /// Per-check input hash: the content identity of just this check's impact-
    /// matched files. The cache's real key (with project + check name).
    pub(crate) input_hash: String,
    pub(crate) check_name: String,
    pub(crate) exit_code: i32,
    pub(crate) passed: bool,
    pub(crate) output_tail: String,
    pub(crate) duration_ms: i64,
    pub(crate) ran_at: i64,
    pub(crate) target_results_json: Option<String>,
    pub(crate) job_id: Option<String>,
    pub(crate) cached: Option<bool>,
    /// Terminal classification of a FAILING check — `"timed_out"`,
    /// `"spawn_error"`, or `"killed"` — refining the binary `passed` verdict so
    /// abnormal deaths render as themselves. `None` for a pass, an ordinary
    /// non-zero exit, and legacy rows. See [`crate::execution::checks::CheckFailureKind`].
    pub(crate) failure_kind: Option<String>,
    /// Consecutive infrastructure failures recorded at this triple. `0` for a row
    /// whose latest evaluation produced a genuine verdict; at
    /// [`OBSERVED_INFRA_FAILURE_BOUND`] the triple is suppressed and no longer executed.
    pub(crate) infra_failure_streak: i64,
    pub(crate) executor_id: Option<String>,
    pub(crate) executor_device_id: Option<String>,
    pub(crate) executor_connection_generation: Option<i64>,
    pub(crate) executor_cell_id: Option<String>,
    pub(crate) executor_lease_epoch: Option<i64>,
    pub(crate) executor_started_at_unix_ms: Option<i64>,
    pub(crate) executor_finished_at_unix_ms: Option<i64>,
    pub(crate) toolchain_fingerprint: Option<String>,
    /// The commit whose `.cairn/config.yaml` declared the check this row's
    /// verdict came from. `None` on legacy rows written before provenance was
    /// recorded; such a row is diagnostic only and is never an exact reusable
    /// hit, because nothing proves which definition produced it.
    pub(crate) defined_by_commit_sha: Option<String>,
}

/// A reusable hot-cache result and the immutable observation that produced it.
#[derive(Debug, Clone)]
pub(crate) struct ReusableCheckResult {
    pub(crate) entry: CheckResultCacheEntry,
    pub(crate) source_observation_id: String,
    pub(crate) environment_fingerprint: String,
}

/// Write payload for a check-result cache row.
#[derive(Debug, Clone)]
pub struct CheckResultCacheWrite {
    pub project_id: String,
    pub tree_hash: String,
    /// Per-check input hash — see [`CheckResultCacheEntry::input_hash`].
    pub input_hash: String,
    pub check_name: String,
    pub exit_code: i32,
    pub passed: bool,
    pub output_tail: String,
    pub duration_ms: i64,
    pub target_results_json: Option<String>,
    pub job_id: Option<String>,
    pub cached: Option<bool>,
    /// Terminal classification of a FAILING check — see
    /// [`CheckResultCacheEntry::failure_kind`].
    pub failure_kind: Option<String>,
    pub executor_id: Option<String>,
    pub executor_device_id: Option<String>,
    pub executor_connection_generation: Option<i64>,
    pub executor_cell_id: Option<String>,
    pub executor_lease_epoch: Option<i64>,
    pub executor_started_at_unix_ms: Option<i64>,
    pub executor_finished_at_unix_ms: Option<i64>,
    pub toolchain_fingerprint: Option<String>,
    /// The commit whose `.cairn/config.yaml` declared this check — see
    /// [`CheckResultCacheEntry::defined_by_commit_sha`].
    pub defined_by_commit_sha: Option<String>,
    /// Empty for legacy verdict rows; a head-scoped namespace for infrastructure
    /// breaker rows so overlapping jobs and commits cannot mutate each other.
    pub environment_fingerprint: String,
    pub verdict_platform: Option<String>,
    pub verdict_arch: Option<String>,
    pub verdict_environment_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckTestResultRow {
    pub test_id: String,
    pub status: String,
    pub duration_ms: Option<i64>,
    pub attempt_count: Option<i64>,
    pub failure_excerpt: Option<String>,
    pub skip_reason: Option<String>,
    pub declaration_source: Option<String>,
    pub flaky: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FreshCheckObservationWrite {
    pub id: String,
    pub public_handle: String,
    pub project_id: String,
    /// The commit whose content this execution evaluated.
    pub commit_sha: String,
    /// The commit whose `.cairn/config.yaml` declared the executed check. The
    /// same commit as `commit_sha` for every cadence; they diverge only where a
    /// definition read from one commit is deliberately re-run against the commit
    /// a fix produced.
    pub defined_by_commit_sha: String,
    pub tree_hash: String,
    pub check_name: String,
    pub input_hash: String,
    pub environment_fingerprint: String,
    pub verdict_platform: Option<String>,
    pub verdict_arch: Option<String>,
    pub verdict_environment_hash: Option<String>,
    pub exit_code: i32,
    pub verdict: String,
    pub failure_kind: Option<String>,
    pub complete: bool,
    pub reusable: bool,
    pub non_reusable_reason: Option<String>,
    pub parser_version: i64,
    pub result_schema_version: i64,
    pub ran_at: i64,
    pub duration_ms: i64,
    pub job_id: Option<String>,
    pub run_id: Option<String>,
    pub cadence: String,
    pub executor_id: Option<String>,
    pub executor_device_id: Option<String>,
    pub executor_connection_generation: Option<i64>,
    pub executor_cell_id: Option<String>,
    pub executor_lease_epoch: Option<i64>,
    pub executor_started_at_unix_ms: Option<i64>,
    pub executor_finished_at_unix_ms: Option<i64>,
    pub runner_build_id: Option<String>,
    pub toolchain_fingerprint: Option<String>,
    pub output_tail: String,
    pub target_results_json: Option<String>,
    pub tests: Vec<CheckTestResultRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedCheckObservationWrite {
    pub project_id: String,
    /// The commit the reused verdict is being aliased ONTO.
    pub commit_sha: String,
    /// The commit whose `.cairn/config.yaml` declared the check at `commit_sha`
    /// — the definition this reuse was admitted under. The source observation
    /// keeps its own defining commit, untouched.
    pub defined_by_commit_sha: String,
    pub tree_hash: String,
    pub check_name: String,
    pub input_hash: String,
    pub environment_fingerprint: String,
    pub result_schema_version: i64,
    pub source_observation_id: String,
    pub evaluated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckResultObservationProjection {
    pub disposition: String,
    /// The defining commit recorded at the EVALUATED commit's alias.
    pub defined_by_commit_sha: Option<String>,
    /// The defining commit recorded on the SOURCE observation, which may be a
    /// different commit when the verdict was reused.
    pub source_defined_by_commit_sha: Option<String>,
    pub evaluated_tree_hash: String,
    pub evaluated_input_hash: String,
    pub evaluated_at: i64,
    pub observation_id: String,
    pub project_id: String,
    pub source_commit_sha: String,
    pub source_tree_hash: String,
    pub check_name: String,
    pub source_input_hash: String,
    pub environment_fingerprint: String,
    pub exit_code: i32,
    pub verdict: String,
    pub failure_kind: Option<String>,
    pub complete: bool,
    pub reusable: bool,
    pub non_reusable_reason: Option<String>,
    pub parser_version: i64,
    pub result_schema_version: i64,
    pub ran_at: i64,
    pub duration_ms: i64,
    pub job_id: Option<String>,
    pub run_id: Option<String>,
    pub cadence: String,
    pub executor_id: Option<String>,
    pub executor_device_id: Option<String>,
    pub executor_connection_generation: Option<i64>,
    pub executor_cell_id: Option<String>,
    pub executor_lease_epoch: Option<i64>,
    pub executor_started_at_unix_ms: Option<i64>,
    pub executor_finished_at_unix_ms: Option<i64>,
    pub runner_build_id: Option<String>,
    pub toolchain_fingerprint: Option<String>,
    pub output_tail: String,
    pub tests: Vec<CheckTestResultRow>,
    pub test_total: usize,
    pub test_offset: usize,
}

/// Atomically persist an immutable fresh observation, its test manifest, commit
/// alias, and hot projection. Reuse remains gated by the immutable observation.
#[allow(dead_code)]
pub(crate) fn record_fresh_check_observation(
    db: Arc<LocalDb>,
    observation: FreshCheckObservationWrite,
) -> Result<(), String> {
    run_checkpoint_cache_db(async move {
        db.write(|conn| {
            let observation = observation.clone();
            Box::pin(async move {
                // Keep immutable equality and insertion under Turso's positional
                // bind limit by carrying the trailing provenance as one value.
                let trailing_provenance = serde_json::json!({
                    "definedByCommitSha": observation.defined_by_commit_sha,
                    "publicHandle": observation.public_handle,
                    "verdictPlatform": observation.verdict_platform,
                    "verdictArch": observation.verdict_arch,
                    "verdictEnvironmentHash": observation.verdict_environment_hash,
                })
                .to_string();
                let mut existing = conn
                    .query(
                        "SELECT COUNT(*) FROM check_result_observations
                         WHERE id=?1 AND project_id=?2 AND commit_sha=?3 AND tree_hash=?4
                           AND check_name=?5 AND input_hash=?6 AND environment_fingerprint=?7
                           AND exit_code=?8 AND verdict=?9 AND failure_kind IS ?10
                           AND complete=?11 AND reusable=?12 AND non_reusable_reason IS ?13
                           AND parser_version=?14 AND result_schema_version=?15 AND ran_at=?16
                           AND duration_ms=?17 AND job_id IS ?18 AND run_id IS ?19 AND cadence=?20
                           AND executor_id IS ?21 AND executor_device_id IS ?22
                           AND executor_connection_generation IS ?23 AND executor_slot_id IS ?24
                           AND executor_lease_epoch IS ?25 AND executor_started_at_unix_ms IS ?26
                           AND executor_finished_at_unix_ms IS ?27 AND runner_build_id IS ?28
                           AND toolchain_fingerprint IS ?29 AND output_tail=?30
                           AND defined_by_commit_sha IS json_extract(?31,'$.definedByCommitSha')
                           AND public_handle IS json_extract(?31,'$.publicHandle')
                           AND verdict_platform IS json_extract(?31,'$.verdictPlatform')
                           AND verdict_arch IS json_extract(?31,'$.verdictArch')
                           AND verdict_environment_hash IS json_extract(?31,'$.verdictEnvironmentHash')",
                        params![observation.id.as_str(), observation.project_id.as_str(),
                            observation.commit_sha.as_str(), observation.tree_hash.as_str(),
                            observation.check_name.as_str(), observation.input_hash.as_str(),
                            observation.environment_fingerprint.as_str(), observation.exit_code as i64,
                            observation.verdict.as_str(), observation.failure_kind.as_deref(),
                            i64::from(observation.complete), i64::from(observation.reusable),
                            observation.non_reusable_reason.as_deref(), observation.parser_version,
                            observation.result_schema_version, observation.ran_at, observation.duration_ms,
                            observation.job_id.as_deref(), observation.run_id.as_deref(),
                            observation.cadence.as_str(), observation.executor_id.as_deref(),
                            observation.executor_device_id.as_deref(), observation.executor_connection_generation,
                            observation.executor_cell_id.as_deref(), observation.executor_lease_epoch,
                            observation.executor_started_at_unix_ms, observation.executor_finished_at_unix_ms,
                            observation.runner_build_id.as_deref(), observation.toolchain_fingerprint.as_deref(),
                            observation.output_tail.as_str(), trailing_provenance.as_str()],
                    )
                    .await?;
                let exact_existing = existing.next().await?.expect("COUNT returns a row").i64(0)? == 1;
                let mut any_existing = conn
                    .query("SELECT COUNT(*) FROM check_result_observations WHERE id=?1", params![observation.id.as_str()])
                    .await?;
                let id_exists = any_existing.next().await?.expect("COUNT returns a row").i64(0)? == 1;
                if id_exists {
                    if !exact_existing {
                        return Err(crate::storage::DbError::internal(format!(
                            "observation id {} already exists with different immutable fields",
                            observation.id
                        )));
                    }
                    let mut tests = conn.query(
                        "SELECT test_id,status,duration_ms,attempt_count,failure_excerpt,skip_reason,declaration_source,flaky
                           FROM check_test_results WHERE observation_id=?1 ORDER BY test_id",
                        params![observation.id.as_str()],
                    ).await?;
                    let mut stored = Vec::new();
                    while let Some(row) = tests.next().await? {
                        stored.push(CheckTestResultRow {
                            test_id: row.text(0)?, status: row.text(1)?, duration_ms: row.opt_i64(2)?,
                            attempt_count: row.opt_i64(3)?, failure_excerpt: row.opt_text(4)?,
                            skip_reason: row.opt_text(5)?, declaration_source: row.opt_text(6)?,
                            flaky: row.i64(7)? != 0,
                        });
                    }
                    let mut expected = observation.tests.clone();
                    expected.sort_by(|a, b| a.test_id.cmp(&b.test_id));
                    if stored != expected {
                        return Err(crate::storage::DbError::internal(format!(
                            "observation id {} already exists with a different test manifest",
                            observation.id
                        )));
                    }
                    return Ok(());
                }

                conn.execute(
                    "INSERT INTO check_result_observations (
                        id, project_id, commit_sha, tree_hash, check_name, input_hash,
                        environment_fingerprint, exit_code, verdict, failure_kind, complete,
                        reusable, non_reusable_reason, parser_version, result_schema_version,
                        ran_at, duration_ms, job_id, run_id, cadence, executor_id,
                        executor_device_id, executor_connection_generation, executor_slot_id,
                        executor_lease_epoch, executor_started_at_unix_ms,
                        executor_finished_at_unix_ms, runner_build_id, toolchain_fingerprint,
                        output_tail, defined_by_commit_sha, public_handle, verdict_platform,
                        verdict_arch, verdict_environment_hash
                    ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,
                              ?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,
                              json_extract(?31,'$.definedByCommitSha'),
                              json_extract(?31,'$.publicHandle'),
                              json_extract(?31,'$.verdictPlatform'),
                              json_extract(?31,'$.verdictArch'),
                              json_extract(?31,'$.verdictEnvironmentHash'))
                       ON CONFLICT(id) DO NOTHING",
                    Vec::<cairn_db::turso::Value>::from([
                        observation.id.as_str().into(), observation.project_id.as_str().into(),
                        observation.commit_sha.as_str().into(), observation.tree_hash.as_str().into(),
                        observation.check_name.as_str().into(), observation.input_hash.as_str().into(),
                        observation.environment_fingerprint.as_str().into(), (observation.exit_code as i64).into(),
                        observation.verdict.as_str().into(), observation.failure_kind.as_deref().into(),
                        i64::from(observation.complete).into(), i64::from(observation.reusable).into(),
                        observation.non_reusable_reason.as_deref().into(), observation.parser_version.into(),
                        observation.result_schema_version.into(), observation.ran_at.into(),
                        observation.duration_ms.into(), observation.job_id.as_deref().into(),
                        observation.run_id.as_deref().into(), observation.cadence.as_str().into(),
                        observation.executor_id.as_deref().into(), observation.executor_device_id.as_deref().into(),
                        observation.executor_connection_generation.into(), observation.executor_cell_id.as_deref().into(),
                        observation.executor_lease_epoch.into(), observation.executor_started_at_unix_ms.into(),
                        observation.executor_finished_at_unix_ms.into(), observation.runner_build_id.as_deref().into(),
                        observation.toolchain_fingerprint.as_deref().into(), observation.output_tail.as_str().into(),
                        trailing_provenance.into()
                    ]),
                ).await?;
                for test in &observation.tests {
                    conn.execute(
                        "INSERT INTO check_test_results (
                            observation_id, test_id, status, duration_ms, attempt_count,
                            failure_excerpt, skip_reason, declaration_source, flaky
                         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
                         ON CONFLICT(observation_id, test_id) DO NOTHING",
                        params![observation.id.as_str(), test.test_id.as_str(), test.status.as_str(),
                            test.duration_ms, test.attempt_count, test.failure_excerpt.as_deref(),
                            test.skip_reason.as_deref(), test.declaration_source.as_deref(),
                            i64::from(test.flaky)],
                    ).await?;
                }
                // A commit coordinate names the latest evaluation, not the
                // immutable evidence itself. An explicit retry at the same
                // coordinate supersedes this pointer while both source
                // observations remain immutable and independently citable.
                conn.execute(
                    "DELETE FROM check_result_commit_aliases
                      WHERE project_id=?1 AND commit_sha=?2 AND check_name=?3
                        AND environment_fingerprint=?4 AND result_schema_version=?5",
                    params![observation.project_id.as_str(), observation.commit_sha.as_str(),
                        observation.check_name.as_str(), observation.environment_fingerprint.as_str(),
                        observation.result_schema_version],
                ).await?;
                let alias_inserted = conn.execute(
                    "INSERT INTO check_result_commit_aliases (
                        project_id, commit_sha, check_name, environment_fingerprint,
                        result_schema_version, source_observation_id, tree_hash, input_hash,
                        disposition, evaluated_at, defined_by_commit_sha
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'fresh',?9,?10)
                     ON CONFLICT(project_id, commit_sha, check_name, environment_fingerprint,
                                 result_schema_version) DO NOTHING",
                    params![observation.project_id.as_str(), observation.commit_sha.as_str(),
                        observation.check_name.as_str(), observation.environment_fingerprint.as_str(),
                        observation.result_schema_version, observation.id.as_str(),
                        observation.tree_hash.as_str(), observation.input_hash.as_str(), observation.ran_at,
                        observation.defined_by_commit_sha.as_str()],
                ).await?;
                if alias_inserted != 1 {
                    return Err(crate::storage::DbError::internal(
                        "fresh observation could not establish its commit alias",
                    ));
                }
                conn.execute(
                        "INSERT INTO check_result_cache (
                            project_id, tree_hash, input_hash, check_name, environment_fingerprint,
                            result_schema_version, source_observation_id, exit_code, passed,
                            output_tail, duration_ms, ran_at, job_id, cached, failure_kind,
                            executor_id, executor_device_id, executor_connection_generation,
                            executor_slot_id, executor_lease_epoch, executor_started_at_unix_ms,
                            executor_finished_at_unix_ms, toolchain_fingerprint,
                            defined_by_commit_sha, verdict_platform, verdict_arch, verdict_environment_hash
                         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,0,?14,
                                   ?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26)
                         ON CONFLICT(project_id, check_name, input_hash, environment_fingerprint,
                                     result_schema_version) DO UPDATE SET
                            tree_hash=excluded.tree_hash, source_observation_id=excluded.source_observation_id,
                            exit_code=excluded.exit_code, passed=excluded.passed, output_tail=excluded.output_tail,
                            duration_ms=excluded.duration_ms, ran_at=excluded.ran_at, job_id=excluded.job_id,
                            cached=0, failure_kind=excluded.failure_kind, executor_id=excluded.executor_id,
                            executor_device_id=excluded.executor_device_id,
                            executor_connection_generation=excluded.executor_connection_generation,
                            executor_slot_id=excluded.executor_slot_id,
                            executor_lease_epoch=excluded.executor_lease_epoch,
                            executor_started_at_unix_ms=excluded.executor_started_at_unix_ms,
                            executor_finished_at_unix_ms=excluded.executor_finished_at_unix_ms,
                            toolchain_fingerprint=excluded.toolchain_fingerprint,
                            defined_by_commit_sha=excluded.defined_by_commit_sha,
                            verdict_platform=excluded.verdict_platform,
                            verdict_arch=excluded.verdict_arch,
                            verdict_environment_hash=excluded.verdict_environment_hash",
                        params![observation.project_id.as_str(), observation.tree_hash.as_str(),
                            observation.input_hash.as_str(), observation.check_name.as_str(),
                            observation.environment_fingerprint.as_str(), observation.result_schema_version,
                            observation.id.as_str(), observation.exit_code as i64,
                            i64::from(observation.verdict == "passed"), observation.output_tail.as_str(),
                            observation.duration_ms, observation.ran_at, observation.job_id.as_deref(),
                            observation.failure_kind.as_deref(), observation.executor_id.as_deref(),
                            observation.executor_device_id.as_deref(), observation.executor_connection_generation,
                            observation.executor_cell_id.as_deref(), observation.executor_lease_epoch,
                            observation.executor_started_at_unix_ms, observation.executor_finished_at_unix_ms,
                            observation.toolchain_fingerprint.as_deref(),
                            observation.defined_by_commit_sha.as_str(),
                            observation.verdict_platform.as_deref(), observation.verdict_arch.as_deref(),
                            observation.verdict_environment_hash.as_deref()],
                    ).await?;
                    conn.execute(
                        "UPDATE check_result_cache SET target_results_json=?1
                          WHERE project_id=?2 AND check_name=?3 AND input_hash=?4
                            AND environment_fingerprint=?5 AND result_schema_version=?6
                            AND source_observation_id=?7",
                        params![observation.target_results_json.as_deref(), observation.project_id.as_str(),
                            observation.check_name.as_str(), observation.input_hash.as_str(),
                            observation.environment_fingerprint.as_str(), observation.result_schema_version,
                            observation.id.as_str()],
                    ).await?;
                let infrastructure = observation
                    .failure_kind
                    .as_deref()
                    .and_then(crate::execution::checks::CheckFailureKind::from_stored)
                    .is_some_and(crate::execution::checks::CheckFailureKind::is_infrastructure);
                if !infrastructure {
                    if let Some(job_id) = observation.job_id.as_deref() {
                        let scope = infra_suppression_scope(job_id, &observation.commit_sha);
                        conn.execute(
                            "UPDATE check_result_cache
                                SET infra_failure_streak = 0, infra_escalated_at = NULL
                              WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                                AND environment_fingerprint = ?4
                                AND result_schema_version = 0",
                            params![
                                observation.project_id.as_str(),
                                observation.check_name.as_str(),
                                observation.input_hash.as_str(),
                                scope.as_str()
                            ],
                        )
                        .await?;
                    }
                }
                Ok(())
            })
        }).await.map_err(|e| format!("Failed to record fresh check observation: {e}"))
    })
}

/// Record reuse at another commit without mutating either the source observation
/// or its original timestamp/provenance.
#[allow(dead_code)]
pub(crate) fn record_cached_check_observation(
    db: Arc<LocalDb>,
    observation: CachedCheckObservationWrite,
) -> Result<(), String> {
    run_checkpoint_cache_db(async move {
        db.write(|conn| {
            let observation = observation.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO check_result_commit_aliases (
                        project_id, commit_sha, check_name, environment_fingerprint,
                        result_schema_version, source_observation_id, tree_hash, input_hash,
                        disposition, evaluated_at, defined_by_commit_sha
                     ) SELECT ?1,?2,?3,?4,?5,?6,?7,?8,'cached',?9,?10
                       WHERE EXISTS (
                         SELECT 1 FROM check_result_observations
                          WHERE id=?6 AND project_id=?1 AND check_name=?3
                            AND environment_fingerprint=?4 AND result_schema_version=?5
                            AND input_hash=?8 AND reusable=1)
                     ON CONFLICT(project_id, commit_sha, check_name, environment_fingerprint,
                                 result_schema_version) DO NOTHING",
                    params![
                        observation.project_id.as_str(),
                        observation.commit_sha.as_str(),
                        observation.check_name.as_str(),
                        observation.environment_fingerprint.as_str(),
                        observation.result_schema_version,
                        observation.source_observation_id.as_str(),
                        observation.tree_hash.as_str(),
                        observation.input_hash.as_str(),
                        observation.evaluated_at,
                        observation.defined_by_commit_sha.as_str()
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to record cached check observation: {e}"))
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn get_check_result_observation(
    db: Arc<LocalDb>,
    project_id: &str,
    commit_sha: &str,
    check_name: &str,
    environment_fingerprint: &str,
    result_schema_version: i64,
    status: Option<&str>,
    name: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<Option<CheckResultObservationProjection>, String> {
    let keys = (
        project_id.to_string(),
        commit_sha.to_string(),
        check_name.to_string(),
        environment_fingerprint.to_string(),
        status.map(str::to_string),
        name.map(str::to_string),
    );
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let keys = keys.clone();
            Box::pin(async move {
                let mut rows = conn.query(
                    "SELECT a.disposition,a.tree_hash,a.input_hash,a.evaluated_at,
                            o.id,o.project_id,o.commit_sha,o.tree_hash,o.check_name,o.input_hash,
                            o.environment_fingerprint,o.exit_code,o.verdict,o.failure_kind,o.complete,
                            o.reusable,o.non_reusable_reason,o.parser_version,o.result_schema_version,
                            o.ran_at,o.duration_ms,o.job_id,o.run_id,o.cadence,o.executor_id,
                            o.executor_device_id,o.executor_connection_generation,o.executor_slot_id,
                            o.executor_lease_epoch,o.executor_started_at_unix_ms,
                            o.executor_finished_at_unix_ms,o.runner_build_id,o.toolchain_fingerprint,
                            o.output_tail,a.defined_by_commit_sha,o.defined_by_commit_sha
                       FROM check_result_commit_aliases a
                       JOIN check_result_observations o ON o.id=a.source_observation_id
                      WHERE a.project_id=?1 AND a.commit_sha=?2 AND a.check_name=?3
                        AND a.environment_fingerprint=?4 AND a.result_schema_version=?5",
                    params![keys.0.as_str(),keys.1.as_str(),keys.2.as_str(),keys.3.as_str(),result_schema_version]
                ).await?;
                let Some(row) = rows.next().await? else { return Ok(None); };
                let observation_id = row.text(4)?;
                let mut projection = CheckResultObservationProjection {
                    disposition: row.text(0)?,
                    defined_by_commit_sha: row.opt_text(34)?,
                    source_defined_by_commit_sha: row.opt_text(35)?,
                    evaluated_tree_hash: row.text(1)?,
                    evaluated_input_hash: row.text(2)?, evaluated_at: row.i64(3)?,
                    observation_id: observation_id.clone(), project_id: row.text(5)?,
                    source_commit_sha: row.text(6)?, source_tree_hash: row.text(7)?,
                    check_name: row.text(8)?, source_input_hash: row.text(9)?,
                    environment_fingerprint: row.text(10)?, exit_code: row.i64(11)? as i32,
                    verdict: row.text(12)?, failure_kind: row.opt_text(13)?,
                    complete: row.i64(14)? != 0, reusable: row.i64(15)? != 0,
                    non_reusable_reason: row.opt_text(16)?, parser_version: row.i64(17)?,
                    result_schema_version: row.i64(18)?, ran_at: row.i64(19)?,
                    duration_ms: row.i64(20)?, job_id: row.opt_text(21)?, run_id: row.opt_text(22)?,
                    cadence: row.text(23)?, executor_id: row.opt_text(24)?,
                    executor_device_id: row.opt_text(25)?, executor_connection_generation: row.opt_i64(26)?,
                    executor_cell_id: row.opt_text(27)?, executor_lease_epoch: row.opt_i64(28)?,
                    executor_started_at_unix_ms: row.opt_i64(29)?,
                    executor_finished_at_unix_ms: row.opt_i64(30)?, runner_build_id: row.opt_text(31)?,
                    toolchain_fingerprint: row.opt_text(32)?, output_tail: row.text(33)?, tests: Vec::new(),
                    test_total: 0, test_offset: offset,
                };
                let mut totals = conn.query(
                    "SELECT COUNT(*) FROM check_test_results
                      WHERE observation_id=?1 AND (?2 IS NULL OR status=?2)
                        AND (?3 IS NULL OR instr(test_id,?3)>0)",
                    params![observation_id.as_str(), keys.4.as_deref(), keys.5.as_deref()]
                ).await?;
                projection.test_total = totals.next().await?.expect("COUNT always returns a row").i64(0)? as usize;
                let mut tests = conn.query(
                    "SELECT test_id,status,duration_ms,attempt_count,failure_excerpt,skip_reason,
                            declaration_source,flaky FROM check_test_results
                      WHERE observation_id=?1 AND (?2 IS NULL OR status=?2)
                        AND (?3 IS NULL OR instr(test_id,?3)>0)
                      ORDER BY test_id ASC LIMIT ?4 OFFSET ?5",
                    params![observation_id.as_str(), keys.4.as_deref(), keys.5.as_deref(), limit as i64, offset as i64]
                ).await?;
                while let Some(test) = tests.next().await? {
                    projection.tests.push(CheckTestResultRow {
                        test_id: test.text(0)?, status: test.text(1)?, duration_ms: test.opt_i64(2)?,
                        attempt_count: test.opt_i64(3)?, failure_excerpt: test.opt_text(4)?,
                        skip_reason: test.opt_text(5)?, declaration_source: test.opt_text(6)?,
                        flaky: test.i64(7)? != 0,
                    });
                }
                Ok(Some(projection))
            })
        }).await.map_err(|e| format!("Failed to load check observation: {e}"))
    })
}

/// Load one immutable observation by its internal key. Public callers decode an
/// opaque permalink handle before crossing this boundary; the UUID never renders.
pub(crate) fn get_check_result_observation_by_handle(
    db: Arc<LocalDb>,
    project_id: &str,
    public_handle: &str,
) -> Result<Option<CheckResultObservationProjection>, String> {
    let keys = (project_id.to_string(), public_handle.to_string());
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let keys = keys.clone();
            Box::pin(async move {
                let mut rows = conn.query(
                    "SELECT o.id,o.project_id,o.commit_sha,o.tree_hash,o.check_name,o.input_hash,
                            o.environment_fingerprint,o.exit_code,o.verdict,o.failure_kind,o.complete,
                            o.reusable,o.non_reusable_reason,o.parser_version,o.result_schema_version,
                            o.ran_at,o.duration_ms,o.job_id,o.run_id,o.cadence,o.executor_id,
                            o.executor_device_id,o.executor_connection_generation,o.executor_slot_id,
                            o.executor_lease_epoch,o.executor_started_at_unix_ms,
                            o.executor_finished_at_unix_ms,o.runner_build_id,o.toolchain_fingerprint,
                            o.output_tail,o.defined_by_commit_sha
                       FROM check_result_observations o
                      WHERE o.project_id=?1 AND o.public_handle=?2",
                    params![keys.0.as_str(), keys.1.as_str()],
                ).await?;
                let Some(row) = rows.next().await? else { return Ok(None); };
                let id = row.text(0)?;
                let mut projection = CheckResultObservationProjection {
                    disposition: "fresh".into(),
                    defined_by_commit_sha: row.opt_text(30)?,
                    source_defined_by_commit_sha: row.opt_text(30)?,
                    evaluated_tree_hash: row.text(3)?, evaluated_input_hash: row.text(5)?,
                    evaluated_at: row.i64(15)?, observation_id: id.clone(), project_id: row.text(1)?,
                    source_commit_sha: row.text(2)?, source_tree_hash: row.text(3)?,
                    check_name: row.text(4)?, source_input_hash: row.text(5)?,
                    environment_fingerprint: row.text(6)?, exit_code: row.i64(7)? as i32,
                    verdict: row.text(8)?, failure_kind: row.opt_text(9)?, complete: row.i64(10)? != 0,
                    reusable: row.i64(11)? != 0, non_reusable_reason: row.opt_text(12)?,
                    parser_version: row.i64(13)?, result_schema_version: row.i64(14)?,
                    ran_at: row.i64(15)?, duration_ms: row.i64(16)?, job_id: row.opt_text(17)?,
                    run_id: row.opt_text(18)?, cadence: row.text(19)?, executor_id: row.opt_text(20)?,
                    executor_device_id: row.opt_text(21)?, executor_connection_generation: row.opt_i64(22)?,
                    executor_cell_id: row.opt_text(23)?, executor_lease_epoch: row.opt_i64(24)?,
                    executor_started_at_unix_ms: row.opt_i64(25)?, executor_finished_at_unix_ms: row.opt_i64(26)?,
                    runner_build_id: row.opt_text(27)?, toolchain_fingerprint: row.opt_text(28)?,
                    output_tail: row.text(29)?, tests: Vec::new(), test_total: 0, test_offset: 0,
                };
                let mut tests = conn.query(
                    "SELECT test_id,status,duration_ms,attempt_count,failure_excerpt,skip_reason,
                            declaration_source,flaky FROM check_test_results
                      WHERE observation_id=?1 ORDER BY test_id ASC",
                    params![id.as_str()],
                ).await?;
                while let Some(test) = tests.next().await? {
                    projection.tests.push(CheckTestResultRow {
                        test_id: test.text(0)?, status: test.text(1)?, duration_ms: test.opt_i64(2)?,
                        attempt_count: test.opt_i64(3)?, failure_excerpt: test.opt_text(4)?,
                        skip_reason: test.opt_text(5)?, declaration_source: test.opt_text(6)?,
                        flaky: test.i64(7)? != 0,
                    });
                }
                projection.test_total = projection.tests.len();
                Ok(Some(projection))
            })
        }).await.map_err(|e| format!("Failed to load check observation: {e}"))
    })
}

pub(crate) fn get_check_observation_public_handle(
    db: Arc<LocalDb>,
    project_id: &str,
    observation_id: &str,
) -> Result<Option<String>, String> {
    let keys = (project_id.to_string(), observation_id.to_string());
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let keys = keys.clone();
            Box::pin(async move {
                let mut rows = conn.query(
                    "SELECT public_handle FROM check_result_observations WHERE project_id=?1 AND id=?2",
                    params![keys.0.as_str(), keys.1.as_str()],
                ).await?;
                match rows.next().await? {
                    Some(row) => Ok(row.opt_text(0)?),
                    None => Ok(None),
                }
            })
        }).await.map_err(|e| format!("Failed to load check observation handle: {e}"))
    })
}

#[cfg(test)]
fn get_exact_reusable_check_result(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    environment: &str,
    result_schema_version: i64,
) -> Result<Option<CheckResultCacheEntry>, String> {
    get_reusable_check_result(
        db,
        ReusableCheckLookup {
            project_id,
            check_name,
            input_hash,
            verdict_platforms: &["linux".into()],
            implementation_identity: cairn_common::check_environment::implementation_identity(),
            verdict_environment_hash: environment,
            result_schema_version,
        },
    )
    .map(|result| result.map(|result| result.entry))
}

#[cfg(test)]
fn get_reusable_check_observation_id(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    environment: &str,
    result_schema_version: i64,
) -> Result<Option<String>, String> {
    get_reusable_check_result(
        db,
        ReusableCheckLookup {
            project_id,
            check_name,
            input_hash,
            verdict_platforms: &["linux".into()],
            implementation_identity: cairn_common::check_environment::implementation_identity(),
            verdict_environment_hash: environment,
            result_schema_version,
        },
    )
    .map(|result| result.map(|result| result.source_observation_id))
}

/// Normalize a shell command string for stable cache key comparison.
pub(crate) fn normalize_command(cmd: &str) -> String {
    cmd.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Resolve a job's current runner-owned logical head. The durable branch and
/// project repository are the complete coordinate; process cwd never participates.
pub(crate) async fn resolve_job_logical_head(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<String, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id = job_id.to_string();
    let (branch, repository) = db
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.branch, p.repo_path
                         FROM jobs j
                         JOIN projects p ON p.id = j.project_id
                         WHERE j.id = ?1 AND j.branch IS NOT NULL
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::Row(format!(
                        "job {job_id} has no resolvable logical coordinate"
                    ))
                })?;
                Ok((row.text(0)?, row.text(1)?))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let repository = std::path::PathBuf::from(repository);
    let store = crate::jj::project_store_dir(&orch.config_dir, &repository);
    let coordinate_repository = if crate::jj::is_jj_dir(&store) {
        store
    } else {
        repository
    };
    cairn_vcs::resolve_coordinate(&coordinate_repository, &branch)
        .await
        .map_err(|error| format!("job branch '{branch}' is unresolvable: {error}"))
}

pub(crate) fn get_job_logical_head_sha(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<String, String> {
    let orch = orch.clone();
    let job_id = job_id.to_string();
    run_checkpoint_cache_db(async move { resolve_job_logical_head(&orch, &job_id).await })
}

/// Get the checkpoint cache result for a job.
/// Returns the cached CI/checkpoint command result if one exists.
pub fn get_checkpoint_cache_result_impl(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<CheckpointCacheResult>, String> {
    let db = run_checkpoint_cache_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id = job_id.to_string();
    let job_id_for_head = job_id.clone();
    let row = run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT c.command, c.exit_code, c.commit_sha, c.is_dirty,
                               c.ran_at
                        FROM checkpoint_command_cache c
                        WHERE c.job_id = ?1
                        ORDER BY c.ran_at DESC
                        LIMIT 1
                        ",
                        (job_id.as_str(),),
                    )
                    .await?;

                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };

                Ok(Some((
                    row.text(0)?,
                    row.i64(1)? as i32,
                    row.text(2)?,
                    row.i64(3)?,
                    row.i64(4)? as i32,
                )))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;

    let Some((command, exit_code, commit_sha, is_dirty, ran_at)) = row else {
        return Ok(None);
    };

    let current_sha = get_job_logical_head_sha(orch, &job_id_for_head).unwrap_or_default();
    let is_valid = commit_sha == current_sha && is_dirty == 0;

    Ok(Some(CheckpointCacheResult {
        command,
        exit_code,
        commit_sha: commit_sha[..7.min(commit_sha.len())].to_string(),
        is_valid,
        ran_at,
    }))
}

fn run_checkpoint_cache_db<T>(
    future: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    fn run<T>(future: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(future)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run(future))
            .join()
            .map_err(|_| "Checkpoint cache DB runtime thread panicked".to_string())?
    } else {
        run(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A passing write. The middle argument seeds both `tree_hash` and
    /// `input_hash` to the same value so tests that don't care about the
    /// distinction stay terse; tests that exercise the two independently set
    /// `input_hash` explicitly on the returned struct.
    fn test_result(project_id: &str, hash: &str, check_name: &str) -> CheckResultCacheWrite {
        CheckResultCacheWrite {
            project_id: project_id.to_string(),
            tree_hash: hash.to_string(),
            input_hash: hash.to_string(),
            check_name: check_name.to_string(),
            exit_code: 0,
            passed: true,
            output_tail: "ok".to_string(),
            duration_ms: 123,
            target_results_json: None,
            job_id: None,
            cached: None,
            failure_kind: None,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some(format!("commit-{hash}")),
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
        }
    }

    fn observation(id: &str, commit: &str, environment: &str) -> FreshCheckObservationWrite {
        FreshCheckObservationWrite {
            id: id.to_string(),
            public_handle: format!("{id:0<24}"),
            project_id: "project-a".to_string(),
            commit_sha: commit.to_string(),
            defined_by_commit_sha: commit.to_string(),
            tree_hash: format!("tree-{commit}"),
            check_name: "rust".to_string(),
            input_hash: "input-rust".to_string(),
            environment_fingerprint: environment.to_string(),
            verdict_platform: Some("linux".to_string()),
            verdict_arch: Some("x86_64".to_string()),
            verdict_environment_hash: Some(environment.to_string()),
            exit_code: 0,
            verdict: "passed".to_string(),
            failure_kind: None,
            complete: true,
            reusable: true,
            non_reusable_reason: None,
            parser_version: 1,
            result_schema_version: 1,
            ran_at: 100,
            duration_ms: 50,
            job_id: Some("job-1".to_string()),
            run_id: Some("run-1".to_string()),
            cadence: "write".to_string(),
            executor_id: Some("executor-1".to_string()),
            executor_device_id: Some("device-1".to_string()),
            executor_connection_generation: Some(2),
            executor_cell_id: Some("cell-1".to_string()),
            executor_lease_epoch: Some(3),
            executor_started_at_unix_ms: Some(90),
            executor_finished_at_unix_ms: Some(100),
            runner_build_id: Some(
                cairn_common::check_environment::implementation_identity().to_string(),
            ),
            toolchain_fingerprint: Some("tools-1".to_string()),
            output_tail: "ok".to_string(),
            target_results_json: None,
            tests: vec![CheckTestResultRow {
                test_id: "crate::passes".to_string(),
                status: "passed".to_string(),
                duration_ms: Some(10),
                attempt_count: Some(1),
                failure_excerpt: None,
                skip_reason: None,
                declaration_source: None,
                flaky: false,
            }],
        }
    }

    async fn cache_db() -> Arc<LocalDb> {
        let db = crate::storage::migrated_test_db("check-result-cache-test.db").await;
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-a', 'default', 'Project A', 'PA', '/tmp/project-a', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-b', 'default', 'Project B', 'PB', '/tmp/project-b', 1, 1);
            ",
        )
        .await
        .unwrap();
        Arc::new(db)
    }

    /// A failing write with an explicit classification, for the suppression
    /// tests. `kind` is the persisted `failure_kind` string.
    fn failing_result(
        project_id: &str,
        hash: &str,
        check_name: &str,
        kind: Option<&str>,
    ) -> CheckResultCacheWrite {
        CheckResultCacheWrite {
            exit_code: 1,
            passed: false,
            output_tail: match kind {
                Some(kind) => format!("diagnostic for {kind}"),
                None => "assertion failed".to_string(),
            },
            failure_kind: kind.map(str::to_string),
            cached: Some(false),
            environment_fingerprint: infra_suppression_scope("job-test", "commit-test"),
            ..test_result(project_id, hash, check_name)
        }
    }

    /// The stored counter for one triple. Read through the ordinary listing so the
    /// tests exercise the same column every surface reads.
    async fn streak_of(db: &Arc<LocalDb>, hash: &str, check_name: &str) -> i64 {
        list_check_results(db.clone(), "project-a", hash)
            .unwrap()
            .into_iter()
            .find(|row| row.check_name == check_name)
            .expect("the triple must have a stored row")
            .infra_failure_streak
    }

    fn suppressed(db: &Arc<LocalDb>, check_name: &str, input_hash: &str) -> bool {
        get_suppressed_check_result(
            db.clone(),
            "project-a",
            check_name,
            input_hash,
            "job-test",
            "commit-test",
        )
        .unwrap()
        .is_some()
    }

    /// One full evaluation of a triple whose command infrastructure-fails:
    /// reserve the attempt, and only then "execute" and store. Returns whether
    /// the command would have run.
    ///
    /// Tests must drive the counter through this rather than through
    /// [`store_check_result`] alone. A retry is counted when it is RESERVED, so a
    /// test that only stores models an attempt that was never admitted — it can
    /// never observe the bound, which is exactly the hole that let concurrent
    /// cadences overshoot it.
    fn evaluate_infra_failure(db: &Arc<LocalDb>, check_name: &str, input_hash: &str) -> bool {
        match claim_check_execution(
            db.clone(),
            "project-a",
            check_name,
            input_hash,
            "job-test",
            "commit-test",
        )
        .unwrap()
        {
            CheckExecutionClaim::Suppressed => false,
            CheckExecutionClaim::Clear => {
                store_check_result(
                    db.clone(),
                    failing_result("project-a", input_hash, check_name, Some("infrastructure")),
                )
                .unwrap();
                true
            }
        }
    }

    /// Spend a triple's entire retry budget, leaving it suppressed.
    fn drive_to_bound(db: &Arc<LocalDb>, check_name: &str, input_hash: &str) {
        drive_scope_to_bound(db, check_name, input_hash, "job-test", "commit-test");
    }

    fn drive_scope_to_bound(
        db: &Arc<LocalDb>,
        check_name: &str,
        input_hash: &str,
        job_id: &str,
        commit_sha: &str,
    ) {
        for _ in 1..=OBSERVED_INFRA_FAILURE_BOUND {
            assert!(matches!(
                claim_check_execution(
                    db.clone(),
                    "project-a",
                    check_name,
                    input_hash,
                    job_id,
                    commit_sha,
                )
                .unwrap(),
                CheckExecutionClaim::Clear
            ));
            let mut failure =
                failing_result("project-a", input_hash, check_name, Some("infrastructure"));
            failure.environment_fingerprint = infra_suppression_scope(job_id, commit_sha);
            store_check_result(db.clone(), failure).unwrap();
        }
    }

    /// The bound itself. Each consecutive infrastructure failure moves the counter
    /// one step, and the triple becomes suppressed exactly AT the bound — not
    /// before it, which would strip a transient stumble of its retries.
    #[tokio::test]
    async fn consecutive_infrastructure_failures_suppress_the_triple_at_the_bound() {
        let db = cache_db().await;
        for attempt in 1..=OBSERVED_INFRA_FAILURE_BOUND {
            assert!(
                evaluate_infra_failure(&db, "rust", "ih-rust"),
                "attempt {attempt} must still be admitted — stopping early would \
                 strip a transient failure of its retries"
            );
            assert_eq!(
                streak_of(&db, "ih-rust", "rust").await,
                attempt,
                "attempt {attempt} must advance the counter by exactly one"
            );
            assert_eq!(
                suppressed(&db, "rust", "ih-rust"),
                attempt >= OBSERVED_INFRA_FAILURE_BOUND,
                "suppression must begin at the bound and not before it"
            );
        }
        assert!(
            !evaluate_infra_failure(&db, "rust", "ih-rust"),
            "the evaluation after the bound must not execute"
        );

        // Suppression is not a verdict: the reusable-result lookup still misses,
        // so nothing downstream can mistake it for a cached green.
        assert!(get_check_result(db.clone(), "project-a", "rust", "ih-rust")
            .unwrap()
            .is_none());
    }

    /// Reproduces CAIRN-3583: one head exhausts the infrastructure retry budget,
    /// then a fresh head with byte-identical check inputs arrives after repair.
    #[tokio::test]
    async fn new_head_rearms_suppressed_check_with_unchanged_inputs() {
        let db = cache_db().await;
        drive_to_bound(&db, "rust", "ih-rust");

        assert!(matches!(
            claim_check_execution(
                db.clone(),
                "project-a",
                "rust",
                "ih-rust",
                "job-test",
                "commit-after-repair",
            )
            .unwrap(),
            CheckExecutionClaim::Clear
        ));
        assert!(
            get_suppressed_check_result(
                db.clone(),
                "project-a",
                "rust",
                "ih-rust",
                "job-test",
                "commit-after-repair",
            )
            .unwrap()
            .is_none(),
            "the repaired head must never inherit the old head's diagnostic"
        );

        let mut repaired = test_result("project-a", "tree-after-repair", "rust");
        repaired.input_hash = "ih-rust".to_string();
        store_check_result(db.clone(), repaired).unwrap();
        assert!(
            get_check_result(db, "project-a", "rust", "ih-rust")
                .unwrap()
                .is_some(),
            "the admitted lane must be able to replace suppression with a real verdict"
        );
    }

    /// A late result from an older head cannot mutate the newer head's breaker.
    /// Each scope spends its own retry budget and retains its own diagnostic even
    /// when claims and completions interleave.
    #[tokio::test]
    async fn overlapping_heads_cannot_reset_or_overwrite_each_other() {
        let db = cache_db().await;
        drive_to_bound(&db, "rust", "ih-rust");

        assert!(matches!(
            claim_check_execution(
                db.clone(),
                "project-a",
                "rust",
                "ih-rust",
                "job-test",
                "commit-new",
            )
            .unwrap(),
            CheckExecutionClaim::Clear
        ));

        let mut late_old = failing_result("project-a", "ih-rust", "rust", Some("infrastructure"));
        late_old.output_tail = "old-head diagnostic".to_string();
        store_check_result(db.clone(), late_old).unwrap();

        for attempt in 1..=OBSERVED_INFRA_FAILURE_BOUND {
            if attempt > 1 {
                assert!(matches!(
                    claim_check_execution(
                        db.clone(),
                        "project-a",
                        "rust",
                        "ih-rust",
                        "job-test",
                        "commit-new",
                    )
                    .unwrap(),
                    CheckExecutionClaim::Clear
                ));
            }
            let mut failure =
                failing_result("project-a", "ih-rust", "rust", Some("infrastructure"));
            failure.environment_fingerprint = infra_suppression_scope("job-test", "commit-new");
            failure.output_tail = "new-head diagnostic".to_string();
            store_check_result(db.clone(), failure).unwrap();
        }

        assert!(matches!(
            claim_check_execution(
                db.clone(),
                "project-a",
                "rust",
                "ih-rust",
                "job-test",
                "commit-new",
            )
            .unwrap(),
            CheckExecutionClaim::Suppressed
        ));
        let new_head = get_suppressed_check_result(
            db.clone(),
            "project-a",
            "rust",
            "ih-rust",
            "job-test",
            "commit-new",
        )
        .unwrap()
        .unwrap();
        assert_eq!(new_head.output_tail, "new-head diagnostic");
        let old_head = get_suppressed_check_result(
            db,
            "project-a",
            "rust",
            "ih-rust",
            "job-test",
            "commit-test",
        )
        .unwrap()
        .unwrap();
        assert_eq!(old_head.output_tail, "old-head diagnostic");
    }

    /// The specimen CAIRN-3823 was built from. A verdict recorded through the
    /// production writer carries the environment identity that produced it, and
    /// the surface that renders lanes must be able to see it. The predicate this
    /// replaced tested for the EMPTY fingerprint, so from the moment verdicts
    /// began carrying a real identity every lane on every node rendered
    /// `pending` over a store full of recorded green, and settle-waits declared
    /// those same lanes verdictless.
    #[tokio::test]
    async fn head_listing_returns_a_verdict_carrying_a_real_environment_identity() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-1", "commit-1", "env-a"))
            .unwrap();

        let rows =
            list_check_results_for_head(db, "project-a", "tree-commit-1", "job-1", "commit-1")
                .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.check_name.as_str())
                .collect::<Vec<_>>(),
            vec!["rust"],
            "the lane's own recorded verdict must reach the surface that renders it"
        );
        assert!(rows[0].passed);
    }

    /// An infrastructure row is diagnosis, not a verdict, and stays bound to the
    /// head that hit it: an unchanged tree on a new head must not render another
    /// head's stumble. Restoring the verdict families to this listing must not
    /// cost that scoping.
    #[tokio::test]
    async fn head_listing_shows_only_this_heads_infrastructure_row() {
        let db = cache_db().await;
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                environment_fingerprint: infra_suppression_scope("job-1", "commit-1"),
                ..failing_result("project-a", "tree-commit-1", "mine", Some("infrastructure"))
            },
        )
        .unwrap();
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                environment_fingerprint: infra_suppression_scope("job-2", "commit-2"),
                ..failing_result(
                    "project-a",
                    "tree-commit-1",
                    "theirs",
                    Some("infrastructure"),
                )
            },
        )
        .unwrap();

        let rows =
            list_check_results_for_head(db, "project-a", "tree-commit-1", "job-1", "commit-1")
                .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.check_name.as_str())
                .collect::<Vec<_>>(),
            vec!["mine"]
        );
    }

    /// One tree can hold one check's verdict from several environments at once,
    /// one per machine that ran it. A lane renders ONE state, so it renders the
    /// most recent evidence rather than whichever row a name-keyed map happened
    /// to keep last.
    #[tokio::test]
    async fn head_listing_keeps_the_newest_of_one_checks_per_environment_verdicts() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-green", "commit-1", "env-a"))
            .unwrap();
        let mut red = observation("obs-red", "commit-1", "env-b");
        red.ran_at = 200;
        red.verdict = "failed".to_string();
        red.exit_code = 1;
        red.tests[0].status = "failed".to_string();
        record_fresh_check_observation(db.clone(), red).unwrap();

        let rows =
            list_check_results_for_head(db, "project-a", "tree-commit-1", "job-1", "commit-1")
                .unwrap();
        assert_eq!(rows.len(), 1, "one lane renders one state");
        assert!(
            !rows[0].passed,
            "the newer red must not hide behind an older green from another environment"
        );
    }

    /// The bound must hold when two cadences evaluate one triple at the same
    /// moment. This is precisely what a plain read cannot cover: at
    /// `BOUND - 1` both would observe "not suppressed", both would launch, and
    /// the triple would cost `BOUND + 1` executions. Reserving the attempt in
    /// the same statement that grants it is what lets only one through.
    #[tokio::test]
    async fn two_concurrent_evaluations_at_the_bound_cannot_both_execute() {
        let db = cache_db().await;
        for _ in 1..OBSERVED_INFRA_FAILURE_BOUND {
            assert!(evaluate_infra_failure(&db, "rust", "ih-rust"));
        }
        assert_eq!(
            streak_of(&db, "ih-rust", "rust").await,
            OBSERVED_INFRA_FAILURE_BOUND - 1,
            "the triple must start one retry short of the bound"
        );

        let racers: Vec<_> = (0..2)
            .map(|_| {
                let db = db.clone();
                tokio::task::spawn_blocking(move || {
                    matches!(
                        claim_check_execution(
                            db,
                            "project-a",
                            "rust",
                            "ih-rust",
                            "job-test",
                            "commit-test",
                        )
                        .unwrap(),
                        CheckExecutionClaim::Clear
                    )
                })
            })
            .collect();
        let mut admitted = 0i64;
        for racer in racers {
            if racer.await.unwrap() {
                admitted += 1;
            }
        }

        assert_eq!(
            admitted, 1,
            "exactly one of two simultaneous evaluations may spend the last retry"
        );
        assert_eq!(
            streak_of(&db, "ih-rust", "rust").await,
            OBSERVED_INFRA_FAILURE_BOUND,
            "the refused evaluation must not have advanced the counter past the bound"
        );
    }

    /// Overshoot is bounded by the reservation rather than by luck: however many
    /// cadences evaluate a failing triple at once, the total number of admitted
    /// executions is exactly the bound.
    #[tokio::test]
    async fn concurrent_evaluations_never_exceed_the_bound() {
        let db = cache_db().await;
        // The first completed failure opens the streak; every retry after it is
        // reserved, so eight racers contend for the remaining budget.
        assert!(evaluate_infra_failure(&db, "rust", "ih-rust"));

        let racers: Vec<_> = (0..8)
            .map(|_| {
                let db = db.clone();
                tokio::task::spawn_blocking(move || {
                    matches!(
                        claim_check_execution(
                            db,
                            "project-a",
                            "rust",
                            "ih-rust",
                            "job-test",
                            "commit-test",
                        )
                        .unwrap(),
                        CheckExecutionClaim::Clear
                    )
                })
            })
            .collect();
        let mut admitted = 1i64;
        for racer in racers {
            if racer.await.unwrap() {
                admitted += 1;
            }
        }

        assert_eq!(
            admitted, OBSERVED_INFRA_FAILURE_BOUND,
            "eight simultaneous evaluations must still cost exactly \
             {OBSERVED_INFRA_FAILURE_BOUND} executions"
        );
        assert!(suppressed(&db, "rust", "ih-rust"));
    }

    /// A healthy triple is never rationed. This is not the bound being lax: at a
    /// streak of zero the database cannot yet distinguish healthy contention from
    /// a broken check, so a cumulative gate here would refuse a perfectly good
    /// check its verdict and render it `suppressed after 3 infrastructure
    /// failures` when nothing had failed — turning the kill switch into the
    /// outage. The cost of that choice is measured by
    /// [`concurrent_first_attempts_are_not_deduplicated_and_can_exceed_the_bound`]
    /// below, and the right fix for it is deduplication, not rationing
    /// (CAIRN-3271).
    #[tokio::test]
    async fn concurrent_evaluations_of_a_healthy_triple_are_all_admitted() {
        let db = cache_db().await;
        let racers: Vec<_> = (0..OBSERVED_INFRA_FAILURE_BOUND + 3)
            .map(|_| {
                let db = db.clone();
                tokio::task::spawn_blocking(move || {
                    matches!(
                        claim_check_execution(
                            db,
                            "project-a",
                            "rust",
                            "ih-rust",
                            "job-test",
                            "commit-test",
                        )
                        .unwrap(),
                        CheckExecutionClaim::Clear
                    )
                })
            })
            .collect();
        for racer in racers {
            assert!(
                racer.await.unwrap(),
                "a triple with no infrastructure history must never be refused"
            );
        }
    }

    /// The measured cost of not rationing a streak-0 triple, pinned so it is
    /// never mistaken for a guarantee.
    ///
    /// Concurrent FIRST attempts are not deduplicated: with no streak to ration,
    /// every simultaneous caller is admitted, and each completed failure only
    /// OPENS the streak, so `K` concurrent first attempts cost `K + (BOUND - 1)`
    /// executions instead of `BOUND`. The important property still holds and is
    /// asserted here — the triple CONVERGES on suppression rather than looping,
    /// which is what the kill switch exists for. Closing the remaining gap needs
    /// same-triple single-flight, because the duplicate work is redundant by
    /// construction (one input hash IS one set of inputs) rather than merely
    /// over-budget. Tracked in CAIRN-3271.
    #[tokio::test]
    async fn concurrent_first_attempts_are_not_deduplicated_and_can_exceed_the_bound() {
        let db = cache_db().await;

        // Two cadences reach a brand-new triple at the same moment.
        let racers: Vec<_> = (0..2)
            .map(|_| {
                let db = db.clone();
                tokio::task::spawn_blocking(move || {
                    matches!(
                        claim_check_execution(
                            db,
                            "project-a",
                            "rust",
                            "ih-rust",
                            "job-test",
                            "commit-test",
                        )
                        .unwrap(),
                        CheckExecutionClaim::Clear
                    )
                })
            })
            .collect();
        let mut launched = 0i64;
        for racer in racers {
            if racer.await.unwrap() {
                launched += 1;
            }
        }
        assert_eq!(
            launched, 2,
            "a triple with no infrastructure history is never rationed, so both run"
        );

        // Both commands come back as infrastructure failures.
        for _ in 0..2 {
            store_check_result(
                db.clone(),
                failing_result("project-a", "ih-rust", "rust", Some("infrastructure")),
            )
            .unwrap();
        }
        assert_eq!(
            streak_of(&db, "ih-rust", "rust").await,
            1,
            "a completed failure only OPENS the streak; retries are counted when reserved"
        );

        // From here every retry is reserved, so the triple converges.
        while evaluate_infra_failure(&db, "rust", "ih-rust") {
            launched += 1;
            assert!(
                launched <= OBSERVED_INFRA_FAILURE_BOUND * 4,
                "the triple must converge on suppression, never loop"
            );
        }

        assert!(suppressed(&db, "rust", "ih-rust"));
        assert_eq!(
            launched,
            OBSERVED_INFRA_FAILURE_BOUND + 1,
            "exactly one un-deduplicated concurrent first attempt above the bound \
             — the overshoot CAIRN-3271 removes"
        );
    }

    /// Every infrastructure kind counts, and only infrastructure kinds do. A
    /// timeout is a fact about the check's own command, so it must not spend the
    /// substrate's budget.
    #[tokio::test]
    async fn only_infrastructure_kinds_advance_the_counter() {
        let db = cache_db().await;
        for kind in ["infrastructure", "spawn_error", "runner_error"] {
            let hash = format!("ih-{kind}");
            store_check_result(
                db.clone(),
                failing_result("project-a", &hash, "rust", Some(kind)),
            )
            .unwrap();
            assert_eq!(
                streak_of(&db, &hash, "rust").await,
                1,
                "{kind} is an infrastructure failure"
            );
        }
        for kind in [Some("timed_out"), Some("killed"), None] {
            let hash = format!("ih-{}", kind.unwrap_or("ordinary"));
            store_check_result(db.clone(), failing_result("project-a", &hash, "rust", kind))
                .unwrap();
            assert_eq!(
                streak_of(&db, &hash, "rust").await,
                0,
                "{kind:?} is a verdict about the change, not about Cairn"
            );
        }
    }

    /// The reset rule. A genuine verdict at the same input hash proves the
    /// substrate works, so it clears the counter AND the escalation stamp — a
    /// later relapse escalates again rather than failing silently.
    #[tokio::test]
    async fn a_genuine_verdict_clears_the_streak_and_the_escalation_stamp() {
        let db = cache_db().await;
        drive_to_bound(&db, "rust", "ih-rust");
        assert!(suppressed(&db, "rust", "ih-rust"));
        assert!(claim_infra_escalation(
            db.clone(),
            "project-a",
            "rust",
            "ih-rust",
            "job-test",
            "commit-test",
        )
        .unwrap());

        // An ORDINARY red is a genuine verdict too: the command ran and answered.
        store_check_result(
            db.clone(),
            failing_result("project-a", "ih-rust", "rust", None),
        )
        .unwrap();
        assert_eq!(streak_of(&db, "ih-rust", "rust").await, 0);
        assert!(!suppressed(&db, "rust", "ih-rust"));

        // Relapse: the cleared stamp means the operator hears about it again.
        drive_to_bound(&db, "rust", "ih-rust");
        assert!(suppressed(&db, "rust", "ih-rust"));
        assert!(
            claim_infra_escalation(
                db.clone(),
                "project-a",
                "rust",
                "ih-rust",
                "job-test",
                "commit-test",
            )
            .unwrap(),
            "a relapse after a genuine verdict is a new escalation, not a duplicate"
        );
    }

    /// A pass carries an executor environment identity, while suppression carries
    /// the job/head scope. The production observation writer must bridge those
    /// identities without clearing another head's independent breaker.
    #[tokio::test]
    async fn a_pass_clears_only_its_scopes_streak() {
        let db = cache_db().await;
        drive_scope_to_bound(&db, "rust", "input-rust", "job-test", "commit-test");
        drive_scope_to_bound(&db, "rust", "input-rust", "job-other", "commit-other");

        let mut pass = observation("obs-pass-reset", "commit-test", "executor-environment");
        pass.job_id = Some("job-test".to_string());
        record_fresh_check_observation(db.clone(), pass).unwrap();

        assert!(!get_suppressed_check_result(
            db.clone(),
            "project-a",
            "rust",
            "input-rust",
            "job-test",
            "commit-test"
        )
        .unwrap()
        .is_some());
        assert!(get_suppressed_check_result(
            db,
            "project-a",
            "rust",
            "input-rust",
            "job-other",
            "commit-other"
        )
        .unwrap()
        .is_some());
    }

    /// The counter lives on the input-hash-keyed row, so a change to the check's
    /// inputs gets a full budget without any explicit un-suppression. This is why
    /// suppression can never outlive the thing it was about.
    #[tokio::test]
    async fn a_new_input_hash_starts_fresh_at_zero() {
        let db = cache_db().await;
        drive_to_bound(&db, "rust", "ih-old");
        assert!(suppressed(&db, "rust", "ih-old"));
        assert!(
            !suppressed(&db, "rust", "ih-new"),
            "a triple with no history of its own is never suppressed"
        );

        store_check_result(
            db.clone(),
            failing_result("project-a", "ih-new", "rust", Some("infrastructure")),
        )
        .unwrap();
        assert_eq!(streak_of(&db, "ih-new", "rust").await, 1);
        assert!(!suppressed(&db, "rust", "ih-new"));
    }

    /// Exactly one escalation per triple, enforced by the database rather than by
    /// an invariant about how often the bound is crossed.
    #[tokio::test]
    async fn exactly_one_escalation_is_claimed_per_triple() {
        let db = cache_db().await;
        drive_to_bound(&db, "rust", "ih-rust");
        assert!(claim_infra_escalation(
            db.clone(),
            "project-a",
            "rust",
            "ih-rust",
            "job-test",
            "commit-test",
        )
        .unwrap());
        for _ in 0..5 {
            assert!(
                !claim_infra_escalation(
                    db.clone(),
                    "project-a",
                    "rust",
                    "ih-rust",
                    "job-test",
                    "commit-test",
                )
                .unwrap(),
                "the escalation fires once per triple, not once per evaluation"
            );
        }
    }

    /// An unsuppressed triple has nothing to escalate, however hard it is asked.
    #[tokio::test]
    async fn an_unsuppressed_triple_never_escalates() {
        let db = cache_db().await;
        store_check_result(
            db.clone(),
            failing_result("project-a", "ih-rust", "rust", Some("infrastructure")),
        )
        .unwrap();
        assert!(!claim_infra_escalation(
            db.clone(),
            "project-a",
            "rust",
            "ih-rust",
            "job-test",
            "commit-test",
        )
        .unwrap());
    }

    /// A cache-hit re-stamp reports an older verdict onto a new tree. Nothing
    /// executed, so it must move neither the counter nor the escalation stamp —
    /// otherwise a suppressed check would un-suppress itself simply by being
    /// re-listed at each new commit.
    #[tokio::test]
    async fn a_cache_hit_restamp_holds_the_counter_still() {
        let db = cache_db().await;
        drive_to_bound(&db, "rust", "ih-rust");
        assert!(claim_infra_escalation(
            db.clone(),
            "project-a",
            "rust",
            "ih-rust",
            "job-test",
            "commit-test",
        )
        .unwrap());

        let mut restamp = failing_result("project-a", "tree-later", "rust", Some("infrastructure"));
        restamp.input_hash = "ih-rust".to_string();
        restamp.cached = Some(true);
        store_check_result(db.clone(), restamp).unwrap();

        assert_eq!(
            streak_of(&db, "tree-later", "rust").await,
            OBSERVED_INFRA_FAILURE_BOUND,
            "a re-stamp is not an attempt"
        );
        assert!(suppressed(&db, "rust", "ih-rust"));
        assert!(
            !claim_infra_escalation(
                db.clone(),
                "project-a",
                "rust",
                "ih-rust",
                "job-test",
                "commit-test",
            )
            .unwrap(),
            "a re-stamp must not reopen the escalation"
        );
    }

    /// The un-suppression trigger: a Cairn restart frees every suppressed triple,
    /// including its escalation stamp, so a repaired substrate is re-proven rather
    /// than assumed still broken.
    #[tokio::test]
    async fn clearing_suppressions_frees_every_triple_and_reopens_escalation() {
        let db = cache_db().await;
        for check in ["rust", "frontend"] {
            drive_to_bound(&db, check, &format!("ih-{check}"));
            assert!(claim_infra_escalation(
                db.clone(),
                "project-a",
                check,
                &format!("ih-{check}"),
                "job-test",
                "commit-test",
            )
            .unwrap());
        }

        assert_eq!(clear_infra_suppressions(&db).await.unwrap(), 2);
        for check in ["rust", "frontend"] {
            assert!(!suppressed(&db, check, &format!("ih-{check}")));
            assert_eq!(streak_of(&db, &format!("ih-{check}"), check).await, 0);
        }

        // A second sweep with nothing suppressed is a no-op, so the startup path
        // stays silent on a healthy install.
        assert_eq!(clear_infra_suppressions(&db).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn fresh_observation_records_tests_alias_and_hot_identity_atomically() {
        let db = cache_db().await;
        let observation = observation("obs-1", "commit-1", "env-a");
        let public_handle = observation.public_handle.clone();
        record_fresh_check_observation(db.clone(), observation).unwrap();
        let loaded = get_check_result_observation(
            db.clone(),
            "project-a",
            "commit-1",
            "rust",
            "env-a",
            1,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .expect("fresh alias");
        assert_eq!(loaded.disposition, "fresh");
        assert_eq!(loaded.observation_id, "obs-1");
        let permalink =
            get_check_result_observation_by_handle(db.clone(), "project-a", &public_handle)
                .unwrap()
                .expect("the public handle resolves the exact immutable row");
        assert_eq!(permalink.observation_id, "obs-1");
        assert_eq!(loaded.tests.len(), 1);
        assert_eq!(loaded.tests[0].test_id, "crate::passes");
        assert_eq!(
            get_reusable_check_observation_id(
                db.clone(),
                "project-a",
                "rust",
                "input-rust",
                "env-a",
                1
            )
            .unwrap()
            .as_deref(),
            Some("obs-1")
        );
        assert!(get_reusable_check_observation_id(
            db.clone(),
            "project-a",
            "rust",
            "input-rust",
            "env-b",
            1
        )
        .unwrap()
        .is_none());
        assert!(get_reusable_check_observation_id(
            db,
            "project-a",
            "rust",
            "input-rust",
            "env-a",
            2
        )
        .unwrap()
        .is_none());
    }

    #[tokio::test]
    async fn observation_test_pages_are_stable_filtered_and_exhaustive() {
        let db = cache_db().await;
        let mut write = observation("obs-pages", "commit-pages", "env-a");
        write.tests = (0..251)
            .map(|index| CheckTestResultRow {
                test_id: format!("case::{index:04}"),
                status: if index % 3 == 0 { "failed" } else { "passed" }.to_string(),
                duration_ms: None,
                attempt_count: Some(1),
                failure_excerpt: None,
                skip_reason: None,
                declaration_source: None,
                flaky: false,
            })
            .collect();
        record_fresh_check_observation(db.clone(), write).unwrap();

        let mut names = Vec::new();
        for offset in [0, 100, 200] {
            let page = get_check_result_observation(
                db.clone(),
                "project-a",
                "commit-pages",
                "rust",
                "env-a",
                1,
                None,
                None,
                100,
                offset,
            )
            .unwrap()
            .unwrap();
            assert_eq!(page.test_total, 251);
            assert_eq!(page.test_offset, offset);
            names.extend(page.tests.into_iter().map(|test| test.test_id));
        }
        assert_eq!(names.len(), 251);
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), 251, "page boundaries duplicated a test");
        assert!(
            names.windows(2).all(|pair| pair[0] < pair[1]),
            "pages must preserve stable test-name order"
        );

        let filtered = get_check_result_observation(
            db,
            "project-a",
            "commit-pages",
            "rust",
            "env-a",
            1,
            Some("failed"),
            Some("case::00"),
            10,
            10,
        )
        .unwrap()
        .unwrap();
        assert_eq!(filtered.test_total, 34);
        assert_eq!(filtered.tests.len(), 10);
        assert!(filtered
            .tests
            .iter()
            .all(|test| test.status == "failed" && test.test_id.contains("case::00")));
    }

    #[tokio::test]
    async fn cache_hit_adds_alias_without_rewriting_source_provenance() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-1", "commit-1", "env-a"))
            .unwrap();
        record_cached_check_observation(
            db.clone(),
            CachedCheckObservationWrite {
                project_id: "project-a".to_string(),
                commit_sha: "commit-2".to_string(),
                defined_by_commit_sha: "commit-2".to_string(),
                tree_hash: "tree-commit-2".to_string(),
                check_name: "rust".to_string(),
                input_hash: "input-rust".to_string(),
                environment_fingerprint: "env-a".to_string(),
                result_schema_version: 1,
                source_observation_id: "obs-1".to_string(),
                evaluated_at: 200,
            },
        )
        .unwrap();
        let cached = get_check_result_observation(
            db,
            "project-a",
            "commit-2",
            "rust",
            "env-a",
            1,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .expect("cached alias");
        assert_eq!(cached.disposition, "cached");
        assert_eq!(cached.evaluated_at, 200);
        assert_eq!(cached.source_commit_sha, "commit-1");
        assert_eq!(cached.ran_at, 100);
        assert_eq!(cached.executor_id.as_deref(), Some("executor-1"));
    }

    /// Three provenance values survive the round trip and stay distinguishable:
    /// the evaluated commit, the commit whose config declared the check at that
    /// coordinate, and the commit the reused verdict was produced at.
    #[tokio::test]
    async fn evaluated_defining_and_source_commits_round_trip_separately() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-1", "commit-1", "env-a"))
            .unwrap();
        record_cached_check_observation(
            db.clone(),
            CachedCheckObservationWrite {
                project_id: "project-a".to_string(),
                commit_sha: "commit-2".to_string(),
                defined_by_commit_sha: "commit-2".to_string(),
                tree_hash: "tree-commit-2".to_string(),
                check_name: "rust".to_string(),
                input_hash: "input-rust".to_string(),
                environment_fingerprint: "env-a".to_string(),
                result_schema_version: 1,
                source_observation_id: "obs-1".to_string(),
                evaluated_at: 200,
            },
        )
        .unwrap();

        let fresh = get_check_result_observation(
            db.clone(),
            "project-a",
            "commit-1",
            "rust",
            "env-a",
            1,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .expect("fresh alias");
        assert_eq!(fresh.defined_by_commit_sha.as_deref(), Some("commit-1"));
        assert_eq!(
            fresh.source_defined_by_commit_sha.as_deref(),
            Some("commit-1")
        );

        let cached = get_check_result_observation(
            db.clone(),
            "project-a",
            "commit-2",
            "rust",
            "env-a",
            1,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .expect("cached alias");
        assert_eq!(cached.source_commit_sha, "commit-1");
        assert_eq!(
            cached.defined_by_commit_sha.as_deref(),
            Some("commit-2"),
            "the alias records the definition the reuse was admitted under"
        );
        assert_eq!(
            cached.source_defined_by_commit_sha.as_deref(),
            Some("commit-1"),
            "the source observation keeps its own defining commit"
        );

        // The hot row alone identifies one coherent commit: the check name, the
        // tree it was evaluated against, and the commit that defined it.
        let row = list_check_results(db, "project-a", "tree-commit-1")
            .unwrap()
            .pop()
            .expect("a hot row for the evaluated tree");
        assert_eq!(row.check_name, "rust");
        assert_eq!(row.tree_hash, "tree-commit-1");
        assert_eq!(row.defined_by_commit_sha.as_deref(), Some("commit-1"));
    }

    /// A row whose defining commit was never recorded cannot prove which
    /// definition produced it, so it stays diagnostic-only on both sides of the
    /// join: the hot index row and the immutable observation behind it.
    #[tokio::test]
    async fn a_row_without_definition_provenance_is_never_an_exact_hit() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-1", "commit-1", "env-a"))
            .unwrap();
        let hit = |db: Arc<LocalDb>| {
            get_exact_reusable_check_result(db, "project-a", "rust", "input-rust", "env-a", 1)
                .unwrap()
                .is_some()
        };
        assert!(hit(db.clone()), "a fully provenanced row is reusable");

        db.execute(
            "UPDATE check_result_cache SET defined_by_commit_sha = NULL WHERE check_name='rust'",
            (),
        )
        .await
        .unwrap();
        assert!(!hit(db.clone()), "a legacy hot row is not an exact hit");
        assert!(get_reusable_check_observation_id(
            db.clone(),
            "project-a",
            "rust",
            "input-rust",
            "env-a",
            1
        )
        .unwrap()
        .is_none());

        // Restore the hot row and strip the observation instead: an immutable
        // legacy observation is equally unusable as reuse evidence.
        db.execute(
            "UPDATE check_result_cache SET defined_by_commit_sha = 'commit-1' WHERE check_name='rust'",
            (),
        )
        .await
        .unwrap();
        assert!(hit(db.clone()));
        db.execute(
            "INSERT INTO check_result_observations (
                 id, project_id, commit_sha, tree_hash, check_name, input_hash,
                 environment_fingerprint, exit_code, verdict, complete, reusable,
                 parser_version, result_schema_version, ran_at, duration_ms, cadence,
                 output_tail
             ) VALUES ('obs-legacy','project-a','commit-0','tree-commit-0','rust','input-rust',
                       'env-a',0,'passed',1,1,1,1,1,1,'review','ok')",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE check_result_cache SET source_observation_id = 'obs-legacy' WHERE check_name='rust'",
            (),
        )
        .await
        .unwrap();
        assert!(
            !hit(db.clone()),
            "an observation with no defining commit cannot be reused"
        );
    }

    #[tokio::test]
    async fn invalid_test_row_rolls_back_entire_fresh_observation() {
        let db = cache_db().await;
        let mut invalid = observation("obs-invalid", "commit-invalid", "env-a");
        invalid.tests[0].status = "unknown".to_string();
        assert!(record_fresh_check_observation(db.clone(), invalid).is_err());
        assert!(get_check_result_observation(
            db.clone(),
            "project-a",
            "commit-invalid",
            "rust",
            "env-a",
            1,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .is_none());
        assert!(get_reusable_check_observation_id(
            db,
            "project-a",
            "rust",
            "input-rust",
            "env-a",
            1,
        )
        .unwrap()
        .is_none());
    }

    #[tokio::test]
    async fn reused_observation_id_requires_exact_payload_without_extending_tests() {
        let db = cache_db().await;
        let original = observation("obs-1", "commit-1", "env-a");
        record_fresh_check_observation(db.clone(), original.clone()).unwrap();
        record_fresh_check_observation(db.clone(), original).unwrap();

        let mut conflicting = observation("obs-1", "commit-1", "env-a");
        conflicting.tests.push(CheckTestResultRow {
            test_id: "crate::must_not_appear".to_string(),
            status: "passed".to_string(),
            duration_ms: Some(2),
            attempt_count: Some(1),
            failure_excerpt: None,
            skip_reason: None,
            declaration_source: None,
            flaky: false,
        });
        assert!(record_fresh_check_observation(db.clone(), conflicting).is_err());

        let loaded = get_check_result_observation(
            db,
            "project-a",
            "commit-1",
            "rust",
            "env-a",
            1,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .expect("original observation remains visible");
        assert_eq!(loaded.tests.len(), 1);
        assert_eq!(loaded.tests[0].test_id, "crate::passes");
    }

    #[tokio::test]
    async fn later_fresh_observation_supersedes_alias_and_hot_projection() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-1", "commit-1", "env-a"))
            .unwrap();
        let mut retry = observation("obs-2", "commit-1", "env-a");
        retry.ran_at = 200;
        retry.exit_code = 1;
        retry.verdict = "failed".to_string();
        retry.reusable = true;
        retry.output_tail = "retry result".to_string();
        record_fresh_check_observation(db.clone(), retry).unwrap();

        let observation_count = db
            .query_one(
                "SELECT COUNT(*) FROM check_result_observations WHERE id IN ('obs-1','obs-2')",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        let alias_source = db
            .query_one(
                "SELECT source_observation_id FROM check_result_commit_aliases
                  WHERE project_id='project-a' AND commit_sha='commit-1' AND check_name='rust'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        let hot_source = db
            .query_one(
                "SELECT source_observation_id FROM check_result_cache
                  WHERE project_id='project-a' AND check_name='rust' AND input_hash='input-rust'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(observation_count, 2, "both immutable observations remain");
        assert_eq!(alias_source, "obs-2");
        assert_eq!(hot_source, "obs-2");
    }

    #[tokio::test]
    async fn non_reusable_fresh_result_is_visible_but_cannot_be_reused() {
        let db = cache_db().await;
        let mut failed = observation("obs-failed", "commit-failed", "env-a");
        failed.exit_code = 1;
        failed.verdict = "failed".to_string();
        failed.reusable = false;
        failed.non_reusable_reason = Some("failed verdict".to_string());
        record_fresh_check_observation(db.clone(), failed).unwrap();

        let visible = list_check_results(db.clone(), "project-a", "tree-commit-failed").unwrap();
        assert_eq!(visible.len(), 1);
        assert!(!visible[0].passed);
        assert!(
            get_exact_reusable_check_result(db, "project-a", "rust", "input-rust", "env-a", 1,)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn complete_genuine_red_is_an_exact_reusable_verdict() {
        let db = cache_db().await;
        let mut failed = observation("obs-failed", "commit-failed", "env-a");
        failed.exit_code = 1;
        failed.verdict = "failed".to_string();
        failed.reusable = true;
        failed.output_tail = "assertion failed".to_string();
        record_fresh_check_observation(db.clone(), failed).unwrap();

        let hit =
            get_exact_reusable_check_result(db, "project-a", "rust", "input-rust", "env-a", 1)
                .unwrap()
                .expect("a complete genuine red is a verdict");
        assert!(!hit.passed);
        assert_eq!(hit.output_tail, "assertion failed");
    }

    #[tokio::test]
    async fn infrastructure_failure_is_never_an_exact_reusable_verdict() {
        let db = cache_db().await;
        let mut failed = observation("obs-infra", "commit-infra", "env-a");
        failed.exit_code = 1;
        failed.verdict = "failed".to_string();
        failed.failure_kind = Some("infrastructure".to_string());
        failed.reusable = false;
        failed.non_reusable_reason = Some("infrastructure failure".to_string());
        record_fresh_check_observation(db.clone(), failed).unwrap();

        assert!(
            get_exact_reusable_check_result(db, "project-a", "rust", "input-rust", "env-a", 1,)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn source_observations_and_aliases_are_immutable() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-1", "commit-1", "env-a"))
            .unwrap();
        assert!(db
            .execute(
                "UPDATE check_result_observations SET output_tail='changed' WHERE id='obs-1'",
                ()
            )
            .await
            .is_err());
        assert!(db.execute("UPDATE check_result_commit_aliases SET disposition='cached' WHERE source_observation_id='obs-1'", ()).await.is_err());
    }

    #[tokio::test]
    async fn deleting_a_project_cascades_observation_history() {
        let db = cache_db().await;
        record_fresh_check_observation(db.clone(), observation("obs-1", "commit-1", "env-a"))
            .unwrap();
        db.execute("DELETE FROM projects WHERE id='project-a'", ())
            .await
            .unwrap();
        assert!(get_check_result_observation(
            db.clone(),
            "project-a",
            "commit-1",
            "rust",
            "env-a",
            1,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .is_none());
        assert!(get_reusable_check_observation_id(
            db,
            "project-a",
            "rust",
            "input-rust",
            "env-a",
            1,
        )
        .unwrap()
        .is_none());
    }

    #[tokio::test]
    async fn check_result_cache_hit_and_miss() {
        let db = cache_db().await;
        assert!(get_check_result(db.clone(), "project-a", "rust", "input-a")
            .unwrap()
            .is_none());

        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        let row = get_check_result(db, "project-a", "rust", "input-a")
            .unwrap()
            .expect("stored result should be cached");
        assert_eq!(row.project_id, "project-a");
        assert_eq!(row.input_hash, "input-a");
        assert_eq!(row.check_name, "rust");
        assert_eq!(row.exit_code, 0);
        assert!(row.passed);
        assert_eq!(row.output_tail, "ok");
        assert_eq!(row.duration_ms, 123);
    }

    #[tokio::test]
    async fn check_result_cache_round_trips_job_id_and_cached_stamp() {
        let db = cache_db().await;
        let mut write = test_result("project-a", "input-a", "rust");
        write.job_id = Some("job-1".to_string());
        write.cached = Some(false);
        store_check_result(db.clone(), write).unwrap();

        let row = get_check_result(db.clone(), "project-a", "rust", "input-a")
            .unwrap()
            .expect("stored result should be cached");
        assert_eq!(row.job_id.as_deref(), Some("job-1"));
        assert_eq!(row.cached, Some(false));

        let mut restamp = test_result("project-a", "tree-2", "rust");
        restamp.input_hash = "input-a".to_string();
        restamp.job_id = Some("job-1".to_string());
        restamp.cached = Some(true);
        store_check_result(db.clone(), restamp).unwrap();

        let row = get_check_result(db, "project-a", "rust", "input-a")
            .unwrap()
            .expect("restamped result should remain cached");
        assert_eq!(row.tree_hash, "tree-2");
        assert_eq!(row.cached, Some(true));
    }

    #[tokio::test]
    async fn check_result_cache_carries_forward_across_equivalent_tree_commits() {
        // `sealed_tree_hash` is content-addressed (the sealed commit's git tree),
        // so a squash/rebase that rewrites the commit id while preserving file
        // content resolves to the SAME tree hash. The cache seam keys on that
        // hash, so the pre-squash verdict is returned for the post-squash commit
        // without re-running the check — the carry-forward this whole change buys.
        let db = cache_db().await;
        let equivalent_input = "shared-input-sha";
        store_check_result(
            db.clone(),
            test_result("project-a", equivalent_input, "rust"),
        )
        .unwrap();

        // A distinct commit whose matching files hash the same hits the verdict.
        let row = get_check_result(db, "project-a", "rust", equivalent_input)
            .unwrap()
            .expect("equivalent-input commit reuses the cached verdict");
        assert!(row.passed);
        assert_eq!(row.input_hash, equivalent_input);
    }

    #[tokio::test]
    async fn check_result_cache_isolates_input_hashes() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-a", "rust", "input-b")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn check_result_cache_isolates_check_names() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-a", "frontend", "input-a")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn check_result_cache_isolates_projects() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-b", "rust", "input-a")
            .unwrap()
            .is_none());
    }

    /// The hit/miss key is the input hash; `tree_hash` is only the listing
    /// pointer. Two rows with the same tree but different inputs are distinct.
    #[tokio::test]
    async fn get_keys_by_input_hash_not_tree_hash() {
        let db = cache_db().await;
        let mut row = test_result("project-a", "tree-1", "rust");
        row.input_hash = "input-a".to_string();
        store_check_result(db.clone(), row).unwrap();

        assert!(get_check_result(db.clone(), "project-a", "rust", "input-a")
            .unwrap()
            .is_some());
        assert!(get_check_result(db, "project-a", "rust", "other-input")
            .unwrap()
            .is_none());
    }

    /// A later commit with the SAME input hash but a new whole-tree hash re-stamps
    /// the single input-keyed row (upsert updates `tree_hash`) rather than adding
    /// a second row — so the tree-keyed listing follows the current tree.
    #[tokio::test]
    async fn restamp_moves_tree_pointer_for_listing() {
        let db = cache_db().await;
        let mut r1 = test_result("project-a", "tree-1", "rust");
        r1.input_hash = "IH".to_string();
        store_check_result(db.clone(), r1).unwrap();
        assert_eq!(
            list_check_results(db.clone(), "project-a", "tree-1")
                .unwrap()
                .len(),
            1
        );

        let mut r2 = test_result("project-a", "tree-2", "rust");
        r2.input_hash = "IH".to_string();
        store_check_result(db.clone(), r2).unwrap();

        assert!(list_check_results(db.clone(), "project-a", "tree-1")
            .unwrap()
            .is_empty());
        let rows = list_check_results(db, "project-a", "tree-2").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].check_name, "rust");
    }

    #[tokio::test]
    async fn store_check_result_replaces_red_with_later_red() {
        let db = cache_db().await;
        let mut initial = test_result("project-a", "input-a", "rust");
        initial.exit_code = 2;
        initial.passed = false;
        initial.output_tail = "first failure".to_string();
        store_check_result(db.clone(), initial).unwrap();

        let mut replacement = test_result("project-a", "input-a", "rust");
        replacement.exit_code = 1;
        replacement.passed = false;
        replacement.output_tail = "failed".to_string();
        replacement.duration_ms = 456;
        replacement.target_results_json = Some("{\"targets\":[]}".to_string());
        store_check_result(db.clone(), replacement).unwrap();

        assert!(get_check_result(db.clone(), "project-a", "rust", "input-a")
            .unwrap()
            .is_none());
        let row = list_check_results(db, "project-a", "input-a")
            .unwrap()
            .into_iter()
            .next()
            .expect("replacement should remain visible");
        assert_eq!(row.exit_code, 1);
        assert!(!row.passed);
        assert_eq!(row.output_tail, "failed");
        assert_eq!(row.duration_ms, 456);
        assert_eq!(row.target_results_json.as_deref(), Some("{\"targets\":[]}"));
    }

    /// Insert a row with an explicit `ran_at`/`tree_hash` so recency and the
    /// tie-break are deterministic (the public `store_check_result` stamps
    /// `ran_at` with the wall clock, which can't order two same-second writes).
    async fn insert_row(
        db: &LocalDb,
        project_id: &str,
        tree_hash: &str,
        check_name: &str,
        passed: bool,
        output_tail: &str,
        ran_at: i64,
    ) {
        // `input_hash` is the cache key; here it mirrors `tree_hash` so each
        // distinct-tree insert stays a distinct row under the
        // `(project_id, check_name, input_hash)` primary key.
        db.execute_script(&format!(
            "INSERT INTO check_result_cache
               (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                output_tail, duration_ms, ran_at)
             VALUES ('{project_id}', '{tree_hash}', '{tree_hash}', '{check_name}', {exit}, {passed},
                '{output_tail}', 10, {ran_at});",
            exit = if passed { 0 } else { 1 },
            passed = if passed { 1 } else { 0 },
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn latest_per_check_picks_newest_tree_and_isolates_projects() {
        let db = cache_db().await;
        // `rust` ran against an old failing tree, then a newer passing tree.
        insert_row(&db, "project-a", "tree-old", "rust", false, "old fail", 100).await;
        insert_row(&db, "project-a", "tree-new", "rust", true, "new pass", 200).await;
        // A second check, and a same-named check in another project that must not leak.
        insert_row(&db, "project-a", "tree-new", "frontend", true, "fe", 150).await;
        insert_row(&db, "project-b", "tree-new", "rust", true, "other", 999).await;

        let rows = list_latest_check_results_for_project(db, "project-a").unwrap();
        // One row per check name, ordered by name: frontend, then rust.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].check_name, "frontend");
        assert_eq!(rows[1].check_name, "rust");
        // The `rust` verdict is the NEWER tree's pass, not the older fail.
        assert!(rows[1].passed);
        assert_eq!(rows[1].tree_hash, "tree-new");
        assert_eq!(rows[1].output_tail, "new pass");
    }

    #[tokio::test]
    async fn latest_per_check_breaks_ran_at_ties_deterministically() {
        let db = cache_db().await;
        // Same check at two trees with an IDENTICAL ran_at: the tie-break on
        // tree_hash keeps exactly one row (the lexicographically greater hash).
        insert_row(&db, "project-a", "tree-aaa", "rust", false, "a", 500).await;
        insert_row(&db, "project-a", "tree-bbb", "rust", true, "b", 500).await;

        let rows = list_latest_check_results_for_project(db, "project-a").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tree_hash, "tree-bbb");
    }

    async fn insert_job_row(
        db: &LocalDb,
        tree_hash: &str,
        check_name: &str,
        job_id: &str,
        passed: bool,
        ran_at: i64,
    ) {
        db.execute_script(&format!(
            "INSERT INTO check_result_cache
               (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                output_tail, duration_ms, ran_at, job_id)
             VALUES ('project-a', '{tree_hash}', '{tree_hash}', '{check_name}', {exit}, {passed},
                'out', 10, {ran_at}, '{job_id}');",
            exit = if passed { 0 } else { 1 },
            passed = if passed { 1 } else { 0 },
        ))
        .await
        .unwrap();
    }

    /// The narrowing baseline for write-cadence planning must stay on the planning
    /// job's own branch. A sibling branch running the same check concurrently is the
    /// common case in a multi-agent workspace, and its tree is not an anchor on this
    /// lineage: diffing against it yields the symmetric difference of two unrelated
    /// trees, which drags every file the sibling touched into this branch's selector.
    #[tokio::test]
    async fn latest_passing_for_job_ignores_a_newer_sibling_branch() {
        let db = cache_db().await;
        insert_job_row(&db, "tree-mine", "frontend", "job-mine", true, 100).await;
        insert_job_row(&db, "tree-sibling", "frontend", "job-sibling", true, 200).await;

        let rows = list_latest_passing_check_results_for_job(db, "job-mine").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].tree_hash, "tree-mine",
            "a sibling job's newer tree must not become this job's baseline"
        );
    }

    /// Narrowing is anchored on a GREEN verdict, so a later red run must not displace
    /// the green one it superseded. Ranking all rows and discarding a failing winner
    /// throws the branch's real baseline away and collapses selection back to the full
    /// branch diff on every red run — precisely when an agent iterating on a failure
    /// wants the tightest loop.
    #[tokio::test]
    async fn latest_passing_for_job_survives_a_newer_failure() {
        let db = cache_db().await;
        insert_job_row(&db, "tree-green", "frontend", "job-mine", true, 100).await;
        insert_job_row(&db, "tree-red", "frontend", "job-mine", false, 200).await;

        let rows = list_latest_passing_check_results_for_job(db, "job-mine").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tree_hash, "tree-green");
        assert!(rows[0].passed);
    }

    /// Among several green runs on one branch, the newest wins — that is the tightest
    /// sound anchor. Independent per check name.
    #[tokio::test]
    async fn latest_passing_for_job_picks_newest_green_per_check() {
        let db = cache_db().await;
        insert_job_row(&db, "tree-1", "frontend", "job-mine", true, 100).await;
        insert_job_row(&db, "tree-2", "frontend", "job-mine", true, 300).await;
        insert_job_row(&db, "tree-3", "rust", "job-mine", true, 200).await;

        let rows = list_latest_passing_check_results_for_job(db, "job-mine").unwrap();
        let observed: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r.check_name.as_str(), r.tree_hash.as_str()))
            .collect();
        assert_eq!(observed, vec![("frontend", "tree-2"), ("rust", "tree-3")]);
    }

    /// A branch whose first source commit is being planned has no green run of its
    /// own. No baseline is the correct answer: the caller then selects from the plain
    /// branch diff, which for a young branch is already small.
    #[tokio::test]
    async fn latest_passing_for_job_is_empty_without_an_own_green_run() {
        let db = cache_db().await;
        insert_job_row(&db, "tree-red", "frontend", "job-mine", false, 100).await;
        insert_job_row(&db, "tree-other", "frontend", "job-sibling", true, 200).await;

        assert!(
            list_latest_passing_check_results_for_job(db, "job-mine")
                .unwrap()
                .is_empty(),
            "no own green run means no baseline, not a borrowed one"
        );
    }

    #[tokio::test]
    async fn job_listing_picks_latest_per_check_and_isolates_jobs() {
        let db = cache_db().await;
        db.execute_script(
            "
            INSERT INTO check_result_cache
               (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                output_tail, duration_ms, ran_at, job_id, cached)
             VALUES
               ('project-a', 'tree-old', 'input-old', 'rust', 0, 1, 'old', 10, 100, 'job-1', 0),
               ('project-a', 'tree-new', 'input-new', 'rust', 0, 1, 'new', 10, 200, 'job-1', 1),
               ('project-a', 'tree-new', 'frontend-input', 'frontend', 0, 1, 'fe', 10, 150, 'job-1', 0),
               ('project-a', 'tree-other', 'other-input', 'rust', 0, 1, 'other', 10, 999, 'job-2', 0);
            ",
        )
        .await
        .unwrap();

        let rows = list_check_results_for_job(db, "job-1").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].check_name, "frontend");
        assert_eq!(rows[1].check_name, "rust");
        assert_eq!(rows[1].tree_hash, "tree-new");
        assert_eq!(rows[1].cached, Some(true));
    }

    #[tokio::test]
    async fn abnormal_rows_remain_visible_but_are_not_reusable() {
        let db = cache_db().await;
        let mut abnormal = test_result("project-a", "input-abnormal", "rust");
        abnormal.tree_hash = "tree-abnormal".to_string();
        abnormal.exit_code = 254;
        abnormal.passed = false;
        abnormal.failure_kind = Some("infrastructure".to_string());
        store_check_result(db.clone(), abnormal).unwrap();

        assert!(
            get_check_result(db.clone(), "project-a", "rust", "input-abnormal")
                .unwrap()
                .is_none()
        );
        let visible = list_check_results(db.clone(), "project-a", "tree-abnormal").unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].failure_kind.as_deref(), Some("infrastructure"));

        let mut pass = test_result("project-a", "input-abnormal", "rust");
        pass.tree_hash = "tree-recovered".to_string();
        store_check_result(db.clone(), pass).unwrap();
        assert!(get_check_result(db, "project-a", "rust", "input-abnormal")
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn ordinary_red_is_visible_but_not_reusable() {
        let db = cache_db().await;
        let mut red = test_result("project-a", "input-red", "rust");
        red.tree_hash = "tree-red".to_string();
        red.exit_code = 1;
        red.passed = false;
        red.output_tail = "assertion failed".to_string();
        store_check_result(db.clone(), red).unwrap();

        assert!(
            get_check_result(db.clone(), "project-a", "rust", "input-red")
                .unwrap()
                .is_none()
        );
        let visible = list_check_results(db, "project-a", "tree-red").unwrap();
        assert_eq!(visible.len(), 1);
        assert!(!visible[0].passed);
    }

    #[tokio::test]
    async fn pass_is_monotonic_and_preserves_provenance_against_late_red() {
        let db = cache_db().await;
        let mut pass = test_result("project-a", "input-monotonic", "rust");
        pass.executor_id = Some("executor-a".to_string());
        pass.executor_device_id = Some("device-a".to_string());
        pass.executor_connection_generation = Some(7);
        pass.executor_cell_id = Some("slot-a".to_string());
        pass.executor_lease_epoch = Some(9);
        pass.executor_started_at_unix_ms = Some(100);
        pass.executor_finished_at_unix_ms = Some(200);
        pass.toolchain_fingerprint = Some("rustc=test;bun=test".to_string());
        store_check_result(db.clone(), pass).unwrap();

        let mut red = test_result("project-a", "input-monotonic", "rust");
        red.passed = false;
        red.exit_code = 1;
        red.output_tail = "late failure".to_string();
        red.executor_id = Some("executor-b".to_string());
        store_check_result(db.clone(), red).unwrap();

        let row = get_check_result(db.clone(), "project-a", "rust", "input-monotonic")
            .unwrap()
            .expect("pass remains reusable");
        assert!(row.passed);
        assert_eq!(row.executor_id.as_deref(), Some("executor-a"));
        assert_eq!(row.executor_device_id.as_deref(), Some("device-a"));
        assert_eq!(row.executor_connection_generation, Some(7));
        assert_eq!(row.executor_cell_id.as_deref(), Some("slot-a"));
        assert_eq!(row.executor_lease_epoch, Some(9));
        assert_eq!(row.executor_started_at_unix_ms, Some(100));
        assert_eq!(row.executor_finished_at_unix_ms, Some(200));
        assert_eq!(
            row.toolchain_fingerprint.as_deref(),
            Some("rustc=test;bun=test")
        );

        let attributed =
            list_check_results_for_executor_generation(db, "project-a", "executor-a", 7).unwrap();
        assert_eq!(attributed.len(), 1);
        assert_eq!(attributed[0].input_hash, "input-monotonic");
    }

    #[tokio::test]
    async fn latest_per_check_empty_when_no_results() {
        let db = cache_db().await;
        assert!(list_latest_check_results_for_project(db, "project-a")
            .unwrap()
            .is_empty());
    }

    /// The cache grows a few rows per commit and is never pruned, and this read
    /// sits inside the synchronous write-check planning unit, so its cost lands
    /// on the critical path of every source-touching commit. It must scale with
    /// the number of CHECKS, not with the project's accumulated history.
    ///
    /// The correlated `NOT EXISTS` anti-join this replaced was quadratic in the
    /// row count: at the 11,472 rows one real project had accumulated it measured
    /// 108.7 SECONDS (CAIRN-3108). The bound below is deliberately loose relative
    /// to the window query's real cost (tens of milliseconds at this size) so the
    /// test cannot flake on a loaded machine, while still being orders of
    /// magnitude under anything quadratic.
    #[tokio::test]
    async fn latest_per_check_does_not_scale_with_accumulated_history() {
        let db = cache_db().await;
        let checks = ["rust-fmt", "lockfile", "migrations"];
        let rows_per_check = 1_200;
        let mut script = String::new();
        for check in checks {
            for run in 0..rows_per_check {
                script.push_str(&format!(
                    "INSERT INTO check_result_cache
                       (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                        output_tail, duration_ms, ran_at)
                     VALUES ('project-a', 'tree-{check}-{run}', 'input-{check}-{run}', '{check}',
                             0, 1, 'ok', 10, {run});\n"
                ));
            }
        }
        db.execute_script(&script).await.unwrap();

        let started = std::time::Instant::now();
        let rows = list_latest_check_results_for_project(db, "project-a").unwrap();
        let elapsed = started.elapsed();

        assert_eq!(rows.len(), checks.len(), "exactly one row per check name");
        for row in &rows {
            assert_eq!(
                row.tree_hash,
                format!("tree-{}-{}", row.check_name, rows_per_check - 1),
                "each check resolves to its most recent row"
            );
        }
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "latest-per-check must not rescan history per row; took {elapsed:?} over {} rows",
            checks.len() * rows_per_check
        );
    }
    #[tokio::test]
    async fn reusable_lookup_honors_declared_platform_and_returns_source_identity() {
        let db = cache_db().await;
        let source = observation("obs-trust", "commit-trust", "declared-env");
        record_fresh_check_observation(db.clone(), source).unwrap();

        assert!(get_reusable_check_result(
            db.clone(),
            ReusableCheckLookup {
                project_id: "project-a",
                check_name: "rust",
                input_hash: "input-rust",
                verdict_platforms: &["macos".into()],
                implementation_identity: cairn_common::check_environment::implementation_identity(),
                verdict_environment_hash: "declared-env",
                result_schema_version: 1,
            }
        )
        .unwrap()
        .is_none());

        let reused = get_reusable_check_result(
            db,
            ReusableCheckLookup {
                project_id: "project-a",
                check_name: "rust",
                input_hash: "input-rust",
                verdict_platforms: &["macos".into(), "linux".into()],
                implementation_identity: cairn_common::check_environment::implementation_identity(),
                verdict_environment_hash: "declared-env",
                result_schema_version: 1,
            },
        )
        .unwrap()
        .expect("linux is in the declared trust set");
        assert_eq!(reused.source_observation_id, "obs-trust");
        assert_eq!(reused.environment_fingerprint, "declared-env");
    }
}
