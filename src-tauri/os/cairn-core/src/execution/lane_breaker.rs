//! The lane breaker: identical infrastructure reds are a verdict about the lane.
//!
//! Every other suppression counter in [`crate::execution::cache`] bounds retries
//! within one repair domain (a head, a platform, a machine, the fleet), and each
//! one is reset by the repair it names. None of them notices the shape that
//! cost cairn/5357 hundreds of waves: a review lane that fails as
//! `infrastructure` the same way on every machine, attempt after attempt, while
//! no counter in any one domain reaches its bound. A deterministic red the
//! classifier labels as infrastructure is the usual cause. Rerunning it cannot
//! produce a verdict, so after [`LANE_IDENTICAL_FAILURE_BOUND`] identical
//! failures the lane is held until a fact it was judged under moves.
//!
//! "Identical" is two persisted facts on each failing observation:
//!
//! - its `failure_signature`, a digest of how it failed with the per-run noise
//!   (numbers, hashes, paths) normalized out; and
//! - its `placement_facts`, a digest of the machines and fleet it could be
//!   placed on, which moves when a machine is repaired, re-measured, attached, or
//!   detached.
//!
//! The trip is computed from the observations themselves rather than from a
//! counter, so it lifts by construction: a new input hash is a new key, moved
//! placement facts no longer match, and a genuine verdict for the input resets
//! the window the failures are counted in.
//!
//! It applies to turn-end review waves only, and only they consult it (see
//! [`crate::execution::cache::InfraScopes::lane_breaker`]). A builder's manual
//! run and the write and main cadences are never held by it, so a mislabelled
//! red can always be re-run by hand and can never silence the main cadence.

use cairn_db::turso::params;
use sha2::{Digest, Sha256};

use crate::execution::cache::{CheckResultCacheEntry, SuppressedCheck, SuppressedScope};
use crate::storage::{DbResult, RowExt};

/// Identical infrastructure failures of one review lane before it is held.
///
/// The same number as [`crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND`]
/// for the same reason: three is enough to tell a transient stumble from a
/// standing condition, and few enough that a transient one still gets retries.
pub(crate) const LANE_IDENTICAL_FAILURE_BOUND: i64 = 3;

/// The scope a lane suppression reports itself under, followed by the failure
/// signature it was tripped on. Never written as a counter row: a lane trip is
/// read from observations, so the prefix only names the evidence.
pub(crate) const LANE_SCOPE_PREFIX: &str = "lane:";

/// How a failing observation failed, as a digest that is equal across attempts
/// that failed the same way.
///
/// `cause` is the substrate shape for a failure Cairn's machinery produced, or
/// the classifier's reason for one it read from the command's output, and
/// `evidence` is the line that classification rests on (the `Failing step` line
/// of a substrate failure, the classifier's evidence line otherwise).
pub(crate) fn failure_signature(
    failure_kind: &str,
    cause: &str,
    exit_code: i32,
    evidence: &str,
) -> String {
    let exit_code = exit_code.to_string();
    let evidence = normalize_evidence(evidence);
    digest([failure_kind, cause, exit_code.as_str(), evidence.as_str()])
}

/// The digest of the placement facts a lane was attempted under: its fleet
/// capability scope and every eligible machine's repair identity, sorted.
///
/// The head scopes are excluded on purpose. They name this job at this commit,
/// which is the identity a breaker must hold ACROSS, not a fact about where the
/// lane can run.
pub(crate) fn placement_facts(fleet: Option<&str>, environments: &[&str]) -> String {
    let mut environments = environments.to_vec();
    environments.sort_unstable();
    let mut parts = vec![fleet.unwrap_or("")];
    parts.extend(environments);
    digest(parts)
}

fn digest<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        // A separator no part can contain, so `("ab", "c")` and `("a", "bc")`
        // never collide.
        hasher.update([0x1f]);
    }
    format!("{:x}", hasher.finalize())
}

/// The per-run noise in an evidence line replaced by placeholders: absolute
/// paths (a cell's checkout differs per machine), hex runs of seven characters
/// or more (commits, digests, cell ids), and digit runs (pids, ports, counts,
/// durations, timestamps).
pub(crate) fn normalize_evidence(line: &str) -> String {
    use std::sync::LazyLock;
    static PATH: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#"(^|[\s'"`(=])(?:[A-Za-z]:[\\/]|/)[^\s'"`)]+"#).expect("valid regex")
    });
    static HEX: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\b[0-9a-fA-F]{7,}\b").expect("valid regex"));
    static DIGITS: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"[0-9]+").expect("valid regex"));
    static SPACE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\s+").expect("valid regex"));
    let line = PATH.replace_all(line.trim(), "${1}<path>");
    let line = HEX.replace_all(&line, "<hex>");
    let line = DIGITS.replace_all(&line, "#");
    SPACE.replace_all(&line, " ").into_owned()
}

/// The line a held lane's sentence quotes: the step a substrate failure names,
/// else the first line of the stored diagnostic, bounded for a one-line read.
pub(crate) fn evidence_excerpt(output_tail: &str) -> String {
    const LIMIT: usize = 160;
    let mut lines = output_tail
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let mut first = None;
    while let Some(line) = lines.next() {
        if line == "Failing step:" {
            if let Some(step) = lines.next() {
                first = Some(step);
            }
            break;
        }
        first.get_or_insert(line);
    }
    let line = first.unwrap_or("");
    if line.chars().count() <= LIMIT {
        line.to_string()
    } else {
        format!("{}\u{2026}", line.chars().take(LIMIT).collect::<String>())
    }
}

/// The first characters of a signature, as a reader quotes it.
pub(crate) fn short_signature(signature: &str) -> &str {
    &signature[..signature.len().min(12)]
}

/// Whether a review lane is held by the breaker for this input, under the
/// placement facts it would be attempted under now, and the suppression that
/// says so.
///
/// Counted: review-cadence observations of `(project, check, input)` at the
/// current runner build and result schema, failed as `infrastructure`, carrying
/// these placement facts, newer than the newest genuine verdict for the input.
/// Grouped by verdict environment and failure signature, so the group that
/// trips is a set of attempts that were alike in every respect the record can
/// state. Capacity, not-placeable, and config-unreadable rows are other kinds
/// and are never counted: each is re-armed on its own fact.
///
/// `placement_facts` is the facts the lane would be attempted under now, which
/// is what a wave and the dormant re-arm ask. `None` asks whether ANY set of
/// facts holds a tripped group, which is what a surface asks of a lane whose
/// latest wave only re-stamped it: that wave was held, under facts the record
/// does not otherwise keep.
pub(crate) async fn lane_trip_conn(
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    placement_facts: Option<&str>,
) -> DbResult<Option<SuppressedCheck>> {
    let schema = crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64;
    let runner_build = cairn_checkenv::implementation_identity();
    let mut verdicts = conn
        .query(
            "SELECT ran_at FROM check_result_observations
              INDEXED BY idx_check_result_observations_reusable_lookup
              WHERE project_id=?1 AND check_name=?2 AND input_hash=?3
                AND result_schema_version=?4
                AND complete=1 AND failure_kind IS NULL
              ORDER BY ran_at DESC LIMIT 1",
            params![project_id, check_name, input_hash, schema],
        )
        .await?;
    let since = match verdicts.next().await? {
        Some(row) => row.i64(0)?,
        None => i64::MIN,
    };
    drop(verdicts);
    let mut groups = conn
        .query(
            "SELECT failure_signature, COUNT(*), MAX(ran_at)
               FROM check_result_observations
              INDEXED BY idx_check_result_observations_reusable_lookup
              WHERE project_id=?1 AND check_name=?2 AND input_hash=?3
                AND result_schema_version=?4 AND ran_at > ?5
                AND cadence='review' AND failure_kind='infrastructure'
                AND runner_build_id=?6 AND placement_facts IS NOT NULL
                AND (?7 IS NULL OR placement_facts=?7)
                AND failure_signature IS NOT NULL
              GROUP BY verdict_environment_hash, failure_signature, placement_facts
             HAVING COUNT(*) >= ?8
              ORDER BY MAX(ran_at) DESC
              LIMIT 1",
            params![
                project_id,
                check_name,
                input_hash,
                schema,
                since,
                runner_build,
                placement_facts,
                LANE_IDENTICAL_FAILURE_BOUND
            ],
        )
        .await?;
    let Some(group) = groups.next().await? else {
        return Ok(None);
    };
    let (signature, count, newest) = (group.text(0)?, group.i64(1)?, group.i64(2)?);
    drop(groups);
    let mut rows = conn
        .query(
            "SELECT project_id, tree_hash, input_hash, check_name, exit_code, output_tail,
                    duration_ms, ran_at, job_id, failure_kind, executor_id, executor_device_id,
                    executor_connection_generation, executor_slot_id, executor_lease_epoch,
                    executor_started_at_unix_ms, executor_finished_at_unix_ms,
                    toolchain_fingerprint, defined_by_commit_sha
               FROM check_result_observations
              INDEXED BY idx_check_result_observations_reusable_lookup
              WHERE project_id=?1 AND check_name=?2 AND input_hash=?3
                AND result_schema_version=?4 AND ran_at=?5 AND failure_signature=?6
              LIMIT 1",
            params![
                project_id,
                check_name,
                input_hash,
                schema,
                newest,
                signature.as_str()
            ],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    // The newest failure of the tripped group stands for it everywhere a
    // suppression is rendered or re-stamped, and its streak is the group's size.
    let entry = CheckResultCacheEntry {
        source_observation_id: None,
        project_id: row.text(0)?,
        tree_hash: row.text(1)?,
        input_hash: row.text(2)?,
        check_name: row.text(3)?,
        exit_code: row.i64(4)? as i32,
        passed: false,
        output_tail: row.text(5)?,
        duration_ms: row.i64(6)?,
        ran_at: row.i64(7)?,
        target_results_json: None,
        job_id: row.opt_text(8)?,
        cached: None,
        failure_kind: row.opt_text(9)?,
        infra_failure_streak: count,
        executor_id: row.opt_text(10)?,
        executor_device_id: row.opt_text(11)?,
        executor_connection_generation: row.opt_i64(12)?,
        executor_cell_id: row.opt_text(13)?,
        executor_lease_epoch: row.opt_i64(14)?,
        executor_started_at_unix_ms: row.opt_i64(15)?,
        executor_finished_at_unix_ms: row.opt_i64(16)?,
        toolchain_fingerprint: row.opt_text(17)?,
        defined_by_commit_sha: row.opt_text(18)?,
    };
    Ok(Some(SuppressedCheck {
        domain: crate::execution::cache::InfraRepairDomain::Lane,
        scopes: vec![SuppressedScope {
            scope: format!("{LANE_SCOPE_PREFIX}{signature}"),
            executor_id: None,
            platform: None,
            entry,
        }],
        fleet_blind: Vec::new(),
    }))
}

/// What the dormant re-arm compares against: the newest review failure of one
/// kind recorded for an input, and whether anything has answered it since.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReviewFailureFacts {
    /// The placement facts that failure was attempted under; `None` for a row
    /// written before they were persisted.
    pub(crate) placement_facts: Option<String>,
    /// A genuine verdict for the same input, runner build, and verdict
    /// environment has been recorded since.
    pub(crate) answered_since: bool,
    /// The substrate shape the runner concluded for that failure, as stored on
    /// the observation. `None` for a failure the classifier read from the
    /// command's output, for a row written before the shape was persisted, and
    /// for a token this build does not know.
    pub(crate) substrate_shape: Option<crate::execution::checks::SubstrateFailureShape>,
}

/// The facts of the newest review-cadence failure of `failure_kind` for
/// `(project, check, input)` at the current result schema, or `None` when no
/// observation records one. Review only, like the breaker: a manual re-test
/// under different machines must not stand in for the review lane's record.
pub(crate) async fn newest_review_failure_conn(
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    failure_kind: &str,
) -> DbResult<Option<ReviewFailureFacts>> {
    let schema = crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64;
    let mut failures = conn
        .query(
            "SELECT ran_at, runner_build_id, verdict_environment_hash, placement_facts,
                    substrate_shape
               FROM check_result_observations
              INDEXED BY idx_check_result_observations_reusable_lookup
              WHERE project_id=?1 AND check_name=?2 AND input_hash=?3
                AND result_schema_version=?4 AND failure_kind=?5 AND cadence='review'
              ORDER BY ran_at DESC LIMIT 1",
            params![project_id, check_name, input_hash, schema, failure_kind],
        )
        .await?;
    let Some(row) = failures.next().await? else {
        return Ok(None);
    };
    let (ran_at, runner_build, environment, placement_facts, substrate_shape) = (
        row.i64(0)?,
        row.opt_text(1)?,
        row.opt_text(2)?,
        row.opt_text(3)?,
        row.opt_text(4)?,
    );
    let substrate_shape = substrate_shape
        .as_deref()
        .and_then(crate::execution::checks::SubstrateFailureShape::from_stored);
    drop(failures);
    // A failure that never reached a machine has no verdict environment, so
    // any verdict at this runner build answers it.
    let mut verdicts = conn
        .query(
            "SELECT 1 FROM check_result_observations
              INDEXED BY idx_check_result_observations_reusable_lookup
              WHERE project_id=?1 AND check_name=?2 AND input_hash=?3
                AND result_schema_version=?4 AND ran_at > ?5
                AND complete=1 AND failure_kind IS NULL
                AND runner_build_id IS ?6
                AND (?7 IS NULL OR verdict_environment_hash = ?7)
              LIMIT 1",
            params![
                project_id,
                check_name,
                input_hash,
                schema,
                ran_at,
                runner_build.as_deref(),
                environment.as_deref()
            ],
        )
        .await?;
    let answered_since = verdicts.next().await?.is_some();
    Ok(Some(ReviewFailureFacts {
        placement_facts,
        answered_since,
        substrate_shape,
    }))
}

/// The breaker holding a review lane as its latest wave left it, for a surface
/// that has the record but not the fleet.
///
/// When that wave only re-stamped the lane (`restamped`), it was held: the
/// hold is any tripped group, whatever facts it tripped under. When the wave
/// ran the lane and it failed, it is held only if that failure's own facts
/// complete a tripped group, which is the next wave's answer while the fleet
/// stays as it was.
pub(crate) async fn held_lane_conn(
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    restamped: bool,
) -> DbResult<Option<SuppressedCheck>> {
    if restamped {
        return lane_trip_conn(conn, project_id, check_name, input_hash, None).await;
    }
    let Some(failure) =
        newest_review_failure_conn(conn, project_id, check_name, input_hash, "infrastructure")
            .await?
    else {
        return Ok(None);
    };
    let Some(facts) = failure.placement_facts.filter(|_| !failure.answered_since) else {
        return Ok(None);
    };
    lane_trip_conn(conn, project_id, check_name, input_hash, Some(&facts)).await
}

/// A review lane the breaker holds, as the executors page lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeldLane {
    pub(crate) project: String,
    pub(crate) check_name: String,
    pub(crate) job_id: Option<String>,
    pub(crate) sentence: String,
}

/// Every review lane of live work the breaker holds, across every open
/// database. Driven from unresolved issues' jobs and their infrastructure rows
/// (the access path the dormant re-arm uses), so its cost follows live work
/// rather than the check history.
pub(crate) async fn held_review_lanes(dbs: &crate::db::DbState) -> Result<Vec<HeldLane>, String> {
    let mut held = Vec::new();
    for db in dbs.all_dbs().await {
        let lanes = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT c.project_id, COALESCE(p.key, c.project_id),
                                    c.check_name, c.input_hash, c.job_id,
                                    COALESCE(c.cached, 0)
                               FROM issues i
                              CROSS JOIN jobs j ON j.issue_id = i.id
                              CROSS JOIN check_result_cache c ON c.job_id = j.id
                               LEFT JOIN projects p ON p.id = c.project_id
                              WHERE i.status NOT IN ('merged', 'closed')
                                AND c.failure_kind = 'infrastructure'
                              ORDER BY c.ran_at DESC",
                            (),
                        )
                        .await?;
                    let mut candidates = Vec::new();
                    while let Some(row) = rows.next().await? {
                        candidates.push((
                            row.text(0)?,
                            row.text(1)?,
                            row.text(2)?,
                            row.text(3)?,
                            row.opt_text(4)?,
                            row.i64(5)? != 0,
                        ));
                    }
                    drop(rows);
                    let mut held = Vec::new();
                    let mut seen = std::collections::HashSet::new();
                    // Newest row first, so each lane is judged by its latest wave.
                    for (project_id, project, check_name, input_hash, job_id, restamped) in
                        candidates
                    {
                        if !seen.insert((
                            project_id.clone(),
                            check_name.clone(),
                            input_hash.clone(),
                        )) {
                            continue;
                        }
                        if let Some(suppressed) =
                            held_lane_conn(conn, &project_id, &check_name, &input_hash, restamped)
                                .await?
                        {
                            held.push(HeldLane {
                                project,
                                check_name,
                                job_id,
                                sentence: crate::execution::checks::lane_breaker_sentence(
                                    &suppressed,
                                ),
                            });
                        }
                    }
                    Ok::<_, crate::storage::DbError>(held)
                })
            })
            .await
            .map_err(|error| error.to_string())?;
        held.extend(lanes);
    }
    held.sort_by(|a, b| (&a.project, &a.check_name).cmp(&(&b.project, &b.check_name)));
    Ok(held)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_normalizes_the_noise_that_differs_per_run() {
        let one = normalize_evidence(
            "error: spawn /Users/a/.cairn/build-slots/7/target/debug/x failed after 1203ms (pid 4411, cell 9f3c2ab1d)",
        );
        let two = normalize_evidence(
            "error: spawn /home/b/.cairn/build-slots/12/target/debug/x failed after 87ms (pid 19, cell 0b77e1fa3)",
        );
        assert_eq!(one, two);
        assert_eq!(
            one,
            "error: spawn <path> failed after #ms (pid #, cell <hex>)"
        );
    }

    #[test]
    fn a_signature_is_stable_across_noise_and_moves_with_the_failure() {
        let arb = |selector: &str| {
            failure_signature(
                "infrastructure",
                "substrate:Dispatch",
                -1,
                &format!(
                    "conflicting review check executor selector name: \"bbs\" vs \"{selector}\""
                ),
            )
        };
        assert_eq!(arb("local"), arb("local"));
        assert_ne!(arb("local"), arb("remote"));
        assert_ne!(
            failure_signature("infrastructure", "a", 75, "x"),
            failure_signature("infrastructure", "a", 74, "x"),
            "the exit code is part of how it failed"
        );
    }

    #[test]
    fn placement_facts_ignore_order_and_move_with_any_machine() {
        let base = placement_facts(Some("infra:fleet:f1"), &["infra:env:a", "infra:env:b"]);
        assert_eq!(
            base,
            placement_facts(Some("infra:fleet:f1"), &["infra:env:b", "infra:env:a"])
        );
        assert_ne!(
            base,
            placement_facts(Some("infra:fleet:f1"), &["infra:env:a", "infra:env:c"])
        );
        assert_ne!(
            base,
            placement_facts(Some("infra:fleet:f2"), &["infra:env:a", "infra:env:b"])
        );
    }

    #[test]
    fn the_quoted_evidence_is_the_failing_step_when_there_is_one() {
        assert_eq!(
            evidence_excerpt(
                "Cairn could not start this check.\nFailing step:\nconflicting selector\nmore"
            ),
            "conflicting selector"
        );
        assert_eq!(evidence_excerpt("\n  first line \nsecond"), "first line");
        assert_eq!(evidence_excerpt(""), "");
    }
}
