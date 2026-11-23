//! The handoff cadence (`when: handoff`): the heaviest suites run when a
//! builder hands its work off, not at every turn end.
//!
//! A handoff is a job whose node ships a pull request creating or refreshing
//! its PR artifact. That write arms one wave: one check request per applicable
//! handoff suite at the job's logical head at that moment, filed through the
//! one request seam ([`requests::file_check_request`]) as a requested check,
//! so it is queued, placed, answered from the ledger when its result key
//! matches, re-armed after a restart and cancelled exactly as any request is.
//! Nothing else arms it: no write wave, no turn-end wave, no base-advance
//! replay. Main cadence runs a handoff suite only from its own `mainCadence`
//! block.
//!
//! One handoff lane per (job, suite) is current. A handoff on a newer head
//! cancels the open lane an earlier handoff filed for an older head. A lane
//! that records anyway keeps its verdict on the head it ran on and is never
//! delivered as the current one. Until a handoff, the node's `/checks` reads
//! each handoff suite as `runs at handoff`; after a replay moves the head on,
//! as `not checked since handoff at <sha>`. Neither holds a checks wait open
//! or counts as a missing verdict.
//!
//! A base-advance replay owes the carried head its cheap lanes instead: every
//! applicable write-cadence suite is filed at the carried head through the
//! same seam, where a lane whose inputs the replay did not change is answered
//! from the ledger without running.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::config::project_settings::{CheckCommand, CheckWhen};
use crate::execution::checks_turn_end::{checks_uri_for_job, resolve_job_coords, JobCoords};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

use super::requests::{self, CheckRequestAnswer, CheckRequestRow, CheckRequestSpec, FiledRequest};

/// The `cadence` a handoff lane's row is filed with.
pub(crate) const HANDOFF: &str = "handoff";
/// The `cadence` a carried head's write lane is filed with.
pub(crate) const CARRIED: &str = "carried";

/// The requester label a cadence's rows carry, `handoff:<node home>`: who the
/// lane was filed for, for a reader. Nothing parses it; a row's cadence is its
/// `cadence` column.
fn requester_label(cadence: &str, home: &str) -> String {
    format!("{cadence}:{home}")
}

/// The node home a job's `/checks` hangs off.
fn home_of(coords: &JobCoords) -> String {
    let checks = checks_uri_for_job(coords);
    checks
        .strip_suffix("/checks")
        .map(str::to_string)
        .unwrap_or(checks)
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// The suites of cadence `when` a wave at a head files, in name order:
/// those that apply to the head's change, or every one of them when
/// applicability could not be read (a lane that selects nothing is cheap; a
/// lane silently skipped is a missing verdict).
fn suites_of(
    checks: &HashMap<String, CheckCommand>,
    when: CheckWhen,
    applicable: Option<&HashSet<String>>,
) -> Vec<String> {
    let mut names: Vec<String> = checks
        .iter()
        .filter(|(name, check)| {
            check.when == when && applicable.is_none_or(|names| names.contains(*name))
        })
        .map(|(name, _)| name.clone())
        .collect();
    names.sort();
    names
}

/// The handoff suites one handoff files.
pub(crate) fn handoff_suites(
    checks: &HashMap<String, CheckCommand>,
    applicable: Option<&HashSet<String>>,
) -> Vec<String> {
    suites_of(checks, CheckWhen::Handoff, applicable)
}

/// The lanes a base-advance replay owes its carried head: the applicable
/// write-cadence suites, and never a handoff suite.
pub(crate) fn carried_head_suites(
    checks: &HashMap<String, CheckCommand>,
    applicable: Option<&HashSet<String>>,
) -> Vec<String> {
    suites_of(checks, CheckWhen::Write, applicable)
}

/// Whether the job's recipe node feeds a `pr` node: the topology gate the
/// review cadence uses (`node_ships_a_pr`).
async fn job_ships_a_pr(db: &LocalDb, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT recipe_node_id, execution_id FROM jobs WHERE id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(false);
            };
            let (Some(node_id), Some(execution_id)) = (row.opt_text(0)?, row.opt_text(1)?) else {
                return Ok(false);
            };
            crate::execution::jobs::node_ships_a_pr_conn(conn, &node_id, &execution_id).await
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// What arming a wave did.
#[derive(Debug)]
pub(crate) enum Armed {
    /// The job's node ships no pull request, so nothing is armed.
    NotShipping,
    /// The job's head could not be read; nothing was filed.
    NoHead(String),
    /// The requests filed at `head`, the older-head lanes it cancelled, the
    /// suites whose newest lane was already at `head`, and the suites whose
    /// running older-head lane was kept because its inputs did not change.
    Wave {
        head: String,
        filed: Vec<FiledRequest>,
        superseded: Vec<String>,
        already: Vec<String>,
        kept: Vec<String>,
    },
}

impl Armed {
    /// One line for the log.
    pub(crate) fn summary(&self) -> String {
        match self {
            Self::NotShipping => "not armed: the node ships no pull request".into(),
            Self::NoHead(error) => format!("not armed: no readable head ({error})"),
            Self::Wave {
                head,
                filed,
                superseded,
                already,
                kept,
            } => format!(
                "at {}: filed [{}], superseded {}, already at this head [{}], kept running [{}]",
                short(head),
                filed
                    .iter()
                    .map(|request| request.suite.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                superseded.len(),
                already.join(", "),
                kept.join(", "),
            ),
        }
    }
}

/// A boxed `Send` future. Filing a request can prepare a managed branch,
/// preparing one files its carried head's lanes, and those are requests
/// again: naming the future's type here is what lets the compiler see that
/// cycle as `Send` instead of failing to infer it.
pub(crate) type SendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Arm the handoff wave for a job that just created or refreshed its PR
/// artifact.
pub(crate) fn arm_on_pr_artifact<'a>(
    orch: &'a Orchestrator,
    job_id: &'a str,
) -> SendFuture<'a, Result<Armed, String>> {
    Box::pin(arm(orch, job_id, CheckWhen::Handoff))
}

/// File the lanes a base-advance replay owes the head it carried the job's
/// branch to. Handoff suites are not among them.
pub(crate) fn file_carried_head_lanes<'a>(
    orch: &'a Orchestrator,
    job_id: &'a str,
) -> SendFuture<'a, Result<Armed, String>> {
    Box::pin(arm(orch, job_id, CheckWhen::Write))
}

async fn arm(orch: &Orchestrator, job_id: &str, when: CheckWhen) -> Result<Armed, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    if !job_ships_a_pr(&db, job_id).await? {
        return Ok(Armed::NotShipping);
    }
    let coords = resolve_job_coords(&db, job_id)
        .await?
        .ok_or_else(|| format!("job {job_id} has no node coordinates"))?;
    let head = match crate::execution::cache::resolve_job_logical_head(orch, job_id).await {
        Ok(head) => head,
        Err(error) => return Ok(Armed::NoHead(error.to_string())),
    };
    let (checks, applicable) = crate::execution::checks_status::declared_and_applicable_at(
        orch, &db, job_id, &coords, &head,
    )
    .await
    .ok_or_else(|| format!("the checks contract at {head} is unreadable"))?;
    let (suites, cadence) = match when {
        CheckWhen::Handoff => (handoff_suites(&checks, applicable.as_ref()), HANDOFF),
        _ => (carried_head_suites(&checks, applicable.as_ref()), CARRIED),
    };
    file_wave(orch, &db, &coords, job_id, &head, &suites, cadence).await
}

/// What a wave does for one suite.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SuitePlan<'a> {
    /// The suite's newest lane is already at this head, open or recorded:
    /// file nothing and cancel nothing (a PR-artifact patch that did not move
    /// the head).
    AlreadyAtHead,
    /// An open lane on an older head whose inputs for this suite did not
    /// change since: it is kept, and its verdict answers this head by reuse.
    /// Open lanes whose inputs did change are cancelled.
    Keep {
        running: &'a CheckRequestRow,
        supersede: Vec<&'a CheckRequestRow>,
    },
    /// File a lane at this head, cancelling these open older-head lanes,
    /// whose inputs changed.
    File { supersede: Vec<&'a CheckRequestRow> },
}

/// Decide one suite from the cadence's earlier rows for the job (newest
/// first). `inputs_changed(commit)` answers whether the suite's inputs differ
/// between `commit` and `head`; a lane on no resolved commit counts as changed.
/// Pure, so each case is tested.
pub(crate) fn plan_suite<'a>(
    rows: &'a [CheckRequestRow],
    suite: &str,
    head: &str,
    inputs_changed: impl Fn(&str) -> bool,
) -> SuitePlan<'a> {
    let mut of_suite = rows.iter().filter(|row| row.suite == suite);
    if of_suite.clone().next().is_some_and(|newest| {
        newest.commit_sha.as_deref() == Some(head)
            && (newest.is_open() || newest.state == "recorded")
    }) {
        return SuitePlan::AlreadyAtHead;
    }
    let (unchanged, supersede): (Vec<_>, Vec<_>) = of_suite
        .by_ref()
        .filter(|row| row.is_open() && row.commit_sha.as_deref() != Some(head))
        .partition(|row| {
            row.commit_sha
                .as_deref()
                .is_some_and(|commit| !inputs_changed(commit))
        });
    match unchanged.first().copied() {
        Some(running) => SuitePlan::Keep { running, supersede },
        None => SuitePlan::File { supersede },
    }
}

/// File the cadence's wave at `head`: per suite, nothing when its newest lane
/// is already at `head`; otherwise one request at `head`, after cancelling the
/// open lanes of older heads whose inputs for that suite changed. An open
/// older lane whose inputs did not change is kept instead of restarted.
pub(crate) async fn file_wave(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    coords: &JobCoords,
    job_id: &str,
    head: &str,
    suites: &[String],
    cadence: &'static str,
) -> Result<Armed, String> {
    let earlier = requests::requests_of_cadence(db, job_id, cadence, 200).await?;
    // Which suites' inputs changed between each open older head and this one,
    // by the same impact and scope selection that decides applicability.
    let mut changed_since: HashMap<String, Option<HashSet<String>>> = HashMap::new();
    for row in earlier.iter().filter(|row| row.is_open()) {
        let Some(commit) = row.commit_sha.as_deref().filter(|commit| *commit != head) else {
            continue;
        };
        if !changed_since.contains_key(commit) {
            let changed =
                crate::execution::checks_status::applicable_names_at(orch, coords, head, commit)
                    .await;
            changed_since.insert(commit.to_string(), changed);
        }
    }
    let inputs_changed = |suite: &str, commit: &str| {
        changed_since
            .get(commit)
            .and_then(Option::as_ref)
            .is_none_or(|changed| changed.contains(suite))
    };
    let requester = requester_label(cadence, &home_of(coords));
    let by = format!("the newer lane at {} that superseded it", short(head));
    let (mut filed, mut superseded, mut already, mut kept) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for suite in suites {
        let supersede = match plan_suite(&earlier, suite, head, |commit| {
            inputs_changed(suite, commit)
        }) {
            SuitePlan::AlreadyAtHead => {
                already.push(suite.clone());
                continue;
            }
            SuitePlan::Keep { running, supersede } => {
                log::debug!(
                    "{cadence} {suite} at {}: kept {}, whose inputs are unchanged",
                    short(head),
                    running.id
                );
                kept.push(suite.clone());
                supersede
            }
            SuitePlan::File { supersede } => {
                filed.push(
                    requests::file_check_request(
                        orch,
                        CheckRequestSpec {
                            consumer: super::CheckConsumer::Requested,
                            priority: cairn_common::wire_vocabulary::CellPriority::ReviewCheck,
                            project_id: coords.project_id.clone(),
                            project_key: coords.project_key.clone(),
                            suite: suite.clone(),
                            revision: Some(head.to_string()),
                            subject_job_id: job_id.to_string(),
                            placement: None,
                            retry: false,
                            requester_ref: requester.clone(),
                            requester_job_id: Some(job_id.to_string()),
                            cadence: Some(cadence),
                        },
                    )
                    .await?,
                );
                supersede
            }
        };
        for older in supersede {
            match requests::cancel_check_request(orch, db, older, &by).await {
                Ok(_) => superseded.push(older.id.clone()),
                Err(error) => log::debug!("supersede {}: {error}", older.id),
            }
        }
    }
    orch.node_check_status_cache
        .invalidate(job_id, "handoff-cadence lanes filed");
    Ok(Armed::Wave {
        head: head.to_string(),
        filed,
        superseded,
        already,
        kept,
    })
}

/// The handoff cadence's own lanes on a resolved PR end with it.
pub(crate) async fn cancel_for_issue(orch: &Orchestrator, db: &LocalDb, issue_id: &str) {
    let rows = match requests::open_cadence_requests_of_issue(db, issue_id).await {
        Ok(rows) => rows,
        Err(error) => {
            log::warn!("resolution of issue {issue_id}: {error}");
            return;
        }
    };
    for row in rows {
        if let Err(error) =
            requests::cancel_check_request(orch, db, &row, "the resolution of its pull request")
                .await
        {
            log::debug!("resolution of issue {issue_id}: {error}");
        }
    }
}

// --- settlement --------------------------------------------------------------

/// A settled handoff lane refreshes its node's `/checks`, and a verdict that
/// is still the current one for its suite is delivered to the node under the
/// changed-failing-set rule the turn-end notices use: it wakes only when what
/// the node was last told about that suite changed.
///
/// Boxed, so every driver that settles a request does not carry the delivery's
/// future inline.
pub(crate) fn on_settled<'a>(
    orch: &'a Orchestrator,
    row: &'a CheckRequestRow,
    answer: &'a CheckRequestAnswer,
) -> SendFuture<'a, ()> {
    Box::pin(async move {
        if row.cadence.as_deref() != Some(HANDOFF) {
            return;
        }
        let Some(job_id) = row.requester_job_id.as_deref() else {
            return;
        };
        orch.node_check_status_cache
            .invalidate(job_id, "handoff lane settled");
        if let Err(error) = deliver(orch, job_id, row, answer).await {
            log::warn!("handoff lane {} of job {job_id}: {error}", row.id);
        }
    })
}

async fn deliver(
    orch: &Orchestrator,
    job_id: &str,
    row: &CheckRequestRow,
    answer: &CheckRequestAnswer,
) -> Result<(), String> {
    let CheckRequestAnswer::Complete { result } = answer else {
        return Ok(());
    };
    // Superseded, or not a verdict about the code: nothing for the node.
    if row.cancel_requested_by.is_some()
        || result.no_verdict.is_some()
        || result.placement_unmet.is_some()
    {
        return Ok(());
    }
    let db = requests::request_db(orch, &row.project_id).await?;
    let Some(coords) = resolve_job_coords(&db, job_id).await? else {
        return Ok(());
    };
    let entry = {
        let (db, project, suite, input) = (
            db.clone(),
            row.project_id.clone(),
            row.suite.clone(),
            result.input_hash.clone(),
        );
        tokio::task::spawn_blocking(move || {
            crate::execution::cache::get_check_result(db, &project, &suite, &input)
        })
        .await
        .map_err(|error| error.to_string())??
    };
    let outcome = outcome_of(&row.suite, result, entry);
    crate::execution::checks_turn_end::deliver_handoff_outcome(
        &db,
        job_id,
        &checks_uri_for_job(&coords),
        &row.suite,
        &result.tree_hash,
        outcome,
    )
    .await
    .map(|_| ())
}

/// The lane's verdict as a check outcome, with the per-test detail its ledger
/// row holds, so the failing set is the one the notice compares.
fn outcome_of(
    suite: &str,
    result: &crate::execution::checks::ManualConfiguredCheckResult,
    entry: Option<crate::execution::cache::CheckResultCacheEntry>,
) -> crate::execution::checks::CheckOutcome {
    let parsed = entry
        .as_ref()
        .and_then(|entry| entry.target_results_json.as_deref())
        .and_then(|json| serde_json::from_str(json).ok());
    crate::execution::checks::CheckOutcome {
        consumer_guard: None,
        left_running: None,
        name: suite.to_string(),
        passed: result.passed,
        exit_code: result.exit_code,
        failure_kind: None,
        parsed,
        output_tail: result.output_tail.clone(),
        cached: result.disposition == "cached",
        duration_ms: entry.as_ref().map(|entry| entry.duration_ms).unwrap_or(0),
        suppressed_after: None,
        recorded: None,
        not_recorded: result.not_recorded.clone(),
    }
}

// --- the node's `/checks` ------------------------------------------------------

/// The newest handoff lane of one suite, as `/checks` reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HandoffStanding {
    pub commit: Option<String>,
    pub open: bool,
    pub uri: String,
    /// Its state while open, its answer once settled.
    pub words: String,
}

/// What `/checks` knows of a job's handoff lanes: the head it projects and
/// the newest lane per suite.
#[derive(Debug, Clone, Default)]
pub(crate) struct HandoffLanes {
    pub head: String,
    pub latest: HashMap<String, HandoffStanding>,
}

/// The newest handoff lane per suite for the job.
pub(crate) async fn handoff_lanes(
    db: &LocalDb,
    coords: &JobCoords,
    job_id: &str,
    head: &str,
) -> HandoffLanes {
    let rows = requests::requests_of_cadence(db, job_id, HANDOFF, 200)
        .await
        .inspect_err(|error| log::debug!("handoff lanes of {job_id}: {error}"))
        .unwrap_or_default();
    let mut latest = HashMap::new();
    for row in rows {
        latest
            .entry(row.suite.clone())
            .or_insert_with(|| HandoffStanding {
                commit: row.commit_sha.clone(),
                open: row.is_open(),
                uri: requests::request_uri(&coords.project_key, &row.id),
                words: match CheckRequestAnswer::of_settled(&row) {
                    Some(answer) if !row.is_open() => requests::verdict_words(&answer),
                    _ => requests::standing(&row),
                },
            });
    }
    HandoffLanes {
        head: head.to_string(),
        latest,
    }
}

/// What a handoff suite with no verdict at the projected head says in place
/// of one. Pure, so each state's words are tested.
pub(crate) fn lane_notice(name: &str, standing: Option<&HandoffStanding>, head: &str) -> String {
    match standing {
        None => format!(
            "runs at handoff: one lane is filed when this node writes its PR artifact; a check \
             request runs it sooner: append {{suite:\"{name}\"}} to cairn:~/checks"
        ),
        Some(standing) => {
            let at = standing
                .commit
                .as_deref()
                .map(short)
                .unwrap_or("an unresolved head");
            if standing.commit.as_deref() == Some(head) {
                if standing.open {
                    format!(
                        "handoff lane at {at}: {} ({})",
                        standing.words, standing.uri
                    )
                } else {
                    format!("handoff at {at}: {} ({})", standing.words, standing.uri)
                }
            } else {
                let there = if standing.open {
                    format!("its lane there is {}", standing.words)
                } else {
                    format!("there: {}", standing.words)
                };
                format!(
                    "not checked since handoff at {at} ({there}, {}); it runs again on the next \
                     PR-artifact write or on request",
                    standing.uri
                )
            }
        }
    }
}

#[cfg(test)]
#[path = "handoff_tests.rs"]
mod tests;
