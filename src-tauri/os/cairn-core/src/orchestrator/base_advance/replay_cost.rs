//! What a branch replay actually cost, and the one durable consequence a cost
//! may have (cairn/6456).
//!
//! # Samples
//!
//! Every preparation that reaches detached computation records one versioned
//! [`ReplayCostEvidence`]: the coordinate it computed at, the native engine's
//! own observation of where its time went ([`crate::jj::ReplayCostObservation`]),
//! the preparation's phase timings, the budget it ran under, the host and its
//! load, and how it ended. Fields that could not be measured stay `None` and are
//! rendered as unknown, never invented. Samples are kept per project with
//! bounded retention and are descriptive only: nothing is refused because of a
//! sample. A prediction-based refusal waits for a validated, conservative bound
//! (the issue's benchmark), which samples alone do not supply.
//!
//! # Timeout blockers
//!
//! An explicit replay whose detached computation exhausted its compute budget
//! at exact coordinates records a [`TimeoutBlocker`] keyed by [`TimeoutKey`]:
//! project and store, branch, captured head, destination base, replay versus
//! merge mode, the effective resolution, and the engine version. The next
//! explicit request at an identical key is refused before any expensive work,
//! because the same work under the same budget would end the same way. Any
//! change to a key part is a different attempt; host and load are recorded for
//! attribution but are not part of the key, so they never become an unlimited
//! retry escape.
//!
//! A blocker never outlives its coordinates: the key holds the head and the
//! destination base, so a new commit or a moved base is always a new attempt,
//! stale rows are deleted when the branch is next looked up, and an install of
//! the branch deletes them all. No hand or operator step is needed to clear one.
//!
//! Only explicit-budget exhaustion blocks, and only an explicit retry: the
//! automatic 10 s preparation never records one (its own coordinate-keyed
//! cooldown governs automatic retries) and never consults one. A blocker also
//! holds the compute allowance that ran out, and an attempt whose computation
//! can run materially longer ([`MATERIALLY_LONGER`]) is a different attempt:
//! time lost to lock or reservation waits that have since cleared never turns
//! into a refusal. Lock waiting, contention, export failure, stale coordinates
//! and incomplete recovery never record one at all: they are not compute
//! exhaustion.
//!
//! [`TimeoutBlocker::eligibility`] is the one decision of whether a blocker
//! refuses an attempt. Admission asks it with the full key, the rebase page
//! with what a page can know (the live head, base and mode), and the timeout
//! answer carries the exact blocker its own preparation recorded.

use std::path::Path;
use std::time::{Duration, Instant};

use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::jj::{ComputePhase, ReplayCostObservation, REPLAY_ENGINE_VERSION};
use crate::storage::{LocalDb, RowExt};

/// The shape of a stored [`ReplayCostEvidence`]. A row of another version is
/// shown as unreadable rather than guessed at.
pub(crate) const EVIDENCE_VERSION: u32 = 1;

/// Samples kept per project, newest first. Blockers are never evicted by it.
const SAMPLES_KEPT_PER_PROJECT: i64 = 500;

/// Samples of the same engine a page summarizes beside the branch's own.
const COMPARABLE_SAMPLES: i64 = 50;

/// Which allowance a computation ran under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BudgetClass {
    /// An owner's explicit replay request: one cumulative 60 s deadline.
    Explicit,
    /// Automatic preparation for a commit, check, run or landing: 10 s.
    Background,
}

impl BudgetClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Background => "background",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplayMode {
    /// The branch's commits are replayed onto the base and healed.
    Replay,
    /// Operator-approved catch-up: one merge commit of the base.
    Merge,
}

impl ReplayMode {
    pub(crate) fn of(merge_mode: bool) -> Self {
        if merge_mode {
            Self::Merge
        } else {
            Self::Replay
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Merge => "merge",
        }
    }
}

/// How the measured preparation ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub(crate) enum CostOutcome {
    /// A new head was installed.
    Installed,
    /// Computation produced the head the branch already had.
    AlreadyCurrent,
    /// The replay conflicts with the base.
    Conflicted,
    /// Detached computation ran out of its compute budget.
    ComputeExhausted,
    /// A budgeted step outside detached computation ran out of time.
    TimedOut,
    /// Anything else, by the refusal's stable kind.
    Refused(String),
}

impl CostOutcome {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Installed => "installed".to_string(),
            Self::AlreadyCurrent => "already current".to_string(),
            Self::Conflicted => "conflicted".to_string(),
            Self::ComputeExhausted => "compute budget exhausted".to_string(),
            Self::TimedOut => "timed out outside computation".to_string(),
            Self::Refused(kind) => format!("refused ({kind})"),
        }
    }

    fn column(&self) -> &str {
        match self {
            Self::Installed => "installed",
            Self::AlreadyCurrent => "already_current",
            Self::Conflicted => "conflicted",
            Self::ComputeExhausted => "compute_exhausted",
            Self::TimedOut => "timed_out",
            Self::Refused(_) => "refused",
        }
    }
}

/// Wall time of each preparation phase, in milliseconds. `None` is a phase the
/// preparation never reached.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PhaseTimings {
    /// Explicit requests only: base resolution, the take-committed-tip proof,
    /// the carry plan and the request's acceptance, before preparation began.
    pub(crate) proof_ms: Option<u64>,
    /// From the attempt's start to detached computation: lock and reservation
    /// waits, the snapshot, recovery, the already-current probe and repairs.
    pub(crate) capture_ms: Option<u64>,
    /// Detached computation: rebase, validation and ancestry heal.
    pub(crate) compute_ms: Option<u64>,
    /// Reservation, lock and the local install.
    pub(crate) install_ms: Option<u64>,
    /// The verified Git export and its settlement.
    pub(crate) export_ms: Option<u64>,
    /// From the start of the request's budget to the outcome.
    pub(crate) total_ms: u64,
}

/// One measured preparation. Serialized whole into its row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReplayCostEvidence {
    pub(crate) version: u32,
    pub(crate) engine_version: u32,
    /// The recording executable's identity, for attribution. Engine
    /// comparability is [`Self::engine_version`]'s alone.
    pub(crate) build: Option<String>,
    pub(crate) recorded_at: i64,
    pub(crate) host: Option<String>,
    /// The host's one-minute load average when the outcome was recorded.
    pub(crate) load1: Option<f64>,
    pub(crate) branch: String,
    pub(crate) head: String,
    pub(crate) base_branch: String,
    pub(crate) base: String,
    pub(crate) mode: ReplayMode,
    pub(crate) budget_class: BudgetClass,
    /// [`resolution_fingerprint`] of what the computation carried.
    pub(crate) resolution: String,
    pub(crate) observed: ReplayCostObservation,
    pub(crate) phases: PhaseTimings,
    /// The whole request's budget.
    pub(crate) budget_ms: u64,
    /// What was left of it for detached computation.
    pub(crate) compute_allowance_ms: Option<u64>,
    pub(crate) outcome: CostOutcome,
    /// The step that ran out of time, for a timeout.
    pub(crate) failure_phase: Option<String>,
}

impl ReplayCostEvidence {
    /// Healing time per commit the heal found conflict-flagged: a description
    /// of this one sample, never a prediction.
    pub(crate) fn heal_ms_per_flagged(&self) -> Option<f64> {
        let flagged = self
            .observed
            .flagged_after_rebase
            .filter(|count| *count > 0)?;
        Some(self.observed.heal_ms? as f64 / flagged as f64)
    }
}

/// What a timeout blocker is keyed by: an identical explicit attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TimeoutKey {
    pub(crate) project_id: String,
    pub(crate) store_path: String,
    pub(crate) branch: String,
    pub(crate) head: String,
    pub(crate) base_branch: String,
    pub(crate) base: String,
    pub(crate) mode: ReplayMode,
    pub(crate) resolution: String,
    pub(crate) engine_version: u32,
}

/// A recorded explicit compute-budget exhaustion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TimeoutBlocker {
    pub(crate) key: TimeoutKey,
    /// Where computation was when its budget ran out.
    pub(crate) phase: Option<ComputePhase>,
    pub(crate) compute_ms: u64,
    pub(crate) allowance_ms: u64,
    pub(crate) budget_ms: u64,
    pub(crate) observed: ReplayCostObservation,
    pub(crate) build: Option<String>,
    pub(crate) host: Option<String>,
    pub(crate) recorded_at: i64,
}

/// The explicit request a preparation serves, as its cost evidence needs it.
#[derive(Debug, Clone)]
pub(crate) struct ExplicitCost {
    /// When the request's cumulative budget started.
    pub(crate) started: Instant,
    pub(crate) budget: Duration,
    pub(crate) take_committed_tip: bool,
    pub(crate) drop_incoming: bool,
}

/// How much longer an attempt's computation must be able to run than an
/// exhausted one's before it is a different attempt: one step's minimum. Less
/// is the scheduling jitter between two identical requests.
pub(crate) const MATERIALLY_LONGER: Duration = super::replay_requests::EXPLICIT_STEP_MINIMUM;

/// What is known about an attempt a blocker might refuse. `None` is a
/// dimension the asker cannot know; an unreadable head or base makes the
/// answer unknown, while an unknown mode, resolution or allowance is answered
/// for the blocker's own.
#[derive(Debug, Clone, Default)]
pub(crate) struct Attempt<'a> {
    pub(crate) head: Option<&'a str>,
    pub(crate) base: Option<&'a str>,
    pub(crate) mode: Option<ReplayMode>,
    pub(crate) resolution: Option<&'a str>,
    pub(crate) allowance: Option<Duration>,
}

/// Whether a blocker refuses an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Eligibility {
    /// The attempt would repeat the exhausted one.
    Refused,
    /// The attempt differs; the reason says how.
    Different(String),
    /// Whether it differs cannot be told; the reason says why.
    Unknown(String),
}

impl TimeoutBlocker {
    /// The one decision of whether this blocker refuses `attempt`.
    pub(crate) fn eligibility(&self, attempt: &Attempt) -> Eligibility {
        let key = &self.key;
        if key.engine_version != REPLAY_ENGINE_VERSION {
            return Eligibility::Different(format!(
                "it was recorded by replay engine v{}, and this build runs v{REPLAY_ENGINE_VERSION}",
                key.engine_version
            ));
        }
        match attempt.head {
            None => return Eligibility::Unknown("the branch's head could not be read".into()),
            Some(head) if head != key.head => {
                return Eligibility::Different("the branch has a new head since".into())
            }
            Some(_) => {}
        }
        match attempt.base {
            None => {
                return Eligibility::Unknown(format!(
                    "`{}`'s tip could not be read",
                    key.base_branch
                ))
            }
            Some(base) if base != key.base => {
                return Eligibility::Different(format!("`{}` has advanced since", key.base_branch))
            }
            Some(_) => {}
        }
        if attempt.mode.is_some_and(|mode| mode != key.mode) {
            return Eligibility::Different("the branch's catch-up mode has changed since".into());
        }
        if attempt
            .resolution
            .is_some_and(|resolution| resolution != key.resolution)
        {
            return Eligibility::Different("it carries a different resolution".into());
        }
        if let Some(allowance) = attempt.allowance {
            let exhausted = Duration::from_millis(self.allowance_ms);
            if allowance > exhausted + MATERIALLY_LONGER {
                return Eligibility::Different(format!(
                    "its computation can run {}, where the exhausted one had {}",
                    seconds(millis(allowance)),
                    seconds(self.allowance_ms)
                ));
            }
        }
        Eligibility::Refused
    }
}

/// The effective resolution an explicit replay carries, as one stable string.
/// Take-committed-tip, an attested drop of incoming work, and the exact carried
/// paths and contents each change it; the reason's wording does not.
pub(crate) fn resolution_fingerprint(
    take_committed_tip: bool,
    drop_incoming: bool,
    carry: Option<&crate::jj::ResolutionCarry>,
) -> String {
    let Some(carry) = carry else {
        return match (take_committed_tip, drop_incoming) {
            (false, false) => "none".to_string(),
            (take, drop) => format!("tip={take};drop={drop};carry=none"),
        };
    };
    let mut hash = Sha256::new();
    let mut paths: Vec<_> = carry.paths.iter().collect();
    paths.sort_by(|a, b| a.0.cmp(&b.0));
    for (path, content) in paths {
        hash.update(path.as_bytes());
        hash.update([0]);
        match content {
            crate::jj::CarriedContent::FromTip => hash.update(b"tip"),
            crate::jj::CarriedContent::Text(text) => {
                hash.update(b"text");
                hash.update([0]);
                hash.update(text.as_bytes());
            }
        }
        hash.update([0]);
    }
    let digest = hash.finalize();
    let short: String = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("tip={take_committed_tip};drop={drop_incoming};carry={short}")
}

/// This host's name and one-minute load, each `None` when unavailable.
pub(crate) fn host_context() -> (Option<String>, Option<f64>) {
    let host = hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok());
    let load = crate::pressure::platform::host_load_average().map(|load| load[0]);
    (host, load)
}

/// The recording executable's identity, hashed once per process off the
/// async runtime.
pub(crate) async fn build_identity() -> Option<String> {
    tokio::task::spawn_blocking(|| crate::pr_data::actions::merge_metrics::build_id().to_string())
        .await
        .ok()
}

pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn store_key(store: &Path) -> String {
    store.to_string_lossy().into_owned()
}

/// Persist one sample and trim the project's samples to the retention bound.
pub(crate) async fn record_sample(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    evidence: &ReplayCostEvidence,
) -> Result<(), String> {
    let json = serde_json::to_string(evidence).map_err(|error| error.to_string())?;
    let project_id = project_id.to_string();
    let store = store_key(store);
    let branch = evidence.branch.clone();
    let engine = i64::from(evidence.engine_version);
    let class = evidence.budget_class.as_str();
    let outcome = evidence.outcome.column().to_string();
    let recorded_at = evidence.recorded_at;
    db.write(move |conn| {
        let (project_id, store, branch, outcome, json) = (
            project_id.clone(),
            store.clone(),
            branch.clone(),
            outcome.clone(),
            json.clone(),
        );
        Box::pin(async move {
            conn.execute(
                "INSERT INTO replay_cost_samples
                   (project_id, store_path, branch, engine_version, budget_class, outcome,
                    recorded_at, evidence_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    project_id.as_str(),
                    store.as_str(),
                    branch.as_str(),
                    engine,
                    class,
                    outcome.as_str(),
                    recorded_at,
                    json.as_str()
                ],
            )
            .await?;
            conn.execute(
                "DELETE FROM replay_cost_samples
                  WHERE project_id = ?1
                    AND id NOT IN (SELECT id FROM replay_cost_samples WHERE project_id = ?1
                                    ORDER BY id DESC LIMIT ?2)",
                params![project_id.as_str(), SAMPLES_KEPT_PER_PROJECT],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("record replay cost sample: {error}"))
}

/// A stored sample, or why it cannot be read.
pub(crate) type StoredSample = Result<ReplayCostEvidence, String>;

fn parse_sample(json: &str) -> StoredSample {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|error| format!("unreadable sample: {error}"))?;
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    if version != Some(u64::from(EVIDENCE_VERSION)) {
        return Err(format!(
            "a sample of evidence version {} this build does not read",
            version.map_or_else(|| "unknown".to_string(), |v| v.to_string())
        ));
    }
    serde_json::from_value(value).map_err(|error| format!("unreadable sample: {error}"))
}

async fn samples(
    db: &LocalDb,
    sql: &'static str,
    args: (String, String, i64),
) -> Result<Vec<StoredSample>, String> {
    db.read(move |conn| {
        let args = args.clone();
        Box::pin(async move {
            let mut rows = conn.query(sql, params![args.0, args.1, args.2]).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(parse_sample(&row.text(0)?));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("read replay cost samples: {error}"))
}

/// The branch's newest samples, newest first.
pub(crate) async fn branch_samples(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    limit: i64,
) -> Result<Vec<StoredSample>, String> {
    let store = store_key(store);
    db.read(move |conn| {
        let (project_id, store, branch) =
            (project_id.to_string(), store.clone(), branch.to_string());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT evidence_json FROM replay_cost_samples
                      WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3
                      ORDER BY recorded_at DESC, id DESC LIMIT ?4",
                    params![project_id, store, branch, limit],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(parse_sample(&row.text(0)?));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("read replay cost samples: {error}"))
}

/// The project's newest samples taken by this engine version, any branch.
pub(crate) async fn engine_samples(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<StoredSample>, String> {
    samples(
        db,
        "SELECT evidence_json FROM replay_cost_samples
          WHERE project_id = ?1 AND engine_version = CAST(?2 AS INTEGER)
          ORDER BY recorded_at DESC, id DESC LIMIT ?3",
        (
            project_id.to_string(),
            REPLAY_ENGINE_VERSION.to_string(),
            COMPARABLE_SAMPLES,
        ),
    )
    .await
}

/// Record (or refresh) the blocker for its key.
pub(crate) async fn record_blocker(db: &LocalDb, blocker: &TimeoutBlocker) -> Result<(), String> {
    let json = serde_json::to_string(blocker).map_err(|error| error.to_string())?;
    let key = blocker.key.clone();
    let recorded_at = blocker.recorded_at;
    db.write(move |conn| {
        let (key, json) = (key.clone(), json.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT INTO replay_timeout_blockers
                   (project_id, store_path, branch, head, base, mode, resolution, engine_version,
                    base_branch, recorded_at, evidence_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(project_id, store_path, branch, head, base, mode, resolution,
                             engine_version)
                 DO UPDATE SET base_branch = excluded.base_branch,
                   recorded_at = excluded.recorded_at, evidence_json = excluded.evidence_json",
                params![
                    key.project_id.as_str(),
                    key.store_path.as_str(),
                    key.branch.as_str(),
                    key.head.as_str(),
                    key.base.as_str(),
                    key.mode.as_str(),
                    key.resolution.as_str(),
                    i64::from(key.engine_version),
                    key.base_branch.as_str(),
                    recorded_at,
                    json.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("record replay timeout blocker: {error}"))
}

/// The blocker an explicit attempt at `key`, whose computation would have
/// `allowance`, would repeat, if any, decided by
/// [`TimeoutBlocker::eligibility`]. Rows of the branch at any other head or
/// base, or of another engine, can never refuse again and are deleted here,
/// so no blocker outlives its coordinates.
pub(crate) async fn matching_blocker(
    db: &LocalDb,
    key: &TimeoutKey,
    allowance: Duration,
) -> Result<Option<TimeoutBlocker>, String> {
    let query = key.clone();
    let rows = db
        .write(move |conn| {
            let key = query.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM replay_timeout_blockers
                      WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3
                        AND (head <> ?4 OR base <> ?5 OR engine_version <> ?6)",
                    params![
                        key.project_id.as_str(),
                        key.store_path.as_str(),
                        key.branch.as_str(),
                        key.head.as_str(),
                        key.base.as_str(),
                        i64::from(key.engine_version)
                    ],
                )
                .await?;
                let mut rows = conn
                    .query(
                        "SELECT evidence_json FROM replay_timeout_blockers
                          WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3",
                        params![
                            key.project_id.as_str(),
                            key.store_path.as_str(),
                            key.branch.as_str()
                        ],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row.text(0)?);
                }
                Ok(out)
            })
        })
        .await
        .map_err(|error| format!("read replay timeout blocker: {error}"))?;
    let attempt = Attempt {
        head: Some(&key.head),
        base: Some(&key.base),
        mode: Some(key.mode),
        resolution: Some(&key.resolution),
        allowance: Some(allowance),
    };
    Ok(rows
        .iter()
        .filter_map(|json| serde_json::from_str::<TimeoutBlocker>(json).ok())
        .filter(|blocker| blocker.key.engine_version == key.engine_version)
        .find(|blocker| blocker.eligibility(&attempt) == Eligibility::Refused))
}

/// Every blocker recorded for the branch, newest first, for display.
pub(crate) async fn branch_blockers(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<Vec<TimeoutBlocker>, String> {
    let store = store_key(store);
    db.read(move |conn| {
        let (project_id, store, branch) =
            (project_id.to_string(), store.clone(), branch.to_string());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT evidence_json FROM replay_timeout_blockers
                      WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3
                      ORDER BY recorded_at DESC",
                    params![project_id, store, branch],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                if let Ok(blocker) = serde_json::from_str(&row.text(0)?) {
                    out.push(blocker);
                }
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("read replay timeout blockers: {error}"))
}

/// The branch moved onto a new head: none of its blockers can match again.
pub(crate) async fn clear_blockers(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<(), String> {
    db.execute(
        "DELETE FROM replay_timeout_blockers
          WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3",
        params![project_id, store_key(store).as_str(), branch],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("clear replay timeout blockers: {error}"))
}

fn seconds(ms: u64) -> String {
    format!("{:.1}s", ms as f64 / 1000.0)
}

fn opt_seconds(ms: Option<u64>) -> String {
    ms.map_or_else(|| "not reached".to_string(), seconds)
}

fn opt_count(count: Option<u64>) -> String {
    count.map_or_else(|| "unknown".to_string(), |count| count.to_string())
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// The two fixed budgets, stated wherever a cost is.
pub(crate) fn budgets_line() -> String {
    format!(
        "An explicit replay request has {} in total, shared by the proof, the carry plan, the \
         rebase, the ancestry heal, the install and the export; automatic preparation (a commit, \
         a check, a landing) has {} for the same work. Neither is widened for a large branch.",
        seconds(millis(super::replay_requests::EXPLICIT_REPLAY_BUDGET)),
        seconds(millis(crate::jj::BACKGROUND_RECONCILE_BUDGET)),
    )
}

/// The operational exit for a branch whose replay cannot fit its budget or
/// whose ancestry heal cannot be made exact, as the replay answer, the owner's
/// wake and the rebase page all name it.
pub(crate) fn merge_catch_up_exit() -> &'static str {
    "The operational exit for a branch whose replay cannot fit, or whose ancestry heal cannot \
     be made exact, is merge catch-up mode \
     (cairn/6080): the base is brought in by one merge commit and no branch commit is replayed \
     or healed. It is the operator's decision, never automatic: request it with \
     `write({changes:[{target:\"cairn:~/rebase\",mode:\"patch\",payload:{action:\"catch-up-mode\",\
     mode:\"merge\"}}]})`, which asks the operator to approve. The mode keeps its own safeguards \
     (review-required paths, incoming-change checks at landing), can still conflict, and does \
     not bypass an incomplete rollback."
}

/// Why an identical explicit replay is refused, what would make a different
/// attempt, and the exit; the same words on the page and in the refusal.
pub(crate) fn blocked_guidance(blocker: &TimeoutBlocker) -> String {
    let key = &blocker.key;
    // Only when the budget leaves room for a materially longer computation
    // can a request with more time exist at all.
    let longer = blocker.allowance_ms + millis(MATERIALLY_LONGER);
    let waited = if longer < blocker.budget_ms {
        format!(
            " That attempt's computation had {} of the {} budget, the rest lost to earlier \
             steps or waits; a request whose computation can run longer than {} is a different \
             attempt and is admitted.",
            seconds(blocker.allowance_ms),
            seconds(blocker.budget_ms),
            seconds(longer)
        )
    } else {
        String::new()
    };
    let phase = blocker
        .phase
        .map_or("an unrecorded phase", ComputePhase::label);
    let flagged = blocker
        .observed
        .flagged_after_rebase
        .map(|flagged| {
            format!(
                ", with {flagged} conflict-flagged commit(s) to heal of {} on the branch and {} \
                 restored when time ran out",
                opt_count(blocker.observed.branch_commits),
                blocker.observed.restored_commits
            )
        })
        .unwrap_or_default();
    format!(
        "An explicit replay of `{branch}` at `{head}` onto `{base_branch}` at `{base}` ({mode} \
         mode, resolution `{resolution}`, engine v{engine}) already exhausted its compute \
         budget: it ran {ran} of the {allowance} it had, in {phase}{flagged}. An identical \
         attempt would do the same work under the same budget, so it is refused before \
         computing anything. A different attempt is allowed as soon as one of these changes: a \
         new commit on `{branch}`, `{base_branch}` advancing, a different resolution, a new \
         replay engine version, or the operator's approval of merge catch-up mode. A new base \
         or head is always a new attempt; nobody has to clear this record.{waited}",
        branch = key.branch,
        head = short(&key.head),
        base_branch = key.base_branch,
        base = short(&key.base),
        mode = key.mode.as_str(),
        resolution = key.resolution,
        engine = key.engine_version,
        ran = seconds(blocker.compute_ms),
        allowance = seconds(blocker.allowance_ms),
    )
}

/// The answer's guidance after an explicit timeout: what now holds for a
/// repeat, depending on whether this timeout was recorded as a blocker.
pub(crate) fn timeout_guidance(blocker: Option<&TimeoutBlocker>) -> String {
    match blocker {
        Some(blocker) => format!(
            "{}\n\n{}\n\nIf this needs diagnosis rather than a different attempt, report it on \
             cairn://p/cairn/execution with the phases above.",
            blocked_guidance(blocker),
            merge_catch_up_exit()
        ),
        None => "No repeat was blocked: this timeout was not in the replay's own computation, or \
                 its record could not be written, so it says nothing about what an identical \
                 request would do. If the computation itself runs out of budget, an identical \
                 repeat is refused and merge catch-up mode is the exit."
            .to_string(),
    }
}

/// The live coordinates a page compares stored evidence against.
pub(crate) struct LiveCoordinates<'a> {
    pub(crate) head: Option<&'a str>,
    pub(crate) base_tip: &'a dyn Fn(&str) -> Option<String>,
    /// The branch's catch-up mode, `None` when it could not be read.
    pub(crate) mode: Option<ReplayMode>,
}

impl LiveCoordinates<'_> {
    /// The attempt a fresh request would make, as far as a page can know it.
    fn eligibility(&self, blocker: &TimeoutBlocker) -> Eligibility {
        let base = (self.base_tip)(&blocker.key.base_branch);
        blocker.eligibility(&Attempt {
            head: self.head,
            base: base.as_deref(),
            mode: self.mode,
            resolution: None,
            allowance: None,
        })
    }
}

/// What the page's replay next action becomes: `None` when nothing recorded
/// refuses a fresh request, else the guidance that replaces the command.
pub(crate) fn blocked_next_action(
    blockers: &Result<Vec<TimeoutBlocker>, String>,
    live: &LiveCoordinates,
) -> Option<String> {
    let blocker = blockers
        .as_ref()
        .ok()?
        .iter()
        .find(|blocker| live.eligibility(blocker) == Eligibility::Refused)?;
    Some(format!(
        "## Next action\n\nDo not request the same replay again: the replay cost section above \
         records that an identical one already exhausted its compute budget, for resolution \
         `{}`. A different resolution, a new commit, a base advance, a longer available \
         computation, or merge catch-up mode is a different attempt.\n\n{}",
        blocker.key.resolution,
        merge_catch_up_exit()
    ))
}

fn staleness(
    evidence_head: &str,
    evidence_base: &str,
    base_branch: &str,
    live: &LiveCoordinates,
) -> &'static str {
    let head_moved = live.head.is_some_and(|head| head != evidence_head);
    let base_moved = (live.base_tip)(base_branch).is_some_and(|tip| tip != evidence_base);
    match (live.head.is_none(), head_moved, base_moved) {
        (true, _, _) => "whether the branch has moved since could not be read",
        (_, true, true) => "stale: the branch and its base have both moved since",
        (_, true, false) => "stale: the branch has a new head since",
        (_, false, true) => "stale: the base has advanced since",
        _ => "current: the branch and base are where it was measured",
    }
}

fn age(now: i64, at: i64) -> String {
    let seconds = (now - at).max(0);
    match seconds {
        0..=119 => format!("{seconds}s ago"),
        120..=7199 => format!("{}m ago", seconds / 60),
        7200..=172_799 => format!("{}h ago", seconds / 3600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

fn describe_sample(sample: &ReplayCostEvidence, now: i64, live: &LiveCoordinates) -> String {
    let observed = &sample.observed;
    let mut out = format!(
        "Last measured {age} ({class} budget of {budget}, {mode} mode, engine v{engine}, build \
         `{build}`, host {host}, load {load}) at `{head}` onto `{base_branch}` at `{base}`; {stale}.\n",
        age = age(now, sample.recorded_at),
        class = sample.budget_class.as_str(),
        budget = seconds(sample.budget_ms),
        mode = sample.mode.as_str(),
        engine = sample.engine_version,
        build = sample.build.as_deref().unwrap_or("unknown"),
        host = sample.host.as_deref().unwrap_or("unknown"),
        load = sample
            .load1
            .map_or_else(|| "unknown".to_string(), |load| format!("{load:.2}")),
        head = short(&sample.head),
        base_branch = sample.base_branch,
        base = short(&sample.base),
        stale = staleness(&sample.head, &sample.base, &sample.base_branch, live),
    );
    out.push_str(&format!(
        "- Observed: {} branch commit(s); {} already conflict-flagged in its history; {} \
         conflict-flagged after the rebase onto that base (a newer base may introduce more or \
         fewer, unknown until it runs); {} restored; {} distinct affected path(s).\n",
        opt_count(observed.branch_commits),
        opt_count(observed.flagged_in_history),
        opt_count(observed.flagged_after_rebase),
        observed.restored_commits,
        opt_count(observed.affected_paths),
    ));
    let phases = &sample.phases;
    let proof = phases
        .proof_ms
        .map(|ms| format!("proof and carry plan {}, ", seconds(ms)))
        .unwrap_or_default();
    out.push_str(&format!(
        "- Phases: {proof}capture {}, compute {} (of which rebase {} and ancestry heal {}), \
         install {}, export {}; total {} of {}.\n",
        opt_seconds(phases.capture_ms),
        opt_seconds(phases.compute_ms),
        opt_seconds(observed.rebase_ms),
        opt_seconds(observed.heal_ms),
        opt_seconds(phases.install_ms),
        opt_seconds(phases.export_ms),
        seconds(phases.total_ms),
        seconds(sample.budget_ms),
    ));
    if let Some(per) = sample.heal_ms_per_flagged() {
        out.push_str(&format!(
            "- The heal alone averaged {per:.0} ms per conflict-flagged commit in this one \
             sample. That is part of the request, not its total, and a description, not a \
             prediction.\n"
        ));
    }
    out.push_str(&format!(
        "- Outcome: {}{}.\n",
        sample.outcome.describe(),
        sample
            .failure_phase
            .as_deref()
            .map(|phase| format!(" in {phase}"))
            .unwrap_or_default()
    ));
    out
}

/// A one-line summary of the project's other samples on this engine.
fn comparable_line(engine: &[StoredSample]) -> String {
    let per: Vec<f64> = engine
        .iter()
        .filter_map(|sample| sample.as_ref().ok())
        .filter_map(ReplayCostEvidence::heal_ms_per_flagged)
        .collect();
    if per.is_empty() {
        return format!(
            "No sample of engine v{REPLAY_ENGINE_VERSION} in this project has healed a \
             conflict-flagged commit yet, so the heal's cost here is unknown."
        );
    }
    let low = per.iter().copied().fold(f64::INFINITY, f64::min);
    let high = per.iter().copied().fold(0.0, f64::max);
    format!(
        "Across this project's {} newest heals by engine v{REPLAY_ENGINE_VERSION}, any branch, \
         the heal took {low:.0} to {high:.0} ms per conflict-flagged commit. History shape and \
         host load vary between them; this is a range seen, not an estimate for this branch.",
        per.len()
    )
}

/// The page's cost section, from stored evidence only: no store scan.
pub(crate) fn render_cost_section(
    samples: &Result<Vec<StoredSample>, String>,
    engine: &Result<Vec<StoredSample>, String>,
    blockers: &Result<Vec<TimeoutBlocker>, String>,
    live: &LiveCoordinates,
    now: i64,
) -> String {
    let mut out = format!("\n## Replay cost\n\n{}\n\n", budgets_line());
    match blockers {
        Ok(blockers) => {
            for blocker in blockers {
                match live.eligibility(blocker) {
                    Eligibility::Refused => out.push_str(&format!(
                        "**An identical explicit replay is refused.** {}\n\n{}\n\n",
                        blocked_guidance(blocker),
                        merge_catch_up_exit()
                    )),
                    Eligibility::Different(why) => out.push_str(&format!(
                        "An earlier explicit replay of `{}` onto `{}` timed out, and it refuses \
                         nothing now: {why}.\n\n",
                        short(&blocker.key.head),
                        short(&blocker.key.base)
                    )),
                    Eligibility::Unknown(why) => out.push_str(&format!(
                        "An earlier explicit replay of `{}` onto `{}` timed out; whether it \
                         still refuses an identical request is unknown: {why}.\n\n",
                        short(&blocker.key.head),
                        short(&blocker.key.base)
                    )),
                }
            }
        }
        Err(error) => out.push_str(&format!(
            "Whether an identical explicit replay is refused could not be read ({error}).\n\n"
        )),
    }
    match samples {
        Ok(samples) => match samples.first() {
            Some(Ok(sample)) => out.push_str(&describe_sample(sample, now, live)),
            Some(Err(reason)) => out.push_str(&format!(
                "The newest measurement of this branch cannot be read ({reason}); its cost is \
                 unknown.\n"
            )),
            None => out.push_str(
                "No replay of this branch has been measured yet, so what one costs is unknown. \
                 An unknown cost never refuses a first attempt.\n",
            ),
        },
        Err(error) => out.push_str(&format!(
            "This branch's measurements could not be read ({error}); its cost is unknown.\n"
        )),
    }
    if let Ok(Some(Ok(sample))) = samples.as_ref().map(|samples| samples.first()) {
        if sample.engine_version != REPLAY_ENGINE_VERSION {
            out.push_str(&format!(
                "- That measurement was taken by engine v{}, not the v{REPLAY_ENGINE_VERSION} \
                 this build runs, so it does not describe a replay now.\n",
                sample.engine_version
            ));
        }
    }
    if let Ok(engine) = engine {
        out.push_str(&format!("\n{}\n", comparable_line(engine)));
    }
    out.push('\n');
    out
}

#[cfg(test)]
#[path = "replay_cost_tests.rs"]
mod tests;
