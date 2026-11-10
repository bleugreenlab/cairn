//! The durable conflict resolution session.
//!
//! A conflicting rebase is rolled back the instant it is detected, which is the
//! safety property and is not negotiable. The cost is that the evidence dies
//! with it: after the rollback the branch sits on its own clean content and a
//! later probe finds nothing to enumerate. [`crate::jj::ConflictDiagnostic`]
//! captures that evidence inside the window; this module is where it becomes
//! durable, so the session outlives the reconcile pass that discovered it and
//! can be read, acted on, and closed later.
//!
//! What is stored is coordinates and classification, never patches. The three
//! commits are immutable, so either side of the merge is recomputable on demand
//! and a stored patch could only age. What is NOT recomputable — which paths jj
//! recorded as conflicting, versus which the incoming change merely carries — is
//! exactly what the child inventory table holds.

use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

use crate::jj::{
    assess_paths, ConflictCondition, ConflictDiagnostic, DroppedWork, IncomingClassification,
    IncomingCommit, JjEnv, PathAssessment, RestoreVerdict,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// How many conflicting paths one assessment will look at.
///
/// Each path costs a handful of jj subprocess calls, and this runs on a read
/// path and inside a replay request. A session with hundreds of conflicting
/// files is already past the point where a per-file answer helps, so the count
/// stays exact and the detail truncates — which the renderer says out loud.
pub(crate) const MAX_ASSESSED_PATHS: usize = 25;

/// How long past its budget a bounded assessment's worker is waited for.
///
/// The worker observes the budget itself: its jj subprocesses are killed and
/// reaped at the deadline, and its in-process merge stops at the next stage
/// boundary ([`crate::jj::MergeDeadline`]). The grace covers that last stage. A
/// worker still running after it is read-only and stops at its next check, so
/// the caller stops waiting for it rather than hold the item.
const ASSESSMENT_JOIN_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

/// An assessment slower than this is logged with its branch and path count.
const SLOW_ASSESSMENT: std::time::Duration = std::time::Duration::from_secs(1);

/// Where that line is logged.
pub(crate) const SLOW_ASSESSMENT_LOG_TARGET: &str = "cairn::slow_store_assessment";

/// What a bounded assessment is, for its timeout and slow-assessment lines.
pub(crate) struct AssessmentScope<'a> {
    pub(crate) phase: &'static str,
    pub(crate) branch: &'a str,
    pub(crate) candidate_paths: usize,
}

/// Run a read-only store assessment off the async runtime, under `budget`.
///
/// `work` runs on the blocking pool with an environment carrying the budget's
/// deadline, so a slow assessment never holds a runtime worker, and both its
/// subprocesses and its in-process merge observe that deadline. It must only
/// read immutable commits: it takes no store lock, and a caller that mutates on
/// its answer revalidates, under the lock, the coordinates the answer was read
/// from.
///
/// Expiry is an error carrying the canonical timeout phrase, which
/// `reconcile_failure_kind` classifies as `reconcile_timeout`. It is never a
/// fallback answer.
pub(crate) async fn bounded_store_assessment<T, F>(
    jj: &JjEnv,
    budget: std::time::Duration,
    scope: AssessmentScope<'_>,
    work: F,
) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&JjEnv) -> Result<T, String> + Send + 'static,
{
    let started = std::time::Instant::now();
    let budgeted = jj.with_operation_budget(budget);
    let worker = tokio::task::spawn_blocking(move || work(&budgeted));
    let outcome = match tokio::time::timeout(budget + ASSESSMENT_JOIN_GRACE, worker).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(format!(
            "{} worker for `{}` failed: {error}",
            scope.phase, scope.branch
        )),
        Err(_) => Err(format!(
            "{} for `{}` timed out: operation budget exhausted ({} candidate path(s)); its \
             read-only worker was not waited for past {:.1}s",
            scope.phase,
            scope.branch,
            scope.candidate_paths,
            (budget + ASSESSMENT_JOIN_GRACE).as_secs_f64()
        )),
    };
    let elapsed = started.elapsed();
    if elapsed > SLOW_ASSESSMENT {
        log::warn!(
            target: SLOW_ASSESSMENT_LOG_TARGET,
            "slow {}: branch={} candidate_paths={} elapsed={:.1}s budget={:.1}s outcome={}",
            scope.phase,
            scope.branch,
            scope.candidate_paths,
            elapsed.as_secs_f64(),
            budget.as_secs_f64(),
            match &outcome {
                Ok(_) => "completed",
                Err(error) if crate::jj::is_jj_timeout_error(error) => "timed out",
                Err(_) => "failed",
            }
        );
    }
    outcome
}

/// Schema version of the persisted diagnostic. Bumped when the meaning of a
/// stored column changes, so a session written by an older build is recognizable
/// rather than silently misread.
pub(crate) const CONFLICT_DIAGNOSTIC_VERSION: i64 = 1;

/// Whether conflict markers are present in the agent's checkout.
///
/// This records what the executor CONFIRMED, never what was requested. A wake or
/// a resource may only tell an agent to resolve markers when this reads
/// [`MarkerState::Materialized`] — instructing someone to act on state the
/// machinery has not made true is the defect this distinction exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum MarkerState {
    /// No materialization has been attempted.
    #[default]
    NotMaterialized,
    /// Requested, not yet confirmed. The durable reconcile worker retries it.
    Pending,
    /// The executor wrote the files and said so.
    Materialized,
    /// Materialization was attempted and refused or failed; see the diagnostic.
    Failed,
}

impl MarkerState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotMaterialized => "not_materialized",
            Self::Pending => "pending",
            Self::Materialized => "materialized",
            Self::Failed => "failed",
        }
    }

    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("pending") => Self::Pending,
            Some("materialized") => Self::Materialized,
            Some("failed") => Self::Failed,
            _ => Self::NotMaterialized,
        }
    }
}

/// Whether the session is still awaiting resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ResolutionState {
    /// The branch still needs to absorb the incoming change.
    #[default]
    Open,
    /// The branch was replayed onto the base cleanly and published.
    Resolved,
    /// A newer session at a newer destination replaced this one.
    Superseded,
}

impl ResolutionState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
            Self::Superseded => "superseded",
        }
    }
}

/// What advanced the base. Carried into the session so the resource can name the
/// incoming change rather than describing an anonymous "the base moved".
#[derive(Debug, Clone, Default)]
pub(crate) struct IncomingIdentity {
    pub(crate) base_branch: String,
    pub(crate) pr_number: Option<i64>,
    /// Rendered issue reference, e.g. `CAIRN-3352`.
    pub(crate) issue: Option<String>,
}

/// One file the incoming change touches, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionFile {
    pub(crate) path: String,
    /// jj's own name-status word (`M`, `A`, `D`, `R`, …), or `C` for a path jj
    /// named as conflicting but the inventory did not.
    pub(crate) status: String,
    pub(crate) classification: String,
    /// How materialization treated this path, once confirmed.
    pub(crate) marker_disposition: Option<String>,
}

impl SessionFile {
    pub(crate) fn is_conflicting(&self) -> bool {
        self.classification == IncomingClassification::Conflicting.as_str()
    }
}

/// A loaded resolution session: the durable form of one rolled-back conflict.
#[derive(Debug, Clone)]
pub(crate) struct ConflictSession {
    pub(crate) intent_id: String,
    pub(crate) bookmark: String,
    pub(crate) project_id: String,
    pub(crate) store_path: String,
    pub(crate) target_branch: String,
    pub(crate) destination_commit: String,
    pub(crate) diagnostic_version: i64,
    pub(crate) condition: String,
    pub(crate) base: Option<String>,
    pub(crate) ours: Option<String>,
    pub(crate) theirs: Option<String>,
    pub(crate) conflicted_tip: Option<String>,
    pub(crate) incoming: IncomingIdentity,
    pub(crate) marker_state: MarkerState,
    pub(crate) marker_diagnostic: Option<String>,
    /// `open` | `resolved` | `superseded`.
    pub(crate) resolution_state: Option<String>,
    pub(crate) updated_at: i64,
    /// When an agent last asked for a replay, if one is outstanding. Cleared
    /// whenever a fresh conflict is recorded for the branch.
    pub(crate) replay_requested_at: Option<i64>,
    pub(crate) files: Vec<SessionFile>,
    /// What each path of the branch's `ours` was resolved against, carried over
    /// from the session open before this one. See
    /// [`ConflictSession::resolution_basis_at`].
    pub(crate) resolution_basis: ResolutionBasis,
    /// The tip a take-committed-tip request was assessed at.
    pub(crate) replay_requested_tip: Option<String>,
    /// The destination that request was actually queued onto, which can be
    /// newer than `destination_commit`.
    pub(crate) replay_requested_destination: Option<String>,
}

/// What the reconcile row that processes a replay request last recorded.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReplayPass {
    pub(crate) item_status: Option<String>,
    pub(crate) intent_status: Option<String>,
    pub(crate) outcome_kind: Option<String>,
    pub(crate) failure_kind: Option<String>,
    pub(crate) diagnostic: Option<String>,
    pub(crate) updated_at: i64,
}

/// Which paths a take-committed-tip replay places from the committed tip, and
/// so which paths its guard has to judge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayScope {
    /// A replay that rebases the branch: the carry places the session's
    /// conflicting paths and every path an earlier resolution covered. A path
    /// carried by an earlier replay and then rolled back with it is no longer
    /// conflicting here, and leaving it out would let the next replay conflict
    /// on it again.
    Carry,
    /// A merge catch-up: the replay is a merge commit of the base, the tip and
    /// the incoming side, and only the session's conflicting paths are taken
    /// from the tip. Every other path is the merge's own result, with both
    /// sides in its ancestry, so no restore of it can drop incoming work.
    CatchUpMerge,
}

/// The merge base and incoming side one path was resolved against.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ResolvedSide {
    pub(crate) base: String,
    pub(crate) theirs: String,
    /// What earned this side. `None` is content evidence: the path changed
    /// while a session was open, or its content holds both sides. `Some` is the
    /// requester's `drop_incoming_reason` for a path the tip DELETES, which no
    /// content can ever show holding the incoming side, so the statement is the
    /// only evidence there is. A later content change to the path replaces it.
    pub(crate) attested: Option<String>,
}

impl ResolvedSide {
    fn is_against(&self, base: &str, theirs: &str) -> bool {
        self.base == base && self.theirs == theirs
    }
}

/// What each path of a branch's committed resolution was made against.
///
/// A resolution is a linear commit. It holds the incoming side's content
/// without that side in its ancestry, so every merge-base computed afterwards
/// still names the stale fork. Without this, a later replay reads the absorbed
/// content as the branch's own work, or restores it whole over a newer base.
///
/// Per path, because evidence of a resolution is per path: a commit that
/// touches one file says nothing about the others, and a path resolved against
/// an earlier base keeps that basis until its own content changes again.
pub(crate) type ResolutionBasis = BTreeMap<String, ResolvedSide>;

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredPathBasis {
    path: String,
    base: String,
    theirs: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attested: Option<String>,
}

fn encode_basis(basis: &ResolutionBasis) -> Result<Option<String>, String> {
    if basis.is_empty() {
        return Ok(None);
    }
    let rows: Vec<StoredPathBasis> = basis
        .iter()
        .map(|(path, side)| StoredPathBasis {
            path: path.clone(),
            base: side.base.clone(),
            theirs: side.theirs.clone(),
            attested: side.attested.clone(),
        })
        .collect();
    serde_json::to_string(&rows)
        .map(Some)
        .map_err(|error| format!("encode resolution basis: {error}"))
}

/// `basis` without every entry whose incoming side is in `contained`: the
/// sides the session's merge base already holds. Pure, so the rule is provable
/// without a store; see [`ConflictSession::without_superseded`].
fn drop_superseded(basis: ResolutionBasis, contained: &BTreeSet<String>) -> ResolutionBasis {
    basis
        .into_iter()
        .filter(|(_, side)| !contained.contains(&side.theirs))
        .collect()
}

/// An undecodable basis reads as empty: every path is then judged against the
/// session's own sides and the carry refuses anything it cannot place, which is
/// the safe direction.
fn decode_basis(json: Option<&str>) -> ResolutionBasis {
    json.and_then(|json| serde_json::from_str::<Vec<StoredPathBasis>>(json).ok())
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            (
                row.path,
                ResolvedSide {
                    base: row.base,
                    theirs: row.theirs,
                    attested: row.attested,
                },
            )
        })
        .collect()
}

impl ConflictSession {
    /// The same three-way fingerprint the wake deduplicates on. A request naming
    /// a fingerprint that no longer matches is acting on a stale view.
    pub(crate) fn fingerprint(&self) -> String {
        let field = |value: &Option<String>| value.clone().unwrap_or_else(|| "?".to_string());
        format!(
            "{}:{}:{}",
            field(&self.base),
            field(&self.ours),
            field(&self.theirs)
        )
    }

    pub(crate) fn conflicting(&self) -> impl Iterator<Item = &SessionFile> {
        self.files.iter().filter(|file| file.is_conflicting())
    }

    pub(crate) fn clean_on_retry(&self) -> impl Iterator<Item = &SessionFile> {
        self.files.iter().filter(|file| !file.is_conflicting())
    }

    pub(crate) fn is_base_drift(&self) -> bool {
        self.condition == ConflictCondition::BaseDrift.as_str()
    }

    pub(crate) fn is_unparseable(&self) -> bool {
        self.condition == ConflictCondition::Unparseable.as_str()
    }

    pub(crate) fn is_open(&self) -> bool {
        self.resolution_state.as_deref() == Some(ResolutionState::Open.as_str())
    }

    /// The basis once the branch's tip has resolved `resolved` against this
    /// session's sides: each of this session's conflicting paths among them
    /// takes those sides, and every other path keeps what it had. Pure, so the
    /// per-path rule is provable without a store.
    ///
    /// A content change replaces a statement, with one exception: a path whose
    /// entry is already attested against these very sides and which is in
    /// `still_deleted` (absent at the tip). That "change" is the attested
    /// deletion itself, made while this session was open, so it differs from
    /// `ours` on every read; replacing it would erase the statement the
    /// moment it was recorded. A file that comes back is not in
    /// `still_deleted`, and its content takes over.
    pub(crate) fn basis_after(
        &self,
        resolved: &BTreeSet<String>,
        still_deleted: &BTreeSet<String>,
    ) -> ResolutionBasis {
        let mut basis = self.resolution_basis.clone();
        if let (Some(base), Some(theirs)) = (self.base.as_ref(), self.theirs.as_ref()) {
            for file in self
                .conflicting()
                .filter(|file| resolved.contains(&file.path))
            {
                let attested_here = basis
                    .get(&file.path)
                    .is_some_and(|side| side.attested.is_some() && side.is_against(base, theirs));
                if attested_here && still_deleted.contains(&file.path) {
                    continue;
                }
                basis.insert(
                    file.path.clone(),
                    ResolvedSide {
                        base: base.clone(),
                        theirs: theirs.clone(),
                        attested: None,
                    },
                );
            }
        }
        basis
    }

    /// `basis` with the requester's statement entered as the resolution of
    /// every replay path in `deleted` not already attested against this
    /// session's own sides, and which paths that newly covered.
    ///
    /// `deleted` is the paths present at this session's `theirs` and absent at
    /// the assessed tip — see [`Self::deleted_at`]. A path both sides deleted
    /// is not among them, so it is never attested. A path attested against an
    /// older side is re-attested; one already attested against these sides
    /// keeps the statement it was attested with. Pure, so the per-path rule is
    /// provable without a store.
    pub(crate) fn attest_deletions(
        &self,
        mut basis: ResolutionBasis,
        deleted: &BTreeSet<String>,
        reason: &str,
        scope: ReplayScope,
    ) -> (ResolutionBasis, Vec<String>) {
        let (false, Some(base), Some(theirs)) = (
            self.is_base_drift(),
            self.base.as_deref(),
            self.theirs.as_deref(),
        ) else {
            return (basis, Vec::new());
        };
        let mut attested = Vec::new();
        for path in self.replay_paths(&basis, scope) {
            let covered = basis
                .get(&path)
                .is_some_and(|side| side.attested.is_some() && side.is_against(base, theirs));
            if !deleted.contains(&path) || covered {
                continue;
            }
            basis.insert(
                path.clone(),
                ResolvedSide {
                    base: base.to_string(),
                    theirs: theirs.to_string(),
                    attested: Some(reason.to_string()),
                },
            );
            attested.push(path);
        }
        (basis, attested)
    }

    /// Which of `paths` the tip deletes while this session's `theirs` still
    /// has them. A failed probe is an error, never "absent everywhere": an
    /// empty answer would attest or excuse paths nobody looked at.
    pub(crate) fn deleted_at(
        &self,
        jj: &JjEnv,
        store: &Path,
        tip: &str,
        paths: &[String],
    ) -> Result<BTreeSet<String>, String> {
        let Some(theirs) = self.theirs.as_deref() else {
            return Ok(BTreeSet::new());
        };
        let at_theirs = crate::jj::paths_present_at(jj, store, theirs, paths)?;
        let at_tip = crate::jj::paths_present_at(jj, store, tip, paths)?;
        Ok(at_theirs.difference(&at_tip).cloned().collect())
    }

    /// What each path of `tip` was resolved against.
    ///
    /// Two kinds of evidence resolve a conflicting path against this session's
    /// sides. Its content changed between this session's `ours` and `tip`: it
    /// was re-resolved while this session was open. Or its content at `tip`
    /// already holds both sides of this session's merge: it was resolved
    /// before the session was recorded, in a commit jj's line merge still
    /// reports as conflicting, and requiring a further edit to it would demand
    /// a cosmetic change to prove what the content already shows. Any other
    /// path — including every path of a commit that only touched unrelated
    /// files — keeps the basis this session inherited.
    ///
    /// This is the one answer the replay guard, the replay's carry, and the
    /// rebase page all read, so none of them can count a resolution another
    /// refuses.
    pub(crate) fn resolution_basis_at(
        &self,
        jj: &JjEnv,
        store: &Path,
        tip: &str,
    ) -> Result<ResolutionBasis, String> {
        let Some(ours) = self.ours.as_deref() else {
            return self.without_superseded(jj, store, self.resolution_basis.clone());
        };
        let paths: Vec<String> = self.conflicting().map(|file| file.path.clone()).collect();
        let mut resolved = crate::jj::paths_changed_between(jj, store, ours, tip, &paths)?;
        // Base drift has nothing to resolve: its sides already agree.
        if let (false, Some(base), Some(theirs)) = (
            self.is_base_drift(),
            self.base.as_deref(),
            self.theirs.as_deref(),
        ) {
            let unchanged: Vec<String> = paths
                .iter()
                .filter(|path| !resolved.contains(*path))
                .cloned()
                .collect();
            resolved.extend(crate::jj::paths_holding_incoming_side(
                jj, store, base, tip, theirs, &unchanged,
            )?);
        }
        // Only a changed path already attested against these sides can keep
        // its statement, so only those are probed; usually there are none.
        let attested_here: Vec<String> = match (self.base.as_deref(), self.theirs.as_deref()) {
            (Some(base), Some(theirs)) => resolved
                .iter()
                .filter(|path| {
                    self.resolution_basis.get(*path).is_some_and(|side| {
                        side.attested.is_some() && side.is_against(base, theirs)
                    })
                })
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        let still_deleted = if attested_here.is_empty() {
            BTreeSet::new()
        } else {
            let present = crate::jj::paths_present_at(jj, store, tip, &attested_here)?;
            attested_here
                .into_iter()
                .filter(|path| !present.contains(path))
                .collect()
        };
        self.without_superseded(jj, store, self.basis_after(&resolved, &still_deleted))
    }

    /// `basis` without the entries this session's merge base has superseded.
    ///
    /// An entry says the tip holds its `theirs` linearly, without that side in
    /// its ancestry, so the branch's work on the path is measured from that
    /// side rather than from the fork. Once this session's base contains the
    /// side — a later replay or catch-up merge put it into the branch's
    /// history — the fork is the newer measurement, and judging the path
    /// against the entry's older sides merges content both sides already share
    /// a second time: a whole-file restore that drops nothing reads as dropping
    /// incoming work. Such an entry is no evidence about this session's merge
    /// either way, so a conflicting path it alone covered is unresolved again.
    ///
    /// An entry whose side the base does not contain is kept. That is what a
    /// replay that carried a path and then rolled back leaves behind: the
    /// branch did not move, so neither did its base, and the next replay still
    /// has to place the path.
    ///
    /// A store that cannot answer keeps every entry, which can only make a
    /// replay refuse; a timeout is an error, never an answer.
    pub(crate) fn without_superseded(
        &self,
        jj: &JjEnv,
        store: &Path,
        basis: ResolutionBasis,
    ) -> Result<ResolutionBasis, String> {
        let Some(base) = self.base.as_deref() else {
            return Ok(basis);
        };
        let sides: Vec<String> = basis
            .values()
            .map(|side| side.theirs.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        match crate::jj::commits_contained_in(jj, store, &sides, base) {
            Ok(contained) => Ok(drop_superseded(basis, &contained)),
            Err(error) if crate::jj::is_jj_timeout_error(&error) => Err(error),
            Err(error) => {
                log::warn!(
                    "conflict session for `{}`: which resolved sides `{base}` contains could not \
                     be read ({error}); keeping every resolution basis entry",
                    self.bookmark
                );
                Ok(basis)
            }
        }
    }

    /// Every path a take-committed-tip replay places from the tip, under
    /// `scope`; see [`ReplayScope`].
    pub(crate) fn replay_paths(&self, basis: &ResolutionBasis, scope: ReplayScope) -> Vec<String> {
        let conflicting = self.conflicting().map(|file| file.path.clone());
        let paths: BTreeSet<String> = match scope {
            ReplayScope::Carry => conflicting.chain(basis.keys().cloned()).collect(),
            ReplayScope::CatchUpMerge => conflicting.collect(),
        };
        paths.into_iter().collect()
    }

    /// Whether this session was written by a build that agrees with this one
    /// about what the stored columns mean. A session from another version is
    /// rendered with its identity and inventory but without interpretation —
    /// silently reading it as current is how a stale schema tells a confident
    /// lie.
    pub(crate) fn version_is_current(&self) -> bool {
        self.diagnostic_version == CONFLICT_DIAGNOSTIC_VERSION
    }
}

/// What the branch's LIVE tip means for this session.
///
/// The session's `ours` is frozen at the pre-rebase tip by design — it is one of
/// three immutable coordinates. That is exactly why it cannot answer "has the
/// agent resolved this yet": a resolution is a new commit the frozen coordinate
/// has never heard of. This probes the bookmark and re-runs the merge against
/// what is actually there now.
#[derive(Debug, Clone)]
pub(crate) struct TipAssessment {
    /// The branch's current tip.
    pub(crate) tip: String,
    pub(crate) paths: Vec<PathAssessment>,
    /// Conflicting paths beyond the cap, not looked at. Always zero for an
    /// exhaustive assessment, which is the only kind the mutation boundary may
    /// decide on.
    pub(crate) truncated: usize,
    /// This session's conflicting paths the tip has not resolved against any
    /// side: unchanged since the session was recorded, holding only the
    /// branch's side, and covered by no earlier resolution. Never capped — it
    /// comes from the resolution basis, which covers every conflicting path.
    pub(crate) unresolved: Vec<String>,
    /// How many conflicting paths this session has.
    pub(crate) conflicting: usize,
    /// What a resolution is measured from, so both the page and the refusal
    /// can say what would count as one.
    pub(crate) recorded: RecordedConflict,
    /// Every ASSESSED replay path the tip deletes while this session's `theirs`
    /// still has it, and whether a statement covers that deletion: like
    /// [`Self::paths`], capped on a read and exhaustive for the guard. A deletion is
    /// never in [`Self::unresolved`]: no commit to an absent file can resolve
    /// it, so what it needs is named here instead.
    pub(crate) deletions: BTreeMap<String, DeletionStanding>,
    /// How many of this session's conflicting paths are deletions no basis
    /// entry covers at all — neither resolved nor unresolved.
    pub(crate) uncovered_deletions: usize,
    /// Every attested entry of the basis this assessment used, so a request
    /// can persist exactly those and the page can name them.
    pub(crate) attested: BTreeMap<String, ResolvedSide>,
    /// The paths this assessment's own request attested.
    pub(crate) newly_attested: Vec<String>,
}

/// Whether a deletion of a file the base still has is covered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeletionStanding {
    /// A statement covers the deletion against this session's own sides.
    Attested,
    /// Nothing covers what the base did to the file since the deletion was
    /// last measured.
    Outpaced(OutpacedDeletion),
}

/// A branch's deletion of a file the base has edited since `from`.
///
/// The one shape the guard, the carry, and the rebase page all describe, so
/// [`describe_outpaced_deletion`] can word it once for all three.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutpacedDeletion {
    /// What the deletion is measured from: the side a statement covered, or
    /// the fork when nothing did.
    pub(crate) from: String,
    /// The statement, when `from` is the side it covered.
    pub(crate) attested: Option<String>,
    /// The version of the file the deletion would land over.
    pub(crate) to: String,
    /// The commits in `from..to` that changed the file, newest first.
    pub(crate) commits: Result<Vec<IncomingCommit>, String>,
}

/// How many incoming commits one outpaced deletion lists before summarizing.
const DELETION_COMMITS_LISTED: usize = 10;

/// What the base did to a file the branch deletes, written for the agent.
/// Never implies a stale resolution: a deletion with no statement has none.
pub(crate) fn describe_outpaced_deletion(deletion: &OutpacedDeletion) -> String {
    let OutpacedDeletion {
        from,
        attested,
        to,
        commits,
    } = deletion;
    let mut out = match attested {
        Some(reason) => format!(
            "your branch deletes this file. Your statement (\"{reason}\") covered the base's \
             version of it at `{from}`, and the base has changed it again since, up to `{to}`"
        ),
        None => format!(
            "your branch deletes this file, and the base changed it between `{from}` and `{to}`; \
             nothing records those changes as carried into whatever replaced it"
        ),
    };
    match commits {
        Ok(commits) if commits.is_empty() => out.push_str(
            ". No commit in that range changes it under this name, so the change arrived under \
             another one",
        ),
        Ok(commits) => {
            out.push_str(", in:\n");
            for commit in commits.iter().take(DELETION_COMMITS_LISTED) {
                out.push_str(&format!("  - `{}` {}\n", commit.id, commit.subject));
            }
            if commits.len() > DELETION_COMMITS_LISTED {
                out.push_str(&format!(
                    "  - and {} more\n",
                    commits.len() - DELETION_COMMITS_LISTED
                ));
            }
            return out;
        }
        Err(error) => out.push_str(&format!(". The commits could not be listed ({error})")),
    }
    out.push('\n');
    out
}

/// What clears an outpaced deletion, given the version it would land over.
pub(crate) fn deletion_remedy(to: &str) -> String {
    format!(
        "Carry those changes into whatever replaced each file, or confirm they are moot, then \
         request again with `drop_incoming_reason:\"…\"` saying where they went. The reason is \
         recorded as that deletion's resolution against `{to}` and shown on `cairn:~/rebase`."
    )
}

/// The session coordinates that define what counts as a resolution.
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordedConflict {
    /// The branch tip when the conflict was recorded. A commit on top of it
    /// that changes a conflicting file resolves that file.
    pub(crate) ours: Option<String>,
    /// The incoming side a file's content must already hold to count as
    /// resolved without such a commit.
    pub(crate) theirs: String,
    /// When the session was recorded.
    pub(crate) at: i64,
}

/// Why a path's whole-file restore could not be PROVEN lossless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unproven {
    /// Positive evidence that incoming work would be discarded.
    Drops(DroppedWork),
    /// The merge could not judge this path at all.
    Unjudged(String),
    /// The tip deletes a file the base edited, and no statement covers that.
    DeletionOutpaced(OutpacedDeletion),
}

impl TipAssessment {
    /// Every path whose whole-file restore this assessment cannot PROVE
    /// lossless.
    ///
    /// This is the mutation boundary's question, and it is deliberately not
    /// [`Self::lossy`]. A read surface reports what it saw, so a path it could
    /// not judge is worth naming and no more. A restore is about to happen to
    /// that path either way, and "I could not tell" is not evidence of safety —
    /// treating it as though it were is how a guard against silent loss comes to
    /// permit silent loss. The escape hatch is what keeps this from being a dead
    /// end: an agent who knows the drop is fine says so and proceeds.
    ///
    /// A deletion is judged by its [`DeletionStanding`] rather than its
    /// verdict, which can only ever say a deletion was not assessed.
    pub(crate) fn unproven(&self) -> Vec<(&str, Unproven)> {
        self.paths
            .iter()
            .filter_map(|assessment| {
                let reason = match (self.deletions.get(&assessment.path), &assessment.verdict) {
                    (Some(DeletionStanding::Attested), _) => return None,
                    (Some(DeletionStanding::Outpaced(deletion)), _) => {
                        Unproven::DeletionOutpaced(deletion.clone())
                    }
                    (None, RestoreVerdict::Lossless) => return None,
                    (None, RestoreVerdict::Lossy(dropped)) => Unproven::Drops(dropped.clone()),
                    (None, RestoreVerdict::NotAssessed(reason)) => {
                        Unproven::Unjudged(reason.clone())
                    }
                };
                Some((assessment.path.as_str(), reason))
            })
            .collect()
    }

    /// Every deletion no statement covers against this session's sides.
    pub(crate) fn outpaced_deletions(&self) -> Vec<(&str, &OutpacedDeletion)> {
        self.deletions
            .iter()
            .filter_map(|(path, standing)| match standing {
                DeletionStanding::Outpaced(deletion) => Some((path.as_str(), deletion)),
                DeletionStanding::Attested => None,
            })
            .collect()
    }

    /// Whether every path assessed came back lossless, or is a deletion a
    /// statement covers. Deliberately false when the detail was truncated: an
    /// answer about 25 of 200 paths is not an answer about the session.
    pub(crate) fn every_path_is_resolved(&self) -> bool {
        self.truncated == 0
            && self.unresolved.is_empty()
            && !self.paths.is_empty()
            && self.unproven().is_empty()
    }

    /// Whether the tip resolves none of this session's conflicting files, so a
    /// take-committed-tip replay has nothing to take. No stated reason changes
    /// this: it cannot supply a resolution that does not exist.
    ///
    /// A conflicting deletion nothing covers is not a resolution, but it is
    /// not unresolved either: a statement is what covers it. So a tip whose
    /// only uncovered paths are deletions is judged by [`Self::unproven`],
    /// which names what the base did to each file.
    pub(crate) fn resolves_nothing(&self) -> bool {
        !self.unresolved.is_empty()
            && self.unresolved.len() + self.uncovered_deletions == self.conflicting
    }

    /// What would count as resolving each unresolved file, and where to get
    /// the content. The ONE wording behind both the rebase page and the
    /// replay's refusal, so they cannot describe different requirements.
    /// `rebase` is the rebase resource's URI as the reader would address it.
    pub(crate) fn what_resolves(&self, rebase: &str) -> String {
        let recorded = crate::clock::stamp(self.recorded.at)
            .map(|at| format!(" (recorded {at})"))
            .unwrap_or_default();
        let commit = match self.recorded.ours.as_deref() {
            Some(ours) => format!(
                "a commit that changes it on top of `{ours}`, your branch's tip when this \
                 conflict was recorded{recorded}"
            ),
            None => format!("a commit that changes it after this conflict was recorded{recorded}"),
        };
        let mut out = format!(
            "A conflicting file counts as resolved once your branch has {commit}, or once its \
             committed content already holds both sides — the incoming `{theirs}` inside every \
             conflicting region as well as outside it. Not yet resolved:\n\n",
            theirs = self.recorded.theirs,
        );
        for path in &self.unresolved {
            out.push_str(&format!(
                "- `{path}` — commit `{rebase}?view=merged&file={path}`, the complete merged \
                 file, or your own resolution of it.\n"
            ));
        }
        out
    }
}

/// Run the loss invariant over a session's conflicting paths, against the
/// branch's live tip, for a READ.
///
/// Capped at [`MAX_ASSESSED_PATHS`], because a page costs subprocess calls per
/// path and a per-file answer about two hundred files does not help anyone. The
/// truncation is reported, never hidden.
///
/// This form must NOT decide a mutation. A capped assessment is silent about
/// every path past the cap, and a guard that reads silence as safety is not a
/// guard — see [`assess_session_tip_exhaustively`].
pub(crate) fn assess_session_tip(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
    scope: ReplayScope,
) -> Option<TipAssessment> {
    assess_session_tip_within(jj, store, session, scope, Some(MAX_ASSESSED_PATHS))
}

/// What an attempt to prove a whole-file restore safe actually produced.
///
/// An `Option` cannot carry this, and conflating the arms is how the guard came
/// to fail open twice: "there is nothing to prove" and "I could not build the
/// proof" both arrive as absence, and absence read as permission is exactly the
/// silent acceptance this whole mechanism exists to prevent. Only the first is
/// ever grounds to proceed.
#[derive(Debug, Clone)]
pub(crate) enum RestoreProof {
    /// Base drift: every conflicting path is already byte-identical between the
    /// branch and the destination, so the restore moves no bytes and there is
    /// genuinely nothing a merge could discard.
    NothingToProve,
    /// The assessment ran over every path it was asked about.
    Assessed(TipAssessment),
    /// The proof could not be constructed at all — a missing coordinate, an
    /// unresolvable bookmark, a store that would not answer. Carries the reason,
    /// written for the requester.
    Unavailable(String),
}

/// The same assessment over EVERY conflicting path, for the replay guard.
///
/// The mutation restores every conflicting path whole, so the decision has to
/// cover every conflicting path. Uncapped deliberately: this runs once per
/// explicit request against an operation that is already asynchronous and
/// durable, so paying a subprocess per path is the cheapest thing in the
/// transaction — far cheaper than discovering the loss from a compiler later.
///
/// `attestation` is the requester's `drop_incoming_reason`. With one, every
/// replay path the tip deletes is entered into the basis against this
/// session's sides before the assessment runs — see
/// [`ConflictSession::attest_deletions`] — and the assessment reports which,
/// for the request to persist once it is accepted.
pub(crate) fn assess_session_tip_exhaustively(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
    attestation: Option<&str>,
    scope: ReplayScope,
) -> RestoreProof {
    if session.is_base_drift() {
        return RestoreProof::NothingToProve;
    }
    let (Some(base), Some(_)) = (session.base.as_deref(), session.theirs.as_deref()) else {
        return RestoreProof::Unavailable(
            "this session did not record both outer coordinates of the merge, so the restore \
             cannot be checked against what arrived"
                .to_string(),
        );
    };
    let Some(tip) = crate::jj::bookmark_commit(jj, store, &session.bookmark) else {
        return RestoreProof::Unavailable(format!(
            "the branch bookmark `{}` did not resolve to a commit, so there is no committed tip to \
             check",
            session.bookmark
        ));
    };
    let basis = match session.resolution_basis_at(jj, store, &tip) {
        Ok(basis) => basis,
        Err(error) => {
            return RestoreProof::Unavailable(format!(
                "which conflicting files changed since the conflict was recorded could not be \
                 read ({error})"
            ))
        }
    };
    let deleted = match session.deleted_at(jj, store, &tip, &session.replay_paths(&basis, scope)) {
        Ok(deleted) => deleted,
        Err(error) => {
            return RestoreProof::Unavailable(format!(
                "which files your branch deletes could not be read ({error})"
            ))
        }
    };
    let (basis, newly_attested) = match attestation {
        Some(reason) => session.attest_deletions(basis, &deleted, reason, scope),
        None => (basis, Vec::new()),
    };
    let mut assessment =
        assess_against_basis(jj, store, session, base, &basis, &deleted, tip, scope, None);
    assessment.newly_attested = newly_attested;
    RestoreProof::Assessed(assessment)
}

fn assess_session_tip_within(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
    scope: ReplayScope,
    limit: Option<usize>,
) -> Option<TipAssessment> {
    if session.is_base_drift() {
        return None;
    }
    session.theirs.as_deref()?;
    let base = session.base.as_deref()?;
    let tip = crate::jj::bookmark_commit(jj, store, &session.bookmark)?;
    // A read reports what it sees. A store that cannot say which files changed
    // still leaves what the session inherited.
    let basis = session
        .resolution_basis_at(jj, store, &tip)
        .unwrap_or_else(|_| session.resolution_basis.clone());
    // A store that cannot say which files are deleted leaves each such path
    // with its own verdict, which reports it as not assessed.
    let deleted = session
        .deleted_at(jj, store, &tip, &session.replay_paths(&basis, scope))
        .unwrap_or_default();
    Some(assess_against_basis(
        jj, store, session, base, &basis, &deleted, tip, scope, limit,
    ))
}

/// Assess every path the replay has to place against what it was resolved
/// against: a path the basis covers against its own recorded sides, any other
/// against the session's. Order is path order.
///
/// `deleted` is [`ConflictSession::deleted_at`] for the replay paths. Each
/// such path is resolved only when a statement covers it against this
/// session's sides; otherwise the commits that edited it since whatever it was
/// last measured from are named.
#[allow(clippy::too_many_arguments)]
fn assess_against_basis(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
    session_base: &str,
    basis: &ResolutionBasis,
    deleted: &BTreeSet<String>,
    tip: String,
    scope: ReplayScope,
    limit: Option<usize>,
) -> TipAssessment {
    let all = session.replay_paths(basis, scope);
    let limit = limit.unwrap_or(all.len());
    let truncated = all.len().saturating_sub(limit);
    let session_theirs = session.theirs.clone().unwrap_or_default();
    // Grouped by the sides alone: an attestation must not split a jj batch.
    let mut groups: BTreeMap<(&str, &str), Vec<String>> = BTreeMap::new();
    for path in all.iter().take(limit) {
        let sides = basis
            .get(path)
            .map_or((session_base, session_theirs.as_str()), |side| {
                (side.base.as_str(), side.theirs.as_str())
            });
        groups.entry(sides).or_default().push(path.clone());
    }
    let mut verdicts: std::collections::HashMap<String, PathAssessment> = groups
        .into_iter()
        .flat_map(|((base, theirs), paths)| assess_paths(jj, store, base, &tip, theirs, &paths))
        .map(|assessment| (assessment.path.clone(), assessment))
        .collect();

    // Standing, and its history lookup, only for the assessed prefix: the
    // capped read renders nothing past it, and each lookup is a subprocess.
    let assessed: Vec<String> = all.iter().take(limit).cloned().collect();
    let deletions = deletion_standings(
        &assessed,
        basis,
        deleted,
        session_base,
        &session_theirs,
        |from, path| crate::jj::commits_changing_path(jj, store, from, &session_theirs, path),
    );

    let unresolved: Vec<String> = session
        .conflicting()
        .filter(|file| !basis.contains_key(&file.path) && !deleted.contains(&file.path))
        .map(|file| file.path.clone())
        .collect();
    let uncovered_deletions = session
        .conflicting()
        .filter(|file| !basis.contains_key(&file.path) && deleted.contains(&file.path))
        .count();
    TipAssessment {
        paths: all
            .iter()
            .take(limit)
            .filter_map(|path| verdicts.remove(path))
            .collect(),
        truncated,
        tip,
        unresolved,
        conflicting: session.conflicting().count(),
        recorded: RecordedConflict {
            ours: session.ours.clone(),
            theirs: session_theirs,
            at: session.updated_at,
        },
        deletions,
        uncovered_deletions,
        attested: basis
            .iter()
            .filter(|(_, side)| side.attested.is_some())
            .map(|(path, side)| (path.clone(), side.clone()))
            .collect(),
        newly_attested: Vec::new(),
    }
}

/// The [`DeletionStanding`] of each of `paths` the tip deletes. `commits_since`
/// lists the base's commits changing a path since a commit, and is asked only
/// about these paths and only when no statement covers one against this
/// session's sides.
fn deletion_standings(
    paths: &[String],
    basis: &ResolutionBasis,
    deleted: &BTreeSet<String>,
    session_base: &str,
    session_theirs: &str,
    mut commits_since: impl FnMut(&str, &str) -> Result<Vec<IncomingCommit>, String>,
) -> BTreeMap<String, DeletionStanding> {
    paths
        .iter()
        .filter(|path| deleted.contains(*path))
        .map(|path| {
            let attested = basis
                .get(path)
                .and_then(|side| side.attested.as_ref().map(|reason| (side, reason)));
            let standing = match attested {
                Some((side, _)) if side.is_against(session_base, session_theirs) => {
                    DeletionStanding::Attested
                }
                _ => {
                    let from = attested
                        .map_or(session_base, |(side, _)| side.theirs.as_str())
                        .to_string();
                    let commits = commits_since(&from, path);
                    DeletionStanding::Outpaced(OutpacedDeletion {
                        from,
                        attested: attested.map(|(_, reason)| reason.clone()),
                        to: session_theirs.to_string(),
                        commits,
                    })
                }
            };
            (path.clone(), standing)
        })
        .collect()
}

/// What the replay guard decided about a proof attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplayDecision {
    /// Nothing blocks the replay.
    Proceed,
    /// Blocked. The string is the refusal, written for the requester.
    Refuse(String),
    /// Permitted only because the requester stated a reason. The string is the
    /// caveat to carry back and record.
    ProceedOnStatedReason(String),
}

/// Decide whether a `take-committed-tip` replay may proceed.
///
/// Pure, and separated from the request path on purpose: this is the whole
/// safety contract of the mechanism, and it should be provable without a store,
/// a database, or a fixture. Every arm that is not a completed proof of safety
/// refuses unless the requester states a reason — except a tip with nothing
/// resolved, where no reason can supply a resolution that does not exist.
///
/// That arm reads the same [`TipAssessment`] the rebase page renders, so the
/// page cannot recommend a request this refuses.
pub(crate) fn decide_replay(proof: &RestoreProof, stated_reason: Option<&str>) -> ReplayDecision {
    let blocker = match proof {
        RestoreProof::NothingToProve => return ReplayDecision::Proceed,
        RestoreProof::Assessed(assessment) if assessment.resolves_nothing() => {
            let mut refusal = format!(
                "Refusing this replay: your committed tip `{tip}` resolves none of this \
                 conflict's files against `{theirs}`, so there is no resolution to take. \
                 `resolution:\"take-committed-tip\"` would land each conflicting file as simply \
                 your side of the conflict.\n\n{what}\nCommit those and request again. To move the \
                 branch without taking content from either side, request a plain \
                 `{{action:\"replay\"}}` instead.",
                tip = assessment.tip,
                theirs = assessment.recorded.theirs,
                what = assessment.what_resolves("cairn:~/rebase"),
            );
            let deletions = assessment.outpaced_deletions();
            if !deletions.is_empty() {
                refusal.push_str("\n\n");
                refusal.push_str(&outpaced_deletions_section(
                    &deletions,
                    &assessment.recorded.theirs,
                ));
            }
            return ReplayDecision::Refuse(refusal);
        }
        RestoreProof::Unavailable(why) => format!(
            "Refusing this replay: `resolution:\"take-committed-tip\"` restores each conflicting \
             file WHOLE from your branch's committed tip, and whether that keeps everything both \
             sides have could not be established — {why}.\n\nThis is not a finding about your \
             resolution; it is the absence of one. Re-reading `cairn:~/rebase` and requesting \
             again is worth trying, since some of these are transient."
        ),
        RestoreProof::Assessed(assessment) if assessment.truncated > 0 => format!(
            "Refusing this replay: only {} of {} conflicting file(s) were checked for dropped \
             work, and a replay is not accepted on an answer about some of them. This is a defect \
             in the guard rather than anything about your branch — please report it.",
            assessment.paths.len(),
            assessment.paths.len() + assessment.truncated
        ),
        RestoreProof::Assessed(assessment) => {
            let unproven = assessment.unproven();
            if unproven.is_empty() {
                return ReplayDecision::Proceed;
            }
            unproven_refusal(&unproven)
        }
    };
    match stated_reason {
        None => ReplayDecision::Refuse(blocker),
        Some(reason) => ReplayDecision::ProceedOnStatedReason(format!(
            "You accepted a whole-file restore that was not proven to keep everything both sides \
             have, because: {reason}. That is recorded in the runner log."
        )),
    }
}

/// The refusal for paths whose whole-file restore cannot be proven lossless.
///
/// The two categories are listed apart because their remedies differ: work that
/// would demonstrably be dropped has a file to commit, while a path the merge
/// could not judge needs a decision only a person can make.
fn unproven_refusal(unproven: &[(&str, Unproven)]) -> String {
    let mut out = format!(
        "Refusing this replay: `resolution:\"take-committed-tip\"` restores each conflicting file \
         WHOLE from your branch's committed tip, and for {} of them that restore cannot be shown \
         to keep everything both sides have. Requesting it as-is could throw work away with \
         nothing saying so.\n\n",
        unproven.len()
    );

    let drops: Vec<(&str, &DroppedWork)> = unproven
        .iter()
        .filter_map(|(path, reason)| match reason {
            Unproven::Drops(dropped) => Some((*path, dropped)),
            Unproven::Unjudged(_) | Unproven::DeletionOutpaced(_) => None,
        })
        .collect();
    if !drops.is_empty() {
        out.push_str("Incoming work that WOULD be dropped:\n\n");
        for (path, dropped) in &drops {
            out.push_str(&format!(
                "- `{path}` — {} hunk(s), {} line(s). Read \
                 `cairn:~/rebase?view=merged&file={path}`.\n\n```diff\n{}\n```\n\n",
                dropped.hunks,
                dropped.added_lines,
                refusal_excerpt(&dropped.diff)
            ));
        }
        out.push_str(
            "Each of those reads gives you the COMPLETE merged file: your resolution kept where \
             both sides changed the same lines, both sides' lines kept where each only inserted \
             at the same spot, the incoming hunks carried everywhere else, and no conflict \
             markers. Commit it and request the replay again — it is accepted then, because your \
             tip genuinely contains both sides and the whole-file restore is exactly right.\n\n",
        );
    }

    let deletions: Vec<(&str, &OutpacedDeletion)> = unproven
        .iter()
        .filter_map(|(path, reason)| match reason {
            Unproven::DeletionOutpaced(deletion) => Some((*path, deletion)),
            Unproven::Drops(_) | Unproven::Unjudged(_) => None,
        })
        .collect();
    if let Some((_, first)) = deletions.first() {
        out.push_str(&outpaced_deletions_section(&deletions, &first.to));
        out.push_str("\n\n");
    }

    let unjudged: Vec<(&str, &str)> = unproven
        .iter()
        .filter_map(|(path, reason)| match reason {
            Unproven::Unjudged(reason) => Some((*path, reason.as_str())),
            Unproven::Drops(_) | Unproven::DeletionOutpaced(_) => None,
        })
        .collect();
    if !unjudged.is_empty() {
        out.push_str("Paths this merge could not judge, which the restore still covers:\n\n");
        for (path, reason) in &unjudged {
            out.push_str(&format!("- `{path}` — {reason}.\n"));
        }
        out.push_str(
            "\nThere is no merged file to hand you for these: the decision is yours. Read both \
             sides, and if keeping your committed version is right, say so below.\n\n",
        );
    }

    out.push_str(
        "If proceeding is deliberate, say why and the request goes through: add \
         `drop_incoming_reason:\"…\"` to this payload. The reason is recorded.",
    );
    out
}

/// The refusal's section for deletions no statement covers. `to` is the side
/// a statement made now would be recorded against.
fn outpaced_deletions_section(deletions: &[(&str, &OutpacedDeletion)], to: &str) -> String {
    let mut out = String::from("Files your branch deletes that the base has edited since:\n\n");
    for (path, deletion) in deletions {
        out.push_str(&format!(
            "- `{path}` — {}",
            describe_outpaced_deletion(deletion)
        ));
    }
    out.push('\n');
    out.push_str(&deletion_remedy(to));
    out
}

/// The most of one path's dropped-work diff a refusal quotes. The refusal
/// names the lines so the requester sees what is missing without a second
/// read; the merged view it points at carries every one of them.
const REFUSAL_DIFF_LINES: usize = 24;

fn refusal_excerpt(diff: &str) -> String {
    let lines: Vec<&str> = diff.lines().collect();
    if lines.len() <= REFUSAL_DIFF_LINES {
        return lines.join("\n");
    }
    format!(
        "{}\n… {} more line(s) in the merged view",
        lines[..REFUSAL_DIFF_LINES].join("\n"),
        lines.len() - REFUSAL_DIFF_LINES
    )
}

/// Persist the diagnostic captured before the rollback onto the reconcile item
/// that already exists for this branch, and replace its file inventory.
///
/// Called immediately after [`super::base_advance::persist_reconcile_item`] has
/// written the item row, so the UPDATE always has a row to land on. A conflict
/// re-detected at the same destination overwrites in place rather than
/// accumulating: the coordinates are the same merge, so there is one session.
///
/// `basis` is what this session inherits: the previous open session's
/// [`ConflictSession::resolution_basis_at`] this session's `ours`, assessed by
/// the branch preparer under a bounded budget before it takes the store lock.
/// The caller computes it from the session open BEFORE this write, which may be
/// the very row being overwritten (a conflict re-detected under the same
/// intent).
pub(crate) async fn record_conflict_session(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
    diagnostic: &ConflictDiagnostic,
    incoming: &IncomingIdentity,
    basis: &ResolutionBasis,
) -> Result<(), String> {
    let basis_json = encode_basis(basis)?;
    db.execute(
        "UPDATE jj_reconcile_items SET
           diagnostic_version = ?3,
           conflict_condition = ?4,
           base_commit = ?5,
           ours_commit = ?6,
           theirs_commit = ?7,
           conflicted_tip = ?8,
           incoming_pr_number = ?9,
           incoming_issue = ?10,
           incoming_base_branch = ?11,
           marker_state = COALESCE(marker_state, ?12),
           resolution_state = ?13,
           -- A fresh conflict means any earlier replay request has resolved into
           -- a new situation, so a surviving timestamp would describe a request
           -- nobody is waiting on. An unserved typed request is the exception:
           -- only the pass that serves it may withdraw it.
           replay_requested_at = CASE WHEN replay_request_id IS NOT NULL
             AND replay_served_at IS NULL THEN replay_requested_at END,
           replay_requested_tip = CASE WHEN replay_request_id IS NOT NULL
             AND replay_served_at IS NULL THEN replay_requested_tip END,
           replay_requested_destination = CASE WHEN replay_request_id IS NOT NULL
             AND replay_served_at IS NULL THEN replay_requested_destination END,
           resolution_basis_json = ?14
         WHERE intent_id = ?1 AND bookmark = ?2",
        params![
            intent_id,
            bookmark,
            CONFLICT_DIAGNOSTIC_VERSION,
            diagnostic.condition.as_str(),
            diagnostic.base.as_deref(),
            diagnostic.ours.as_deref(),
            diagnostic.theirs.as_deref(),
            diagnostic.conflicted_tip.as_deref(),
            incoming.pr_number,
            incoming.issue.as_deref(),
            incoming.base_branch.as_str(),
            MarkerState::NotMaterialized.as_str(),
            ResolutionState::Open.as_str(),
            basis_json.as_deref(),
        ],
    )
    .await
    .map_err(|error| format!("persist conflict session: {error}"))?;

    db.execute(
        "DELETE FROM jj_reconcile_incoming_files WHERE intent_id = ?1 AND bookmark = ?2",
        params![intent_id, bookmark],
    )
    .await
    .map_err(|error| format!("clear conflict session inventory: {error}"))?;

    for file in &diagnostic.incoming {
        db.execute(
            "INSERT INTO jj_reconcile_incoming_files
               (intent_id, bookmark, path, status, classification)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(intent_id, bookmark, path) DO UPDATE SET
               status = excluded.status,
               classification = excluded.classification",
            params![
                intent_id,
                bookmark,
                file.path.as_str(),
                file.status.as_str(),
                file.classification.as_str(),
            ],
        )
        .await
        .map_err(|error| format!("persist conflict session inventory: {error}"))?;
    }
    Ok(())
}

/// Persist a session's resolution basis after a request attested deletions.
///
/// `basis` should be what the session inherited plus the attested entries:
/// the content-evidence entries are recomputed from the store on every read,
/// so storing a live one would only freeze it. Written BEFORE the request is
/// queued, because the worker reloads the session from here.
pub(crate) async fn record_attested_basis(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
    basis: &ResolutionBasis,
) -> Result<(), String> {
    let basis_json = encode_basis(basis)?;
    db.execute(
        "UPDATE jj_reconcile_items SET resolution_basis_json = ?3
         WHERE intent_id = ?1 AND bookmark = ?2",
        params![intent_id, bookmark, basis_json.as_deref()],
    )
    .await
    .map_err(|error| format!("record attested deletions: {error}"))?;
    Ok(())
}

const REPLAY_RESOLUTION_PLAIN: &str = "plain";
const REPLAY_RESOLUTION_TAKE_COMMITTED_TIP: &str = "take-committed-tip";

/// Whether a stored resolution is take-committed-tip. An unknown value is an
/// error, never read as a plain replay: the request's authority depends on it.
fn parse_replay_resolution(
    request_id: &str,
    bookmark: &str,
    resolution: Option<&str>,
) -> Result<bool, String> {
    match resolution {
        Some(REPLAY_RESOLUTION_TAKE_COMMITTED_TIP) => Ok(true),
        Some(REPLAY_RESOLUTION_PLAIN) => Ok(false),
        other => Err(format!(
            "pending replay request `{request_id}` for `{bookmark}` has an unknown resolution \
             {other:?}"
        )),
    }
}

/// An explicit replay request that no completed attempt has served yet.
///
/// It lives on a reconcile item at the destination it was accepted against.
/// Only [`mark_replay_request_served`] (an attempt that reached a verdict) or
/// [`close_replay_request`] (a request superseded or unreadable) consumes it,
/// and both compare by id, so a newer request is never consumed by an older
/// attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingReplayRequest {
    /// The compare token: a pass consumes exactly the request it loaded.
    pub(crate) id: String,
    pub(crate) intent_id: String,
    pub(crate) project_id: String,
    pub(crate) store_path: String,
    pub(crate) target_branch: String,
    pub(crate) destination_commit: String,
    pub(crate) bookmark: String,
    /// The tip the request was accepted at. Recovery serves the request only
    /// while the branch is still there.
    pub(crate) requested_tip: Option<String>,
    pub(crate) take_committed_tip: bool,
    pub(crate) drop_incoming_reason: Option<String>,
    /// Unix seconds.
    pub(crate) requested_at: i64,
}

/// Durably record an accepted explicit replay request for `bookmark` onto
/// `destination`.
///
/// Ensures the reconcile intent at these coordinates exists and the branch's
/// item under it, then writes the request onto that item. A new intent is
/// created `completed` and an existing one is left exactly as it is: demand
/// preparation refuses a `pending`/`running` coordinate as legacy work, and
/// recovery finds requests by their item columns, not by intent status, so
/// nothing needs the intent reopened. An item that already exists keeps its
/// status, notification and pass history. Every older unserved request for the
/// same store and branch is stamped served in the same transaction, so the
/// newest request is the only authority.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn queue_replay_request(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    target_branch: &str,
    destination: &str,
    bookmark: &str,
    tip: Option<&str>,
    take_committed_tip: bool,
    drop_incoming_reason: Option<&str>,
) -> Result<PendingReplayRequest, String> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let fresh_intent_id = uuid::Uuid::new_v4().to_string();
    let requested_at = chrono::Utc::now().timestamp();
    let resolution = if take_committed_tip {
        REPLAY_RESOLUTION_TAKE_COMMITTED_TIP
    } else {
        REPLAY_RESOLUTION_PLAIN
    };
    let sources = serde_json::to_string(&[format!("replay requested for {bookmark}")])
        .map_err(|error| format!("encode replay request source: {error}"))?;
    let mut request = PendingReplayRequest {
        id: request_id,
        intent_id: String::new(),
        project_id: project_id.to_string(),
        store_path: store.to_string_lossy().into_owned(),
        target_branch: target_branch.to_string(),
        destination_commit: destination.to_string(),
        bookmark: bookmark.to_string(),
        requested_tip: tip.map(str::to_string),
        take_committed_tip,
        drop_incoming_reason: drop_incoming_reason.map(str::to_string),
        requested_at,
    };
    let queued = request.clone();
    let intent_id = db
        .write(|conn| {
            let request = queued.clone();
            let fresh_intent_id = fresh_intent_id.clone();
            let sources = sources.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jj_reconcile_intents
                       (id, project_id, store_path, target_branch, destination_commit,
                        trigger_sources_json, status, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'completed', ?7, ?7)
                     ON CONFLICT(project_id, store_path, target_branch, destination_commit)
                     DO NOTHING",
                    params![
                        fresh_intent_id.as_str(),
                        request.project_id.as_str(),
                        request.store_path.as_str(),
                        request.target_branch.as_str(),
                        request.destination_commit.as_str(),
                        sources.as_str(),
                        request.requested_at
                    ],
                )
                .await?;
                let mut rows = conn
                    .query(
                        "SELECT id FROM jj_reconcile_intents
                         WHERE project_id = ?1 AND store_path = ?2
                           AND target_branch = ?3 AND destination_commit = ?4",
                        params![
                            request.project_id.as_str(),
                            request.store_path.as_str(),
                            request.target_branch.as_str(),
                            request.destination_commit.as_str()
                        ],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                let intent_id = row.text(0)?;
                drop(rows);

                conn.execute(
                    "UPDATE jj_reconcile_items SET replay_served_at = ?3
                     WHERE bookmark = ?1
                       AND replay_request_id IS NOT NULL AND replay_served_at IS NULL
                       AND intent_id IN (SELECT id FROM jj_reconcile_intents WHERE store_path = ?2)",
                    params![
                        request.bookmark.as_str(),
                        request.store_path.as_str(),
                        request.requested_at
                    ],
                )
                .await?;

                conn.execute(
                    "INSERT INTO jj_reconcile_items
                       (intent_id, bookmark, observed_tip, status, updated_at,
                        replay_request_id, replay_resolution, replay_drop_incoming_reason,
                        replay_served_at, replay_requested_at, replay_requested_tip,
                        replay_requested_destination)
                     VALUES (?1, ?2, ?3, 'pending', ?4, ?5, ?6, ?7, NULL, ?4, ?3, ?8)
                     ON CONFLICT(intent_id, bookmark) DO UPDATE SET
                       replay_request_id = excluded.replay_request_id,
                       replay_resolution = excluded.replay_resolution,
                       replay_drop_incoming_reason = excluded.replay_drop_incoming_reason,
                       replay_served_at = NULL,
                       replay_result_json = NULL,
                       replay_requested_at = excluded.replay_requested_at,
                       replay_requested_tip = excluded.replay_requested_tip,
                       replay_requested_destination = excluded.replay_requested_destination",
                    params![
                        intent_id.as_str(),
                        request.bookmark.as_str(),
                        request.requested_tip.as_deref(),
                        request.requested_at,
                        request.id.as_str(),
                        resolution,
                        request.drop_incoming_reason.as_deref(),
                        request.destination_commit.as_str()
                    ],
                )
                .await?;
                Ok(Some(intent_id))
            })
        })
        .await
        .map_err(|error| format!("queue replay request: {error}"))?;
    request.intent_id = intent_id.ok_or_else(|| {
        format!(
            "queue replay request for `{bookmark}`: the reconcile intent at `{destination}` \
             vanished while it was being ensured"
        )
    })?;
    Ok(request)
}

const PENDING_REQUEST_COLUMNS: &str = "i.replay_request_id, i.intent_id, n.project_id, \
     n.store_path, n.target_branch, n.destination_commit, i.bookmark, i.replay_requested_tip, \
     i.replay_resolution, i.replay_drop_incoming_reason, i.replay_requested_at";

/// One unserved request row, with its stored resolution not yet judged.
fn pending_request_row(
    row: &cairn_db::turso::Row,
) -> cairn_db::storage::DbResult<(PendingReplayRequest, Option<String>)> {
    Ok((
        PendingReplayRequest {
            id: row.text(0)?,
            intent_id: row.text(1)?,
            project_id: row.text(2)?,
            store_path: row.text(3)?,
            target_branch: row.text(4)?,
            destination_commit: row.text(5)?,
            bookmark: row.text(6)?,
            requested_tip: row.opt_text(7)?,
            take_committed_tip: false,
            drop_incoming_reason: row.opt_text(9)?,
            requested_at: row.opt_i64(10)?.unwrap_or(0),
        },
        row.opt_text(8)?,
    ))
}

fn judged_request(
    (mut request, resolution): (PendingReplayRequest, Option<String>),
) -> Result<PendingReplayRequest, String> {
    request.take_committed_tip =
        parse_replay_resolution(&request.id, &request.bookmark, resolution.as_deref())?;
    Ok(request)
}

/// The newest unserved explicit replay request for `bookmark` in `store`.
///
/// Not filtered by destination: a request retargeted onto a newer base keeps
/// its authority. A row with a request timestamp but no request id predates
/// typed requests and is never authority.
pub(crate) async fn load_pending_replay_request(
    db: &LocalDb,
    store: &Path,
    bookmark: &str,
) -> Result<Option<PendingReplayRequest>, String> {
    let store = store.to_string_lossy().into_owned();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        let (store, bookmark) = (store.clone(), bookmark.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT {PENDING_REQUEST_COLUMNS}
                         FROM jj_reconcile_items i
                         JOIN jj_reconcile_intents n ON n.id = i.intent_id
                         WHERE i.bookmark = ?1 AND n.store_path = ?2
                           AND i.replay_request_id IS NOT NULL AND i.replay_served_at IS NULL
                           AND i.replay_requested_at IS NOT NULL
                         ORDER BY i.replay_requested_at DESC, i.rowid DESC LIMIT 1"
                    ),
                    params![bookmark, store],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(pending_request_row(&row)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| format!("load pending replay request: {error}"))?
    .map(judged_request)
    .transpose()
}

/// One unserved request found by [`list_pending_replay_requests`]. The id,
/// store and branch are known even when the stored resolution is unreadable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListedReplayRequest {
    pub(crate) id: String,
    pub(crate) store_path: String,
    pub(crate) bookmark: String,
    pub(crate) request: Result<PendingReplayRequest, String>,
}

/// The newest unserved explicit replay request of every (store, branch), for
/// exact-branch recovery. Read from the item columns whatever the intent's
/// status: retiring the legacy queue marks old intents `superseded` and leaves
/// their request columns untouched. Each entry names the request's id even
/// when its stored resolution is unreadable, so the caller can close exactly
/// that request instead of guessing a resolution for it.
pub(crate) async fn list_pending_replay_requests(
    db: &LocalDb,
) -> Result<Vec<ListedReplayRequest>, String> {
    let rows = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT {PENDING_REQUEST_COLUMNS}
                             FROM jj_reconcile_items i
                             JOIN jj_reconcile_intents n ON n.id = i.intent_id
                             WHERE i.replay_request_id IS NOT NULL
                               AND i.replay_served_at IS NULL
                               AND i.replay_requested_at IS NOT NULL
                             ORDER BY n.store_path, i.bookmark,
                                      i.replay_requested_at DESC, i.rowid DESC"
                        ),
                        (),
                    )
                    .await?;
                let mut all = Vec::new();
                while let Some(row) = rows.next().await? {
                    all.push(pending_request_row(&row)?);
                }
                Ok(all)
            })
        })
        .await
        .map_err(|error| format!("list pending replay requests: {error}"))?;
    let mut newest = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        if seen.insert((row.0.store_path.clone(), row.0.bookmark.clone())) {
            newest.push(ListedReplayRequest {
                id: row.0.id.clone(),
                store_path: row.0.store_path.clone(),
                bookmark: row.0.bookmark.clone(),
                request: judged_request(row),
            });
        }
    }
    Ok(newest)
}

/// The verdict frozen onto a request when it was served.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ServedReplay {
    pass: ReplayPass,
    destination: String,
}

/// Where one explicit replay request stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplayRequestState {
    /// No completed pass has served it.
    Pending,
    /// A pass attempted it; `pass` is what that pass recorded on the item it
    /// ran, onto `destination`, frozen at the moment it was served.
    Served {
        pass: ReplayPass,
        destination: String,
    },
    /// A newer request replaced it, or its row is gone. Whatever happens next
    /// answers the newer request, not this one.
    Superseded,
}

/// Consume exactly `request`, once a pass has attempted it (landed or failed).
///
/// `current_intent_id` is the intent the attempting pass ran, which may differ
/// from the request's own after a retarget. That pass's item verdict is
/// snapshotted onto the request's row in the same transaction, so a later pass
/// over either item cannot rewrite the first result. A serving row that is
/// missing or carries no outcome or failure is refused with an error and the
/// request stays pending. Compared by request id, so
/// a newer request queued while this pass ran is left pending. Returns whether
/// this call consumed it; `false` means it was already served, superseded, or
/// overwritten by a newer request.
pub(crate) async fn mark_replay_request_served(
    db: &LocalDb,
    request: &PendingReplayRequest,
    current_intent_id: &str,
) -> Result<bool, String> {
    let request = request.clone();
    let current_intent_id = current_intent_id.to_string();
    db.write(|conn| {
        let request = request.clone();
        let current_intent_id = current_intent_id.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let mut rows = conn
                .query(
                    "SELECT i.status, n.status, i.outcome_kind, i.failure_kind,
                            i.last_diagnostic, i.updated_at, n.destination_commit
                     FROM jj_reconcile_items i
                     JOIN jj_reconcile_intents n ON n.id = i.intent_id
                     WHERE i.intent_id = ?1 AND i.bookmark = ?2",
                    params![current_intent_id.as_str(), request.bookmark.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(Err(format!(
                    "mark replay request `{}` served: the serving pass `{current_intent_id}` \
                     recorded nothing for `{}`; the request stays pending",
                    request.id, request.bookmark
                )));
            };
            let served = ServedReplay {
                pass: ReplayPass {
                    item_status: row.opt_text(0)?,
                    intent_status: row.opt_text(1)?,
                    outcome_kind: row.opt_text(2)?,
                    failure_kind: row.opt_text(3)?,
                    diagnostic: row.opt_text(4)?,
                    updated_at: row.opt_i64(5)?.unwrap_or(now),
                },
                destination: row.text(6)?,
            };
            drop(rows);
            // Only a pass that reached a verdict serves a request: a deferral
            // records neither an outcome nor a failure.
            if served.pass.outcome_kind.is_none() && served.pass.failure_kind.is_none() {
                return Ok(Err(format!(
                    "mark replay request `{}` served: the serving pass `{current_intent_id}` \
                     has no terminal outcome for `{}`; the request stays pending",
                    request.id, request.bookmark
                )));
            }
            // Serializing plain strings and integers cannot fail.
            let served = serde_json::to_string(&served).unwrap_or_default();
            let consumed = conn
                .execute(
                    "UPDATE jj_reconcile_items SET replay_served_at = ?4, replay_result_json = ?5
                     WHERE intent_id = ?1 AND bookmark = ?2 AND replay_request_id = ?3
                       AND replay_served_at IS NULL",
                    params![
                        request.intent_id.as_str(),
                        request.bookmark.as_str(),
                        request.id.as_str(),
                        now,
                        served.as_str()
                    ],
                )
                .await?;
            Ok(Ok(consumed > 0))
        })
    })
    .await
    .map_err(|error| format!("mark replay request served: {error}"))?
}

/// Serve exactly the request `request_id` on `bookmark` without an attempt:
/// it was superseded (the branch moved past the tip it was accepted at, or no
/// live job owns the branch) or its stored resolution is unreadable. The
/// reason is frozen as the result's `failure_kind` and `diagnostic`, so the
/// requester's page says why nothing was replayed. Compared by id; returns
/// whether this call closed it.
pub(crate) async fn close_replay_request(
    db: &LocalDb,
    request_id: &str,
    bookmark: &str,
    failure_kind: &str,
    diagnostic: &str,
) -> Result<bool, String> {
    let (request_id, bookmark) = (request_id.to_string(), bookmark.to_string());
    let (failure_kind, diagnostic) = (failure_kind.to_string(), diagnostic.to_string());
    db.write(|conn| {
        let (request_id, bookmark) = (request_id.clone(), bookmark.clone());
        let (failure_kind, diagnostic) = (failure_kind.clone(), diagnostic.clone());
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let mut rows = conn
                .query(
                    "SELECT n.destination_commit FROM jj_reconcile_items i
                     JOIN jj_reconcile_intents n ON n.id = i.intent_id
                     WHERE i.bookmark = ?1 AND i.replay_request_id = ?2
                       AND i.replay_served_at IS NULL",
                    params![bookmark.as_str(), request_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(false);
            };
            let served = ServedReplay {
                pass: ReplayPass {
                    failure_kind: Some(failure_kind),
                    diagnostic: Some(diagnostic),
                    updated_at: now,
                    ..ReplayPass::default()
                },
                destination: row.text(0)?,
            };
            drop(rows);
            let served = serde_json::to_string(&served).unwrap_or_default();
            let closed = conn
                .execute(
                    "UPDATE jj_reconcile_items SET replay_served_at = ?3, replay_result_json = ?4
                     WHERE bookmark = ?1 AND replay_request_id = ?2
                       AND replay_served_at IS NULL",
                    params![bookmark.as_str(), request_id.as_str(), now, served.as_str()],
                )
                .await?;
            Ok(closed > 0)
        })
    })
    .await
    .map_err(|error| format!("close replay request: {error}"))
}

/// Write why `request` could not be served yet onto its own row, without
/// consuming it: the reason stays visible and the next trigger retries.
pub(crate) async fn note_replay_request(
    db: &LocalDb,
    request: &PendingReplayRequest,
    diagnostic: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET last_diagnostic = ?4
         WHERE intent_id = ?1 AND bookmark = ?2 AND replay_request_id = ?3
           AND replay_served_at IS NULL",
        params![
            request.intent_id.as_str(),
            request.bookmark.as_str(),
            request.id.as_str(),
            diagnostic
        ],
    )
    .await
    .map_err(|error| format!("note replay request: {error}"))?;
    Ok(())
}

/// The newest explicit replay request recorded for `bookmark` in the project
/// of `job_id`, served or not: the rebase page's one source of request status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LatestReplayRequest {
    pub(crate) id: String,
    /// Unix seconds.
    pub(crate) requested_at: i64,
    /// The tip the request was accepted at.
    pub(crate) requested_tip: Option<String>,
    /// The base it was accepted onto, by name and commit.
    pub(crate) target_branch: String,
    pub(crate) destination_commit: String,
    pub(crate) state: ReplayRequestState,
}

pub(crate) async fn latest_replay_request(
    db: &LocalDb,
    job_id: &str,
    bookmark: &str,
) -> Result<Option<LatestReplayRequest>, String> {
    let (job_id, bookmark) = (job_id.to_string(), bookmark.to_string());
    let row = db
        .read(move |conn| {
            let (job_id, bookmark) = (job_id.clone(), bookmark.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.replay_request_id, i.replay_requested_at, i.replay_served_at,
                                i.replay_result_json, i.replay_requested_tip, n.target_branch,
                                n.destination_commit
                         FROM jj_reconcile_items i
                         JOIN jj_reconcile_intents n ON n.id = i.intent_id
                         WHERE i.bookmark = ?2 AND i.replay_request_id IS NOT NULL
                           AND n.project_id = (SELECT project_id FROM jobs WHERE id = ?1)
                         ORDER BY i.replay_requested_at DESC, i.rowid DESC LIMIT 1",
                        params![job_id, bookmark],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((
                    row.text(0)?,
                    row.opt_i64(1)?.unwrap_or(0),
                    row.opt_i64(2)?,
                    row.opt_text(3)?,
                    row.opt_text(4)?,
                    row.text(5)?,
                    row.text(6)?,
                )))
            })
        })
        .await
        .map_err(|error| format!("load the latest replay request: {error}"))?;
    let Some((id, requested_at, served_at, result, requested_tip, target_branch, destination)) =
        row
    else {
        return Ok(None);
    };
    let state = match (served_at, result) {
        (None, _) => ReplayRequestState::Pending,
        (Some(_), None) => ReplayRequestState::Superseded,
        (Some(_), Some(result)) => {
            let served: ServedReplay = serde_json::from_str(&result).map_err(|error| {
                format!("replay request `{id}` has an unreadable result: {error}")
            })?;
            ReplayRequestState::Served {
                pass: served.pass,
                destination: served.destination,
            }
        }
    };
    Ok(Some(LatestReplayRequest {
        id,
        requested_at,
        requested_tip,
        target_branch,
        destination_commit: destination,
        state,
    }))
}

/// Where `request` stands, read from the row that owns it.
pub(crate) async fn replay_request_state(
    db: &LocalDb,
    request: &PendingReplayRequest,
) -> Result<ReplayRequestState, String> {
    let (intent_id, bookmark) = (request.intent_id.clone(), request.bookmark.clone());
    let row = db
        .read(move |conn| {
            let (intent_id, bookmark) = (intent_id.clone(), bookmark.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT replay_request_id, replay_served_at, replay_result_json
                         FROM jj_reconcile_items WHERE intent_id = ?1 AND bookmark = ?2",
                        params![intent_id, bookmark],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((row.opt_text(0)?, row.opt_i64(1)?, row.opt_text(2)?)))
            })
        })
        .await
        .map_err(|error| format!("load replay request state: {error}"))?;
    let Some((Some(id), served_at, result)) = row else {
        return Ok(ReplayRequestState::Superseded);
    };
    if id != request.id {
        return Ok(ReplayRequestState::Superseded);
    }
    if served_at.is_none() {
        return Ok(ReplayRequestState::Pending);
    }
    let Some(result) = result else {
        return Ok(ReplayRequestState::Superseded);
    };
    let served: ServedReplay = serde_json::from_str(&result).map_err(|error| {
        format!(
            "replay request `{}` for `{}` has an unreadable result: {error}",
            request.id, request.bookmark
        )
    })?;
    Ok(ReplayRequestState::Served {
        pass: served.pass,
        destination: served.destination,
    })
}

/// Supersede every other open session for this branch, so exactly one is active.
///
/// A base that advances twice produces two intents; the older session's
/// coordinates describe a merge nobody will perform again.
pub(crate) async fn supersede_stale_sessions(
    db: &LocalDb,
    bookmark: &str,
    keep_intent_id: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET resolution_state = ?3
         WHERE bookmark = ?1 AND intent_id <> ?2 AND resolution_state = ?4",
        params![
            bookmark,
            keep_intent_id,
            ResolutionState::Superseded.as_str(),
            ResolutionState::Open.as_str()
        ],
    )
    .await
    .map_err(|error| format!("supersede stale conflict sessions: {error}"))?;
    Ok(())
}

const SESSION_COLUMNS: &str = "i.intent_id, i.bookmark, n.project_id, n.store_path, \
     n.target_branch, n.destination_commit, i.diagnostic_version, i.conflict_condition, \
     i.base_commit, i.ours_commit, i.theirs_commit, i.conflicted_tip, i.incoming_pr_number, \
     i.incoming_issue, i.incoming_base_branch, i.marker_state, i.marker_diagnostic, \
     i.resolution_state, i.updated_at, i.replay_requested_at, i.resolution_basis_json, \
     i.replay_requested_tip, i.replay_requested_destination";

/// The active resolution session for a branch, if one is open.
pub(crate) async fn load_active_session(
    db: &LocalDb,
    bookmark: &str,
) -> Result<Option<ConflictSession>, String> {
    load_session(db, bookmark, true).await
}

/// The most recent session for a branch whatever its state.
///
/// A resolved session is the only thing that can say how a conflict ENDED. Left
/// unread, a branch that queued a replay and had it published shows the same
/// bare "no open session" as a branch that never conflicted at all, so the arc
/// from queued through replaying to published simply vanishes at the moment it
/// completes.
pub(crate) async fn load_latest_session(
    db: &LocalDb,
    bookmark: &str,
) -> Result<Option<ConflictSession>, String> {
    load_session(db, bookmark, false).await
}

async fn load_session(
    db: &LocalDb,
    bookmark: &str,
    open_only: bool,
) -> Result<Option<ConflictSession>, String> {
    let bookmark_owned = bookmark.to_string();
    let open_filter = if open_only {
        format!(
            "AND i.resolution_state = '{}'",
            ResolutionState::Open.as_str()
        )
    } else {
        String::new()
    };
    let sql = format!(
        "SELECT {SESSION_COLUMNS}
         FROM jj_reconcile_items i
         JOIN jj_reconcile_intents n ON n.id = i.intent_id
         WHERE i.bookmark = ?1 {open_filter}
           AND i.diagnostic_version IS NOT NULL
         ORDER BY i.updated_at DESC LIMIT 1"
    );
    let mut session = db
        .read(move |conn| {
            let bookmark = bookmark_owned.clone();
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, params![bookmark]).await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some(ConflictSession {
                    intent_id: row.text(0)?,
                    bookmark: row.text(1)?,
                    project_id: row.text(2)?,
                    store_path: row.text(3)?,
                    target_branch: row.text(4)?,
                    destination_commit: row.text(5)?,
                    diagnostic_version: row.opt_i64(6)?.unwrap_or(0),
                    condition: row
                        .opt_text(7)?
                        .unwrap_or_else(|| ConflictCondition::ContentConflict.as_str().to_string()),
                    base: row.opt_text(8)?,
                    ours: row.opt_text(9)?,
                    theirs: row.opt_text(10)?,
                    conflicted_tip: row.opt_text(11)?,
                    incoming: IncomingIdentity {
                        pr_number: row.opt_i64(12)?,
                        issue: row.opt_text(13)?,
                        base_branch: row.opt_text(14)?.unwrap_or_default(),
                    },
                    marker_state: MarkerState::parse(row.opt_text(15)?.as_deref()),
                    marker_diagnostic: row.opt_text(16)?,
                    resolution_state: row.opt_text(17)?,
                    updated_at: row.opt_i64(18)?.unwrap_or(0),
                    replay_requested_at: row.opt_i64(19)?,
                    files: Vec::new(),
                    resolution_basis: decode_basis(row.opt_text(20)?.as_deref()),
                    replay_requested_tip: row.opt_text(21)?,
                    replay_requested_destination: row.opt_text(22)?,
                }))
            })
        })
        .await
        .map_err(|error| format!("load conflict session: {error}"))?;

    if let Some(session) = session.as_mut() {
        session.files = load_session_files(db, &session.intent_id, &session.bookmark).await?;
    }
    Ok(session)
}

/// What the last reconcile pass onto `destination` recorded for `bookmark`.
///
/// For a replay request this is the row that processes it: `request_replay`
/// reopens and runs the intent at the request's destination, so that row — not
/// the one the session was recorded on — carries what the last pass did. For a
/// branch with no session it is what the automatic advance did, if anything.
/// Keyed by store and destination commit rather than base name, because the
/// base may have been re-resolved to a new name.
pub(crate) async fn load_reconcile_pass(
    db: &LocalDb,
    bookmark: &str,
    store_path: &str,
    destination: &str,
) -> Result<Option<ReplayPass>, String> {
    let (bookmark, store_path, destination) = (
        bookmark.to_string(),
        store_path.to_string(),
        destination.to_string(),
    );
    db.read(move |conn| {
        let (bookmark, store_path, destination) =
            (bookmark.clone(), store_path.clone(), destination.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.status, n.status, i.outcome_kind, i.failure_kind, i.last_diagnostic,
                            i.updated_at
                     FROM jj_reconcile_items i
                     JOIN jj_reconcile_intents n ON n.id = i.intent_id
                     WHERE i.bookmark = ?1 AND n.store_path = ?2 AND n.destination_commit = ?3
                     ORDER BY i.updated_at DESC LIMIT 1",
                    params![bookmark, store_path, destination],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReplayPass {
                item_status: row.opt_text(0)?,
                intent_status: row.opt_text(1)?,
                outcome_kind: row.opt_text(2)?,
                failure_kind: row.opt_text(3)?,
                diagnostic: row.opt_text(4)?,
                updated_at: row.opt_i64(5)?.unwrap_or(0),
            }))
        })
    })
    .await
    .map_err(|error| format!("load replay pass: {error}"))
}

/// The incoming change's complete file inventory for one session, path-ordered.
pub(crate) async fn load_session_files(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
) -> Result<Vec<SessionFile>, String> {
    let intent_id = intent_id.to_string();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        let intent_id = intent_id.clone();
        let bookmark = bookmark.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT path, status, classification, marker_disposition
                     FROM jj_reconcile_incoming_files
                     WHERE intent_id = ?1 AND bookmark = ?2
                     ORDER BY path",
                    params![intent_id, bookmark],
                )
                .await?;
            let mut files = Vec::new();
            while let Some(row) = rows.next().await? {
                files.push(SessionFile {
                    path: row.text(0)?,
                    status: row.text(1)?,
                    classification: row.text(2)?,
                    marker_disposition: row.opt_text(3)?,
                });
            }
            Ok(files)
        })
    })
    .await
    .map_err(|error| format!("load conflict session inventory: {error}"))
}

/// Record what materialization did, keyed to what the executor confirmed.
///
/// `dispositions` is empty for every state but [`MarkerState::Materialized`];
/// only a confirmed materialization knows how each path was treated.
pub(crate) async fn record_marker_state(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
    state: MarkerState,
    diagnostic: Option<&str>,
    dispositions: &[(String, String)],
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET marker_state = ?3, marker_diagnostic = ?4
         WHERE intent_id = ?1 AND bookmark = ?2",
        params![intent_id, bookmark, state.as_str(), diagnostic],
    )
    .await
    .map_err(|error| format!("record marker state: {error}"))?;
    for (path, disposition) in dispositions {
        db.execute(
            "UPDATE jj_reconcile_incoming_files SET marker_disposition = ?4
             WHERE intent_id = ?1 AND bookmark = ?2 AND path = ?3",
            params![intent_id, bookmark, path.as_str(), disposition.as_str()],
        )
        .await
        .map_err(|error| format!("record marker disposition: {error}"))?;
    }
    Ok(())
}

/// Close every open session for a branch that has just absorbed its base.
pub(crate) async fn close_open_sessions_for_branch(
    db: &LocalDb,
    bookmark: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET resolution_state = ?2, marker_state = ?3
         WHERE bookmark = ?1 AND resolution_state = ?4",
        params![
            bookmark,
            ResolutionState::Resolved.as_str(),
            MarkerState::NotMaterialized.as_str(),
            ResolutionState::Open.as_str()
        ],
    )
    .await
    .map_err(|error| format!("close open conflict sessions: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod bounded_assessment_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn scope() -> AssessmentScope<'static> {
        AssessmentScope {
            phase: "resolution basis assessment",
            branch: "agent/slow",
            candidate_paths: 3,
        }
    }

    /// One runtime worker: if the assessment ran on it, the ticker could not
    /// tick until the assessment returned.
    #[tokio::test(flavor = "current_thread")]
    async fn a_bounded_assessment_never_holds_the_runtime_worker() {
        let home = tempfile::tempdir().unwrap();
        let jj = JjEnv::with_binary("jj", home.path());
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticker = tokio::spawn({
            let ticks = ticks.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    ticks.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        let budget = Duration::from_millis(200);

        // Ignores its deadline entirely: the caller still returns at budget
        // plus grace, as a timeout.
        let started = Instant::now();
        let stuck = bounded_store_assessment(&jj, budget, scope(), |_| {
            std::thread::sleep(Duration::from_secs(4));
            Ok(())
        })
        .await
        .unwrap_err();
        let waited = started.elapsed();
        assert!(crate::jj::is_jj_timeout_error(&stuck), "{stuck}");
        assert_eq!(
            crate::jj::reconcile_failure_kind(&stuck),
            crate::jj::RECONCILE_TIMEOUT
        );
        assert!(
            stuck.contains("agent/slow") && stuck.contains("3 candidate path(s)"),
            "{stuck}"
        );
        assert!(
            waited < budget + ASSESSMENT_JOIN_GRACE + Duration::from_millis(700),
            "{waited:?}"
        );
        assert!(
            ticks.load(Ordering::SeqCst) >= 20,
            "the runtime kept running: {} ticks in {waited:?}",
            ticks.load(Ordering::SeqCst)
        );

        // Observes it: returns its own timeout at the deadline, not the grace.
        let started = Instant::now();
        let cooperative = bounded_store_assessment::<(), _>(&jj, budget, scope(), |jj| loop {
            jj.merge_deadline().check("probe")?;
            std::thread::sleep(Duration::from_millis(5));
        })
        .await
        .unwrap_err();
        assert!(cooperative.starts_with("probe timed out"), "{cooperative}");
        assert!(
            started.elapsed() < budget + Duration::from_millis(400),
            "{:?}",
            started.elapsed()
        );

        let answered = bounded_store_assessment(&jj, budget, scope(), |_| Ok(7)).await;
        assert_eq!(answered, Ok(7));
        ticker.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assessment(verdicts: Vec<(&str, RestoreVerdict)>, truncated: usize) -> TipAssessment {
        TipAssessment {
            tip: "tip".to_string(),
            truncated,
            unresolved: Vec::new(),
            conflicting: verdicts.len() + truncated,
            recorded: RecordedConflict {
                ours: Some("c1".to_string()),
                theirs: "d1".to_string(),
                at: 0,
            },
            paths: verdicts
                .into_iter()
                .map(|(path, verdict)| PathAssessment {
                    path: path.to_string(),
                    verdict,
                })
                .collect(),
            deletions: BTreeMap::new(),
            uncovered_deletions: 0,
            attested: BTreeMap::new(),
            newly_attested: Vec::new(),
        }
    }

    fn drops() -> RestoreVerdict {
        RestoreVerdict::Lossy(DroppedWork {
            candidate: "merged\n".to_string(),
            diff: "@@ -1 +1,2 @@\n ours\n+incoming".to_string(),
            added_lines: 1,
            hunks: 1,
        })
    }

    #[test]
    fn a_fully_resolved_assessment_blocks_nothing() {
        let clean = assessment(
            vec![
                ("a.rs", RestoreVerdict::Lossless),
                ("b.rs", RestoreVerdict::Lossless),
            ],
            0,
        );
        assert!(clean.unproven().is_empty());
        assert!(clean.every_path_is_resolved());
    }

    #[test]
    fn a_path_that_would_drop_incoming_work_is_unproven() {
        let lossy = assessment(
            vec![("a.rs", RestoreVerdict::Lossless), ("b.rs", drops())],
            0,
        );
        let unproven = lossy.unproven();
        assert_eq!(unproven.len(), 1);
        assert_eq!(unproven[0].0, "b.rs");
        assert!(matches!(unproven[0].1, Unproven::Drops(_)));
    }

    /// The hole a review caught: "the merge could not judge this" is not
    /// evidence of safety, and the whole-file restore covers that path either
    /// way. Treating it as safe is how a guard against silent loss comes to
    /// permit silent loss — a branch whose only conflicting file is binary would
    /// have had the incoming version discarded with nothing said.
    #[test]
    fn a_path_the_merge_could_not_judge_is_unproven_rather_than_assumed_safe() {
        let binary = assessment(
            vec![(
                "logo.png",
                RestoreVerdict::NotAssessed("one side of this path is not UTF-8 text".to_string()),
            )],
            0,
        );
        let unproven = binary.unproven();
        assert_eq!(
            unproven.len(),
            1,
            "an unjudged path must block: {unproven:?}"
        );
        assert_eq!(unproven[0].0, "logo.png");
        assert!(
            matches!(unproven[0].1, Unproven::Unjudged(_)),
            "and it is reported as unjudged rather than as positive loss evidence — the remedies \
             differ, so the refusal lists them apart"
        );
    }

    /// The other half of the same hole. A capped assessment says nothing about
    /// the paths past its cap, and the restore covers them regardless, so a
    /// truncated assessment can never conclude that a session is resolved.
    #[test]
    fn a_truncated_assessment_never_reads_as_resolved() {
        let truncated = assessment(
            vec![
                ("a.rs", RestoreVerdict::Lossless),
                ("b.rs", RestoreVerdict::Lossless),
            ],
            24,
        );
        assert!(
            truncated.unproven().is_empty(),
            "the paths it DID look at are genuinely clean"
        );
        assert!(
            !truncated.every_path_is_resolved(),
            "but an answer about 2 of 26 paths is not an answer about the session"
        );
    }

    #[test]
    fn an_empty_assessment_is_not_a_resolution() {
        assert!(!assessment(Vec::new(), 0).every_path_is_resolved());
    }

    fn refusal(proof: &RestoreProof) -> String {
        match decide_replay(proof, None) {
            ReplayDecision::Refuse(refusal) => refusal,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_proof_that_every_path_is_lossless_lets_the_replay_through() {
        let proved = RestoreProof::Assessed(assessment(
            vec![
                ("a.rs", RestoreVerdict::Lossless),
                ("b.rs", RestoreVerdict::Lossless),
            ],
            0,
        ));
        assert_eq!(decide_replay(&proved, None), ReplayDecision::Proceed);
    }

    /// Base drift is the ONE absence that may proceed: the sides are already
    /// byte-identical, so the restore moves no bytes and there is nothing a
    /// merge could discard.
    #[test]
    fn base_drift_is_the_only_absence_that_proceeds() {
        assert_eq!(
            decide_replay(&RestoreProof::NothingToProve, None),
            ReplayDecision::Proceed
        );
    }

    /// The second fail-open a review caught: an unresolvable bookmark, a missing
    /// coordinate, or a store that would not answer used to arrive as the same
    /// `None` base drift did, and the guard read that absence as permission. A
    /// transient failure must never become authority to overwrite work.
    #[test]
    fn a_proof_that_could_not_be_built_refuses_rather_than_falling_through() {
        let unavailable = RestoreProof::Unavailable(
            "the branch bookmark `agent/x` did not resolve to a commit".to_string(),
        );
        let refusal = refusal(&unavailable);
        assert!(
            refusal.contains("did not resolve"),
            "the refusal carries why it could not check: {refusal}"
        );
        assert!(
            refusal.contains("not a finding about your resolution"),
            "and distinguishes an absent answer from a negative one: {refusal}"
        );
    }

    #[test]
    fn a_truncated_proof_refuses_because_it_answers_for_only_some_paths() {
        let truncated =
            RestoreProof::Assessed(assessment(vec![("a.rs", RestoreVerdict::Lossless)], 25));
        assert!(refusal(&truncated).contains("1 of 26"));
    }

    #[test]
    fn a_refusal_for_dropped_work_names_the_file_and_the_merged_view() {
        let lossy = RestoreProof::Assessed(assessment(vec![("b.rs", drops())], 0));
        let refusal = refusal(&lossy);
        assert!(refusal.contains("`b.rs`"), "{refusal}");
        assert!(
            refusal.contains("view=merged&file=b.rs"),
            "the remedy is one read away: {refusal}"
        );
        assert!(refusal.contains("drop_incoming_reason"), "{refusal}");
    }

    /// Bug-sink #372: the guard read a tip that kept only its own insertion at
    /// a spot where the incoming side inserted too as lossless, so the replay
    /// landed without the incoming line. Run through the real merge, the
    /// refusal must name the dropped line itself, not only count it.
    #[test]
    fn a_tip_lacking_a_same_spot_incoming_insertion_is_refused_naming_the_line() {
        let sides = crate::jj::MergeSides {
            base: crate::jj::FileContent::Text("A\n".to_string()),
            ours: crate::jj::FileContent::Text("X\nA\n".to_string()),
            theirs: crate::jj::FileContent::Text("Y\nA\n".to_string()),
        };
        let verdict = crate::jj::assess_whole_file_restore(&sides);
        let refusal = refusal(&RestoreProof::Assessed(assessment(
            vec![("mod.rs", verdict)],
            0,
        )));
        assert!(refusal.contains("`mod.rs`"), "{refusal}");
        assert!(
            refusal.contains("\n+Y\n"),
            "the dropped line is quoted: {refusal}"
        );
    }

    #[test]
    fn a_long_dropped_diff_is_quoted_up_to_its_cap_and_says_how_much_more() {
        let diff = (0..40)
            .map(|n| format!("+line {n}"))
            .collect::<Vec<_>>()
            .join("\n");
        let excerpt = refusal_excerpt(&diff);
        assert!(
            excerpt.contains("+line 23") && !excerpt.contains("+line 24"),
            "{excerpt}"
        );
        assert!(
            excerpt.ends_with("16 more line(s) in the merged view"),
            "{excerpt}"
        );
        assert_eq!(refusal_excerpt("+one\n+two"), "+one\n+two");
    }

    #[test]
    fn a_refusal_for_an_unjudged_path_offers_no_merged_file_because_there_is_none() {
        let unjudged = RestoreProof::Assessed(assessment(
            vec![(
                "logo.png",
                RestoreVerdict::NotAssessed("one side is not UTF-8 text".to_string()),
            )],
            0,
        ));
        let refusal = refusal(&unjudged);
        assert!(refusal.contains("could not judge"), "{refusal}");
        assert!(
            !refusal.contains("view=merged&file=logo.png"),
            "there is no merged file to offer for it: {refusal}"
        );
        assert!(refusal.contains("the decision is yours"), "{refusal}");
    }

    /// Every refusing arm is escapable by stating a reason, so the strict
    /// default is never a dead end.
    #[test]
    fn a_stated_reason_carries_every_blocked_arm_through_with_a_caveat() {
        let blocked = [
            RestoreProof::Unavailable("the store would not answer".to_string()),
            RestoreProof::Assessed(assessment(vec![("b.rs", drops())], 0)),
            RestoreProof::Assessed(assessment(vec![("a.rs", RestoreVerdict::Lossless)], 9)),
        ];
        for proof in &blocked {
            let ReplayDecision::ProceedOnStatedReason(caveat) =
                decide_replay(proof, Some("the incoming version is obsolete"))
            else {
                panic!("a stated reason must carry {proof:?} through");
            };
            assert!(
                caveat.contains("the incoming version is obsolete"),
                "the caveat repeats the stated reason: {caveat}"
            );
        }
    }

    fn session(ours: &str, theirs: &str, conflicting: &[&str]) -> ConflictSession {
        ConflictSession {
            intent_id: "intent".to_string(),
            bookmark: "agent/test".to_string(),
            project_id: "project".to_string(),
            store_path: "/store".to_string(),
            target_branch: "main".to_string(),
            destination_commit: theirs.to_string(),
            diagnostic_version: CONFLICT_DIAGNOSTIC_VERSION,
            condition: ConflictCondition::ContentConflict.as_str().to_string(),
            base: Some("fork".to_string()),
            ours: Some(ours.to_string()),
            theirs: Some(theirs.to_string()),
            conflicted_tip: None,
            incoming: IncomingIdentity::default(),
            marker_state: MarkerState::NotMaterialized,
            marker_diagnostic: None,
            resolution_state: Some("open".to_string()),
            updated_at: 0,
            replay_requested_at: None,
            files: conflicting
                .iter()
                .map(|path| SessionFile {
                    path: path.to_string(),
                    status: "M".to_string(),
                    classification: IncomingClassification::Conflicting.as_str().to_string(),
                    marker_disposition: None,
                })
                .collect(),
            resolution_basis: ResolutionBasis::new(),
            replay_requested_tip: None,
            replay_requested_destination: None,
        }
    }

    fn side(theirs: &str) -> ResolvedSide {
        ResolvedSide {
            base: "fork".to_string(),
            theirs: theirs.to_string(),
            attested: None,
        }
    }

    fn attested(theirs: &str, reason: &str) -> ResolvedSide {
        ResolvedSide {
            attested: Some(reason.to_string()),
            ..side(theirs)
        }
    }

    /// A statement covers each deleted replay path the session's sides do not
    /// already have one for: an uncovered deletion, one covered against an
    /// older side, and one content placed against these sides. It keeps the
    /// statement an earlier request made against these same sides, and never
    /// reaches a path the tip still has or one outside the replay.
    #[test]
    fn a_statement_attests_only_the_replays_deleted_paths_not_yet_covered_against_its_sides() {
        let mut open = session("c1", "d1", &["fence.rs", "list.txt", "moved.rs"]);
        open.resolution_basis
            .insert("auth.rs".to_string(), attested("d0", "split into auth/"));
        open.resolution_basis
            .insert("kept.rs".to_string(), attested("d1", "first statement"));
        open.resolution_basis
            .insert("moved.rs".to_string(), side("d1"));
        let deleted = changed(&["fence.rs", "auth.rs", "kept.rs", "moved.rs", "elsewhere.rs"]);

        let (basis, newly) = open.attest_deletions(
            open.resolution_basis.clone(),
            &deleted,
            "ported #4372",
            ReplayScope::Carry,
        );
        assert_eq!(newly, vec!["auth.rs", "fence.rs", "moved.rs"]);
        for path in ["auth.rs", "fence.rs", "moved.rs"] {
            assert_eq!(
                basis.get(path),
                Some(&attested("d1", "ported #4372")),
                "{path}"
            );
        }
        assert_eq!(
            basis.get("kept.rs"),
            Some(&attested("d1", "first statement")),
            "an attestation against these sides keeps its own statement"
        );
        assert_eq!(basis.get("list.txt"), None, "the tip still has it");
        assert_eq!(basis.get("elsewhere.rs"), None, "not a replay path");

        let mut drift = open.clone();
        drift.condition = ConflictCondition::BaseDrift.as_str().to_string();
        let (unchanged, none) = drift.attest_deletions(
            drift.resolution_basis.clone(),
            &deleted,
            "ported #4372",
            ReplayScope::Carry,
        );
        assert!(none.is_empty());
        assert_eq!(unchanged, drift.resolution_basis);
    }

    /// Content evidence outranks a statement: a file attested as deleted that
    /// comes back and changes is resolved by that content, unattested.
    #[test]
    fn a_content_change_to_an_attested_path_replaces_the_statement() {
        let mut open = session("c1", "d2", &["fence.rs"]);
        open.resolution_basis
            .insert("fence.rs".to_string(), attested("d1", "ported"));
        let basis = open.basis_after(&changed(&["fence.rs"]), &changed(&["fence.rs"]));
        assert_eq!(
            basis.get("fence.rs"),
            Some(&side("d2")),
            "a statement against an older side is not this session's to keep"
        );
    }

    /// Review finding on cairn/5473: a file deleted WHILE the session is open
    /// differs from `ours` on every read. Read as a content change, it erased
    /// the statement the moment it was recorded — on the worker's reread, the
    /// page's, and the rollback's inheritance. The same deletion keeps its
    /// statement; the file coming back replaces it.
    #[test]
    fn a_deletion_attested_during_the_session_keeps_its_statement_until_the_file_returns() {
        let mut open = session("c1", "d1", &["fence.rs"]);
        open.resolution_basis
            .insert("fence.rs".to_string(), attested("d1", "ported #4372"));
        let still_deleted = open.basis_after(&changed(&["fence.rs"]), &changed(&["fence.rs"]));
        assert_eq!(
            still_deleted.get("fence.rs"),
            Some(&attested("d1", "ported #4372"))
        );
        let returned = open.basis_after(&changed(&["fence.rs"]), &changed(&[]));
        assert_eq!(returned.get("fence.rs"), Some(&side("d1")));
    }

    /// Review finding on cairn/5473: the capped page read looks up history
    /// only for the deletions it assesses, never for the paths past its cap.
    #[test]
    fn deletion_history_is_looked_up_only_for_the_paths_assessed() {
        let mut basis = ResolutionBasis::new();
        basis.insert("covered.rs".to_string(), attested("d1", "moot"));
        basis.insert("older.rs".to_string(), attested("d0", "ported"));
        let deleted = changed(&["covered.rs", "older.rs", "plain.rs", "hidden.rs"]);
        let assessed: Vec<String> = ["covered.rs", "kept.rs", "older.rs", "plain.rs"]
            .iter()
            .map(|path| path.to_string())
            .collect();
        let mut asked = Vec::new();
        let standings =
            deletion_standings(&assessed, &basis, &deleted, "fork", "d1", |from, path| {
                asked.push((from.to_string(), path.to_string()));
                Ok(Vec::new())
            });
        assert_eq!(
            asked,
            vec![
                ("d0".to_string(), "older.rs".to_string()),
                ("fork".to_string(), "plain.rs".to_string()),
            ],
            "not the covered deletion, not a path the tip has, not one past the cap"
        );
        assert_eq!(
            standings.get("covered.rs"),
            Some(&DeletionStanding::Attested)
        );
        assert!(!standings.contains_key("hidden.rs"));
        assert!(!standings.contains_key("kept.rs"));
    }

    #[test]
    fn a_stored_attestation_round_trips_and_a_row_without_one_decodes_unattested() {
        let mut basis = ResolutionBasis::new();
        basis.insert("fence.rs".to_string(), attested("d1", "ported #4372"));
        basis.insert("list.txt".to_string(), side("d1"));
        let json = encode_basis(&basis).unwrap().unwrap();
        assert_eq!(decode_basis(Some(&json)), basis);
        assert_eq!(
            json.matches("attested").count(),
            1,
            "an unattested side writes no field: {json}"
        );
        let legacy = r#"[{"path":"list.txt","base":"fork","theirs":"d1"}]"#;
        assert_eq!(
            decode_basis(Some(legacy)).get("list.txt"),
            Some(&side("d1"))
        );
    }

    fn deleted_assessment(standing: DeletionStanding) -> TipAssessment {
        let mut tip = assessment(
            vec![(
                "fence.rs",
                RestoreVerdict::NotAssessed("your branch deletes this path".to_string()),
            )],
            0,
        );
        tip.deletions.insert("fence.rs".to_string(), standing);
        tip
    }

    fn outpaced(attested: Option<&str>) -> DeletionStanding {
        DeletionStanding::Outpaced(OutpacedDeletion {
            from: if attested.is_some() { "d1" } else { "fork" }.to_string(),
            attested: attested.map(str::to_string),
            to: "d2".to_string(),
            commits: Ok(vec![IncomingCommit {
                id: "f98e1e1d".to_string(),
                subject: "fence: refuse a stale grant (#4372)".to_string(),
            }]),
        })
    }

    /// cairn/5473's endgame: the tip's only conflicting files are deletions a
    /// statement covers against these sides. That is a resolution to take, not
    /// "resolves none", and nothing about it is unproven.
    #[test]
    fn a_tip_whose_only_conflicts_are_attested_deletions_proceeds() {
        let covered = deleted_assessment(DeletionStanding::Attested);
        assert!(covered.unproven().is_empty());
        assert!(!covered.resolves_nothing());
        assert!(covered.every_path_is_resolved());
        assert_eq!(
            decide_replay(&RestoreProof::Assessed(covered), None),
            ReplayDecision::Proceed
        );
    }

    /// An uncovered deletion is refused by the commits that edited the file,
    /// never as "resolves none" (no commit to an absent file can resolve it)
    /// and never as a stale resolution (there is none).
    #[test]
    fn an_uncovered_deletion_is_refused_naming_the_base_commits_that_edited_it() {
        let mut uncovered = deleted_assessment(outpaced(None));
        uncovered.uncovered_deletions = 1;
        assert!(!uncovered.resolves_nothing());
        assert!(!uncovered.every_path_is_resolved());
        assert!(matches!(
            uncovered.unproven()[..],
            [("fence.rs", Unproven::DeletionOutpaced(_))]
        ));
        let named = refusal(&RestoreProof::Assessed(uncovered));
        assert!(
            named.contains("`f98e1e1d` fence: refuse a stale grant (#4372)"),
            "{named}"
        );
        assert!(named.contains("between `fork` and `d2`"), "{named}");
        assert!(named.contains("drop_incoming_reason"), "{named}");
        assert!(!named.contains("resolves none"), "{named}");
        assert!(!named.contains("since that resolution was made"), "{named}");

        let older = refusal(&RestoreProof::Assessed(deleted_assessment(outpaced(Some(
            "ported into auth/",
        )))));
        assert!(
            older.contains(
                "Your statement (\"ported into auth/\") covered the base's version of it at `d1`"
            ),
            "{older}"
        );
        assert!(older.contains("`f98e1e1d`"), "{older}");
    }

    /// A tip resolving nothing still says what the base did to the files it
    /// deletes, so the agent learns both requirements from one refusal.
    #[test]
    fn a_tip_resolving_nothing_also_names_its_uncovered_deletions() {
        let mut nothing = deleted_assessment(outpaced(None));
        nothing.paths.push(PathAssessment {
            path: "list.txt".to_string(),
            verdict: RestoreVerdict::Lossless,
        });
        nothing.conflicting = 2;
        nothing.unresolved = vec!["list.txt".to_string()];
        nothing.uncovered_deletions = 1;
        assert!(nothing.resolves_nothing());
        let both = refusal(&RestoreProof::Assessed(nothing));
        assert!(both.contains("resolves none"), "{both}");
        assert!(both.contains("- `list.txt`"), "{both}");
        assert!(both.contains("`f98e1e1d`"), "{both}");
    }

    #[test]
    fn a_long_list_of_incoming_commits_is_capped_and_counted() {
        let deletion = OutpacedDeletion {
            from: "fork".to_string(),
            attested: None,
            to: "d2".to_string(),
            commits: Ok((0..13)
                .map(|n| IncomingCommit {
                    id: format!("c{n:07}"),
                    subject: format!("edit {n}"),
                })
                .collect()),
        };
        let described = describe_outpaced_deletion(&deletion);
        assert!(described.contains("`c0000009` edit 9"), "{described}");
        assert!(!described.contains("`c0000010`"), "{described}");
        assert!(described.contains("and 3 more"), "{described}");
    }

    fn changed(paths: &[&str]) -> BTreeSet<String> {
        paths.iter().map(|path| path.to_string()).collect()
    }

    /// A conflicting file the agent changed while the D1 session was open was
    /// resolved against D1; one it did not touch has no resolution.
    #[test]
    fn only_a_path_whose_own_content_changed_takes_the_sessions_sides() {
        let open = session("c1", "d1", &["f.txt", "g.txt"]);
        let basis = open.basis_after(&changed(&["f.txt", "unrelated.txt"]), &changed(&[]));
        assert_eq!(basis.get("f.txt"), Some(&side("d1")));
        assert_eq!(basis.get("g.txt"), None, "g was not touched");
        assert_eq!(basis.get("unrelated.txt"), None, "not a conflicting path");
    }

    /// The review's finding: a session recorded over a resolution made against
    /// D1 must not promote that path to its own D2 because the agent committed
    /// some OTHER file. The path keeps D1 until its own content changes; the
    /// same rule covers a conflict re-detected under the same intent, whose
    /// previous session is the row being overwritten.
    #[test]
    fn a_commit_to_another_file_keeps_each_paths_earlier_basis() {
        let mut over_resolution = session("resolved", "d2", &["list.txt"]);
        over_resolution
            .resolution_basis
            .insert("list.txt".to_string(), side("d1"));
        let after_unrelated = over_resolution.basis_after(&changed(&["other.txt"]), &changed(&[]));
        assert_eq!(after_unrelated.get("list.txt"), Some(&side("d1")));
        let after_reresolving = over_resolution.basis_after(&changed(&["list.txt"]), &changed(&[]));
        assert_eq!(after_reresolving.get("list.txt"), Some(&side("d2")));
    }

    /// A path an earlier replay carried and then rolled back is no longer
    /// conflicting, and still has to be placed by the next replay.
    #[test]
    fn the_replay_places_every_resolved_path_not_only_the_current_conflicts() {
        let mut retried = session("resolved", "d2", &["g.txt"]);
        retried
            .resolution_basis
            .insert("f.txt".to_string(), side("d1"));
        retried
            .resolution_basis
            .insert("g.txt".to_string(), side("d1"));
        assert_eq!(
            retried.replay_paths(&retried.resolution_basis, ReplayScope::Carry),
            vec!["f.txt".to_string(), "g.txt".to_string()]
        );
        // A catch-up merge takes only the conflicts from the tip; `f.txt` is
        // the merge's own result there, so neither its placement nor its
        // deletion is the replay's to judge.
        assert_eq!(
            retried.replay_paths(&retried.resolution_basis, ReplayScope::CatchUpMerge),
            vec!["g.txt".to_string()]
        );
        let (_, attested) = retried.attest_deletions(
            retried.resolution_basis.clone(),
            &changed(&["f.txt"]),
            "moved",
            ReplayScope::CatchUpMerge,
        );
        assert!(attested.is_empty(), "{attested:?}");
    }

    /// An entry whose side the session's base already contains is superseded,
    /// attested or not; one whose side it does not contain — the rolled-back
    /// carry above, or a resolution against this session's own sides — stays.
    #[test]
    fn only_entries_whose_side_the_base_contains_are_superseded() {
        let mut basis = ResolutionBasis::new();
        basis.insert("config.yaml".to_string(), side("d0"));
        basis.insert("gone.rs".to_string(), attested("d0", "moved"));
        basis.insert("f.txt".to_string(), side("d1"));
        basis.insert("g.txt".to_string(), side("d2"));
        let kept = drop_superseded(basis, &changed(&["d0"]));
        assert_eq!(
            kept.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["f.txt", "g.txt"]
        );
    }

    #[test]
    fn a_stored_basis_round_trips_and_an_unreadable_one_is_empty() {
        let mut basis = ResolutionBasis::new();
        basis.insert("list.txt".to_string(), side("d1"));
        let json = encode_basis(&basis).unwrap();
        assert_eq!(decode_basis(json.as_deref()), basis);
        assert_eq!(encode_basis(&ResolutionBasis::new()).unwrap(), None);
        assert!(decode_basis(Some("not json")).is_empty());
    }

    /// A tip whose every conflicting file is still one-sided has nothing to
    /// take, and a stated reason cannot conjure a resolution. Even though each
    /// file is LOSSLESS — the incoming change lives entirely inside the
    /// conflict, which the loss invariant takes from ours — the refusal names
    /// each file and exactly what would count as resolving it.
    #[test]
    fn a_one_sided_tip_is_refused_by_name_even_with_a_stated_reason() {
        let mut one_sided = assessment(
            vec![
                ("list.txt", RestoreVerdict::Lossless),
                ("other.txt", RestoreVerdict::Lossless),
            ],
            0,
        );
        one_sided.unresolved = vec!["list.txt".to_string(), "other.txt".to_string()];
        assert!(one_sided.resolves_nothing());
        assert!(!one_sided.every_path_is_resolved());
        let proof = RestoreProof::Assessed(one_sided);
        for reason in [None, Some("trust me")] {
            let ReplayDecision::Refuse(refusal) = decide_replay(&proof, reason) else {
                panic!("nothing resolved must refuse with {reason:?}");
            };
            assert!(refusal.contains("resolves none"), "{refusal}");
            assert!(refusal.contains("`d1`"), "{refusal}");
            for path in ["list.txt", "other.txt"] {
                assert!(refusal.contains(&format!("- `{path}`")), "{refusal}");
                assert!(
                    refusal.contains(&format!("cairn:~/rebase?view=merged&file={path}")),
                    "{refusal}"
                );
            }
            assert!(
                refusal.contains("changes it on top of `c1`"),
                "the refusal says which commit qualifies: {refusal}"
            );
            assert!(
                refusal.contains("already holds both sides"),
                "and that content already holding both sides qualifies too: {refusal}"
            );
        }
    }

    /// One resolved file is a resolution to take: the request is judged by the
    /// loss invariant like any other, and the still-unresolved file is named on
    /// the page rather than refused here, because the base's declared
    /// structural merge may yet settle it when the replay carries it.
    #[test]
    fn a_partly_resolved_tip_is_judged_by_the_loss_invariant() {
        let mut partly = assessment(
            vec![
                ("list.txt", RestoreVerdict::Lossless),
                ("other.txt", RestoreVerdict::Lossless),
            ],
            0,
        );
        partly.unresolved = vec!["other.txt".to_string()];
        assert!(!partly.resolves_nothing());
        assert!(
            !partly.every_path_is_resolved(),
            "the page must not claim both sides while a file is one-sided"
        );
        assert_eq!(
            decide_replay(&RestoreProof::Assessed(partly), None),
            ReplayDecision::Proceed
        );
    }

    /// A stated reason is not a licence to skip the check: a proof that already
    /// passed does not get labelled as an accepted risk.
    #[test]
    fn a_stated_reason_on_a_clean_proof_is_still_a_plain_proceed() {
        let clean = RestoreProof::Assessed(assessment(vec![("a.rs", RestoreVerdict::Lossless)], 0));
        assert_eq!(
            decide_replay(&clean, Some("just in case")),
            ReplayDecision::Proceed
        );
    }

    mod pending_replay {
        use super::super::*;

        const STORE: &str = "/store";
        const BRANCH: &str = "agent/replay";

        async fn seeded(path: &Path) -> LocalDb {
            let db = LocalDb::open_migrated_for_test(path.to_path_buf())
                .await
                .unwrap();
            db.execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                 VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1)
                 ON CONFLICT(id) DO NOTHING",
                (),
            )
            .await
            .unwrap();
            db
        }

        async fn fresh() -> LocalDb {
            let dir = tempfile::tempdir().unwrap().keep();
            seeded(&dir.join("pending-replay.db")).await
        }

        async fn queue(
            db: &LocalDb,
            destination: &str,
            take_committed_tip: bool,
        ) -> PendingReplayRequest {
            queue_replay_request(
                db,
                "proj-1",
                Path::new(STORE),
                "main",
                destination,
                BRANCH,
                Some("tip-1"),
                take_committed_tip,
                take_committed_tip.then_some("incoming change is obsolete"),
            )
            .await
            .unwrap()
        }

        async fn pending(db: &LocalDb) -> Option<PendingReplayRequest> {
            load_pending_replay_request(db, Path::new(STORE), BRANCH)
                .await
                .unwrap()
        }

        async fn item_state(db: &LocalDb, intent_id: &str) -> (String, i64, String) {
            let intent_id = intent_id.to_string();
            db.read(move |conn| {
                let intent_id = intent_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT i.status, i.notification_sent, n.status
                             FROM jj_reconcile_items i
                             JOIN jj_reconcile_intents n ON n.id = i.intent_id
                             WHERE i.intent_id = ?1 AND i.bookmark = ?2",
                            params![intent_id, BRANCH],
                        )
                        .await?;
                    let row = rows.next().await?.expect("item exists");
                    Ok((row.text(0)?, row.opt_i64(1)?.unwrap_or(0), row.text(2)?))
                })
            })
            .await
            .unwrap()
        }

        async fn serve(db: &LocalDb, request: &PendingReplayRequest, current: &str) -> bool {
            mark_replay_request_served(db, request, current)
                .await
                .unwrap()
        }

        async fn finish_pass(db: &LocalDb, intent_id: &str, outcome: &str, at: i64) {
            db.execute(
                "UPDATE jj_reconcile_items SET status = 'completed', outcome_kind = ?3,
                   last_diagnostic = ?3, updated_at = ?4
                 WHERE intent_id = ?1 AND bookmark = ?2",
                params![intent_id, BRANCH, outcome, at],
            )
            .await
            .unwrap();
        }

        /// A queued request is durable: it reads back whole from a reopened
        /// database, onto a `completed` intent and a pending item the queue
        /// created.
        #[tokio::test]
        async fn a_queued_request_survives_reopening_the_database() {
            let path = tempfile::tempdir().unwrap().keep().join("reload.db");
            let db = seeded(&path).await;
            let queued = queue(&db, "dest-a", true).await;
            assert_eq!(
                item_state(&db, &queued.intent_id).await,
                ("pending".to_string(), 0, "completed".to_string())
            );
            drop(db);

            let reopened = seeded(&path).await;
            let loaded = pending(&reopened).await.expect("request is pending");
            assert_eq!(loaded, queued);
            assert!(loaded.take_committed_tip);
            assert_eq!(
                loaded.drop_incoming_reason.as_deref(),
                Some("incoming change is obsolete")
            );
            assert_eq!(loaded.destination_commit, "dest-a");
        }

        /// Queueing onto an intent a worker already owns writes the request
        /// without disturbing the intent or the item's pass state.
        #[tokio::test]
        async fn queueing_leaves_an_existing_intent_and_item_as_they_are() {
            let db = fresh().await;
            db.execute_script(format!(
                "INSERT INTO jj_reconcile_intents
                   (id, project_id, store_path, target_branch, destination_commit, status,
                    created_at, updated_at)
                 VALUES ('intent-run', 'proj-1', '{STORE}', 'main', 'dest-a', 'running', 1, 1);
                 INSERT INTO jj_reconcile_items
                   (intent_id, bookmark, status, notification_sent, updated_at)
                 VALUES ('intent-run', '{BRANCH}', 'graph_moved', 1, 5);"
            ))
            .await
            .unwrap();

            let queued = queue(&db, "dest-a", false).await;
            assert_eq!(queued.intent_id, "intent-run");
            assert_eq!(
                item_state(&db, "intent-run").await,
                ("graph_moved".to_string(), 1, "running".to_string())
            );
            assert_eq!(pending(&db).await, Some(queued));
        }

        /// Demand preparation refuses a `pending`/`running` intent as legacy
        /// work, so queueing never creates or reopens one: a new intent is
        /// `completed`, a completed one stays completed with its lease as it
        /// was, and a running one keeps its lease.
        #[tokio::test]
        async fn queueing_never_creates_or_reopens_a_claimable_intent() {
            let db = fresh().await;
            db.execute_script(format!(
                "INSERT INTO jj_reconcile_intents
                   (id, project_id, store_path, target_branch, destination_commit, status,
                    lease_owner, lease_expires_at, created_at, updated_at)
                 VALUES ('intent-done', 'proj-1', '{STORE}', 'main', 'dest-a', 'completed',
                         'old-owner', 50, 1, 1),
                        ('intent-held', 'proj-1', '{STORE}', 'main', 'dest-b', 'running',
                         'worker', 9999999999, 1, 1);"
            ))
            .await
            .unwrap();
            queue(&db, "dest-a", false).await;
            queue(&db, "dest-b", false).await;
            let created = queue(&db, "dest-c", false).await;

            let intents = db
                .read(|conn| {
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                "SELECT id, status, lease_owner, lease_expires_at
                                 FROM jj_reconcile_intents ORDER BY destination_commit",
                                (),
                            )
                            .await?;
                        let mut intents = Vec::new();
                        while let Some(row) = rows.next().await? {
                            intents.push((
                                row.text(0)?,
                                row.text(1)?,
                                row.opt_text(2)?,
                                row.opt_i64(3)?,
                            ));
                        }
                        Ok(intents)
                    })
                })
                .await
                .unwrap();
            assert_eq!(
                intents,
                vec![
                    (
                        "intent-done".to_string(),
                        "completed".to_string(),
                        Some("old-owner".to_string()),
                        Some(50)
                    ),
                    (
                        "intent-held".to_string(),
                        "running".to_string(),
                        Some("worker".to_string()),
                        Some(9999999999)
                    ),
                    (created.intent_id, "completed".to_string(), None, None),
                ]
            );
        }

        /// A pass that defers, a fresh commit on the branch, and a conflict
        /// re-recorded on the item all leave the request pending; only serving
        /// it consumes it, and only the exact request.
        #[tokio::test]
        async fn only_serving_the_exact_request_consumes_it() {
            let db = fresh().await;
            let queued = queue(&db, "dest-a", true).await;

            db.execute(
                "UPDATE jj_reconcile_items SET status = 'pending', observed_tip = 'tip-2',
                   last_diagnostic = 'deferred: ref lock held', attempt_count = attempt_count + 1,
                   updated_at = updated_at + 10
                 WHERE intent_id = ?1 AND bookmark = ?2",
                params![queued.intent_id.as_str(), BRANCH],
            )
            .await
            .unwrap();
            record_conflict_session(
                &db,
                &queued.intent_id,
                BRANCH,
                &ConflictDiagnostic {
                    condition: ConflictCondition::ContentConflict,
                    base: Some("base".to_string()),
                    ours: Some("tip-2".to_string()),
                    theirs: Some("dest-a".to_string()),
                    conflicted_tip: None,
                    incoming: Vec::new(),
                    ..Default::default()
                },
                &IncomingIdentity::default(),
                &ResolutionBasis::new(),
            )
            .await
            .unwrap();
            assert_eq!(pending(&db).await, Some(queued.clone()));

            assert!(
                mark_replay_request_served(&db, &queued, &queued.intent_id)
                    .await
                    .is_err(),
                "a deferred pass has no verdict to serve the request with"
            );
            assert_eq!(pending(&db).await, Some(queued.clone()));

            finish_pass(&db, &queued.intent_id, "published", 60).await;
            let mut stranger = queued.clone();
            stranger.id = "someone-else".to_string();
            assert!(!serve(&db, &stranger, &queued.intent_id).await);
            assert_eq!(pending(&db).await, Some(queued.clone()));
            assert_eq!(
                replay_request_state(&db, &queued).await.unwrap(),
                ReplayRequestState::Pending
            );

            assert!(serve(&db, &queued, &queued.intent_id).await);
            assert_eq!(pending(&db).await, None);
            assert!(!serve(&db, &queued, &queued.intent_id).await);
        }

        /// A serving pass that left no row on its intent cannot serve the
        /// request: nothing is consumed and nothing is frozen.
        #[tokio::test]
        async fn a_missing_serving_row_consumes_nothing() {
            let db = fresh().await;
            let request = queue(&db, "dest-a", false).await;
            assert!(mark_replay_request_served(&db, &request, "intent-gone")
                .await
                .is_err());
            assert_eq!(pending(&db).await, Some(request.clone()));
            assert_eq!(
                replay_request_state(&db, &request).await.unwrap(),
                ReplayRequestState::Pending
            );
        }

        /// A failed pass is a verdict too: its failure kind and diagnostic are
        /// frozen and returned after a later pass rewrites the item.
        #[tokio::test]
        async fn a_rollback_failure_is_frozen_as_the_served_result() {
            let db = fresh().await;
            let request = queue(&db, "dest-a", true).await;
            let unsafe_diagnostic =
                "rollback failed: branch left at conflicted tip; not safe to retry";
            db.execute(
                "UPDATE jj_reconcile_items SET status = 'suppressed', failure_kind = 'rollback_failed',
                   last_diagnostic = ?3, updated_at = 70
                 WHERE intent_id = ?1 AND bookmark = ?2",
                params![request.intent_id.as_str(), BRANCH, unsafe_diagnostic],
            )
            .await
            .unwrap();
            assert!(serve(&db, &request, &request.intent_id).await);

            db.execute(
                "UPDATE jj_reconcile_items SET status = 'completed', failure_kind = NULL,
                   outcome_kind = 'rebased_clean', last_diagnostic = NULL, updated_at = 99
                 WHERE intent_id = ?1 AND bookmark = ?2",
                params![request.intent_id.as_str(), BRANCH],
            )
            .await
            .unwrap();
            let ReplayRequestState::Served { pass, destination } =
                replay_request_state(&db, &request).await.unwrap()
            else {
                panic!("request is served");
            };
            assert_eq!(destination, "dest-a");
            assert_eq!(pass.item_status.as_deref(), Some("suppressed"));
            assert_eq!(pass.failure_kind.as_deref(), Some("rollback_failed"));
            assert_eq!(pass.outcome_kind, None);
            assert_eq!(pass.diagnostic.as_deref(), Some(unsafe_diagnostic));
            assert_eq!(pass.updated_at, 70);
            assert_eq!(pending(&db).await, None);
        }

        /// The first terminal verdict is frozen onto the request: a later pass
        /// over the same item, or a newer request, does not rewrite it.
        #[tokio::test]
        async fn a_served_result_survives_later_passes_and_newer_requests() {
            let db = fresh().await;
            let first = queue(&db, "dest-a", false).await;
            let retargeted_intent = {
                let other = queue(&db, "dest-b", false).await;
                finish_pass(&db, &other.intent_id, "rebased_clean", 30).await;
                assert!(serve(&db, &other, &other.intent_id).await);
                other.intent_id
            };
            // `first` was superseded by the dest-b request, served without a result.
            assert_eq!(
                replay_request_state(&db, &first).await.unwrap(),
                ReplayRequestState::Superseded
            );

            let request = queue(&db, "dest-a", true).await;
            finish_pass(&db, &retargeted_intent, "published", 40).await;
            assert!(serve(&db, &request, &retargeted_intent).await);
            let expected = ReplayRequestState::Served {
                pass: ReplayPass {
                    item_status: Some("completed".to_string()),
                    intent_status: Some("completed".to_string()),
                    outcome_kind: Some("published".to_string()),
                    failure_kind: None,
                    diagnostic: Some("published".to_string()),
                    updated_at: 40,
                },
                destination: "dest-b".to_string(),
            };
            assert_eq!(replay_request_state(&db, &request).await.unwrap(), expected);

            finish_pass(&db, &retargeted_intent, "conflicted", 90).await;
            finish_pass(&db, &request.intent_id, "conflicted", 90).await;
            assert!(!serve(&db, &request, &retargeted_intent).await);
            assert_eq!(replay_request_state(&db, &request).await.unwrap(), expected);

            let newer = queue(&db, "dest-b", false).await;
            assert_eq!(replay_request_state(&db, &request).await.unwrap(), expected);
            assert_eq!(
                replay_request_state(&db, &newer).await.unwrap(),
                ReplayRequestState::Pending
            );
            let renewed = queue(&db, "dest-a", false).await;
            assert_eq!(
                replay_request_state(&db, &request).await.unwrap(),
                ReplayRequestState::Superseded,
                "a newer request on the same row replaces the old one"
            );
            assert_eq!(pending(&db).await, Some(renewed));
        }

        /// The newest request wins, on the same item or retargeted onto a new
        /// destination, and a pass still holding the older one cannot consume it.
        #[tokio::test]
        async fn a_newer_request_survives_the_older_ones_consumption() {
            let db = fresh().await;
            let first = queue(&db, "dest-a", false).await;
            let same_item = queue(&db, "dest-a", true).await;
            assert_eq!(same_item.intent_id, first.intent_id);
            finish_pass(&db, &first.intent_id, "published", 40).await;
            assert!(!serve(&db, &first, &first.intent_id).await);
            assert_eq!(pending(&db).await, Some(same_item.clone()));

            let retargeted = queue(&db, "dest-b", false).await;
            assert_ne!(retargeted.intent_id, same_item.intent_id);
            finish_pass(&db, &retargeted.intent_id, "published", 50).await;
            assert!(!serve(&db, &same_item, &retargeted.intent_id).await);
            assert_eq!(pending(&db).await, Some(retargeted.clone()));

            assert!(serve(&db, &retargeted, &retargeted.intent_id).await);
            assert_eq!(pending(&db).await, None);
        }

        /// Requests are scoped by store and branch.
        #[tokio::test]
        async fn another_store_or_branch_does_not_see_the_request() {
            let db = fresh().await;
            queue(&db, "dest-a", false).await;
            assert!(
                load_pending_replay_request(&db, Path::new("/other-store"), BRANCH)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                load_pending_replay_request(&db, Path::new(STORE), "agent/other")
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        /// A request timestamp written before typed requests carries no
        /// identity, so it never becomes authority.
        #[tokio::test]
        async fn a_legacy_request_timestamp_is_not_authority() {
            let db = fresh().await;
            db.execute_script(format!(
                "INSERT INTO jj_reconcile_intents
                   (id, project_id, store_path, target_branch, destination_commit, status,
                    created_at, updated_at)
                 VALUES ('intent-old', 'proj-1', '{STORE}', 'main', 'dest-a', 'pending', 1, 1);
                 INSERT INTO jj_reconcile_items
                   (intent_id, bookmark, status, updated_at, replay_requested_at,
                    replay_requested_tip, replay_requested_destination)
                 VALUES ('intent-old', '{BRANCH}', 'pending', 5, 100, 'tip-1', 'dest-a');"
            ))
            .await
            .unwrap();
            assert_eq!(pending(&db).await, None);
            assert!(list_pending_replay_requests(&db).await.unwrap().is_empty());
        }

        /// Recovery lists the newest unserved request of each branch, including
        /// one under an intent the queue retirement marked `superseded`, and a
        /// served request is not listed.
        #[tokio::test]
        async fn recovery_lists_the_newest_unserved_request_per_branch_whatever_the_intent() {
            let db = fresh().await;
            let older = queue(&db, "dest-a", false).await;
            let newest = queue(&db, "dest-b", true).await;
            db.execute(
                "UPDATE jj_reconcile_intents SET status = 'superseded' WHERE id = ?1",
                params![newest.intent_id.as_str()],
            )
            .await
            .unwrap();
            let other = queue_replay_request(
                &db,
                "proj-1",
                Path::new(STORE),
                "main",
                "dest-a",
                "agent/other",
                Some("tip-o"),
                false,
                None,
            )
            .await
            .unwrap();

            let listed = list_pending_replay_requests(&db).await.unwrap();
            let mut ids: Vec<_> = listed
                .into_iter()
                .map(|listed| (listed.id, listed.request.unwrap()))
                .collect();
            ids.sort_by(|a, b| a.1.bookmark.cmp(&b.1.bookmark));
            assert_eq!(
                ids,
                vec![
                    (other.id.clone(), other.clone()),
                    (newest.id.clone(), newest.clone())
                ]
            );
            assert_ne!(older.id, newest.id, "the older request was superseded");

            finish_pass(&db, &newest.intent_id, "rebased_clean", 70).await;
            assert!(serve(&db, &newest, &newest.intent_id).await);
            let remaining = list_pending_replay_requests(&db).await.unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].id, other.id);
        }

        /// An unreadable stored resolution is an error naming the request, never
        /// a plain replay, and closing it by id freezes the reason.
        #[tokio::test]
        async fn a_malformed_resolution_refuses_and_is_closed_by_id() {
            let db = fresh().await;
            let request = queue(&db, "dest-a", true).await;
            db.execute(
                "UPDATE jj_reconcile_items SET replay_resolution = 'whatever'
                 WHERE replay_request_id = ?1",
                params![request.id.as_str()],
            )
            .await
            .unwrap();
            assert!(load_pending_replay_request(&db, Path::new(STORE), BRANCH)
                .await
                .is_err());
            let listed = list_pending_replay_requests(&db).await.unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].id, request.id);
            assert_eq!(listed[0].bookmark, BRANCH);
            let error = listed[0].request.clone().unwrap_err();
            assert!(error.contains("unknown resolution"), "{error}");

            assert!(
                close_replay_request(&db, &request.id, BRANCH, "malformed", &error)
                    .await
                    .unwrap()
            );
            assert!(
                !close_replay_request(&db, &request.id, BRANCH, "malformed", &error)
                    .await
                    .unwrap(),
                "closed once"
            );
            let ReplayRequestState::Served { pass, destination } =
                replay_request_state(&db, &request).await.unwrap()
            else {
                panic!("the closed request carries its reason");
            };
            assert_eq!(destination, "dest-a");
            assert_eq!(pass.failure_kind.as_deref(), Some("malformed"));
            assert_eq!(pass.diagnostic.as_deref(), Some(error.as_str()));
            assert!(list_pending_replay_requests(&db).await.unwrap().is_empty());
        }
    }
}
