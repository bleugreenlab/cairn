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
    JjEnv, PathAssessment, RestoreVerdict,
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
    /// The reconcile item's own progress through the requested work.
    pub(crate) item_status: Option<String>,
    /// The owning intent's lifecycle: pending, running, completed, superseded.
    pub(crate) intent_status: Option<String>,
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
    /// The reconcile item that processes an outstanding request: the one under
    /// the intent for `replay_requested_destination`, which is often not the
    /// row this session was recorded on. `None` when no request is outstanding
    /// or no such row exists yet.
    pub(crate) replay_pass: Option<ReplayPass>,
}

/// What the reconcile row that processes a replay request last recorded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReplayPass {
    pub(crate) item_status: Option<String>,
    pub(crate) intent_status: Option<String>,
    pub(crate) outcome_kind: Option<String>,
    pub(crate) failure_kind: Option<String>,
    pub(crate) diagnostic: Option<String>,
    pub(crate) updated_at: i64,
}

/// The merge base and incoming side one path was resolved against.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ResolvedSide {
    pub(crate) base: String,
    pub(crate) theirs: String,
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
        })
        .collect();
    serde_json::to_string(&rows)
        .map(Some)
        .map_err(|error| format!("encode resolution basis: {error}"))
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

    pub(crate) fn is_open(&self) -> bool {
        self.resolution_state.as_deref() == Some(ResolutionState::Open.as_str())
    }

    /// Where an outstanding replay request has got to, if there is one.
    ///
    /// Composed from what is already stored rather than modelled again: the
    /// request timestamp says an agent asked, and the row that processes the
    /// request says what its last pass did. That row is keyed by the request's
    /// destination, not by where this session was recorded — reading the
    /// session's own row is what once left a failed request reading QUEUED
    /// forever.
    pub(crate) fn replay_progress(&self) -> Option<ReplayProgress> {
        let requested_at = self.replay_requested_at?;
        let own = ReplayPass {
            item_status: self.item_status.clone(),
            intent_status: self.intent_status.clone(),
            updated_at: self.updated_at,
            ..ReplayPass::default()
        };
        let state = replay_state(self.replay_pass.as_ref().unwrap_or(&own), requested_at);
        Some(ReplayProgress {
            requested_at,
            state,
            destination: self
                .replay_requested_destination
                .clone()
                .unwrap_or_else(|| self.destination_commit.clone()),
        })
    }

    /// The basis after the branch re-resolved `changed` while this session was
    /// open: each of this session's conflicting paths among them was resolved
    /// against this session's sides, and every other path keeps what it had.
    /// Pure, so the per-path rule is provable without a store.
    pub(crate) fn basis_after(&self, changed: &BTreeSet<String>) -> ResolutionBasis {
        let mut basis = self.resolution_basis.clone();
        if let (Some(base), Some(theirs)) = (self.base.as_ref(), self.theirs.as_ref()) {
            for file in self
                .conflicting()
                .filter(|file| changed.contains(&file.path))
            {
                basis.insert(
                    file.path.clone(),
                    ResolvedSide {
                        base: base.clone(),
                        theirs: theirs.clone(),
                    },
                );
            }
        }
        basis
    }

    /// What each path of `tip` was resolved against.
    ///
    /// A conflicting path whose content changed between this session's `ours`
    /// and `tip` was re-resolved while this session was open, so against this
    /// session's sides. Any other path — including every path of a commit that
    /// only touched unrelated files — keeps the basis this session inherited.
    pub(crate) fn resolution_basis_at(
        &self,
        jj: &JjEnv,
        store: &Path,
        tip: &str,
    ) -> Result<ResolutionBasis, String> {
        let Some(ours) = self.ours.as_deref() else {
            return Ok(self.resolution_basis.clone());
        };
        let paths: Vec<String> = self.conflicting().map(|file| file.path.clone()).collect();
        let changed = crate::jj::paths_changed_between(jj, store, ours, tip, &paths)?;
        Ok(self.basis_after(&changed))
    }

    /// Every path a take-committed-tip replay has to place: the paths this
    /// session conflicts on, and every path an earlier resolution covered. A
    /// path carried by an earlier replay and then rolled back with it is no
    /// longer conflicting here, and dropping it would let the next replay
    /// conflict on it again.
    pub(crate) fn replay_paths(&self, basis: &ResolutionBasis) -> Vec<String> {
        self.conflicting()
            .map(|file| file.path.clone())
            .chain(basis.keys().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
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

/// An outstanding replay request and how far it has got.
#[derive(Debug, Clone)]
pub(crate) struct ReplayProgress {
    pub(crate) requested_at: i64,
    pub(crate) state: ReplayState,
    /// Where the request was queued: the base's head when it was asked, not
    /// necessarily the destination this session was recorded against.
    pub(crate) destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplayState {
    /// Waiting for the reconcile worker, which has not yet processed it.
    Queued,
    /// The reconcile worker has claimed the intent and is acting on it.
    Running,
    /// The intent carrying the request was replaced by one at a newer
    /// destination. Its own status no longer describes the request, so it is
    /// not reported as queued.
    Superseded,
    /// The last pass lost the ref store's `packed-refs.lock`. Infrastructure,
    /// not the branch; the worker retries it. `diagnosis` is the give-up line.
    Retrying { diagnosis: String, at: i64 },
    /// The last pass failed for another reason.
    Failed {
        kind: String,
        diagnostic: Option<String>,
        at: i64,
    },
    /// The last pass reached the branch and stopped at the mutation boundary
    /// because one of its run batches was in flight, attempting nothing. The
    /// intent stays pending for the durable sweep. `diagnostic` is what the
    /// pass recorded, which names the batch that held it then.
    Deferred { diagnostic: Option<String>, at: i64 },
    /// The last pass finished with this outcome (`conflicted`, `rebased_clean`, …).
    Processed { outcome: String, at: i64 },
    /// Nothing has processed the request and nothing is queued to: its intent
    /// finished without it. Never reported as queued.
    NotProcessed { intent_status: String },
}

/// The request's state from the row that processes it. Pure, so each state is
/// provable without a database.
///
/// `Queued` is only ever a positive fact — the intent is pending and its row
/// has recorded nothing since the request — and anything that matches no arm
/// says so rather than falling through to it.
fn replay_state(pass: &ReplayPass, requested_at: i64) -> ReplayState {
    let intent = pass.intent_status.as_deref();
    if intent == Some("superseded") {
        return ReplayState::Superseded;
    }
    if intent == Some("running") {
        return ReplayState::Running;
    }
    let at = pass.updated_at;
    if at >= requested_at {
        let ref_lock = pass
            .failure_kind
            .as_deref()
            .is_some_and(crate::jj::reconcile_failure_is_ref_lock);
        if ref_lock && intent == Some("pending") {
            let diagnosis = pass
                .diagnostic
                .as_deref()
                .and_then(crate::jj::ref_lock_diagnosis)
                .unwrap_or_else(|| "the ref store's packed-refs lock was held".to_string());
            return ReplayState::Retrying { diagnosis, at };
        }
        match pass.outcome_kind.as_deref() {
            Some("failed") => {
                return ReplayState::Failed {
                    kind: pass
                        .failure_kind
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    diagnostic: pass.diagnostic.clone(),
                    at,
                }
            }
            // A deferral attempted nothing, but it is a positive fact about
            // why nothing moved, and the one that tells the reader what to
            // end. Reported as such only while the sweep still owes the pass;
            // otherwise the intent's own lifecycle below decides.
            Some("deferred") if intent == Some("pending") => {
                return ReplayState::Deferred {
                    diagnostic: pass.diagnostic.clone(),
                    at,
                }
            }
            Some("deferred") | None => {}
            Some(outcome) => {
                return ReplayState::Processed {
                    outcome: outcome.to_string(),
                    at,
                }
            }
        }
    }
    if intent == Some("pending") {
        return ReplayState::Queued;
    }
    ReplayState::NotProcessed {
        intent_status: intent.unwrap_or("missing").to_string(),
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
    /// Whether it has moved off the session's `ours` — i.e. whether the agent
    /// has committed anything since the conflict was recorded.
    pub(crate) moved: bool,
    pub(crate) paths: Vec<PathAssessment>,
    /// Conflicting paths beyond the cap, not looked at. Always zero for an
    /// exhaustive assessment, which is the only kind the mutation boundary may
    /// decide on.
    pub(crate) truncated: usize,
}

/// Why a path's whole-file restore could not be PROVEN lossless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unproven {
    /// Positive evidence that incoming work would be discarded.
    Drops(DroppedWork),
    /// The merge could not judge this path at all.
    Unjudged(String),
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
    pub(crate) fn unproven(&self) -> Vec<(&str, Unproven)> {
        self.paths
            .iter()
            .filter_map(|assessment| {
                let reason = match &assessment.verdict {
                    RestoreVerdict::Lossless => return None,
                    RestoreVerdict::Lossy(dropped) => Unproven::Drops(dropped.clone()),
                    RestoreVerdict::NotAssessed(reason) => Unproven::Unjudged(reason.clone()),
                };
                Some((assessment.path.as_str(), reason))
            })
            .collect()
    }

    /// Whether every path assessed came back lossless. Deliberately false when
    /// the detail was truncated: an answer about 25 of 200 paths is not an
    /// answer about the session.
    pub(crate) fn every_path_is_resolved(&self) -> bool {
        self.truncated == 0
            && !self.paths.is_empty()
            && self
                .paths
                .iter()
                .all(|assessment| assessment.verdict == RestoreVerdict::Lossless)
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
) -> Option<TipAssessment> {
    assess_session_tip_within(jj, store, session, Some(MAX_ASSESSED_PATHS))
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
    /// None of this session's conflicting files has changed since the conflict
    /// was recorded, and no earlier session recorded a resolution of any of
    /// them. There is no resolution to take, so a stated reason cannot make one.
    NothingResolved { theirs: String },
}

/// The same assessment over EVERY conflicting path, for the replay guard.
///
/// The mutation restores every conflicting path whole, so the decision has to
/// cover every conflicting path. Uncapped deliberately: this runs once per
/// explicit request against an operation that is already asynchronous and
/// durable, so paying a subprocess per path is the cheapest thing in the
/// transaction — far cheaper than discovering the loss from a compiler later.
pub(crate) fn assess_session_tip_exhaustively(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
) -> RestoreProof {
    if session.is_base_drift() {
        return RestoreProof::NothingToProve;
    }
    let (Some(base), Some(theirs)) = (session.base.as_deref(), session.theirs.as_deref()) else {
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
    if !session
        .conflicting()
        .any(|file| basis.contains_key(&file.path))
    {
        return RestoreProof::NothingResolved {
            theirs: theirs.to_string(),
        };
    }
    RestoreProof::Assessed(assess_against_basis(
        jj, store, session, base, &basis, tip, None,
    ))
}

fn assess_session_tip_within(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
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
    Some(assess_against_basis(
        jj, store, session, base, &basis, tip, limit,
    ))
}

/// Assess every path the replay has to place against what it was resolved
/// against: a path the basis covers against its own recorded sides, any other
/// against the session's. Order is path order.
fn assess_against_basis(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
    session_base: &str,
    basis: &ResolutionBasis,
    tip: String,
    limit: Option<usize>,
) -> TipAssessment {
    let all = session.replay_paths(basis);
    let limit = limit.unwrap_or(all.len());
    let truncated = all.len().saturating_sub(limit);
    let assessed: Vec<String> = all.into_iter().take(limit).collect();
    let session_side = ResolvedSide {
        base: session_base.to_string(),
        theirs: session.theirs.clone().unwrap_or_default(),
    };
    let mut groups: BTreeMap<&ResolvedSide, Vec<String>> = BTreeMap::new();
    for path in &assessed {
        groups
            .entry(basis.get(path).unwrap_or(&session_side))
            .or_default()
            .push(path.clone());
    }
    let mut verdicts: std::collections::HashMap<String, PathAssessment> = groups
        .into_iter()
        .flat_map(|(side, paths)| assess_paths(jj, store, &side.base, &tip, &side.theirs, &paths))
        .map(|assessment| (assessment.path.clone(), assessment))
        .collect();

    TipAssessment {
        moved: session.ours.as_deref() != Some(tip.as_str()),
        paths: assessed
            .iter()
            .filter_map(|path| verdicts.remove(path))
            .collect(),
        truncated,
        tip,
    }
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
pub(crate) fn decide_replay(proof: &RestoreProof, stated_reason: Option<&str>) -> ReplayDecision {
    let blocker = match proof {
        RestoreProof::NothingToProve => return ReplayDecision::Proceed,
        RestoreProof::NothingResolved { theirs } => {
            return ReplayDecision::Refuse(format!(
                "Refusing this replay: you have committed nothing since this conflict was recorded \
                 against `{theirs}`, so there is no resolution to take. \
                 `resolution:\"take-committed-tip\"` lands the conflicting files as your branch \
                 last committed them, which here is simply your side of the conflict.\n\nWrite the \
                 merged files, commit them, and request again — `cairn:~/rebase?view=merged` hands \
                 you each complete merged file. To move the branch without taking content from \
                 either side, request a plain `{{action:\"replay\"}}` instead."
            ))
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
            Unproven::Unjudged(_) => None,
        })
        .collect();
    if !drops.is_empty() {
        out.push_str("Incoming work that WOULD be dropped:\n\n");
        for (path, dropped) in &drops {
            out.push_str(&format!(
                "- `{path}` — {} hunk(s), {} line(s). Read \
                 `cairn:~/rebase?view=merged&file={path}`.\n",
                dropped.hunks, dropped.added_lines
            ));
        }
        out.push_str(
            "\nEach of those reads gives you the COMPLETE merged file: your resolution kept inside \
             the conflicting region, the incoming hunks carried everywhere else, and no conflict \
             markers. Commit it and request the replay again — it is accepted then, because your \
             tip genuinely contains both sides and the whole-file restore is exactly right.\n\n",
        );
    }

    let unjudged: Vec<(&str, &str)> = unproven
        .iter()
        .filter_map(|(path, reason)| match reason {
            Unproven::Unjudged(reason) => Some((*path, reason.as_str())),
            Unproven::Drops(_) => None,
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

/// Persist the diagnostic captured before the rollback onto the reconcile item
/// that already exists for this branch, and replace its file inventory.
///
/// Called immediately after [`super::base_advance::persist_reconcile_item`] has
/// written the item row, so the UPDATE always has a row to land on. A conflict
/// re-detected at the same destination overwrites in place rather than
/// accumulating: the coordinates are the same merge, so there is one session.
///
/// `basis` is what this session inherits; see [`inherited_basis`]. The caller
/// computes it from the session open BEFORE this write, which may be the very
/// row being overwritten (a conflict re-detected under the same intent).
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
           -- nobody is waiting on.
           replay_requested_at = NULL,
           replay_requested_tip = NULL,
           replay_requested_destination = NULL,
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

/// The resolution basis a newly recorded session inherits from the session open
/// before it.
///
/// The new session's `ours` is the tip the rebase just failed with. Each of the
/// previous session's conflicting paths whose content changed since that
/// session's `ours` was re-resolved against that session's sides; every other
/// path keeps what the previous session already knew. A store that cannot say
/// which paths changed keeps the previous basis unchanged, which can only make
/// a later replay refuse, never lose.
pub(crate) fn inherited_basis(
    jj: &JjEnv,
    store: &Path,
    previous: Option<&ConflictSession>,
    new_ours: Option<&str>,
) -> ResolutionBasis {
    let (Some(previous), Some(new_ours)) = (previous, new_ours) else {
        return previous
            .map(|previous| previous.resolution_basis.clone())
            .unwrap_or_default();
    };
    previous
        .resolution_basis_at(jj, store, new_ours)
        .unwrap_or_else(|error| {
            log::warn!(
                "conflict session for `{}`: could not tell which files changed since `{}` ({error}); \
                 keeping the earlier resolution basis",
                previous.bookmark,
                previous.ours.as_deref().unwrap_or("?")
            );
            previous.resolution_basis.clone()
        })
}

/// Record that an agent asked for a take-committed-tip or plain replay: when,
/// at which tip, and onto which destination. Advisory: a failure here must not
/// fail a request the reconcile worker has already accepted, except that the
/// worker reads the tip back and refuses a branch that has moved since.
pub(crate) async fn record_replay_request(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
    tip: Option<&str>,
    destination: &str,
) {
    if let Err(error) = db
        .execute(
            "UPDATE jj_reconcile_items SET replay_requested_at = ?3, replay_requested_tip = ?4,
               replay_requested_destination = ?5
             WHERE intent_id = ?1 AND bookmark = ?2",
            // Unix seconds, matching every other timestamp on this table so the
            // resource can render it with the same `clock::stamp`.
            params![
                intent_id,
                bookmark,
                chrono::Utc::now().timestamp(),
                tip,
                destination
            ],
        )
        .await
    {
        log::warn!("replay request for `{bookmark}`: could not record the request: {error}");
    }
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
     i.resolution_state, i.updated_at, i.replay_requested_at, i.status, n.status, \
     i.resolution_basis_json, i.replay_requested_tip, i.replay_requested_destination";

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
                    item_status: row.opt_text(20)?,
                    intent_status: row.opt_text(21)?,
                    files: Vec::new(),
                    resolution_basis: decode_basis(row.opt_text(22)?.as_deref()),
                    replay_requested_tip: row.opt_text(23)?,
                    replay_requested_destination: row.opt_text(24)?,
                    replay_pass: None,
                }))
            })
        })
        .await
        .map_err(|error| format!("load conflict session: {error}"))?;

    if let Some(session) = session.as_mut() {
        session.files = load_session_files(db, &session.intent_id, &session.bookmark).await?;
        if session.replay_requested_at.is_some() {
            let destination = session
                .replay_requested_destination
                .clone()
                .unwrap_or_else(|| session.destination_commit.clone());
            session.replay_pass =
                load_reconcile_pass(db, &session.bookmark, &session.store_path, &destination)
                    .await?;
        }
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
mod tests {
    use super::*;

    fn assessment(verdicts: Vec<(&str, RestoreVerdict)>, truncated: usize) -> TipAssessment {
        TipAssessment {
            tip: "tip".to_string(),
            moved: true,
            truncated,
            paths: verdicts
                .into_iter()
                .map(|(path, verdict)| PathAssessment {
                    path: path.to_string(),
                    verdict,
                })
                .collect(),
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
            item_status: None,
            intent_status: None,
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
            replay_pass: None,
        }
    }

    fn side(theirs: &str) -> ResolvedSide {
        ResolvedSide {
            base: "fork".to_string(),
            theirs: theirs.to_string(),
        }
    }

    fn changed(paths: &[&str]) -> BTreeSet<String> {
        paths.iter().map(|path| path.to_string()).collect()
    }

    /// A conflicting file the agent changed while the D1 session was open was
    /// resolved against D1; one it did not touch has no resolution.
    #[test]
    fn only_a_path_whose_own_content_changed_takes_the_sessions_sides() {
        let open = session("c1", "d1", &["f.txt", "g.txt"]);
        let basis = open.basis_after(&changed(&["f.txt", "unrelated.txt"]));
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
        let after_unrelated = over_resolution.basis_after(&changed(&["other.txt"]));
        assert_eq!(after_unrelated.get("list.txt"), Some(&side("d1")));
        let after_reresolving = over_resolution.basis_after(&changed(&["list.txt"]));
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
            retried.replay_paths(&retried.resolution_basis),
            vec!["f.txt".to_string(), "g.txt".to_string()]
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

    /// Nothing committed since the conflict means nothing to take, and a stated
    /// reason cannot conjure a resolution.
    #[test]
    fn a_tip_with_nothing_resolved_is_refused_even_with_a_stated_reason() {
        let proof = RestoreProof::NothingResolved {
            theirs: "d1".to_string(),
        };
        for reason in [None, Some("trust me")] {
            let ReplayDecision::Refuse(refusal) = decide_replay(&proof, reason) else {
                panic!("nothing resolved must refuse with {reason:?}");
            };
            assert!(refusal.contains("committed nothing"), "{refusal}");
            assert!(refusal.contains("`d1`"), "{refusal}");
        }
    }

    /// The intent a session was recorded under can be superseded while the
    /// request it carries is still outstanding; that is not "queued", and the
    /// destination named is where the request went.
    #[test]
    fn a_superseded_intent_is_not_reported_as_queued_onto_its_old_destination() {
        let mut asked = session("resolved", "d1", &["list.txt"]);
        asked.replay_requested_at = Some(1);
        asked.replay_requested_destination = Some("d2".to_string());
        asked.intent_status = Some("superseded".to_string());
        let progress = asked.replay_progress().expect("a request is outstanding");
        assert_eq!(progress.state, ReplayState::Superseded);
        assert_eq!(progress.destination, "d2");

        asked.intent_status = Some("pending".to_string());
        assert_eq!(asked.replay_progress().unwrap().state, ReplayState::Queued);
        asked.replay_requested_destination = None;
        assert_eq!(asked.replay_progress().unwrap().destination, "d1");
    }

    fn pass(intent: &str, outcome: Option<&str>, kind: Option<&str>, at: i64) -> ReplayPass {
        ReplayPass {
            item_status: Some("pending".to_string()),
            intent_status: Some(intent.to_string()),
            outcome_kind: outcome.map(str::to_string),
            failure_kind: kind.map(str::to_string),
            diagnostic: kind.map(|_| {
                "jj rebase failed: The lock for the packed-ref file could not be obtained\n\
                 packed-refs lock: contention — held by live process 77 (git) after 30s"
                    .to_string()
            }),
            updated_at: at,
        }
    }

    /// cairn/5002's shape: the request is stamped, the intent that carried it
    /// finished, and its row recorded nothing since. Nothing will process it,
    /// so it must never read as queued.
    #[test]
    fn a_request_no_pass_processed_is_never_reported_as_queued() {
        let finished = pass("completed", Some("conflicted"), None, 50);
        assert_eq!(
            replay_state(&finished, 100),
            ReplayState::NotProcessed {
                intent_status: "completed".to_string()
            }
        );
        assert_eq!(
            replay_state(&pass("pending", None, None, 50), 100),
            ReplayState::Queued,
            "queued is the positive fact of a pending intent with nothing recorded since"
        );
    }

    /// A pass that stopped at the mutation boundary is DEFERRED, carrying the
    /// diagnostic that names the batch, never folded into QUEUED: queued reads
    /// as "about to happen" while the branch cannot move until a batch ends.
    #[test]
    fn a_pass_deferred_at_the_mutation_boundary_reads_as_deferred() {
        let mut deferred = pass("pending", Some("deferred"), None, 120);
        deferred.diagnostic = Some("deferred at the mutation boundary: run batch `r1`".into());
        assert_eq!(
            replay_state(&deferred, 100),
            ReplayState::Deferred {
                diagnostic: Some("deferred at the mutation boundary: run batch `r1`".into()),
                at: 120
            }
        );
        assert_eq!(
            replay_state(&pass("pending", Some("deferred"), None, 50), 100),
            ReplayState::Queued,
            "a deferral from before the request says nothing about the request"
        );
        assert_eq!(
            replay_state(&pass("running", Some("deferred"), None, 120), 100),
            ReplayState::Running,
            "a pass under way now outranks the deferral it is retrying"
        );
    }

    #[test]
    fn a_pass_that_lost_the_ref_lock_reads_as_retrying_with_its_diagnosis() {
        let locked = pass(
            "pending",
            Some("failed"),
            Some(crate::jj::REF_LOCK_CONTENTION),
            120,
        );
        assert_eq!(
            replay_state(&locked, 100),
            ReplayState::Retrying {
                diagnosis: "packed-refs lock: contention — held by live process 77 (git) after 30s"
                    .to_string(),
                at: 120
            }
        );
        // A deferral behind another branch's lock carries the same kind.
        let deferred = pass(
            "pending",
            Some("deferred"),
            Some(crate::jj::REF_LOCK_STALE),
            120,
        );
        assert!(matches!(
            replay_state(&deferred, 100),
            ReplayState::Retrying { .. }
        ));
    }

    #[test]
    fn every_processed_outcome_is_reported_as_what_it_was() {
        let mut failed = pass("pending", Some("failed"), None, 120);
        failed.failure_kind = Some("transient_process".to_string());
        failed.diagnostic = Some("jj exited 1".to_string());
        assert_eq!(
            replay_state(&failed, 100),
            ReplayState::Failed {
                kind: "transient_process".to_string(),
                diagnostic: Some("jj exited 1".to_string()),
                at: 120
            }
        );
        assert_eq!(
            replay_state(&pass("completed", Some("conflicted"), None, 100), 100),
            ReplayState::Processed {
                outcome: "conflicted".to_string(),
                at: 100
            },
            "a pass in the request's own second counts as processing it"
        );
        assert_eq!(
            replay_state(&pass("running", Some("failed"), None, 120), 100),
            ReplayState::Running,
            "a running intent outranks what its last pass recorded"
        );
    }

    /// The request's own row, not the session's, decides: a session recorded
    /// under a superseded intent whose request is queued at a newer
    /// destination is queued.
    #[test]
    fn the_request_row_outranks_the_session_row() {
        let mut asked = session("resolved", "d1", &["list.txt"]);
        asked.replay_requested_at = Some(100);
        asked.intent_status = Some("superseded".to_string());
        asked.replay_pass = Some(pass("pending", None, None, 10));
        assert_eq!(asked.replay_progress().unwrap().state, ReplayState::Queued);
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
}
