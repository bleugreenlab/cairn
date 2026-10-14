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
    /// intent's lifecycle says whether the worker has picked the work up, and
    /// the request timestamp says an agent asked at all. Only the second was
    /// missing, which is why this is a surfacing problem and not a data one.
    pub(crate) fn replay_progress(&self) -> Option<ReplayProgress> {
        let requested_at = self.replay_requested_at?;
        let running = self.intent_status.as_deref() == Some("running")
            || self.item_status.as_deref() == Some("graph_moved");
        Some(ReplayProgress {
            requested_at,
            running,
        })
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
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplayProgress {
    pub(crate) requested_at: i64,
    /// The reconcile worker has claimed the intent and is acting on it.
    pub(crate) running: bool,
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
    RestoreProof::Assessed(assess_against_tip(
        jj, store, session, base, theirs, tip, None,
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
    let (base, theirs) = (session.base.as_deref()?, session.theirs.as_deref()?);
    let tip = crate::jj::bookmark_commit(jj, store, &session.bookmark)?;
    Some(assess_against_tip(
        jj, store, session, base, theirs, tip, limit,
    ))
}

#[allow(clippy::too_many_arguments)]
fn assess_against_tip(
    jj: &JjEnv,
    store: &Path,
    session: &ConflictSession,
    base: &str,
    theirs: &str,
    tip: String,
    limit: Option<usize>,
) -> TipAssessment {
    let all: Vec<String> = session
        .conflicting()
        .map(|file| file.path.clone())
        .collect();
    let limit = limit.unwrap_or(all.len());
    let truncated = all.len().saturating_sub(limit);
    let assessed: Vec<String> = all.into_iter().take(limit).collect();

    TipAssessment {
        moved: session.ours.as_deref() != Some(tip.as_str()),
        paths: assess_paths(jj, store, base, &tip, theirs, &assessed),
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
/// refuses unless the requester states a reason.
pub(crate) fn decide_replay(proof: &RestoreProof, stated_reason: Option<&str>) -> ReplayDecision {
    let blocker = match proof {
        RestoreProof::NothingToProve => return ReplayDecision::Proceed,
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
pub(crate) async fn record_conflict_session(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
    diagnostic: &ConflictDiagnostic,
    incoming: &IncomingIdentity,
) -> Result<(), String> {
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
           replay_requested_at = NULL
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
     i.resolution_state, i.updated_at, i.replay_requested_at, i.status, n.status";

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
