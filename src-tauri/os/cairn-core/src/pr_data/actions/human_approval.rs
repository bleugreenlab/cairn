//! The provably-human gate on a base merge: the runner leg of the phone
//! approval ceremony.
//!
//! When a project's human base-merge policy is enabled, a merge into the base
//! branch is asked of one selected phone and lands only on that phone's signed
//! approval of exactly the act it showed. The call blocks while it waits:
//!
//! 1. **Ask.** Resolve the selected enrollment to its installation, refuse
//!    unless every gate check passed on the source tip, build a
//!    `base_merge_v2` intent from live state, issue the challenge, and poll
//!    until the phone answers or a newer request for the same merge
//!    supersedes it. The request has no deadline (tenet #1385).
//! 2. **Verify.** Verify the service's signed Human assertion here, before
//!    anything is consumed.
//! 3. **Revalidate.** Observe every bound fact again and recompute the digest.
//!    Any difference reports the approval stale and ends the attempt unclaimed.
//! 4. **Claim**, then **execute**: the landing world makes a final local
//!    comparison under the store lock and mutates only if nothing moved.
//! 5. **Prove, then complete.** `succeeded` is reported only once the landed
//!    commit is proven to be the approved shape; an unknown outcome is
//!    `indeterminate` and left to [`reconcile_approved_merges`].
//!
//! No store lock is held across any network await this module makes: every
//! approvals-service call and the wait happen between the landing world's own
//! lock scopes. The ceremony's journal is `merge_attempt_events`, one
//! append-only row per transition after the gate decision.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use cairn_common::approvals::{
    derive_commit_subject, validate_display_text, ApprovalIntent, ApprovalMergeAction,
    ApprovalMergeStrategy, ApprovalOutcome, ApprovalProviderFlag, BaseMergeIntentV2,
};
use cairn_db::models::HumanBaseMergePolicy;

use crate::approvals_client::{
    digest_hex, ApprovalAdvisory, ApprovalCheckCounts, ApprovalsClient, ApprovalsError,
    AssertionExpectation, ClaimOutcome, CompletionOutcome, EnrollmentState, InvalidationOutcome,
    ReportOutcome,
};
use crate::execution::checks_status::{
    lane_existence, LaneExistence, NodeCheckState, NodeCheckStatus, WaveCadence,
};
use crate::storage::LocalDb;

use super::merge::{journal_merge_attempt_event, JournalEntry, MergeAttemptJournal};

/// How often the runner asks whether the phone has answered.
pub(super) const POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_TITLE_BYTES: usize = 200;
const MAX_SUMMARY_BYTES: usize = 1000;

/// The sentence a merge returns when the selected phone cannot be asked.
pub(super) fn phone_unavailable_refusal(why: &str) -> String {
    format!(
        "Refusing to merge into the project base branch: the approval phone selected for this \
         project {why}. Choose a phone in project settings."
    )
}

// ─── What the runner observes ────────────────────────────────────────────────────────

/// Whether the project's gate checks passed on the source tip, and the
/// advisory counts the phone shows beside the act.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CheckGate {
    pub counts: ApprovalCheckCounts,
    /// Why the gate is not satisfied; `None` when every gate lane passed.
    pub blocking: Option<String>,
}

/// Judge the check ledger for `source_tip`. `require_checks` is a signed fact
/// on the phone, so it is enforced here, on the executing path: every
/// `gate`-policy lane must hold a passing verdict on exactly this tip. An
/// unreadable ledger blocks.
///
/// A verdict is what [`lane_existence`] calls [`LaneExistence::Complete`], the
/// classification every other merge gate and checks surface uses, so a script
/// check's exit-zero green counts and a runner's green its own result disowns
/// (an empty selection, a cut-short enumeration, a declared unverified run)
/// does not. The advisory counts follow the same rule: a lane that ended
/// without a verdict is counted in neither column.
pub(super) fn gate_checks(statuses: Option<&[NodeCheckStatus]>, source_tip: &str) -> CheckGate {
    let Some(statuses) = statuses else {
        return CheckGate {
            counts: ApprovalCheckCounts::default(),
            blocking: Some("the check ledger could not be read".to_string()),
        };
    };
    let mut counts = ApprovalCheckCounts::default();
    let mut blocking = None;
    for lane in statuses {
        let on_tip = lane
            .verified_on
            .as_ref()
            .is_none_or(|verified| verified.head == source_tip);
        let existence = lane_existence(lane, WaveCadence::default());
        let green = match existence {
            Some(LaneExistence::Complete) => Some(lane.state != NodeCheckState::Failed),
            _ => None,
        };
        match (green, lane.state) {
            (Some(true), _) if on_tip => counts.passed += 1,
            (Some(false), _) => counts.failed += 1,
            (None, NodeCheckState::Running | NodeCheckState::Pending) => counts.pending += 1,
            _ => {}
        }
        if lane.policy != "gate" || blocking.is_some() || (green == Some(true) && on_tip) {
            continue;
        }
        let why = match (green, &existence) {
            (Some(true), _) => "it passed only on an earlier commit".to_string(),
            (Some(false), _) => "it failed".to_string(),
            (None, Some(existence)) => format!("it has no verdict: {}", existence.describe()),
            (None, None) => "it has no verdict for this head".to_string(),
        };
        blocking = Some(format!(
            "the gate check `{}` has not passed on {} ({why})",
            lane.name,
            short(source_tip)
        ));
    }
    CheckGate { counts, blocking }
}

fn short(oid: &str) -> &str {
    &oid[..oid.len().min(12)]
}

/// Every fact a base-merge intent binds, read from the provider and Git.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Observation {
    pub repository_id: String,
    pub repository_name: String,
    pub target_ref: String,
    pub target_oid: String,
    pub source_ref: String,
    pub source_oid: String,
    /// The source tip's full commit message; the subject is derived from it.
    pub source_message: String,
    pub source_commit_count: u32,
    pub merge_base_oid: Option<String>,
    pub candidate_oid: Option<String>,
    pub result_tree_oid: String,
    pub strategy: ApprovalMergeStrategy,
    /// Whether the landing deletes the source branch after merging.
    pub deletes_branch: bool,
    pub checks: CheckGate,
}

/// Who is asking, for what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IntentContext {
    pub installation_id: String,
    pub project_id: String,
    /// The merge attempt's id.
    pub merge_plan_id: String,
    /// The merging job's id.
    pub initiating_request_id: String,
    pub audience: String,
}

/// Map a landing method onto the closed strategy vocabulary. `via_github`
/// matters because the store fold's `merge` is a rebase onto the base and a
/// fast-forward, not a merge commit, and the phone must not be told otherwise.
pub(super) fn approval_strategy(
    method: &str,
    via_github: bool,
) -> Result<ApprovalMergeStrategy, String> {
    match (method, via_github) {
        ("squash", _) => Ok(ApprovalMergeStrategy::Squash),
        ("merge", true) => Ok(ApprovalMergeStrategy::Merge),
        ("merge", false) | ("rebase", _) => Ok(ApprovalMergeStrategy::Rebase),
        (other, _) => Err(format!(
            "the landing method `{other}` cannot be described to the approval phone"
        )),
    }
}

/// The intent the phone signs, from live observation only.
pub(super) fn build_intent(
    observed: &Observation,
    context: &IntentContext,
) -> Result<BaseMergeIntentV2, String> {
    if observed.strategy == ApprovalMergeStrategy::Rebase {
        // A rebase lands every source commit rewritten, and proving that chain
        // against the approval is not built; refusing is the only honest
        // answer until it is.
        return Err(
            "Rebase merges can't be approved from a phone yet; choose squash or merge.".to_string(),
        );
    }
    let mut provider_flags = vec![ApprovalProviderFlag::RequireChecks];
    if observed.deletes_branch {
        provider_flags.insert(0, ApprovalProviderFlag::DeleteBranch);
    }
    let intent = BaseMergeIntentV2 {
        installation_id: context.installation_id.clone(),
        project_id: context.project_id.clone(),
        repository_id: observed.repository_id.clone(),
        repository_name: observed.repository_name.clone(),
        target_ref: observed.target_ref.clone(),
        target_oid: observed.target_oid.clone(),
        source_ref: observed.source_ref.clone(),
        source_oid: observed.source_oid.clone(),
        source_commit_subject: derive_commit_subject(&observed.source_message),
        source_commit_count: observed.source_commit_count,
        merge_base_oid: observed.merge_base_oid.clone(),
        candidate_oid: observed.candidate_oid.clone(),
        merge_strategy: observed.strategy,
        provider_flags,
        action: ApprovalMergeAction::MergePullRequest,
        policy_id: HumanBaseMergePolicy::POLICY_ID.to_string(),
        policy_version: 1,
        merge_plan_id: context.merge_plan_id.clone(),
        initiating_request_id: context.initiating_request_id.clone(),
        result_commit_oid: None,
        result_tree_oid: Some(observed.result_tree_oid.clone()),
        audience: context.audience.clone(),
    };
    intent
        .validate()
        .map_err(|error| format!("the merge cannot be described to the phone: {error}"))?;
    Ok(intent)
}

pub(super) fn intent_digest(intent: &BaseMergeIntentV2) -> Result<[u8; 32], String> {
    ApprovalIntent::BaseMergeV2(intent.clone())
        .digest_sha256()
        .map_err(|error| error.to_string())
}

/// Which bound fields differ between two observations, for the journal.
fn changed_fields(approved: &BaseMergeIntentV2, observed: &BaseMergeIntentV2) -> Vec<&'static str> {
    let mut changed = Vec::new();
    let mut differs = |name: &'static str, same: bool| {
        if !same {
            changed.push(name);
        }
    };
    differs(
        "repository_id",
        approved.repository_id == observed.repository_id,
    );
    differs(
        "repository_name",
        approved.repository_name == observed.repository_name,
    );
    differs("target_ref", approved.target_ref == observed.target_ref);
    differs("target_oid", approved.target_oid == observed.target_oid);
    differs("source_ref", approved.source_ref == observed.source_ref);
    differs("source_oid", approved.source_oid == observed.source_oid);
    differs(
        "source_commit_subject",
        approved.source_commit_subject == observed.source_commit_subject,
    );
    differs(
        "source_commit_count",
        approved.source_commit_count == observed.source_commit_count,
    );
    differs(
        "merge_base_oid",
        approved.merge_base_oid == observed.merge_base_oid,
    );
    differs(
        "candidate_oid",
        approved.candidate_oid == observed.candidate_oid,
    );
    differs(
        "result_tree_oid",
        approved.result_tree_oid == observed.result_tree_oid,
    );
    differs(
        "merge_strategy",
        approved.merge_strategy == observed.merge_strategy,
    );
    differs(
        "provider_flags",
        approved.provider_flags == observed.provider_flags,
    );
    changed
}

/// The display-only context the phone shows beside the signed act. Text the
/// display rule refuses is sent as `null`, never cleaned up into something the
/// author did not write.
pub(super) fn advisory(
    title: Option<&str>,
    description: Option<&str>,
    counts: ApprovalCheckCounts,
) -> ApprovalAdvisory {
    let title = title
        .filter(|text| validate_display_text("title", text, MAX_TITLE_BYTES, false).is_ok())
        .map(str::to_string);
    let summary = description
        .map(|text| {
            let mut end = text.len().min(MAX_SUMMARY_BYTES);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            &text[..end]
        })
        .filter(|text| validate_display_text("summary", text, MAX_SUMMARY_BYTES, true).is_ok())
        .map(str::to_string);
    ApprovalAdvisory {
        pull_request_title: title,
        pull_request_summary: summary,
        checks: Some(counts),
    }
}

// ─── The landing the ceremony gates ─────────────────────────────────────────────────

/// What a landing produced when the ceremony let it mutate.
#[derive(Debug)]
pub(super) enum Execution<L> {
    /// The final locked comparison found a bound ref or the judged tree moved.
    /// Nothing was mutated.
    BoundStateChanged { observed: serde_json::Value },
    /// The provider definitively refused; nothing landed.
    Refused(String),
    /// Whether anything landed is unknown. `approved_head` is the head the
    /// mutation was bound to, when it got that far, so reconciliation can
    /// prove a merge it never saw land.
    Unknown {
        reason: String,
        approved_head: Option<String>,
    },
    /// The provider reports the merge landed as `commit`.
    Landed {
        commit: String,
        /// The head that was merged: the source tip, or the bound head.
        approved_head: String,
        landing: L,
    },
}

/// A landed commit's shape, as the provider or the store reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LandedShape {
    pub parents: Vec<String>,
    pub tree: String,
}

/// The landing a ceremony gates: observation, the locked mutation, and the
/// read that proves what landed. Implemented by the default-branch landing;
/// tests substitute a scripted one.
#[allow(async_fn_in_trait)]
pub(super) trait LandingWorld {
    type Landing;
    /// Build the prospect and read every bound fact. Takes the store lock only
    /// to build the prospect, never across a network read.
    async fn observe(&mut self) -> Result<Observation, String>;
    /// Under the store lock, compare the local state with `approved`; mutate
    /// only if nothing moved. Never holds the lock across a network call.
    async fn execute(&mut self, approved: &Observation) -> Execution<Self::Landing>;
    /// Read a landed commit's parents and tree.
    async fn landed_shape(&self, commit: &str) -> Result<LandedShape, String>;
    /// `github` or `store`.
    fn route(&self) -> &'static str;
}

/// What a landed commit's shape says about the approval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Proof {
    /// It is exactly the approved act.
    Proven,
    /// It is provably not the approved act.
    Mismatch(String),
    /// What it would be compared against is unknown, so it proves nothing
    /// either way. Never read as a mismatch.
    CannotProve(String),
}

/// Prove a landed commit is the shape the approval named. `approved_head` is
/// the head that was merged, when known.
pub(super) fn prove_landing(
    intent: &BaseMergeIntentV2,
    approved_head: Option<&str>,
    shape: &LandedShape,
) -> Proof {
    let expected_parents = match intent.merge_strategy {
        ApprovalMergeStrategy::Squash => vec![intent.target_oid.clone()],
        ApprovalMergeStrategy::Merge => match approved_head {
            Some(head) => vec![intent.target_oid.clone(), head.to_string()],
            None => {
                return Proof::CannotProve(
                    "the merged head is not recorded, so the merge cannot be proven".into(),
                )
            }
        },
        ApprovalMergeStrategy::Rebase => {
            return Proof::CannotProve("a rebase landing is never approved".into())
        }
    };
    let tree = intent.result_tree_oid.as_deref().unwrap_or_default();
    if shape.tree != tree {
        return Proof::Mismatch(format!(
            "the landed tree is {}, not the approved {}",
            short(&shape.tree),
            short(tree)
        ));
    }
    if shape.parents != expected_parents {
        return Proof::Mismatch(format!(
            "the landed commit's parents are {:?}, not the approved {:?}",
            shape.parents, expected_parents
        ));
    }
    Proof::Proven
}

// ─── The journal ──────────────────────────────────────────────────────────────────────

/// The state name an outcome is journaled under.
pub(super) fn outcome_state(outcome: ApprovalOutcome) -> String {
    serde_json::to_value(outcome)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "failed".to_string())
}

/// One merge attempt's journal after its gate decision (sequence 0).
pub(super) struct AttemptJournal<'a> {
    db: &'a LocalDb,
    attempt: MergeAttemptJournal<'a>,
    next: AtomicI64,
}

impl<'a> AttemptJournal<'a> {
    pub fn new(db: &'a LocalDb, attempt: MergeAttemptJournal<'a>, next_sequence: i64) -> Self {
        Self {
            db,
            attempt,
            next: AtomicI64::new(next_sequence),
        }
    }

    pub async fn append(
        &self,
        outcome: ApprovalOutcome,
        entry: JournalEntry,
    ) -> Result<(), String> {
        let sequence = self.next.fetch_add(1, Ordering::SeqCst);
        journal_merge_attempt_event(
            self.db,
            &self.attempt,
            sequence,
            &outcome_state(outcome),
            &entry,
        )
        .await
    }
}

// ─── Time ──────────────────────────────────────────────────────────────────────────

/// The ceremony's clock, so a test can wait five minutes instantly.
pub(super) trait Clock: Send + Sync {
    fn now(&self) -> i64;
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

pub(super) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

// ─── In-flight attempts ────────────────────────────────────────────────────────────────

fn in_flight() -> &'static Mutex<HashSet<String>> {
    static IN_FLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    IN_FLIGHT.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Marks an attempt as running in this process, so reconciliation leaves it
/// to the call that is still driving it.
struct InFlight(String);

impl InFlight {
    fn enter(attempt_id: &str) -> Self {
        if let Ok(mut set) = in_flight().lock() {
            set.insert(attempt_id.to_string());
        }
        Self(attempt_id.to_string())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if let Ok(mut set) = in_flight().lock() {
            set.remove(&self.0);
        }
    }
}

fn is_in_flight(attempt_id: &str) -> bool {
    in_flight()
        .lock()
        .map(|set| set.contains(attempt_id))
        .unwrap_or(true)
}

// ─── The ceremony ────────────────────────────────────────────────────────────────────

pub(super) struct Ceremony<'a> {
    pub client: &'a ApprovalsClient<'a>,
    pub journal: &'a AttemptJournal<'a>,
    pub clock: &'a dyn Clock,
    pub enrollment_id: &'a str,
    pub project_id: &'a str,
    pub attempt_id: &'a str,
    pub job_id: &'a str,
    pub title: Option<String>,
    pub description: Option<String>,
}

/// What an approved landing produced.
#[derive(Debug)]
pub(super) struct ApprovedLanding<L> {
    pub landing: L,
    /// Whether the landed commit was proven to be the approved shape. When it
    /// was not readable, the approvals record is `indeterminate` until
    /// reconciliation proves it.
    pub proven: bool,
}

/// Identifiers the claim and its reports reuse on any replay.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionIds {
    claim_id: String,
    report_id: String,
    reconcile_report_id: String,
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn detail_json(value: serde_json::Value) -> Option<String> {
    Some(value.to_string())
}

impl Ceremony<'_> {
    async fn note(&self, outcome: ApprovalOutcome, entry: JournalEntry) -> Result<(), String> {
        self.journal.append(outcome, entry).await
    }

    async fn refuse(
        &self,
        outcome: ApprovalOutcome,
        mut entry: JournalEntry,
        detail: &str,
        sentence: String,
    ) -> String {
        if entry.detail.is_none() {
            entry.detail = Some(detail.to_string());
        }
        if let Err(error) = self.note(outcome, entry).await {
            log::error!("merge attempt {}: {error}", self.attempt_id);
        }
        sentence
    }

    /// Report a completion, and journal the outcome only once the service
    /// acknowledged it. An unacknowledged report leaves the attempt where
    /// reconciliation picks it up again.
    #[allow(clippy::too_many_arguments)]
    async fn report(
        &self,
        challenge_id: &str,
        digest: &str,
        ids: &ExecutionIds,
        report_id: &str,
        outcome: CompletionOutcome,
        evidence: serde_json::Value,
        entry: JournalEntry,
    ) -> bool {
        match self
            .client
            .complete(
                challenge_id,
                digest,
                &ids.claim_id,
                report_id,
                outcome,
                evidence,
            )
            .await
        {
            Ok(ReportOutcome::Recorded | ReportOutcome::AlreadyRecorded) => {
                let journaled = match outcome {
                    CompletionOutcome::Succeeded => ApprovalOutcome::Succeeded,
                    CompletionOutcome::Failed => ApprovalOutcome::Failed,
                    CompletionOutcome::Indeterminate => ApprovalOutcome::Indeterminate,
                    CompletionOutcome::Violation => ApprovalOutcome::Violation,
                };
                if let Err(error) = self.note(journaled, entry).await {
                    log::error!("merge attempt {}: {error}", self.attempt_id);
                }
                true
            }
            Ok(ReportOutcome::Unavailable) => {
                log::error!(
                    "merge attempt {}: the approvals service refused the {outcome:?} report",
                    self.attempt_id
                );
                false
            }
            Err(error) => {
                log::warn!(
                    "merge attempt {}: the {outcome:?} report did not reach the approvals \
                     service ({error}); reconciliation will report it",
                    self.attempt_id
                );
                false
            }
        }
    }

    pub async fn run<W: LandingWorld>(
        &self,
        world: &mut W,
    ) -> Result<ApprovedLanding<W::Landing>, String> {
        let _in_flight = InFlight::enter(self.attempt_id);
        let empty = JournalEntry::default;

        // ── Phase A: ask ──
        let installation_id = match self.client.enrollment(self.enrollment_id).await {
            Ok(Some(standing)) if standing.state == EnrollmentState::Live => {
                standing.installation_id
            }
            Ok(Some(standing)) => {
                let why = match standing.state {
                    EnrollmentState::Reset => "was reset",
                    _ => "was revoked",
                };
                return Err(self
                    .refuse(
                        ApprovalOutcome::PolicyUnavailable,
                        empty(),
                        &format!("selected enrollment {} {why}", self.enrollment_id),
                        phone_unavailable_refusal(why),
                    )
                    .await);
            }
            Ok(None) => {
                return Err(self
                    .refuse(
                        ApprovalOutcome::PolicyUnavailable,
                        empty(),
                        &format!("selected enrollment {} is not enrolled", self.enrollment_id),
                        phone_unavailable_refusal("is no longer enrolled"),
                    )
                    .await);
            }
            Err(error) => {
                return Err(self
                    .refuse(
                        ApprovalOutcome::PolicyUnavailable,
                        empty(),
                        &format!("the approvals service could not be asked: {error}"),
                        format!(
                            "Refusing to merge into the project base branch: the approvals \
                             service could not be reached to ask the approval phone ({error})."
                        ),
                    )
                    .await);
            }
        };
        let credential = self.client.credential();
        let context = IntentContext {
            installation_id,
            project_id: self.project_id.to_string(),
            merge_plan_id: self.attempt_id.to_string(),
            initiating_request_id: self.job_id.to_string(),
            audience: credential.audience(),
        };

        let first = match world.observe().await {
            Ok(observed) => observed,
            Err(error) => {
                return Err(self
                    .refuse(ApprovalOutcome::Failed, empty(), &error, error.clone())
                    .await)
            }
        };
        if let Some(blocking) = &first.checks.blocking {
            return Err(self
                .refuse(
                    ApprovalOutcome::Failed,
                    empty(),
                    blocking,
                    format!(
                        "Refusing to merge into the project base branch: {blocking}. Base \
                         merges here require passing checks, so the approval phone was not asked."
                    ),
                )
                .await);
        }
        let intent = match build_intent(&first, &context) {
            Ok(intent) => intent,
            Err(error) => {
                return Err(self
                    .refuse(ApprovalOutcome::Failed, empty(), &error, error.clone())
                    .await)
            }
        };
        let digest = intent_digest(&intent)?;
        let digest_text = digest_hex(&digest);
        let advisory = advisory(
            self.title.as_deref(),
            self.description.as_deref(),
            first.checks.counts,
        );
        let issued = match self.client.issue(&intent, &advisory).await {
            Ok(issued) => issued,
            Err(ApprovalsError::Refused {
                status: 409,
                message,
            }) => {
                return Err(self
                    .refuse(
                        ApprovalOutcome::PolicyUnavailable,
                        JournalEntry {
                            intent_digest_sha256: Some(digest_text.clone()),
                            ..empty()
                        },
                        &message,
                        phone_unavailable_refusal("is no longer enrolled"),
                    )
                    .await);
            }
            Err(error) => {
                return Err(self
                    .refuse(
                        ApprovalOutcome::Failed,
                        JournalEntry {
                            intent_digest_sha256: Some(digest_text.clone()),
                            ..empty()
                        },
                        &error.to_string(),
                        format!(
                            "Refusing to merge into the project base branch: the approval could \
                             not be requested ({error})."
                        ),
                    )
                    .await);
            }
        };
        let challenge_id = issued.challenge.challenge_id.clone();
        if issued.challenge.intent_digest_sha256 != digest {
            return Err(self
                .refuse(
                    ApprovalOutcome::Failed,
                    JournalEntry {
                        intent_digest_sha256: Some(digest_text.clone()),
                        challenge_id: Some(challenge_id),
                        ..empty()
                    },
                    "the service digested a different intent",
                    "Refusing to merge: the approvals service described a different merge than \
                     this runner asked about."
                        .to_string(),
                )
                .await);
        }
        let bound = |entry: JournalEntry| JournalEntry {
            intent_digest_sha256: Some(digest_text.clone()),
            challenge_id: Some(challenge_id.clone()),
            ..entry
        };
        self.note(
            ApprovalOutcome::Pending,
            bound(JournalEntry {
                detail: detail_json(serde_json::json!({
                    "intent": intent,
                    "route": world.route(),
                })),
                ..empty()
            }),
        )
        .await?;

        // Wait for the phone. The request has no deadline: it ends when the
        // person answers, when a newer request for this merge supersedes it,
        // or, after an approval, when revalidation below finds a bound fact
        // moved. Nothing has been consumed, so an unreachable service is
        // retried; a service that answers it no longer knows this request is
        // final, because no later read can.
        let status = loop {
            match self.client.status(&challenge_id).await {
                Ok(status) if status.state == ApprovalOutcome::Pending => {}
                Ok(status) => break status,
                Err(error @ ApprovalsError::Refused { .. }) => {
                    return Err(self
                        .refuse(
                            ApprovalOutcome::Failed,
                            bound(empty()),
                            &format!("the challenge status read was refused: {error}"),
                            format!(
                                "The approvals service stopped answering for this approval \
                                 request ({error}). Nothing was merged; merge again to send a \
                                 new request."
                            ),
                        )
                        .await);
                }
                Err(error) => log::debug!("approval {challenge_id}: status read failed: {error}"),
            }
            self.clock.sleep(POLL_INTERVAL).await;
        };
        let refused_by = |state: ApprovalOutcome| match state {
            ApprovalOutcome::Denied => Some("Merge was denied on the approval phone."),
            ApprovalOutcome::Superseded => Some(
                "A newer merge request replaced this approval request on the phone. Nothing was \
                 merged by this one.",
            ),
            ApprovalOutcome::Stale => Some(
                "The approval request went out of date before it was approved. Merge again to \
                 request a new approval.",
            ),
            ApprovalOutcome::AuthorizationRevoked => Some(
                "The approval phone was revoked or reset, so this request was withdrawn. Nothing \
                 was merged.",
            ),
            ApprovalOutcome::InvalidSignature | ApprovalOutcome::WrongAudience => {
                Some("The approval phone's answer could not be verified. Nothing was merged.")
            }
            _ => None,
        };
        if status.state != ApprovalOutcome::AuthorizedNotExecuted {
            let sentence = refused_by(status.state)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    format!(
                        "The approval is in an unexpected state ({}). Nothing was merged.",
                        outcome_state(status.state)
                    )
                });
            let journaled = if refused_by(status.state).is_some() {
                status.state
            } else {
                ApprovalOutcome::Failed
            };
            return Err(self
                .refuse(
                    journaled,
                    bound(empty()),
                    &format!("the challenge reads {}", outcome_state(status.state)),
                    sentence,
                )
                .await);
        }

        // ── Verify before anything is consumed ──
        let Some(token) = status.service_assertion.as_deref() else {
            return Err(self
                .refuse(
                    ApprovalOutcome::InvalidSignature,
                    bound(empty()),
                    "the authorized challenge carries no signed assertion",
                    "The approval phone's answer could not be verified. Nothing was merged."
                        .to_string(),
                )
                .await);
        };
        let expectation = AssertionExpectation {
            issuer: &issued.assertion_issuer,
            audience: &context.audience,
            organization: credential.org_id(),
            challenge_id: &challenge_id,
            intent_digest: &digest,
            enrollment_id: self.enrollment_id,
        };
        let claims = match self
            .client
            .verify_assertion(token, &expectation, self.clock.now())
            .await
        {
            Ok(claims) => claims,
            Err(rejection) => {
                return Err(self
                    .refuse(
                        rejection.journal_state(),
                        bound(empty()),
                        rejection.detail(),
                        format!(
                            "The approval phone's answer could not be verified ({}). Nothing was \
                             merged.",
                            rejection.detail()
                        ),
                    )
                    .await);
            }
        };
        let human = |entry: JournalEntry| JournalEntry {
            approval_id: Some(claims.jti.clone()),
            human_principal_json: serde_json::to_string(&claims.principal).ok(),
            human_appearance_json: serde_json::to_string(&claims.appearance).ok(),
            ..bound(entry)
        };
        let ids = ExecutionIds {
            claim_id: new_id("clm"),
            report_id: new_id("rpt"),
            reconcile_report_id: new_id("rpt"),
        };
        self.note(
            ApprovalOutcome::AuthorizedNotExecuted,
            human(JournalEntry {
                verification_json: detail_json(serde_json::json!({
                    "jti": claims.jti,
                    "enrollmentId": claims.enrollment_id,
                    "issuer": claims.iss,
                    "iat": claims.iat,
                })),
                detail: serde_json::to_string(&ids).ok(),
                ..empty()
            }),
        )
        .await?;

        // ── Phase B: revalidate every bound field ──
        let observed = world.observe().await;
        let observed_intent = observed
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|second| build_intent(second, &context));
        let stale = match &observed_intent {
            Ok(observed_intent) => {
                let changed = changed_fields(&intent, observed_intent);
                (!changed.is_empty() || intent_digest(observed_intent)? != digest).then(|| {
                    (
                        digest_hex(&intent_digest(observed_intent).unwrap_or([0; 32])),
                        format!("changed after approval: {}", changed.join(", ")),
                    )
                })
            }
            Err(error) => Some((
                digest_hex(&[0; 32]),
                format!("could not be observed again: {error}"),
            )),
        };
        if let Some((observed_digest, detail)) = stale {
            match self
                .client
                .invalidate(&challenge_id, &digest_text, &observed_digest)
                .await
            {
                Ok(InvalidationOutcome::Stale | InvalidationOutcome::AlreadyStale) => {}
                Ok(InvalidationOutcome::Unavailable) => log::warn!(
                    "approval {challenge_id}: the service would not mark the approval stale"
                ),
                Err(error) => log::warn!(
                    "approval {challenge_id}: the stale report did not reach the service: {error}"
                ),
            }
            return Err(self
                .refuse(
                    ApprovalOutcome::Stale,
                    human(empty()),
                    &detail,
                    "A branch changed after you approved. Merge again to request a new approval."
                        .to_string(),
                )
                .await);
        }
        let second = observed.expect("an unchanged observation was observed");
        if let Some(blocking) = &second.checks.blocking {
            return Err(self
                .refuse(
                    ApprovalOutcome::Failed,
                    human(empty()),
                    blocking,
                    format!(
                        "Refusing to merge into the project base branch: {blocking} since it \
                         was approved. Nothing was merged."
                    ),
                )
                .await);
        }

        // ── Phase C: claim ──
        let mut claimed = None;
        for _ in 0..3 {
            match self
                .client
                .claim(&challenge_id, &digest_text, &ids.claim_id)
                .await
            {
                Ok(outcome) => {
                    claimed = Some(outcome);
                    break;
                }
                Err(error) => {
                    log::warn!("approval {challenge_id}: claim did not complete: {error}");
                    self.clock.sleep(POLL_INTERVAL).await;
                }
            }
        }
        match claimed {
            Some(ClaimOutcome::Claimed | ClaimOutcome::AlreadyClaimed) => {}
            Some(ClaimOutcome::Unavailable) => {
                let state = self
                    .client
                    .status(&challenge_id)
                    .await
                    .map(|status| status.state)
                    .ok()
                    .filter(|state| refused_by(*state).is_some())
                    .unwrap_or(ApprovalOutcome::Failed);
                return Err(self
                    .refuse(
                        state,
                        human(empty()),
                        "the approval could not be claimed",
                        "The approval is no longer available to execute. Nothing was merged; \
                         merge again to request a new approval."
                            .to_string(),
                    )
                    .await);
            }
            None => {
                // The claim may have landed with its answer lost. Nothing was
                // mutated, so report that under the same claim id: the service
                // records it only if the claim did land, and refuses otherwise.
                let sentence = "The approval could not be claimed because the approvals service \
                                was unreachable. Nothing was merged."
                    .to_string();
                let no_mutation = serde_json::json!({
                    "reason": "claim_unconfirmed",
                    "mutationIssued": false,
                });
                let outcome = self
                    .client
                    .complete(
                        &challenge_id,
                        &digest_text,
                        &ids.claim_id,
                        &ids.report_id,
                        CompletionOutcome::Failed,
                        no_mutation.clone(),
                    )
                    .await;
                let (journaled, detail) = match outcome {
                    // Recorded, or refused because the claim never landed:
                    // either way the act is settled as not executed.
                    Ok(_) => (ApprovalOutcome::Failed, no_mutation),
                    // Still unknown whether the service holds an executing
                    // claim. Journal it executing with no mutation issued, so
                    // reconciliation settles it as failed once it can.
                    Err(_) => (ApprovalOutcome::Executing, no_mutation),
                };
                return Err(self
                    .refuse(
                        journaled,
                        human(JournalEntry {
                            detail: Some(detail.to_string()),
                            ..empty()
                        }),
                        "",
                        sentence,
                    )
                    .await);
            }
        }
        self.note(
            ApprovalOutcome::Executing,
            human(JournalEntry {
                detail: detail_json(serde_json::json!({ "route": world.route() })),
                ..empty()
            }),
        )
        .await?;

        // ── Phase D: the final locked comparison, then the mutation ──
        match world.execute(&second).await {
            Execution::BoundStateChanged { observed } => {
                self.report(
                    &challenge_id,
                    &digest_text,
                    &ids,
                    &ids.report_id,
                    CompletionOutcome::Failed,
                    serde_json::json!({
                        "reason": "bound_state_changed_after_claim",
                        "mutationIssued": false,
                        "observed": observed,
                    }),
                    human(JournalEntry {
                        detail: Some(
                            "bound state changed after the claim; nothing was mutated".into(),
                        ),
                        ..empty()
                    }),
                )
                .await;
                Err(
                    "A branch changed while merging. Nothing was merged. Merge again to request a \
                     new approval."
                        .to_string(),
                )
            }
            Execution::Refused(reason) => {
                self.report(
                    &challenge_id,
                    &digest_text,
                    &ids,
                    &ids.report_id,
                    CompletionOutcome::Failed,
                    serde_json::json!({ "reason": reason, "providerRefused": true }),
                    human(JournalEntry {
                        detail: Some(reason.clone()),
                        ..empty()
                    }),
                )
                .await;
                Err(reason)
            }
            Execution::Unknown {
                reason,
                approved_head,
            } => {
                let evidence = serde_json::json!({
                    "route": world.route(),
                    "reason": reason,
                    "approvedHead": approved_head,
                });
                self.report(
                    &challenge_id,
                    &digest_text,
                    &ids,
                    &ids.report_id,
                    CompletionOutcome::Indeterminate,
                    evidence.clone(),
                    human(JournalEntry {
                        detail: Some(evidence.to_string()),
                        ..empty()
                    }),
                )
                .await;
                Err(format!(
                    "Whether the merge landed is not yet known ({reason}). Cairn will reconcile \
                     it; check the pull request before merging again."
                ))
            }
            // ── Phase E: prove, then complete ──
            Execution::Landed {
                commit,
                approved_head,
                landing,
            } => {
                let evidence = serde_json::json!({
                    "route": world.route(),
                    "landedCommit": commit,
                    "approvedHead": approved_head,
                });
                let landed_entry = || {
                    human(JournalEntry {
                        detail: Some(evidence.to_string()),
                        ..empty()
                    })
                };
                match world.landed_shape(&commit).await {
                    Ok(shape) => match prove_landing(&intent, Some(&approved_head), &shape) {
                        Proof::CannotProve(why) => {
                            self.report(
                                &challenge_id,
                                &digest_text,
                                &ids,
                                &ids.report_id,
                                CompletionOutcome::Indeterminate,
                                serde_json::json!({
                                    "route": world.route(),
                                    "landedCommit": commit,
                                    "approvedHead": approved_head,
                                    "unproven": why,
                                }),
                                landed_entry(),
                            )
                            .await;
                            Ok(ApprovedLanding {
                                landing,
                                proven: false,
                            })
                        }
                        Proof::Proven => {
                            self.report(
                                &challenge_id,
                                &digest_text,
                                &ids,
                                &ids.report_id,
                                CompletionOutcome::Succeeded,
                                evidence.clone(),
                                landed_entry(),
                            )
                            .await;
                            Ok(ApprovedLanding {
                                landing,
                                proven: true,
                            })
                        }
                        Proof::Mismatch(mismatch) => {
                            let violation = serde_json::json!({
                                "route": world.route(),
                                "landedCommit": commit,
                                "approvedHead": approved_head,
                                "mismatch": mismatch,
                            });
                            if self
                                .report(
                                    &challenge_id,
                                    &digest_text,
                                    &ids,
                                    &ids.report_id,
                                    CompletionOutcome::Indeterminate,
                                    violation.clone(),
                                    landed_entry(),
                                )
                                .await
                            {
                                self.report(
                                    &challenge_id,
                                    &digest_text,
                                    &ids,
                                    &ids.reconcile_report_id,
                                    CompletionOutcome::Violation,
                                    violation,
                                    human(JournalEntry {
                                        detail: Some(mismatch.clone()),
                                        ..empty()
                                    }),
                                )
                                .await;
                            }
                            Err(format!(
                                "The merge landed as {}, but not as approved: {mismatch}. The \
                                 approval is recorded as violated.",
                                short(&commit)
                            ))
                        }
                    },
                    Err(unreadable) => {
                        self.report(
                            &challenge_id,
                            &digest_text,
                            &ids,
                            &ids.report_id,
                            CompletionOutcome::Indeterminate,
                            serde_json::json!({
                                "route": world.route(),
                                "landedCommit": commit,
                                "approvedHead": approved_head,
                                "unreadable": unreadable,
                            }),
                            landed_entry(),
                        )
                        .await;
                        Ok(ApprovedLanding {
                            landing,
                            proven: false,
                        })
                    }
                }
            }
        }
    }
}

// ─── Reconciliation ──────────────────────────────────────────────────────────────────

/// What the provider can say about an approved merge whose outcome was never
/// settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LandingEvidence {
    /// The merge landed as this commit, with this shape.
    Landed { commit: String, shape: LandedShape },
    /// The merge did not happen and now cannot.
    DefinitelyNotLanded(String),
    /// Nothing conclusive.
    Unknown(String),
}

/// The completion a reconciliation reports, and from which state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Reconciliation {
    Report(Vec<CompletionOutcome>),
    Leave,
}

/// Decide what an unsettled attempt reports. Absence of evidence is never
/// failure: only a definitive "did not and cannot happen" is. Evidence is
/// already attributed to this attempt (see [`store_evidence`] and
/// [`github_attribution`]); `approved_head` is the head this attempt bound.
pub(super) fn reconcile(
    latest: ApprovalOutcome,
    intent: &BaseMergeIntentV2,
    approved_head: Option<&str>,
    evidence: &LandingEvidence,
) -> Reconciliation {
    let from_executing = latest == ApprovalOutcome::Executing;
    let unknown = if from_executing {
        Reconciliation::Report(vec![CompletionOutcome::Indeterminate])
    } else {
        Reconciliation::Leave
    };
    match evidence {
        LandingEvidence::Landed { shape, .. } => {
            match prove_landing(intent, approved_head, shape) {
                Proof::Proven => Reconciliation::Report(vec![CompletionOutcome::Succeeded]),
                Proof::Mismatch(_) if from_executing => Reconciliation::Report(vec![
                    CompletionOutcome::Indeterminate,
                    CompletionOutcome::Violation,
                ]),
                Proof::Mismatch(_) => Reconciliation::Report(vec![CompletionOutcome::Violation]),
                Proof::CannotProve(_) => unknown,
            }
        }
        LandingEvidence::DefinitelyNotLanded(_) => {
            Reconciliation::Report(vec![CompletionOutcome::Failed])
        }
        LandingEvidence::Unknown(_) => unknown,
    }
}

/// The store's evidence for one attempt, attributed to it. The approved base
/// is found on the target's first-parent line; the commit whose first parent it
/// is decides. That commit is this attempt's fold only if it is exactly the
/// approved squash (one parent, the approved tree): anything else is another
/// change that landed on the approved base, after which this attempt's fold
/// (atomic, under the store lock, and no longer running) can no longer happen.
pub(super) fn store_evidence(
    tip: &str,
    intent: &BaseMergeIntentV2,
    read: impl Fn(&str) -> Result<LandedShape, String>,
) -> LandingEvidence {
    const MAX_STEPS: usize = 1000;
    if tip == intent.target_oid {
        return LandingEvidence::DefinitelyNotLanded(
            "the target still holds the approved base, and no fold is running".into(),
        );
    }
    let mut commit = tip.to_string();
    for _ in 0..MAX_STEPS {
        let shape = match read(&commit) {
            Ok(shape) => shape,
            Err(error) => return LandingEvidence::Unknown(error),
        };
        let Some(first) = shape.parents.first().cloned() else {
            return LandingEvidence::DefinitelyNotLanded(
                "the target's history no longer passes through the approved base".into(),
            );
        };
        if first == intent.target_oid {
            let ours = shape.parents.len() == 1
                && Some(shape.tree.as_str()) == intent.result_tree_oid.as_deref();
            return if ours {
                LandingEvidence::Landed { commit, shape }
            } else {
                LandingEvidence::DefinitelyNotLanded(format!(
                    "{} landed on the approved base instead",
                    short(&commit)
                ))
            };
        }
        commit = first;
    }
    LandingEvidence::Unknown("the approved base is too far behind the target to find".into())
}

/// A pull request's merge state, as reconciliation reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PullRequestState {
    pub merged: bool,
    pub closed: bool,
    pub head: Option<String>,
    pub merge_commit: Option<String>,
}

/// Whether a pull request's merge is this attempt's: `Ok(commit)` to read and
/// prove, or the evidence when there is nothing to read. A merge of a head
/// other than the one this attempt bound is someone else's, and this attempt
/// can no longer happen; without a recorded head nothing is attributed.
pub(super) fn github_attribution(
    pr: &PullRequestState,
    approved_head: Option<&str>,
) -> Result<String, LandingEvidence> {
    if pr.merged {
        let Some(approved) = approved_head else {
            return Err(LandingEvidence::Unknown(
                "the head this attempt bound is not recorded".into(),
            ));
        };
        if pr.head.as_deref() != Some(approved) {
            return Err(LandingEvidence::DefinitelyNotLanded(
                "the pull request merged a head other than the approved one".into(),
            ));
        }
        return pr
            .merge_commit
            .clone()
            .ok_or_else(|| LandingEvidence::Unknown("GitHub names no merge commit".into()));
    }
    if pr.closed {
        return Err(LandingEvidence::DefinitelyNotLanded(
            "the pull request was closed without merging".into(),
        ));
    }
    Err(LandingEvidence::Unknown(
        "the pull request has not merged".into(),
    ))
}

/// The approved merge of `merge_request_id` whose outcome is still unsettled,
/// if any. A second approved merge of the same pull request waits for it, so
/// whatever lands is never credited to the wrong attempt.
pub(super) async fn unsettled_attempt_for(
    db: &LocalDb,
    merge_request_id: &str,
) -> Result<Option<String>, String> {
    db.query_opt_text(
        "SELECT e.attempt_id FROM merge_attempt_events e
          WHERE e.merge_request_id = ?1
            AND e.state IN ('executing', 'indeterminate')
            AND e.sequence = (SELECT MAX(l.sequence) FROM merge_attempt_events l
                               WHERE l.attempt_id = e.attempt_id)
          LIMIT 1",
        (merge_request_id.to_string(),),
    )
    .await
    .map_err(|error| format!("the merge journal is unreadable: {error}"))
}

/// One unsettled approved merge, as its journal records it.
#[derive(Debug, Clone)]
pub(super) struct UnsettledAttempt {
    pub attempt_id: String,
    pub project_id: String,
    pub merge_request_id: String,
    pub job_id: String,
    pub target_ref: String,
    pub source_ref: String,
    pub policy_json: String,
    pub next_sequence: i64,
    pub latest: ApprovalOutcome,
    pub challenge_id: String,
    pub intent: BaseMergeIntentV2,
    pub route: String,
    pub claim_id: String,
    pub report_id: String,
    pub reconcile_report_id: String,
    pub human_principal_json: Option<String>,
    pub human_appearance_json: Option<String>,
    pub approval_id: Option<String>,
    /// The head the mutation was bound to, as the attempt's rows recorded it.
    pub approved_head: Option<String>,
    /// `Some(false)` when the attempt recorded that no mutation was issued.
    pub mutation_issued: Option<bool>,
}

impl UnsettledAttempt {
    /// The head this attempt bound: recorded, or the candidate an unbound
    /// landing named in the intent itself.
    pub fn bound_head(&self) -> Option<&str> {
        self.approved_head
            .as_deref()
            .or(self.intent.candidate_oid.as_deref())
    }
}

/// Every attempt whose latest event is `executing` or `indeterminate`.
pub(super) async fn unsettled_attempts(db: &LocalDb) -> Result<Vec<UnsettledAttempt>, String> {
    let rows = db
        .read(|conn| {
            Box::pin(async move {
                use crate::storage::RowExt;
                let mut rows = conn
                    .query(
                        r#"SELECT e.attempt_id, e.project_id, e.merge_request_id, e.job_id,
                                e.target_ref, e.source_ref, e.policy_json, e.sequence, e.state,
                                e.challenge_id, e.human_principal_json, e.human_appearance_json,
                                e.approval_id,
                                (SELECT d.detail FROM merge_attempt_events d
                                  WHERE d.attempt_id = e.attempt_id
                                    AND d.detail LIKE '%"approvedHead":"%'
                                  ORDER BY d.sequence DESC LIMIT 1),
                                e.detail
                           FROM merge_attempt_events e
                          WHERE e.state IN ('executing', 'indeterminate')
                            AND e.sequence = (SELECT MAX(l.sequence) FROM merge_attempt_events l
                                               WHERE l.attempt_id = e.attempt_id)"#,
                        (),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((
                        row.text(0)?,
                        row.text(1)?,
                        row.opt_text(2)?,
                        row.text(3)?,
                        row.text(4)?,
                        row.text(5)?,
                        row.text(6)?,
                        row.i64(7)?,
                        row.text(8)?,
                        row.opt_text(9)?,
                        row.opt_text(10)?,
                        row.opt_text(11)?,
                        row.opt_text(12)?,
                        row.opt_text(13)?,
                        row.opt_text(14)?,
                    ));
                }
                Ok(out)
            })
        })
        .await
        .map_err(|error| format!("unsettled merge attempts are unreadable: {error}"))?;
    let mut attempts = Vec::new();
    for row in rows {
        let (
            attempt_id,
            project_id,
            mr,
            job_id,
            target_ref,
            source_ref,
            policy_json,
            sequence,
            state,
            challenge_id,
            principal,
            appearance,
            approval_id,
            head_detail,
            latest_detail,
        ) = row;
        let json = |text: Option<String>| {
            text.and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        };
        let approved_head =
            json(head_detail).and_then(|value| value["approvedHead"].as_str().map(str::to_string));
        let mutation_issued =
            json(latest_detail).and_then(|value| value["mutationIssued"].as_bool());
        let details = attempt_details(db, &attempt_id).await?;
        let (Some(challenge_id), Some((intent, route)), Some(ids)) =
            (challenge_id, details.0, details.1)
        else {
            log::warn!("merge attempt {attempt_id} is unsettled but its journal is incomplete");
            continue;
        };
        attempts.push(UnsettledAttempt {
            attempt_id,
            project_id,
            merge_request_id: mr.unwrap_or_default(),
            job_id,
            target_ref,
            source_ref,
            policy_json,
            next_sequence: sequence + 1,
            latest: if state == "executing" {
                ApprovalOutcome::Executing
            } else {
                ApprovalOutcome::Indeterminate
            },
            challenge_id,
            intent,
            route,
            claim_id: ids.claim_id,
            report_id: ids.report_id,
            reconcile_report_id: ids.reconcile_report_id,
            human_principal_json: principal,
            human_appearance_json: appearance,
            approval_id,
            approved_head,
            mutation_issued,
        });
    }
    Ok(attempts)
}

/// The intent and route the pending row recorded, and the execution ids the
/// authorized row recorded.
async fn attempt_details(
    db: &LocalDb,
    attempt_id: &str,
) -> Result<(Option<(BaseMergeIntentV2, String)>, Option<ExecutionIds>), String> {
    let attempt = attempt_id.to_string();
    let rows = db
        .read(move |conn| {
            let attempt = attempt.clone();
            Box::pin(async move {
                use crate::storage::RowExt;
                let mut rows = conn
                    .query(
                        "SELECT state, detail FROM merge_attempt_events
                          WHERE attempt_id = ?1 AND state IN ('pending', 'authorized_not_executed')",
                        (attempt,),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.opt_text(1)?));
                }
                Ok(out)
            })
        })
        .await
        .map_err(|error| format!("merge attempt {attempt_id} is unreadable: {error}"))?;
    let mut intent = None;
    let mut ids = None;
    for (state, detail) in rows {
        let Some(detail) = detail else { continue };
        if state == "pending" {
            #[derive(serde::Deserialize)]
            struct Pending {
                intent: BaseMergeIntentV2,
                route: String,
            }
            intent = serde_json::from_str::<Pending>(&detail)
                .ok()
                .map(|pending| (pending.intent, pending.route));
        } else {
            ids = serde_json::from_str::<ExecutionIds>(&detail).ok();
        }
    }
    Ok((intent, ids))
}

/// Settle one attempt against `evidence`: report to the service with the
/// attempt's own claim and report ids, and journal each acknowledged report.
pub(super) async fn settle_attempt(
    db: &LocalDb,
    client: &ApprovalsClient<'_>,
    attempt: &UnsettledAttempt,
    evidence: &LandingEvidence,
) -> Result<Option<ApprovalOutcome>, String> {
    if is_in_flight(&attempt.attempt_id) {
        return Ok(None);
    }
    let Reconciliation::Report(outcomes) = reconcile(
        attempt.latest,
        &attempt.intent,
        attempt.bound_head(),
        evidence,
    ) else {
        return Ok(None);
    };
    let digest = digest_hex(&intent_digest(&attempt.intent)?);
    let journal = AttemptJournal::new(
        db,
        MergeAttemptJournal {
            attempt_id: &attempt.attempt_id,
            project_id: &attempt.project_id,
            merge_request_id: &attempt.merge_request_id,
            job_id: &attempt.job_id,
            target_ref: &attempt.target_ref,
            source_ref: &attempt.source_ref,
            policy_json: &attempt.policy_json,
        },
        attempt.next_sequence,
    );
    let head = attempt.bound_head();
    let evidence_json = match evidence {
        LandingEvidence::Landed { commit, .. } => serde_json::json!({
            "route": attempt.route, "landedCommit": commit, "approvedHead": head,
            "reconciled": true,
        }),
        LandingEvidence::DefinitelyNotLanded(why) | LandingEvidence::Unknown(why) => {
            serde_json::json!({
                "route": attempt.route, "reason": why, "approvedHead": head, "reconciled": true,
            })
        }
    };
    let mut settled = None;
    for outcome in outcomes {
        // The first report from `executing` reuses the attempt's own report
        // id; anything after `indeterminate` uses the reconcile id.
        let from_executing = settled.is_none() && attempt.latest == ApprovalOutcome::Executing;
        let report_id = if from_executing {
            &attempt.report_id
        } else {
            &attempt.reconcile_report_id
        };
        let reported = client
            .complete(
                &attempt.challenge_id,
                &digest,
                &attempt.claim_id,
                report_id,
                outcome,
                evidence_json.clone(),
            )
            .await
            .map_err(|error| error.to_string())?;
        if reported == ReportOutcome::Unavailable {
            return Err(format!(
                "the approvals service refused the {outcome:?} report for {}",
                attempt.challenge_id
            ));
        }
        let journaled = match outcome {
            CompletionOutcome::Succeeded => ApprovalOutcome::Succeeded,
            CompletionOutcome::Failed => ApprovalOutcome::Failed,
            CompletionOutcome::Indeterminate => ApprovalOutcome::Indeterminate,
            CompletionOutcome::Violation => ApprovalOutcome::Violation,
        };
        journal
            .append(
                journaled,
                JournalEntry {
                    detail: Some(evidence_json.to_string()),
                    intent_digest_sha256: Some(digest.clone()),
                    challenge_id: Some(attempt.challenge_id.clone()),
                    approval_id: attempt.approval_id.clone(),
                    human_principal_json: attempt.human_principal_json.clone(),
                    human_appearance_json: attempt.human_appearance_json.clone(),
                    verification_json: None,
                },
            )
            .await?;
        settled = Some(journaled);
    }
    Ok(settled)
}

#[cfg(test)]
#[path = "human_approval_tests.rs"]
mod tests;
