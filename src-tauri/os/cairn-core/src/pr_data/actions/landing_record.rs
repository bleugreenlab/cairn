//! Cairn's landing record as the authority for a pull request's merge state.
//!
//! A change Cairn publishes by fast-forwarding the target to a validated
//! squash commit never passes through GitHub's merge API. GitHub's `merged`
//! flag is therefore not evidence either way: GitHub sets it only for merges it
//! performs (or, sometimes, when it notices the head became reachable from the
//! target). The record is Cairn's own:
//!
//! - the `merge_requests` row moved to `merged`, with its `merged_commit`, by
//!   the one resolution claim in `resolution::resolve_pr_node`. Every merge-state
//!   consumer (the `pr_state` gate, issue resolution, the thread census, the PR
//!   artifact) reads this row; GitHub's `github_state` is a cache for display;
//! - the immutable `merge_attempt_events` journal of the attempt that
//!   published it, whose `executing` event names what was published before the
//!   ref moved.
//!
//! # Protocol
//!
//! 1. [`journal_publication_intent`] writes the attempt's `executing` event
//!    (the expected old target, the landing commit and its tree) BEFORE the ref
//!    update. Without it nothing is pushed.
//! 2. The transport updates the target, fast-forward only, expecting the old
//!    target.
//! 3. A refusal journals `failed`, an unknowable result `indeterminate`
//!    ([`journal_publication_outcome`]).
//! 4. An accepted publication is recorded by the merge path's ordinary tail
//!    (`merge.rs`): `resolve_pr_node_landed` moves the row first, then the
//!    journal's `succeeded` is written. A failure to record leaves the outcome
//!    unjournaled, so the `executing` intent stays the recovery handle.
//! 5. [`settle_github_after_landing`] comments on the pull request naming the
//!    landing commit (once: the comment carries a marker) and closes it if
//!    GitHub still shows it open. A pull request GitHub shows closed while Cairn
//!    shows merged is the expected rendering; the comment explains it.
//!
//! A failure or crash between 2 and 4 leaves the intent unsettled, and
//! [`reconcile_pending_landings`] (startup and a slow timer) or the next PR
//! refresh settles it from the target's history: the landing happened exactly
//! when the target's first-parent history passes through a commit whose only
//! parent is the expected old target and whose tree is the validated tree
//! (`human_approval::history_step`). Until then a refresh never reads a closed
//! pull request as "closed without merging".

use super::context::{resolve_mr_context_for_job, PrNodeResolution};
use super::human_approval::{
    history_step, history_too_long, unmoved_target, HistoryStep, InFlight, LandedShape,
    LandingEvidence,
};
use super::merge::{journal_merge_attempt_event, JournalEntry, MergeAttemptJournal};
use super::resolution::{resolve_pr_node, resolve_pr_node_landed, Landed, PrResolutionAttribution};
use crate::github::api;
use crate::github::credentials::{get_owner_repo, get_owner_repo_for_pr};
use crate::orchestrator::Orchestrator;
use crate::security::broker::github::installation_authority;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

/// An ungated publication's attempt is exactly two events: its intent, then
/// its outcome. Recovery appends after whatever is latest.
const INTENT_SEQUENCE: i64 = 1;
const OUTCOME_SEQUENCE: i64 = 2;
/// How far below origin's tip the proof looks for the landing. A landing is
/// normally the tip or a few commits under it; further than this is
/// undetermined rather than an unbounded walk of remote history.
const MAX_REMOTE_HISTORY_STEPS: usize = 64;
const MAX_COMMENT_PAGES: u32 = 10;
const RECOVERY_SURFACE: &str = "landing-recovery";
/// The approval route of a ceremony that published by fast-forward.
const ORIGIN_ROUTE: &str = "origin";

/// What one fast-forward publication puts on the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "PublicationWire")]
pub(crate) struct Publication {
    pub target_branch: String,
    /// What origin's target held when the publication was leased: the old
    /// value the remote update expects, and where recovery's proof stops.
    pub expected_old: String,
    /// What the store's target held: the exact commit the landing was
    /// prepared, bound and validated on, and the old value of the store's own
    /// target compare-and-swap. It differs from `expected_old` when the store
    /// holds target commits origin does not (an integration branch's
    /// unpublished or rewritten commits); on the default branch they are equal.
    pub target_before: String,
    pub landing_commit: String,
    pub landing_tree: String,
    pub source_branch: String,
    /// The exact prepared source head the landing was validated from.
    pub source_before: String,
    /// The commit the source bookmark is moved to once the target accepts the
    /// publication (the landing commit, for a squash the store re-points the
    /// source onto). This is what the record names for branch cleanup.
    pub source_after: String,
    /// How the landing commit relates to the expected old target. Intents
    /// journaled before this field existed are squashes.
    #[serde(default)]
    pub shape: PublicationShape,
}

/// A journaled publication as written. Intents journaled before
/// `target_before` existed were prepared on the target origin held, so it
/// falls back to `expected_old`; before `shape`, every publication was a
/// squash.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublicationWire {
    target_branch: String,
    expected_old: String,
    #[serde(default)]
    target_before: Option<String>,
    landing_commit: String,
    landing_tree: String,
    source_branch: String,
    source_before: String,
    source_after: String,
    #[serde(default)]
    shape: PublicationShape,
}

impl From<PublicationWire> for Publication {
    fn from(wire: PublicationWire) -> Self {
        Self {
            target_before: wire
                .target_before
                .unwrap_or_else(|| wire.expected_old.clone()),
            target_branch: wire.target_branch,
            expected_old: wire.expected_old,
            landing_commit: wire.landing_commit,
            landing_tree: wire.landing_tree,
            source_branch: wire.source_branch,
            source_before: wire.source_before,
            source_after: wire.source_after,
            shape: wire.shape,
        }
    }
}

/// How a publication's landing commit sits on the expected old target, which
/// decides how recovery recognises it in origin's history.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum PublicationShape {
    /// One commit whose only parent is the expected old target, with the
    /// validated tree.
    #[default]
    Squash,
    /// The prepared head itself: the owner's own commits above the expected
    /// old target, preserved (an integration-branch child landing).
    FastForward,
}

/// The `executing` event's detail. `publication` must stay its first and only
/// key: the journal queries recognise an intent by that prefix.
#[derive(Serialize, Deserialize)]
struct PublicationDetail {
    publication: Publication,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PublicationOutcome {
    /// The target no longer held the expected old commit, or the update was
    /// not a fast-forward. Nothing was published.
    TargetMoved { observed: Option<String> },
    /// The transport cannot say whether the update was applied.
    Unknown(String),
    /// The update was applied; `detail` is what the landing recorded beyond an
    /// ordinary one.
    Accepted { detail: Option<String> },
}

fn short(oid: &str) -> &str {
    &oid[..oid.len().min(12)]
}

fn intent_detail(publication: &Publication) -> Result<String, String> {
    serde_json::to_string(&PublicationDetail {
        publication: publication.clone(),
    })
    .map_err(|error| format!("the publication intent is unserializable: {error}"))
}

fn parse_intent(detail: &str) -> Option<Publication> {
    serde_json::from_str::<PublicationDetail>(detail)
        .ok()
        .map(|detail| detail.publication)
}

/// Journal that `publication` is about to be pushed. A publication whose
/// intent was not journaled must not be pushed: it would be a landing no
/// recovery could find.
pub(super) async fn journal_publication_intent(
    db: &LocalDb,
    attempt: &MergeAttemptJournal<'_>,
    publication: &Publication,
) -> Result<(), String> {
    journal_merge_attempt_event(
        db,
        attempt,
        INTENT_SEQUENCE,
        "executing",
        &JournalEntry {
            detail: Some(intent_detail(publication)?),
            ..JournalEntry::default()
        },
    )
    .await
}

/// Journal how the push of `publication` ended.
pub(super) async fn journal_publication_outcome(
    db: &LocalDb,
    attempt: &MergeAttemptJournal<'_>,
    publication: &Publication,
    outcome: &PublicationOutcome,
) -> Result<(), String> {
    let (state, detail) = match outcome {
        PublicationOutcome::TargetMoved { observed } => (
            "failed",
            Some(format!(
                "target moved: `{}` no longer held {}{}",
                publication.target_branch,
                short(&publication.expected_old),
                observed
                    .as_deref()
                    .map(|now| format!(" (it holds {})", short(now)))
                    .unwrap_or_default()
            )),
        ),
        PublicationOutcome::Unknown(why) => (
            "indeterminate",
            Some(format!(
                "publishing {} to `{}` has no known outcome: {why}",
                short(&publication.landing_commit),
                publication.target_branch
            )),
        ),
        PublicationOutcome::Accepted { detail } => ("succeeded", detail.clone()),
    };
    journal_merge_attempt_event(
        db,
        attempt,
        OUTCOME_SEQUENCE,
        state,
        &JournalEntry {
            detail,
            ..JournalEntry::default()
        },
    )
    .await
}

/// Journal the outcome of a publication origin accepted, as the merge that
/// drove it records it. Recovery settles a publication only once nothing in
/// this process drives it, so finding its outcome already written means a
/// recovery pass got there first (another process, or a mark that lapsed).
/// The same landed commit is the same fact, recorded once: not an error. Any
/// other settlement contradicts what this merge landed, and is returned.
pub(super) async fn journal_accepted_publication(
    db: &LocalDb,
    attempt: &MergeAttemptJournal<'_>,
    publication: &Publication,
    detail: Option<String>,
) -> Result<(), String> {
    let outcome = PublicationOutcome::Accepted { detail };
    let Err(error) = journal_publication_outcome(db, attempt, publication, &outcome).await else {
        return Ok(());
    };
    let settled = db
        .query_opt(
            "SELECT state, detail FROM merge_attempt_events
              WHERE attempt_id = ?1 AND sequence = ?2",
            params![attempt.attempt_id, OUTCOME_SEQUENCE],
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .map_err(|read| format!("{error} (and its existing outcome is unreadable: {read})"))?;
    let Some((state, settled_detail)) = settled else {
        return Err(error);
    };
    let landed = settled_detail
        .as_deref()
        .and_then(|detail| serde_json::from_str::<serde_json::Value>(detail).ok())
        .and_then(|detail| detail.get("landedCommit")?.as_str().map(str::to_string));
    let ours = &publication.landing_commit;
    match (state.as_str(), landed.as_deref()) {
        ("succeeded", Some(commit)) if commit == ours => {
            log::info!(
                "landing: publication {} of {ours} was already settled as landed by recovery; \
                 its outcome stands as recorded",
                attempt.attempt_id
            );
            Ok(())
        }
        (state, landed) => Err(format!(
            "The merge landed {ours} on `{}`, but the journal already settles its publication {} \
             as `{state}` with landed commit {}. The two records contradict each other; the \
             landing on origin is what to check.",
            publication.target_branch,
            attempt.attempt_id,
            landed.unwrap_or("(none)"),
        )),
    }
}

/// The merge attempt an ordinary publication belongs to: the landing loop
/// journals each publication as `{attempt}-publication-{n}`. Any other attempt
/// (an approval ceremony's) publishes under its own id.
fn base_attempt_id(attempt_id: &str) -> &str {
    attempt_id
        .rsplit_once("-publication-")
        .filter(|(_, n)| n.parse::<u32>().is_ok())
        .map_or(attempt_id, |(base, _)| base)
}

/// Who a recovered landing is attributed to: the principal that started the
/// merge, as the merge command journaled it at sequence 0 of the base attempt,
/// under the recovery surface. Only when that is unreadable does the attempt
/// itself stand in.
async fn recovery_attribution(db: &LocalDb, attempt_id: &str) -> PrResolutionAttribution {
    let decision = db
        .query_opt(
            "SELECT authority_decision_json FROM merge_attempt_events
              WHERE attempt_id = ?1 AND sequence = 0",
            params![base_attempt_id(attempt_id).to_string()],
            |row| row.opt_text(0),
        )
        .await
        .ok()
        .flatten()
        .flatten();
    decision
        .as_deref()
        .and_then(initiating_attribution)
        .unwrap_or_else(|| PrResolutionAttribution::internal(RECOVERY_SURFACE, attempt_id))
}

/// The initiating actor of a sequence-0 `authority_decision_json`.
fn initiating_attribution(decision: &str) -> Option<PrResolutionAttribution> {
    let decision: serde_json::Value = serde_json::from_str(decision).ok()?;
    let actor_kind = match decision.get("legacyActorKind")?.as_str()? {
        "operator-ui" => "operator-ui",
        "operator-cli" => "operator-cli",
        "agent" => "agent",
        "internal" => "internal",
        _ => return None,
    };
    Some(PrResolutionAttribution {
        actor_kind,
        actor_identity: decision
            .get("legacyActorIdentity")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        surface: RECOVERY_SURFACE,
    })
}

/// What settling GitHub after a landing did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitHubSettlement {
    /// The row records no landing, so there is nothing to tell GitHub.
    NotRecorded,
    /// The change has no pull request.
    NoPullRequest,
    /// GitHub itself marks the pull request merged; it needs no explanation.
    MergedOnGitHub,
    Settled {
        commented: bool,
        closed: bool,
    },
    /// Another call in this process is settling the same pull request.
    Busy,
}

/// A landing the row records.
struct RecordedLanding {
    commit: String,
    target_branch: String,
    expected_old: Option<String>,
}

async fn recorded_landing(db: &LocalDb, mr_id: &str) -> Result<Option<RecordedLanding>, String> {
    let row = db
        .query_opt(
            "SELECT merged_commit, target_branch FROM merge_requests
              WHERE id = ?1 AND status = 'merged' AND merged_commit IS NOT NULL",
            params![mr_id.to_string()],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("the merge request is unreadable: {error}"))?;
    let Some((commit, target_branch)) = row else {
        return Ok(None);
    };
    let intents = db
        .query_all(
            "SELECT detail FROM merge_attempt_events
              WHERE merge_request_id = ?1 AND state = 'executing'
                AND detail LIKE '{\"publication\":%'",
            params![mr_id.to_string()],
            |row| row.text(0),
        )
        .await
        .map_err(|error| format!("the merge journal is unreadable: {error}"))?;
    let expected_old = intents
        .iter()
        .filter_map(|detail| parse_intent(detail))
        .find(|publication| publication.landing_commit == commit)
        .map(|publication| publication.expected_old);
    Ok(Some(RecordedLanding {
        commit,
        target_branch,
        expected_old,
    }))
}

fn landing_marker(commit: &str) -> String {
    format!("<!-- cairn-landing:{commit} -->")
}

fn landing_comment(landing: &RecordedLanding) -> String {
    let target = &landing.target_branch;
    let from = landing
        .expected_old
        .as_deref()
        .map(|old| format!(" as a fast-forward from {old}"))
        .unwrap_or_default();
    format!(
        "Landed on `{target}` as {commit}.\n\n\
         Cairn validated this change against the exact tip of `{target}` and published it as \
         one squash commit directly onto that tip{from}. GitHub marks a pull request merged \
         only when it performs the merge, so this one shows as closed rather than merged. Its \
         change is on `{target}`, and Cairn records it as merged.\n\n{marker}\n",
        commit = landing.commit,
        marker = landing_marker(&landing.commit),
    )
}

/// Tell GitHub about a landing the row records: comment naming the landing
/// commit, then close the pull request if it is still open.
///
/// Idempotent, so any reader may retry it. The comment is found by its marker
/// before one is posted, and a closed pull request is not closed again. A
/// pull request GitHub itself marks merged is left alone.
pub async fn settle_github_after_landing(
    orch: &Orchestrator,
    owner_id: &str,
) -> Result<GitHubSettlement, String> {
    let db = crate::execution::routing::routing_db_for_id(&orch.db, owner_id)
        .await
        .map_err(|error| error.to_string())?;
    let context = resolve_mr_context_for_job(&db, owner_id).await?;
    let Some(landing) = recorded_landing(&db, &context.mr_id).await? else {
        return Ok(GitHubSettlement::NotRecorded);
    };
    let Some(number) = context.github_pr_number else {
        return Ok(GitHubSettlement::NoPullRequest);
    };
    // Readers of one snapshot settle concurrently; the guard is what keeps two
    // of them from both finding no comment and both posting one.
    let Some(_settling) = InFlight::try_enter(&format!("github-settle:{}", context.mr_id)) else {
        return Ok(GitHubSettlement::Busy);
    };
    let (owner, repo) = get_owner_repo_for_pr(&context.pr_url, &context.repo_path)?;
    let auth = installation_authority(&orch.db.local, &owner).await?;
    let http = &*orch.services.http;
    let state = api::fetch_pr_merge_state(http, &auth, &owner, &repo, number).await?;
    if state.merged {
        return Ok(GitHubSettlement::MergedOnGitHub);
    }
    let marker = landing_marker(&landing.commit);
    let bodies =
        api::list_issue_comment_bodies(http, &auth, &owner, &repo, number, MAX_COMMENT_PAGES)
            .await?;
    let commented = if bodies.iter().any(|body| body.contains(&marker)) {
        false
    } else {
        api::create_issue_comment(
            http,
            &auth,
            &owner,
            &repo,
            number,
            &landing_comment(&landing),
        )
        .await?;
        true
    };
    let closed = if state.state == "open" {
        api::close_pr(http, &auth, &owner, &repo, number).await?;
        true
    } else {
        false
    };
    if commented || closed {
        orch.invalidate_pr_refresh(&context.job_id, "landing-settled-on-github");
    }
    Ok(GitHubSettlement::Settled { commented, closed })
}

/// Settle GitHub, logging rather than failing: the landing is already
/// recorded, and the next refresh of a merged row GitHub still shows open
/// retries.
pub(super) async fn settle_github_best_effort(orch: &Orchestrator, owner_id: &str) {
    match settle_github_after_landing(orch, owner_id).await {
        Ok(GitHubSettlement::Settled { commented, closed }) if commented || closed => log::info!(
            "Settled GitHub for the landing of {owner_id}: commented={commented} closed={closed}"
        ),
        Ok(_) => {}
        Err(error) => log::warn!(
            "The landing of {owner_id} is recorded, but telling GitHub failed (retried on refresh): \
             {error}"
        ),
    }
}

/// The landing evidence origin's `target_branch` holds: whether its
/// first-parent history passes through exactly the squash a landing built on
/// `expected_old` with `tree`.
pub(super) async fn remote_target_evidence(
    orch: &Orchestrator,
    repo_path: &str,
    target_branch: &str,
    expected_old: &str,
    tree: Option<&str>,
) -> LandingEvidence {
    remote_history_evidence(
        orch,
        repo_path,
        target_branch,
        expected_old,
        |commit, shape| history_step(commit, shape, expected_old, tree),
    )
    .await
}

/// The landing evidence origin holds for `publication`, by its shape.
async fn remote_publication_evidence(
    orch: &Orchestrator,
    repo_path: &str,
    publication: &Publication,
) -> LandingEvidence {
    match publication.shape {
        PublicationShape::Squash => {
            remote_target_evidence(
                orch,
                repo_path,
                &publication.target_branch,
                &publication.expected_old,
                Some(&publication.landing_tree),
            )
            .await
        }
        PublicationShape::FastForward => {
            remote_history_evidence(
                orch,
                repo_path,
                &publication.target_branch,
                &publication.expected_old,
                |commit, shape| fast_forward_step(commit, shape, publication),
            )
            .await
        }
    }
}

/// One first-parent step down origin's history for a fast-forward
/// publication, whose landing commit is the prepared head itself (a stack of
/// the owner's commits above the expected old target), so the first commit
/// above that target is not the landing. The landing happened exactly when the
/// walk meets the landing commit with the validated tree. Reaching the
/// expected old target without meeting it means origin never took it: the
/// publication was atomic, so a target that moved elsewhere did not.
fn fast_forward_step(commit: String, shape: LandedShape, publication: &Publication) -> HistoryStep {
    if commit == publication.landing_commit {
        return HistoryStep::Done(if shape.tree == publication.landing_tree {
            LandingEvidence::Landed { commit, shape }
        } else {
            LandingEvidence::DefinitelyNotLanded(format!(
                "{} is on the target, but not with the validated tree",
                short(&commit)
            ))
        });
    }
    match shape.parents.first() {
        Some(parent) if parent == &publication.expected_old => {
            HistoryStep::Done(LandingEvidence::DefinitelyNotLanded(format!(
                "the target moved past {} without the landing {}",
                short(&publication.expected_old),
                short(&publication.landing_commit)
            )))
        }
        Some(parent) => HistoryStep::Next(parent.clone()),
        None => HistoryStep::Done(LandingEvidence::DefinitelyNotLanded(
            "the target's history no longer passes through the approved base".into(),
        )),
    }
}

/// Walk origin's `target_branch` down its first parents from the tip, judging
/// each commit with `step`, within [`MAX_REMOTE_HISTORY_STEPS`].
async fn remote_history_evidence(
    orch: &Orchestrator,
    repo_path: &str,
    target_branch: &str,
    expected_old: &str,
    step: impl Fn(String, LandedShape) -> HistoryStep,
) -> LandingEvidence {
    let Ok((owner, repo)) = get_owner_repo(repo_path) else {
        return LandingEvidence::Unknown("the repository is unresolvable".to_string());
    };
    let auth = match installation_authority(&orch.db.local, &owner).await {
        Ok(auth) => auth,
        Err(error) => return LandingEvidence::Unknown(error),
    };
    let http = &*orch.services.http;
    let tip = match api::fetch_branch_head(http, &auth, &owner, &repo, target_branch).await {
        Ok(tip) => tip,
        Err(error) => return LandingEvidence::Unknown(error),
    };
    if let Some(evidence) = unmoved_target(&tip, expected_old) {
        return evidence;
    }
    let mut commit = tip;
    for _ in 0..MAX_REMOTE_HISTORY_STEPS {
        let shape = match api::fetch_commit_shape(http, &auth, &owner, &repo, &commit).await {
            Ok(shape) => LandedShape {
                parents: shape.parents,
                tree: shape.tree,
            },
            Err(error) => return LandingEvidence::Unknown(error),
        };
        match step(commit, shape) {
            HistoryStep::Next(parent) => commit = parent,
            HistoryStep::Done(evidence) => return evidence,
        }
    }
    history_too_long()
}

/// An ungated publication whose outcome was never settled.
struct PendingPublication {
    attempt_id: String,
    project_id: String,
    merge_request_id: String,
    job_id: String,
    target_ref: String,
    source_ref: String,
    policy_json: String,
    next_sequence: i64,
    publication: Publication,
}

async fn pending_publications(
    db: &LocalDb,
    mr_id: Option<&str>,
) -> Result<Vec<PendingPublication>, String> {
    let rows = db
        .query_all(
            "SELECT e.attempt_id, e.project_id, e.merge_request_id, e.job_id, e.target_ref,
                    e.source_ref, e.policy_json, e.sequence,
                    (SELECT i.detail FROM merge_attempt_events i
                      WHERE i.attempt_id = e.attempt_id AND i.state = 'executing'
                        AND i.detail LIKE '{\"publication\":%'
                      ORDER BY i.sequence LIMIT 1)
               FROM merge_attempt_events e
              WHERE e.state IN ('executing', 'indeterminate')
                AND e.merge_request_id IS NOT NULL
                AND (?1 IS NULL OR e.merge_request_id = ?1)
                AND e.sequence = (SELECT MAX(l.sequence) FROM merge_attempt_events l
                                   WHERE l.attempt_id = e.attempt_id)
                AND EXISTS (SELECT 1 FROM merge_attempt_events i
                             WHERE i.attempt_id = e.attempt_id AND i.state = 'executing'
                               AND i.detail LIKE '{\"publication\":%')",
            params![mr_id.map(str::to_string)],
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.text(5)?,
                    row.text(6)?,
                    row.i64(7)?,
                    row.opt_text(8)?,
                ))
            },
        )
        .await
        .map_err(|error| format!("unsettled publications are unreadable: {error}"))?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(
                attempt_id,
                project_id,
                mr,
                job_id,
                target_ref,
                source_ref,
                policy,
                sequence,
                detail,
            )| {
                let publication = detail.as_deref().and_then(parse_intent);
                if publication.is_none() {
                    log::warn!(
                        "publication {attempt_id} is unsettled but its intent is unreadable"
                    );
                }
                Some(PendingPublication {
                    attempt_id,
                    project_id,
                    merge_request_id: mr,
                    job_id,
                    target_ref,
                    source_ref,
                    policy_json: policy,
                    next_sequence: sequence + 1,
                    publication: publication?,
                })
            },
        )
        .collect())
}

/// Whether `mr_id` has a publication whose outcome Cairn does not yet know.
/// While it does, a closed pull request is not evidence the change did not
/// land.
pub(super) async fn has_unsettled_landing(db: &LocalDb, mr_id: &str) -> bool {
    match pending_publications(db, Some(mr_id)).await {
        Ok(pending) => !pending.is_empty(),
        Err(error) => {
            // Unreadable is treated as unsettled: reading a real landing as a
            // close is the one error this guard exists to prevent.
            log::warn!("{error}");
            true
        }
    }
}

/// How one recovery ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LandingRecovery {
    /// The landing is on the target and now recorded as this commit.
    Recorded(String),
    /// The publication did not happen and now cannot.
    NotLanded(String),
    /// Nothing conclusive yet; left for the next pass.
    Undetermined(String),
    /// Another call in this process is driving the attempt.
    Busy,
}

async fn settle_publication(
    orch: &Orchestrator,
    db: &LocalDb,
    pending: &PendingPublication,
) -> Result<LandingRecovery, String> {
    let Some(_driving) = InFlight::try_enter(&pending.attempt_id) else {
        return Ok(LandingRecovery::Busy);
    };
    let journal = MergeAttemptJournal {
        attempt_id: &pending.attempt_id,
        project_id: &pending.project_id,
        merge_request_id: &pending.merge_request_id,
        job_id: &pending.job_id,
        target_ref: &pending.target_ref,
        source_ref: &pending.source_ref,
        policy_json: &pending.policy_json,
    };
    let publication = &pending.publication;
    let settle = |state: &'static str, detail: serde_json::Value| async move {
        journal_merge_attempt_event(
            db,
            &journal,
            pending.next_sequence,
            state,
            &JournalEntry {
                detail: Some(detail.to_string()),
                ..JournalEntry::default()
            },
        )
        .await
    };

    let repo_path = db
        .query_opt_text(
            "SELECT repo_path FROM projects WHERE id = ?1",
            params![pending.project_id.as_str()],
        )
        .await
        .map_err(|error| format!("the project is unreadable: {error}"))?
        .ok_or_else(|| format!("project {} is gone", pending.project_id))?;

    // The row was recorded and only the journal's outcome was lost.
    if let Some(landing) = recorded_landing(db, &pending.merge_request_id).await? {
        if landing.commit == publication.landing_commit {
            settle(
                "succeeded",
                serde_json::json!({"reconciled": true, "landedCommit": landing.commit}),
            )
            .await?;
            release_landing_pin(orch, &repo_path, &pending.attempt_id);
            settle_github_best_effort(orch, &pending.job_id).await;
            return Ok(LandingRecovery::Recorded(landing.commit));
        }
    }

    let evidence = remote_publication_evidence(orch, &repo_path, publication).await;
    match evidence {
        LandingEvidence::Landed { commit, .. } => {
            // The source stays reserved, and the store's objects leased, from
            // before the first metadata read until the pin is released: in
            // that order, then the store lock for the re-point. A store whose
            // maintenance holds it, or whose bookmark cannot be read, defers
            // the whole recovery with nothing moved.
            let (_reservation, context, store) =
                reserve_landed_source(orch, db, &pending.job_id).await?;
            let _lease = if crate::jj::is_jj_dir(&store) {
                match crate::jj::StoreObjectLease::for_store(&store) {
                    Ok(lease) => Some(lease),
                    Err(error) => {
                        return Ok(LandingRecovery::Undetermined(format!("deferred: {error}")))
                    }
                }
            } else {
                None
            };
            let source = match recovered_source(&store, publication) {
                Ok(source) => source,
                Err(error) => {
                    return Ok(LandingRecovery::Undetermined(format!(
                        "deferred: the source bookmark is unreadable now ({error})"
                    )))
                }
            };
            repoint_reserved_children(orch, db, &context, &store).await?;
            resolve_pr_node_landed(
                orch,
                &pending.job_id,
                Some(recovery_attribution(db, &pending.attempt_id).await),
                Landed {
                    commit: Some(&commit),
                    source: Some(source),
                    resolved_at: None,
                },
            )
            .await?;
            settle(
                "succeeded",
                serde_json::json!({"reconciled": true, "landedCommit": commit}),
            )
            .await?;
            release_landing_pin(orch, &repo_path, &pending.attempt_id);
            settle_github_best_effort(orch, &pending.job_id).await;
            Ok(LandingRecovery::Recorded(commit))
        }
        LandingEvidence::DefinitelyNotLanded(reason) => {
            settle(
                "failed",
                serde_json::json!({"reconciled": true, "reason": reason}),
            )
            .await?;
            release_landing_pin(orch, &repo_path, &pending.attempt_id);
            Ok(LandingRecovery::NotLanded(reason))
        }
        LandingEvidence::Unknown(reason) => Ok(LandingRecovery::Undetermined(reason)),
    }
}

/// Point the work cut from a recovered landing's source (child jobs' declared
/// base, their open pull requests' target) at the branch it landed on, before
/// the landing is recorded, under the source's reservation and the store lock:
/// the same operation, under the same exclusion, as the in-band landing. Until
/// it succeeds the landing is not recorded and its attempt stays unsettled, so
/// a failure is retried by the next pass rather than forgotten. Meanwhile the
/// children still name the landed branch, and the landing's final check
/// refuses to land them onto it.
async fn repoint_landed_children(
    orch: &Orchestrator,
    db: &LocalDb,
    owner_id: &str,
) -> Result<(), String> {
    let (_reservation, context, store) = reserve_landed_source(orch, db, owner_id).await?;
    repoint_reserved_children(orch, db, &context, &store).await
}

/// Reserve the landed source of `owner_id`'s merge request, returning the
/// reservation, the request's context and its store.
async fn reserve_landed_source(
    orch: &Orchestrator,
    db: &LocalDb,
    owner_id: &str,
) -> Result<
    (
        crate::orchestrator::BranchReservation,
        super::context::MergeMrContext,
        std::path::PathBuf,
    ),
    String,
> {
    let context = super::context::resolve_merge_mr_context_for_job(db, owner_id).await?;
    let store = crate::jj::project_store_dir(
        &orch.config_dir,
        std::path::Path::new(&context.mr.repo_path),
    );
    let reservation = orch
        .reserve_branches(
            &store,
            &[context.source_branch.as_str()],
            "retarget children of a recovered landing",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await?;
    Ok((reservation, context, store))
}

/// Re-point the reserved landed source's children under the store lock.
async fn repoint_reserved_children(
    orch: &Orchestrator,
    db: &LocalDb,
    context: &super::context::MergeMrContext,
    store: &std::path::Path,
) -> Result<(), String> {
    let (source, target) = (
        context.source_branch.as_str(),
        context.target_branch.as_str(),
    );
    let guard = orch
        .acquire_jj_store_lock_with_timeout(
            store,
            crate::orchestrator::StoreLockClass::Foreground,
            format!("repoint children of recovered landing {source}"),
            Some(crate::mcp::vcs::STORE_LOCK_TIMEOUT),
        )
        .await
        .map_err(|()| format!("store busy while repointing children of `{source}`"))?;
    crate::orchestrator::base_advance::repoint_children_of_merged_branch(
        db,
        &guard,
        &context.project_id,
        source,
        target,
    )
    .await
    .map_err(|error| {
        format!("the children of `{source}` were not re-pointed at `{target}`: {error}")
    })
}

/// Release the ref that kept an unsettled publication's landing commit alive
/// (`default_landing::landing_pin_ref`), once its outcome is journaled as
/// terminal. A failure only leaves an unreferenced pin behind, so it is logged;
/// the journal, not the pin, is what says the attempt is settled.
fn release_landing_pin(orch: &Orchestrator, repo_path: &str, attempt_id: &str) {
    let pin = super::default_landing::landing_pin_ref(attempt_id);
    let released = orch.services.git.run(
        std::path::Path::new(repo_path),
        vec!["update-ref".into(), "-d".into(), pin.clone()],
    );
    match released {
        Ok(output) if output.success => {}
        Ok(output) => log::warn!(
            "landing recovery: releasing {pin} failed: {}",
            output.stderr
        ),
        Err(error) => log::warn!("landing recovery: releasing {pin} failed: {error}"),
    }
}

/// The source commit a recovered landing records. The in-band path moves the
/// source bookmark from `source_before` to `source_after` only once origin's
/// acceptance is confirmed, so a landing recovered after a failure may find the
/// bookmark still on the validated head. That head is exactly what the landing
/// published, so it is recorded. Anything else (the moved bookmark, or a
/// project without a store) records `source_after`, and a bookmark the owner
/// has since moved further matches neither and is kept by branch cleanup.
///
/// The read holds the store's object lease, so maintenance cannot reclaim jj
/// metadata under it; a store whose maintenance holds the lease, or whose
/// bookmark cannot be read, is an error the caller defers on, never a guess.
fn recovered_source<'a>(
    store: &std::path::Path,
    publication: &'a Publication,
) -> Result<&'a str, String> {
    if !crate::jj::is_jj_dir(store) {
        return Ok(&publication.source_after);
    }
    let current = crate::jj::read_local_branch_state(store, &publication.source_branch)?.bookmark;
    Ok(
        if current.as_deref() == Some(publication.source_before.as_str()) {
            &publication.source_before
        } else {
            &publication.source_after
        },
    )
}

/// Approval-gated landings published to origin whose ceremony proved them but
/// whose row was never moved: the ceremony settles with the approvals service,
/// and only the in-band merge path records the row.
async fn proven_unrecorded_landings(
    db: &LocalDb,
    mr_id: Option<&str>,
) -> Result<Vec<(String, String)>, String> {
    let route = format!("%\"route\":\"{ORIGIN_ROUTE}\"%");
    let rows = db
        .query_all(
            "SELECT e.job_id, e.detail
               FROM merge_attempt_events e
               JOIN merge_requests m ON m.id = e.merge_request_id
              WHERE e.state = 'succeeded'
                AND m.status IN ('open', 'closed')
                AND (?1 IS NULL OR m.id = ?1)
                AND e.detail LIKE ?2
                AND e.detail LIKE '%\"landedCommit\":\"%'",
            params![mr_id.map(str::to_string), route],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("proven landings are unreadable: {error}"))?;
    Ok(rows
        .into_iter()
        .filter_map(|(job_id, detail)| {
            let value = serde_json::from_str::<serde_json::Value>(&detail).ok()?;
            Some((job_id, value["landedCommit"].as_str()?.to_string()))
        })
        .collect())
}

/// Merged requests whose source still has work cut from it: a live job whose
/// declared base is the source, or an open pull request targeting it. Every
/// landing re-points those before it is recorded, but a re-point that failed
/// after the landing was recorded leaves them behind with nothing else to
/// retry it, so the children themselves are the outstanding fact. A source
/// that is live again (an open pull request from it, or a live job owning it)
/// is a reused name, not a landed branch, and is left alone. Selection is
/// deliberately broad; the re-point applies the canonical live-work predicate
/// and is a no-op where it finds nothing.
async fn landed_sources_with_children(
    db: &LocalDb,
    mr_id: Option<&str>,
) -> Result<Vec<String>, String> {
    db.query_all(
        "SELECT m.job_id FROM merge_requests m
          WHERE m.status = 'merged'
            AND (?1 IS NULL OR m.id = ?1)
            AND m.merged_at = (SELECT MAX(l.merged_at) FROM merge_requests l
                                WHERE l.project_id = m.project_id
                                  AND l.source_branch = m.source_branch
                                  AND l.status = 'merged')
            AND NOT EXISTS (SELECT 1 FROM merge_requests o
                             WHERE o.project_id = m.project_id
                               AND o.source_branch = m.source_branch
                               AND o.status = 'open')
            AND NOT EXISTS (SELECT 1 FROM jobs own
                             WHERE own.project_id = m.project_id
                               AND own.branch = m.source_branch
                               AND own.status NOT IN ('complete', 'failed', 'cancelled'))
            AND (EXISTS (SELECT 1 FROM jobs c
                          WHERE c.project_id = m.project_id
                            AND c.base_branch = m.source_branch
                            AND c.status NOT IN ('complete', 'failed', 'cancelled'))
                 OR EXISTS (SELECT 1 FROM merge_requests c
                             WHERE c.project_id = m.project_id
                               AND c.target_branch = m.source_branch
                               AND c.status = 'open'))",
        params![mr_id.map(str::to_string)],
        |row| row.text(0),
    )
    .await
    .map_err(|error| format!("landed sources are unreadable: {error}"))
}

async fn reconcile(
    orch: &Orchestrator,
    db: &LocalDb,
    mr_id: Option<&str>,
) -> Result<Vec<LandingRecovery>, String> {
    let mut outcomes = Vec::new();
    for pending in pending_publications(db, mr_id).await? {
        let outcome = settle_publication(orch, db, &pending).await;
        match &outcome {
            Ok(outcome) => log::info!(
                "landing recovery: publication {} of {}: {outcome:?}",
                pending.attempt_id,
                short(&pending.publication.landing_commit)
            ),
            Err(error) => log::warn!(
                "landing recovery: publication {}: {error}",
                pending.attempt_id
            ),
        }
        if let Ok(outcome) = outcome {
            outcomes.push(outcome);
        }
    }
    for (job_id, commit) in proven_unrecorded_landings(db, mr_id).await? {
        if let Err(error) = repoint_landed_children(orch, db, &job_id).await {
            log::warn!("landing recovery: proven landing of {job_id} left unrecorded: {error}");
            continue;
        }
        let attribution = PrResolutionAttribution::internal(RECOVERY_SURFACE, job_id.clone());
        match resolve_pr_node(
            orch,
            &job_id,
            PrNodeResolution::Merge,
            Some(attribution),
            Some(&commit),
        )
        .await
        {
            Ok(()) => {
                settle_github_best_effort(orch, &job_id).await;
                outcomes.push(LandingRecovery::Recorded(commit));
            }
            Err(error) => log::warn!("landing recovery: proven landing of {job_id}: {error}"),
        }
    }
    // After the attempts above, so a landing recovered in this pass has
    // already re-pointed its children and is not selected again.
    for owner_id in landed_sources_with_children(db, mr_id).await? {
        match repoint_landed_children(orch, db, &owner_id).await {
            Ok(()) => log::info!(
                "landing recovery: re-pointed work left on the landed source of {owner_id}"
            ),
            Err(error) => log::warn!(
                "landing recovery: work cut from the landed source of {owner_id} is still not \
                 re-pointed (retried next pass): {error}"
            ),
        }
    }
    Ok(outcomes)
}

/// Settle every publication whose outcome Cairn does not know, and record
/// every proven landing whose row was never moved. Runs at startup and on a
/// slow timer; an attempt still being driven in this process is left alone.
pub async fn reconcile_pending_landings(orch: &Orchestrator) -> Vec<LandingRecovery> {
    match reconcile(orch, &orch.db.local, None).await {
        Ok(outcomes) => outcomes,
        Err(error) => {
            log::warn!("landing recovery: {error}");
            Vec::new()
        }
    }
}

/// [`reconcile_pending_landings`] for one merge request, as its refresh runs it.
pub(super) async fn reconcile_landing_for(
    orch: &Orchestrator,
    db: &LocalDb,
    mr_id: &str,
) -> Result<Vec<LandingRecovery>, String> {
    reconcile(orch, db, Some(mr_id)).await
}

#[cfg(test)]
mod tests {
    use super::super::default_landing::PublicationInFlight;
    use super::*;
    use crate::db::DbState;
    use crate::execution::gates::vocabulary::{
        GateContext, GateFact, GateOutcome, IssueResolutionFact, PrStateFact,
    };
    use crate::models::PrState;
    use crate::routes::TriggerClause;
    use crate::services::testing::{MockGitClient, MockHttpClient, TestServicesBuilder};
    use crate::services::{HttpClient, HttpMethod, HttpResponse};
    use crate::storage::SearchIndex;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    const TEST_KEY: &str = include_str!("../../../tests/fixtures/test_rsa_key.pem");
    const PR: i32 = 43;

    fn sha(c: char) -> String {
        c.to_string().repeat(40)
    }

    // ── A GitHub the test can still read after handing it over ──

    type Fut<'a> = Pin<Box<dyn Future<Output = Result<HttpResponse, String>> + Send + 'a>>;

    #[derive(Clone)]
    struct Shared(Arc<MockHttpClient>);

    impl HttpClient for Shared {
        fn get(&self, url: &str, headers: reqwest::header::HeaderMap) -> Fut<'_> {
            self.0.get(url, headers)
        }
        fn post(
            &self,
            url: &str,
            body: serde_json::Value,
            h: reqwest::header::HeaderMap,
        ) -> Fut<'_> {
            self.0.post(url, body, h)
        }
        fn put(
            &self,
            url: &str,
            body: serde_json::Value,
            h: reqwest::header::HeaderMap,
        ) -> Fut<'_> {
            self.0.put(url, body, h)
        }
        fn patch(
            &self,
            url: &str,
            body: serde_json::Value,
            h: reqwest::header::HeaderMap,
        ) -> Fut<'_> {
            self.0.patch(url, body, h)
        }
        fn delete(&self, url: &str, headers: reqwest::header::HeaderMap) -> Fut<'_> {
            self.0.delete(url, headers)
        }
    }

    impl Shared {
        fn count(&self, method: HttpMethod, url_contains: &str) -> usize {
            self.0
                .requests()
                .iter()
                .filter(|r| r.method == method && r.url.contains(url_contains))
                .count()
        }
        fn posted_comments(&self) -> Vec<String> {
            self.0
                .requests()
                .into_iter()
                .filter(|r| r.method == HttpMethod::Post && r.url.ends_with("/comments"))
                .filter_map(|r| r.body?["body"].as_str().map(str::to_string))
                .collect()
        }
    }

    fn json(status: u16, value: serde_json::Value) -> HttpResponse {
        HttpResponse::new(status, serde_json::to_vec(&value).unwrap())
    }

    /// What the mocked GitHub reports. Patterns resolve first-match, so the
    /// specific ones come before `/pulls/43`, which every PR URL contains.
    struct World {
        pr_state: &'static str,
        pr_merged: bool,
        comments: Vec<String>,
        /// origin's `main`; `None` makes the ref unreadable.
        origin_main: Option<String>,
        /// `(commit, parents, tree)` for every commit the proof may read.
        commits: Vec<(String, Vec<String>, String)>,
    }

    impl World {
        fn github(&self) -> Shared {
            let mut http = MockHttpClient::new()
                .respond_to(
                    "access_tokens",
                    json(201, serde_json::json!({"token": "ghs_test", "expires_at": "2099-01-01T00:00:00Z"})),
                )
                .respond_to("/pulls/43/reviews", json(200, serde_json::json!([])))
                .respond_to("/pulls/43/files", json(200, serde_json::json!([])))
                .respond_to("/check-runs", json(200, serde_json::json!({"check_runs": []})))
                .respond_to(
                    "comments?per_page",
                    json(
                        200,
                        serde_json::Value::Array(
                            self.comments
                                .iter()
                                .map(|body| serde_json::json!({"body": body}))
                                .collect(),
                        ),
                    ),
                )
                .respond_to("/issues/43/comments", json(201, serde_json::json!({})));
            if let Some(main) = &self.origin_main {
                http = http.respond_to(
                    "/git/ref/heads/main",
                    json(200, serde_json::json!({"object": {"sha": main}})),
                );
            }
            for (commit, parents, tree) in &self.commits {
                http = http.respond_to(
                    &format!("/commits/{commit}"),
                    json(
                        200,
                        serde_json::json!({
                            "parents": parents.iter().map(|p| serde_json::json!({"sha": p})).collect::<Vec<_>>(),
                            "commit": {"tree": {"sha": tree}}
                        }),
                    ),
                );
            }
            let pr = json(
                200,
                serde_json::json!({
                    "title": "My PR", "body": "A description", "state": self.pr_state,
                    "draft": false, "mergeable": null, "mergeable_state": "unknown",
                    "additions": 1, "deletions": 0, "merged": self.pr_merged,
                    "merge_commit_sha": null, "head": {"sha": sha('h')}
                }),
            );
            Shared(Arc::new(http.respond_to("/pulls/43", pr)))
        }
    }

    // ── One remote pull request on issue gh/1 ──

    struct Fixture {
        db: Arc<LocalDb>,
        _repo: tempfile::TempDir,
        cfg: tempfile::TempDir,
        /// Every `git` invocation the orchestrators made, by argument list.
        git_runs: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
        job: &'static str,
        action_run: &'static str,
    }

    const SNAPSHOT: &str = r#"{"recipe":{"id":"recipe-1","name":"R","description":null,"trigger":"manual","nodes":[],"edges":[]},"agents":{},"skills":{},"triggerContext":{"issueId":"issue-gh","projectId":"proj-gh","triggerType":"manual"},"createdAt":1}"#;

    async fn fixture(name: &str) -> Fixture {
        let repo = tempfile::tempdir().unwrap();
        for args in [
            vec!["init", "-b", "main", "-q"],
            vec![
                "remote",
                "add",
                "origin",
                "https://github.com/octo/widget.git",
            ],
        ] {
            let status = std::process::Command::new("git")
                .args(&args)
                .current_dir(repo.path())
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        }
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        db.execute(
            "INSERT INTO github_app (id, app_id, private_key, installation_id) VALUES ('default', 123, ?1, 999)",
            params![TEST_KEY],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj-gh', 'default', 'P', 'gh', ?1, 'main', 1, 1)",
            params![repo.path().to_str().unwrap()],
        )
        .await
        .unwrap();
        db.execute_script(format!(
            "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES ('issue-gh', 'proj-gh', 1, 'Issue', 'active', 'idle', 'none', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
               VALUES ('exec-gh', 'recipe-1', 'issue-gh', 'proj-gh', 'running', 1, 1, '{SNAPSHOT}');
             INSERT INTO jobs (id, project_id, issue_id, execution_id, status, branch, created_at, updated_at)
               VALUES ('job-gh', 'proj-gh', 'issue-gh', 'exec-gh', 'complete', 'feature', 1, 1);
             INSERT INTO action_runs (id, execution_id, recipe_node_id, action_config_id, project_id, status, created_at, parent_job_id, uri_segment)
               VALUES ('ar-gh', 'exec-gh', 'pr-1', 'builtin:pr', 'proj-gh', 'blocked', 1, 'job-gh', 'pr');
             INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                                         status, merge_method, opened_at, updated_at, is_local, github_pr_number, github_pr_url)
               VALUES ('mr-gh', 'job-gh', 'proj-gh', 'issue-gh', 'PR', 'feature', 'main', 'open', 'squash', 1, 1, 0,
                       {PR}, 'https://github.com/octo/widget/pull/{PR}');"
        ))
        .await
        .unwrap();
        Fixture {
            db,
            _repo: repo,
            cfg: tempfile::tempdir().unwrap(),
            git_runs: Arc::default(),
            job: "job-gh",
            action_run: "ar-gh",
        }
    }

    impl Fixture {
        /// A fresh orchestrator (and so a fresh PR snapshot cache) over the
        /// same database, talking to `github`.
        fn orch(&self, github: &Shared) -> Orchestrator {
            let mut git = MockGitClient::new();
            git.expect_status()
                .times(0..)
                .returning(|_| Ok(String::new()));
            let runs = self.git_runs.clone();
            git.expect_run().times(0..).returning(move |_, args| {
                runs.lock().unwrap().push(args);
                Ok(crate::services::GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });
            let root = self.cfg.path().join(uuid::Uuid::new_v4().to_string());
            let state = Arc::new(DbState::new(
                self.db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            ));
            let services = TestServicesBuilder::new()
                .with_http(github.clone())
                .with_git(git)
                .build();
            Orchestrator::builder(state, Arc::new(services), root.join("config")).build()
        }

        async fn text(&self, sql: &'static str) -> Option<String> {
            self.db.query_opt_text(sql, ()).await.unwrap()
        }

        /// How many times the landing pin of `attempt-1` was released.
        fn pin_releases(&self) -> usize {
            let pin = super::super::default_landing::landing_pin_ref("attempt-1");
            self.git_runs
                .lock()
                .unwrap()
                .iter()
                .filter(|args| args[..] == ["update-ref".to_string(), "-d".into(), pin.clone()])
                .count()
        }

        async fn mr(&self) -> (String, Option<String>) {
            self.db
                .query_opt(
                    "SELECT status, merged_commit FROM merge_requests WHERE id = 'mr-gh'",
                    (),
                    |row| Ok((row.text(0)?, row.opt_text(1)?)),
                )
                .await
                .unwrap()
                .unwrap()
        }

        async fn issue(&self) -> String {
            self.text("SELECT status FROM issues WHERE id = 'issue-gh'")
                .await
                .unwrap()
        }

        async fn journal_states(&self) -> Vec<String> {
            self.journal_states_of("attempt-1").await
        }

        async fn journal_states_of(&self, attempt_id: &str) -> Vec<String> {
            self.db
                .query_all(
                    "SELECT state FROM merge_attempt_events WHERE attempt_id = ?1 ORDER BY sequence",
                    params![attempt_id.to_string()],
                    |row| row.text(0),
                )
                .await
                .unwrap()
        }

        async fn close_attributions(&self) -> String {
            self.text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM pr_resolution_attributions
                  WHERE merge_request_id = 'mr-gh' AND action = 'close'",
            )
            .await
            .unwrap()
        }

        async fn journal_intent(&self, publication: &Publication) {
            self.journal_intent_as("attempt-1", publication).await;
        }

        /// The in-flight set is process-wide, so a test that holds an attempt
        /// in flight journals it under an id no other test uses.
        async fn journal_intent_as(&self, attempt_id: &str, publication: &Publication) {
            let attempt = MergeAttemptJournal {
                attempt_id,
                project_id: "proj-gh",
                merge_request_id: "mr-gh",
                job_id: self.job,
                target_ref: "refs/heads/main",
                source_ref: "refs/heads/feature",
                policy_json: "{}",
            };
            journal_publication_intent(&self.db, &attempt, publication)
                .await
                .unwrap();
        }
    }

    fn gate(fact: &str, field: &str, value: &str) -> TriggerClause {
        [("fact", fact), ("issue", "cairn://p/gh/1"), (field, value)]
            .into_iter()
            .map(|(key, value)| (key.to_string(), serde_json::Value::String(value.into())))
            .collect()
    }

    /// Merge state as everything downstream of the record decides on it: the
    /// two gate facts (through issue-URI resolution), the issue row the census
    /// and sidebar read, the rendered PR artifact, and the cached PR read.
    async fn assert_merged_everywhere(fx: &Fixture, orch: &Orchestrator, landing: &str) {
        let ctx = GateContext::new(orch, 0);
        assert_eq!(
            PrStateFact
                .resolve(&ctx, &gate("pr_state", "state", "merged"))
                .await
                .unwrap(),
            GateOutcome::Satisfied
        );
        assert_eq!(
            IssueResolutionFact
                .resolve(&ctx, &gate("issue_resolution", "resolution", "merged"))
                .await
                .unwrap(),
            GateOutcome::Satisfied
        );
        assert_eq!(fx.mr().await, ("merged".into(), Some(landing.to_string())));
        assert_eq!(fx.issue().await, "merged");
        let section =
            super::super::render_live_pr_section(orch, fx.action_run, "cairn://p/gh/1/1/pr", false)
                .await
                .unwrap();
        assert!(section.contains("State: MERGED"), "{section}");
        let cached = crate::merge_requests::queries::get_by_action_run_id(&fx.db, fx.action_run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached.state, PrState::Merged, "the cached read");
        assert_eq!(fx.close_attributions().await, "0", "no close was journaled");
    }

    /// The accepted landing records the row (and through it the issue and the
    /// gates), then tells GitHub once. A later refresh that sees GitHub's
    /// "closed, not merged" changes nothing: the record outranks the cache.
    #[tokio::test]
    async fn a_recorded_landing_stays_merged_when_github_shows_the_pr_closed() {
        let fx = fixture("landing-record-closed.db").await;
        let landing = sha('l');
        let publication = Publication {
            source_after: landing.clone(),
            landing_commit: landing.clone(),
            ..publication()
        };
        let open = World {
            pr_state: "open",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&open);
        // The merge path's tail for an accepted publication: record, then
        // settle GitHub.
        resolve_pr_node_landed(
            &orch,
            fx.job,
            Some(PrResolutionAttribution::operator_ui()),
            Landed {
                commit: Some(&publication.landing_commit),
                source: Some(&publication.source_after),
                resolved_at: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            settle_github_after_landing(&orch, fx.job).await.unwrap(),
            GitHubSettlement::Settled {
                commented: true,
                closed: true
            }
        );
        let comments = open.posted_comments();
        assert_eq!(comments.len(), 1, "one explanatory comment");
        assert!(comments[0].contains(&landing) && comments[0].contains(&landing_marker(&landing)));
        assert_eq!(
            open.count(HttpMethod::Patch, "/pulls/43"),
            1,
            "the PR is closed"
        );

        let closed = World {
            pr_state: "closed",
            pr_merged: false,
            comments: comments.clone(),
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        assert_eq!(
            fx.text("SELECT merged_source_commit FROM merge_requests WHERE id = 'mr-gh'")
                .await
                .as_deref(),
            Some(landing.as_str()),
            "the record names the source commit cleanup may rely on"
        );
        let orch = fx.orch(&closed);
        let cache = crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_eq!(cache.state, PrState::Merged);
        assert_merged_everywhere(&fx, &orch, &landing).await;
        assert!(
            closed.posted_comments().is_empty(),
            "a settled PR is not commented again"
        );
        assert_eq!(closed.count(HttpMethod::Patch, "/pulls/43"), 0);
    }

    /// A retry of the GitHub settlement finds its own comment and posts none;
    /// a merged row GitHub still shows open is closed by the next refresh.
    #[tokio::test]
    async fn github_settlement_is_idempotent_and_retried_by_refresh() {
        let fx = fixture("landing-record-settle.db").await;
        let landing = sha('l');
        fx.db
            .execute(
                "UPDATE merge_requests SET status = 'merged', merged_commit = ?1, merged_at = 5 WHERE id = 'mr-gh'",
                params![landing.as_str()],
            )
            .await
            .unwrap();
        let github = World {
            pr_state: "open",
            pr_merged: false,
            comments: vec![format!("Landed. {}", landing_marker(&landing))],
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&github);
        let cache = crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_eq!(
            cache.state,
            PrState::Merged,
            "open on GitHub, merged in Cairn"
        );
        assert!(
            github.posted_comments().is_empty(),
            "the marker is already there"
        );
        assert_eq!(
            github.count(HttpMethod::Patch, "/pulls/43"),
            1,
            "refresh closed it"
        );

        let merged_on_github = World {
            pr_state: "closed",
            pr_merged: true,
            comments: Vec::new(),
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&merged_on_github);
        assert_eq!(
            settle_github_after_landing(&orch, fx.job).await.unwrap(),
            GitHubSettlement::MergedOnGitHub,
            "a PR GitHub itself marks merged needs no explanation"
        );
        assert!(merged_on_github.posted_comments().is_empty());
    }

    /// A pull request closed on GitHub with no landing behind it is closed,
    /// never promoted to merged.
    #[tokio::test]
    async fn an_unrecorded_closed_pr_is_closed_not_merged() {
        let fx = fixture("landing-record-unmerged.db").await;
        let github = World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&github);
        let cache = crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_eq!(cache.state, PrState::Closed);
        assert_eq!(fx.mr().await.0, "closed");
        assert_eq!(fx.issue().await, "closed");
        let ctx = GateContext::new(&orch, 0);
        assert_eq!(
            PrStateFact
                .resolve(&ctx, &gate("pr_state", "state", "merged"))
                .await
                .unwrap(),
            GateOutcome::Refuted
        );
        assert!(github.posted_comments().is_empty());
    }

    /// The push was accepted and recording it failed. Someone then closes the
    /// PR on GitHub. The refresh must not resolve it as a close: it recovers
    /// the landing from origin's history and records it as merged, once.
    #[tokio::test]
    async fn a_published_landing_whose_record_failed_is_recovered_not_closed() {
        let fx = fixture("landing-record-recover.db").await;
        let (old, landing, tree) = (sha('t'), sha('l'), sha('e'));
        let publication = Publication {
            expected_old: old.clone(),
            landing_commit: landing.clone(),
            landing_tree: tree.clone(),
            source_after: landing.clone(),
            ..publication()
        };
        fx.journal_intent(&publication).await;
        // Live work cut from the landing's source, on another issue.
        fx.db
            .execute_script(
                "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
                   VALUES ('issue-child', 'proj-gh', 2, 'Child', 'active', 'idle', 'none', 1, 1);
                 INSERT INTO jobs (id, project_id, issue_id, status, branch, base_branch, created_at, updated_at)
                   VALUES ('job-child', 'proj-gh', 'issue-child', 'running', 'child', 'feature', 1, 1);",
            )
            .await
            .unwrap();
        // origin moved on past the landing by one more commit.
        let later = sha('n');
        let github = World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: Some(later.clone()),
            commits: vec![
                (later, vec![landing.clone()], sha('f')),
                (landing.clone(), vec![old], tree),
            ],
        }
        .github();
        let orch = fx.orch(&github);
        crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();

        assert_merged_everywhere(&fx, &orch, &landing).await;
        assert_eq!(
            fx.text("SELECT base_branch FROM jobs WHERE id = 'job-child'")
                .await
                .as_deref(),
            Some("main"),
            "work cut from the landed source now follows the branch it landed on"
        );
        assert_eq!(fx.journal_states().await, ["executing", "succeeded"]);
        assert_eq!(
            fx.text(
                "SELECT surface FROM pr_resolution_attributions WHERE merge_request_id = 'mr-gh'"
            )
            .await
            .as_deref(),
            Some(RECOVERY_SURFACE)
        );
        assert_eq!(
            fx.text("SELECT merged_source_commit FROM merge_requests WHERE id = 'mr-gh'")
                .await
                .as_deref(),
            Some(landing.as_str()),
            "with no store to say otherwise, the published source is recorded"
        );
        assert_eq!(github.posted_comments().len(), 1, "explained once");
        assert_eq!(
            github.count(HttpMethod::Patch, "/pulls/43"),
            0,
            "already closed on GitHub"
        );
        assert!(
            reconcile_pending_landings(&orch).await.is_empty(),
            "nothing left"
        );
        assert_eq!(
            fx.pin_releases(),
            1,
            "the recorded attempt's pin is released once"
        );
    }

    /// A workspace pull request's landing keeps its own commits F and L above
    /// the old target T. Its record failed after origin accepted it, and the
    /// PR is closed on GitHub. Journaled as the fast-forward it was, the proof
    /// walks origin's history to L with its tree and records the merge. The
    /// same history journaled as a squash is judged at F, the first commit on
    /// T, whose tree is not the landing's: "not landed", settled failed, which
    /// would let the next refresh close a merged PR.
    #[tokio::test]
    async fn a_preserved_history_landing_is_recovered_by_its_fast_forward_shape() {
        let (old, first, landing, tree) = (sha('t'), sha('c'), sha('l'), sha('e'));
        let history = || World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: Some(landing.clone()),
            commits: vec![
                (landing.clone(), vec![first.clone()], tree.clone()),
                (first.clone(), vec![old.clone()], sha('g')),
            ],
        };
        let preserved = |shape| Publication {
            expected_old: old.clone(),
            target_before: old.clone(),
            landing_commit: landing.clone(),
            landing_tree: tree.clone(),
            source_after: landing.clone(),
            shape,
            ..publication()
        };

        let fx = fixture("landing-record-fast-forward.db").await;
        fx.journal_intent(&preserved(PublicationShape::FastForward))
            .await;
        let github = history().github();
        let orch = fx.orch(&github);
        crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_merged_everywhere(&fx, &orch, &landing).await;
        assert_eq!(fx.journal_states().await, ["executing", "succeeded"]);

        let fx = fixture("landing-record-fast-forward-as-squash.db").await;
        fx.journal_intent(&preserved(PublicationShape::Squash))
            .await;
        let github = history().github();
        let orch = fx.orch(&github);
        crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_eq!(
            fx.journal_states().await,
            ["executing", "failed"],
            "a squash proof judges the preserved history not landed"
        );
        assert_eq!(fx.mr().await.0, "open");
    }

    /// A landing recorded while its in-band re-point of children failed leaves
    /// nothing unsettled in the journal. The children themselves are the
    /// outstanding fact: the next pass re-points them; a branch name reused by
    /// live work again is left alone.
    #[tokio::test]
    async fn children_left_on_a_landed_source_are_re_pointed_by_the_next_pass() {
        let fx = fixture("landing-record-orphaned-children.db").await;
        fx.db
            .execute_script(
                "UPDATE merge_requests SET status = 'merged', merged_at = 5, merged_commit = 'l'
                  WHERE id = 'mr-gh';
                 INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
                   VALUES ('issue-child', 'proj-gh', 2, 'Child', 'active', 'idle', 'none', 1, 1);
                 INSERT INTO jobs (id, project_id, issue_id, status, branch, base_branch, created_at, updated_at)
                   VALUES ('job-child', 'proj-gh', 'issue-child', 'running', 'child', 'feature', 1, 1);",
            )
            .await
            .unwrap();
        let github = World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&github);
        let child_base = "SELECT base_branch FROM jobs WHERE id = 'job-child'";

        // The branch name is live again: an open request from `feature`.
        fx.db
            .execute_script(
                "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                                             status, merge_method, opened_at, updated_at, is_local)
                   VALUES ('mr-reused', 'job-reused', 'proj-gh', 'issue-child', 'Again', 'feature', 'main',
                           'open', 'squash', 9, 9, 1);",
            )
            .await
            .unwrap();
        reconcile_pending_landings(&orch).await;
        assert_eq!(
            fx.text(child_base).await.as_deref(),
            Some("feature"),
            "a reused name is not touched"
        );

        fx.db
            .execute("DELETE FROM merge_requests WHERE id = 'mr-reused'", ())
            .await
            .unwrap();
        reconcile_pending_landings(&orch).await;
        assert_eq!(
            fx.text(child_base).await.as_deref(),
            Some("main"),
            "work left on the landed source follows the branch it landed on"
        );
    }

    /// While origin cannot be read the publication stays unsettled, and a
    /// closed PR is still not taken as "closed without merging".
    #[tokio::test]
    async fn an_unreadable_origin_leaves_the_landing_unsettled_and_the_row_open() {
        let fx = fixture("landing-record-unknown.db").await;
        fx.journal_intent(&publication()).await;
        let github = World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&github);
        crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_eq!(fx.mr().await.0, "open");
        assert_eq!(fx.issue().await, "active");
        assert_eq!(fx.journal_states().await, ["executing"]);
        assert_eq!(fx.close_attributions().await, "0");
        assert_eq!(fx.pin_releases(), 0, "an unsettled landing keeps its pin");
    }

    /// A publication origin never took is settled `failed`, after which the
    /// closed PR resolves as an ordinary close.
    #[tokio::test]
    async fn a_publication_origin_never_took_settles_failed_then_closes() {
        let fx = fixture("landing-record-not-landed.db").await;
        let old = sha('t');
        fx.journal_intent(&publication()).await;
        let github = World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: Some(old),
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&github);
        crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_eq!(fx.journal_states().await, ["executing", "failed"]);
        assert_eq!(
            fx.pin_releases(),
            1,
            "a definitely-not-landed attempt releases its pin"
        );
        assert_eq!(
            fx.mr().await.0,
            "open",
            "the recovering refresh decided nothing else"
        );

        crate::pr_data::actions::refresh_pr_for_job(&orch, fx.job)
            .await
            .unwrap();
        assert_eq!(fx.mr().await.0, "closed");
        assert_eq!(fx.issue().await, "closed");
        assert!(github.posted_comments().is_empty());
    }

    /// A landing still driven by its merge command (intent journaled, outcome
    /// not yet) is left to that command: recovery reports it `Busy` and writes
    /// nothing, even with the PR closed on GitHub. Once the mark is released,
    /// recovery takes the attempt up again.
    #[tokio::test]
    async fn a_publication_its_landing_still_drives_is_left_alone_by_recovery() {
        let fx = fixture("landing-record-in-flight.db").await;
        let attempt = "in-flight-merge-publication-1";
        fx.journal_intent_as(attempt, &publication()).await;
        let github = World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: None,
            commits: Vec::new(),
        }
        .github();
        let orch = fx.orch(&github);

        let driving = PublicationInFlight::enter(attempt).unwrap();
        assert_eq!(
            reconcile_pending_landings(&orch).await,
            [LandingRecovery::Busy]
        );
        assert_eq!(fx.journal_states_of(attempt).await, ["executing"]);
        assert_eq!(fx.mr().await, ("open".to_string(), None));
        assert_eq!(fx.close_attributions().await, "0");
        assert!(
            PublicationInFlight::enter(attempt).is_err(),
            "a second driver of the same publication is refused"
        );

        drop(driving);
        assert!(
            matches!(
                reconcile_pending_landings(&orch).await[..],
                [LandingRecovery::Undetermined(_)]
            ),
            "released, the attempt is recovery's again"
        );
    }

    /// Recovery records a landing as merged by whoever started the merge (the
    /// principal journaled at sequence 0 of the base attempt), through the
    /// recovery surface, not by the publication's attempt id.
    #[tokio::test]
    async fn a_recovered_landing_is_attributed_to_the_principal_that_started_the_merge() {
        let fx = fixture("landing-record-attribution.db").await;
        let (old, landing, tree) = (sha('t'), sha('l'), sha('e'));
        let publication = Publication {
            expected_old: old.clone(),
            landing_commit: landing.clone(),
            landing_tree: tree.clone(),
            source_after: landing.clone(),
            ..publication()
        };
        fx.db
            .execute(
                "INSERT INTO merge_attempt_events (
                    event_id, attempt_id, sequence, project_id, merge_request_id, job_id, state,
                    target_ref, source_ref, policy_json, authority_decision_json, created_at
                 ) VALUES ('attributed-merge', 'attributed-merge', 0, 'proj-gh', 'mr-gh',
                           'job-gh', 'policy_disabled', 'main', 'feature', '{}', ?1, 1)",
                params![serde_json::json!({
                    "legacyActorKind": "operator-ui",
                    "legacyActorIdentity": null,
                    "surface": "operator-ui",
                })
                .to_string()],
            )
            .await
            .unwrap();
        fx.journal_intent_as("attributed-merge-publication-1", &publication)
            .await;
        let github = World {
            pr_state: "closed",
            pr_merged: false,
            comments: Vec::new(),
            origin_main: Some(landing.clone()),
            commits: vec![(landing.clone(), vec![old], tree)],
        }
        .github();
        let orch = fx.orch(&github);

        assert_eq!(
            reconcile_pending_landings(&orch).await,
            [LandingRecovery::Recorded(landing.clone())]
        );
        let attribution = fx
            .db
            .query_opt(
                "SELECT actor_kind, actor_identity, surface FROM pr_resolution_attributions
                  WHERE merge_request_id = 'mr-gh' AND action = 'merge'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?, row.text(2)?)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            attribution,
            (
                "operator-ui".to_string(),
                None,
                RECOVERY_SURFACE.to_string()
            )
        );
    }

    #[test]
    fn a_publication_attempt_names_the_merge_attempt_it_belongs_to() {
        assert_eq!(base_attempt_id("b767751a-publication-1"), "b767751a");
        assert_eq!(base_attempt_id("b767751a-publication-12"), "b767751a");
        assert_eq!(base_attempt_id("attempt-1"), "attempt-1");
        assert_eq!(
            base_attempt_id("x-publication-final"),
            "x-publication-final"
        );
    }

    #[test]
    fn an_unknown_initiating_actor_is_not_attributed() {
        assert_eq!(
            initiating_attribution(
                r#"{"legacyActorKind":"agent","legacyActorIdentity":"cairn://p/x/1/1/builder"}"#
            ),
            Some(PrResolutionAttribution {
                actor_kind: "agent",
                actor_identity: Some("cairn://p/x/1/1/builder".into()),
                surface: RECOVERY_SURFACE,
            })
        );
        assert_eq!(
            initiating_attribution(r#"{"legacyActorKind":"someone"}"#),
            None
        );
        assert_eq!(initiating_attribution("not json"), None);
    }

    fn publication() -> Publication {
        Publication {
            target_branch: "main".into(),
            expected_old: "t".repeat(40),
            target_before: "t".repeat(40),
            landing_commit: "l".repeat(40),
            landing_tree: "e".repeat(40),
            source_branch: "feature".into(),
            source_before: "h".repeat(40),
            source_after: "l".repeat(40),
            shape: PublicationShape::Squash,
        }
    }

    /// An intent journaled before `shape` existed reads as a squash.
    #[test]
    fn an_intent_without_a_shape_is_a_squash() {
        let mut value = serde_json::to_value(PublicationDetail {
            publication: publication(),
        })
        .unwrap();
        let legacy = value["publication"].as_object_mut().unwrap();
        legacy.remove("shape");
        legacy.remove("targetBefore");
        let parsed = parse_intent(&value.to_string()).unwrap();
        assert_eq!(parsed.shape, PublicationShape::Squash);
        assert_eq!(
            parsed.target_before, parsed.expected_old,
            "a legacy intent was prepared on origin's target"
        );
    }

    /// The store's target and origin's are recorded separately, and both
    /// survive the journal.
    #[test]
    fn store_and_origin_targets_round_trip_separately() {
        let publication = Publication {
            target_before: sha('s'),
            ..publication()
        };
        let parsed = parse_intent(&intent_detail(&publication).unwrap()).unwrap();
        assert_eq!(parsed.target_before, sha('s'));
        assert_eq!(parsed.expected_old, "t".repeat(40));
    }

    /// A fast-forward's landing is the prepared head, several commits above the
    /// expected old target: recovery finds that commit itself, rather than
    /// judging the first commit above the target as a squash would (and
    /// wrongly finding "something else landed").
    #[test]
    fn a_fast_forward_is_recognised_by_its_landing_commit_not_the_first_commit_above_the_base() {
        let ff = Publication {
            shape: PublicationShape::FastForward,
            landing_commit: sha('l'),
            landing_tree: sha('e'),
            expected_old: sha('t'),
            ..publication()
        };
        let shape = |parents: &[String], tree: &str| LandedShape {
            parents: parents.to_vec(),
            tree: tree.to_string(),
        };
        // Above the landing, keep walking.
        assert!(matches!(
            fast_forward_step(sha('n'), shape(&[sha('l')], &sha('f')), &ff),
            HistoryStep::Next(next) if next == sha('l')
        ));
        // The landing with its tree.
        assert!(matches!(
            fast_forward_step(sha('l'), shape(&[sha('c')], &sha('e')), &ff),
            HistoryStep::Done(LandingEvidence::Landed { commit, .. }) if commit == sha('l')
        ));
        // C1, the stack's first commit above the base, is not the landing and
        // not evidence against it when reached from above...
        assert!(
            matches!(
                fast_forward_step(sha('c'), shape(&[sha('t')], &sha('g')), &ff),
                HistoryStep::Done(LandingEvidence::DefinitelyNotLanded(_))
            ),
            "reached without passing L, it is someone else's commit on T"
        );
        // The landing commit with a different tree is not this landing.
        assert!(matches!(
            fast_forward_step(sha('l'), shape(&[sha('c')], &sha('x')), &ff),
            HistoryStep::Done(LandingEvidence::DefinitelyNotLanded(_))
        ));
    }

    /// The journal queries find an intent by its detail's prefix, so the
    /// serialized shape is a contract, not a formatting detail.
    #[test]
    fn an_intent_serializes_under_the_prefix_the_journal_queries_match() {
        let detail = intent_detail(&publication()).unwrap();
        assert!(detail.starts_with("{\"publication\":"), "{detail}");
        assert_eq!(parse_intent(&detail), Some(publication()));
    }

    /// The comment names the full landing commit (GitHub links it), says why
    /// the pull request is closed rather than merged, and carries the marker
    /// that keeps a retry from posting it twice.
    #[test]
    fn the_landing_comment_names_the_commit_and_explains_the_closed_state() {
        let landing = RecordedLanding {
            commit: "l".repeat(40),
            target_branch: "main".into(),
            expected_old: Some("t".repeat(40)),
        };
        let comment = landing_comment(&landing);
        assert!(comment.contains(&format!("Landed on `main` as {}", "l".repeat(40))));
        assert!(comment.contains(&format!("fast-forward from {}", "t".repeat(40))));
        assert!(comment.contains("shows as closed rather than merged"));
        assert!(comment.contains(&landing_marker(&"l".repeat(40))));
    }
}
