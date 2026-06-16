//! Typed attention events emitted on actionable facts.
//!
//! `attention_changed` carries `AttentionEvent`s, not bare issue ids. Each emit
//! corresponds to a discrete actionable fact (a question lands, a permission is
//! requested, an artifact is written, an agent terminalizes with work
//! remaining, a PR state change, a terminal resolution) and carries the
//! content the long-poll subscriber needs in a single round-trip.
//!
//! Emit through [`Orchestrator::emit_attention_event`] (declared on
//! `Orchestrator`), which consults a short-window dedupe cache so artifact
//! bursts collapse into a single wake. The cache is keyed by issue + fact-kind
//! + detail-uri, so distinct facts always pass through (artifact-then-idle,
//!   for example, is two emits).

use serde::{Deserialize, Serialize};
use turso::params;

use crate::mcp::types::Question;
use crate::messages::queued::DeliveryUrgency;
use crate::models::{IssueAttention, IssueStatus};
use crate::storage::{DbError, LocalDb, RowExt};

/// Discrete actionable fact that drove this emit.
///
/// Variants carry per-fact inline content so the watch long-poll can return a
/// usable response without a follow-up `read`. The variant set is closed: the
/// CLI formatter and the watch handler match exhaustively.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttentionFact {
    /// A question was stored for the user. Full questions payload is inline
    /// (always small enough for the wire).
    Question {
        #[serde(default)]
        escalate: bool,
        detail_uri: String,
        content: QuestionContent,
    },
    /// A permission prompt was raised. Inline content carries the tool name,
    /// the tool_use_id, and the tool input the user has to authorize.
    Permission {
        #[serde(default)]
        escalate: bool,
        detail_uri: String,
        content: PermissionContent,
    },
    /// An artifact was written or patched. Inline content carries enough to
    /// describe the change (title, summary, output_name, version, confirmed);
    /// the body is read from `detail_uri` if the caller wants it.
    ArtifactWritten {
        #[serde(default)]
        escalate: bool,
        detail_uri: String,
        content: ArtifactSummary,
    },
    /// The agent's turn terminalized while the issue still needs the driver.
    /// This is a *generic envelope* over non-None [`IssueAttention`] states.
    /// deliberately does not split per-attention because the relevant
    /// content lives in the projection (`AttentionEvent::attention`), and
    /// the URI helpers (`pending_question_uri` / `pending_permission_uri` /
    /// `blocked_node_artifact_uri`) only resolve a useful `detail_uri` for
    /// NeedsInput / NeedsAuthorization / NeedsApproval. For other states,
    /// `detail_uri` is best-effort and may be the bare issue URI; consumers
    /// should always inspect `event.attention` to dispatch.
    AgentIdleWithWork {
        #[serde(default)]
        escalate: bool,
        detail_uri: String,
    },
    /// Webhook reported a PR state change. Inline content is the live state.
    PrStateChange {
        #[serde(default)]
        escalate: bool,
        detail_uri: String,
        content: PrStateContent,
    },
    /// A node replied to an external driver. Inline content carries the reply
    /// body so `cairn watch` callers can consume it without a follow-up read.
    ExternalMessageReply {
        #[serde(default)]
        escalate: bool,
        detail_uri: String,
        message_id: String,
        content: ExternalMessageReplyContent,
    },
    /// The issue reached a terminal status (merged, closed, or failed).
    Resolved {
        #[serde(default)]
        escalate: bool,
        final_status: IssueStatus,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalMessageReplyContent {
    pub sender: String,
    pub body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuestionContent {
    pub questions: Vec<Question>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionContent {
    pub tool_name: String,
    pub tool_use_id: String,
    pub input: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactSummary {
    pub output_name: String,
    pub version: i32,
    pub confirmed: bool,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub artifact_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrStateContent {
    pub number: i64,
    pub state: String,
    pub mergeable: Option<String>,
    pub additions: Option<i64>,
    pub deletions: Option<i64>,
    pub title: Option<String>,
}

/// One attention event delivered to a watcher.
///
/// `issue_id` is the internal id used by `watch` to filter subscriptions;
/// `issue_uri` is the rendered `cairn://p/PROJ/N` form returned to callers.
/// `attention` and `status` are the projection at the time of the emit so the
/// long-poll response is self-contained (the watcher does not need to re-read).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttentionEvent {
    pub issue_id: String,
    pub issue_uri: String,
    pub fact: AttentionFact,
    pub attention: IssueAttention,
    pub status: IssueStatus,
    pub updated_at: i64,
}

/// Fact-key for short-window dedupe: same discriminant + same detail uri inside
/// the window collapses into a single wake. Distinct facts always pass through.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AttentionFactKey {
    pub kind: &'static str,
    pub detail_uri: Option<String>,
}

impl AttentionFact {
    /// Canonical delivery urgency for this fact kind.
    ///
    /// Legacy per-fact `escalate` flags are interpreted only at this boundary:
    /// an escalated fact becomes an interrupt, which is the one urgency that
    /// pierces muted wake subscriptions.
    pub fn urgency(&self) -> DeliveryUrgency {
        match self {
            AttentionFact::Question { escalate: true, .. }
            | AttentionFact::Permission { escalate: true, .. }
            | AttentionFact::ArtifactWritten { escalate: true, .. }
            | AttentionFact::AgentIdleWithWork { escalate: true, .. }
            | AttentionFact::PrStateChange { escalate: true, .. }
            | AttentionFact::ExternalMessageReply { escalate: true, .. }
            | AttentionFact::Resolved { escalate: true, .. } => DeliveryUrgency::Interrupt,
            AttentionFact::ExternalMessageReply { .. } => DeliveryUrgency::Steer,
            AttentionFact::Question { .. }
            | AttentionFact::Permission { .. }
            | AttentionFact::ArtifactWritten { .. }
            | AttentionFact::AgentIdleWithWork { .. }
            | AttentionFact::PrStateChange { .. }
            | AttentionFact::Resolved { .. } => DeliveryUrgency::Queue,
        }
    }

    pub fn key(&self) -> AttentionFactKey {
        match self {
            AttentionFact::Question { detail_uri, .. } => AttentionFactKey {
                kind: "question",
                detail_uri: Some(detail_uri.clone()),
            },
            AttentionFact::Permission { detail_uri, .. } => AttentionFactKey {
                kind: "permission",
                detail_uri: Some(detail_uri.clone()),
            },
            AttentionFact::ArtifactWritten { detail_uri, .. } => AttentionFactKey {
                kind: "artifact_written",
                detail_uri: Some(detail_uri.clone()),
            },
            AttentionFact::AgentIdleWithWork { detail_uri, .. } => AttentionFactKey {
                kind: "agent_idle_with_work",
                detail_uri: Some(detail_uri.clone()),
            },
            AttentionFact::PrStateChange { detail_uri, .. } => AttentionFactKey {
                kind: "pr_state_change",
                detail_uri: Some(detail_uri.clone()),
            },
            AttentionFact::ExternalMessageReply { detail_uri, .. } => AttentionFactKey {
                kind: "external_message_reply",
                detail_uri: Some(detail_uri.clone()),
            },
            AttentionFact::Resolved { .. } => AttentionFactKey {
                kind: "resolved",
                detail_uri: None,
            },
        }
    }
}

/// Render `AttentionEvent` to the JSON shape `cairn watch` returns.
///
/// Compatible with the prior shape (`actionable`/`resolved`/`pending`) and
/// additively carries the typed `fact` block. Existing consumers that ignore
/// unknown fields keep working.
pub fn event_to_watch_json(event: &AttentionEvent) -> serde_json::Value {
    if let AttentionFact::Resolved { final_status, .. } = &event.fact {
        return serde_json::json!({
            "status": "resolved",
            "issue_status": final_status.to_string(),
            "updated_at": event.updated_at,
            "issue_uri": event.issue_uri,
            "fact": &event.fact,
        });
    }
    let detail_uri = match &event.fact {
        AttentionFact::Question { detail_uri, .. }
        | AttentionFact::Permission { detail_uri, .. }
        | AttentionFact::ArtifactWritten { detail_uri, .. }
        | AttentionFact::AgentIdleWithWork { detail_uri, .. }
        | AttentionFact::PrStateChange { detail_uri, .. }
        | AttentionFact::ExternalMessageReply { detail_uri, .. } => detail_uri.clone(),
        AttentionFact::Resolved { .. } => event.issue_uri.clone(),
    };
    serde_json::json!({
        "status": "actionable",
        "attention": event.attention.to_string(),
        "updated_at": event.updated_at,
        "issue_uri": event.issue_uri,
        "detail_uri": detail_uri,
        "fact": &event.fact,
    })
}

type PendingNodeDetailBuilder = fn(&str, i32, i32, &str, &str) -> String;

async fn pending_node_detail_uri(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    query: &'static str,
    build_detail_uri: PendingNodeDetailBuilder,
) -> Option<String> {
    let issue_id = issue_id.to_string();
    let project_key = project_key.to_string();
    let resolved: Option<(i64, String, String)> = db
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn.query(query, (issue_id.as_str(),)).await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((row.i64(0)?, row.text(1)?, row.text(2)?))),
                    None => Ok(None),
                }
            })
        })
        .await
        .ok()
        .flatten();

    resolved.map(|(seq, node, segment)| {
        build_detail_uri(&project_key, number, seq as i32, &node, &segment)
    })
}

/// Best-effort: resolve the most recent unanswered question's URI for an issue.
///
/// Shared with `mcp::handlers::watch` so the watch handler can fall back to a
/// synthetic event when waking up from a `Lagged` recovery — the same shape it
/// uses in the catch-up path.
pub async fn pending_question_uri(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
) -> Option<String> {
    pending_node_detail_uri(
        db,
        project_key,
        number,
        issue_id,
        "SELECT e.seq, j.uri_segment, p.uri_segment
                         FROM prompts p
                         JOIN runs r ON p.run_id = r.id
                         JOIN jobs j ON r.job_id = j.id
                         JOIN executions e ON j.execution_id = e.id
                         WHERE r.issue_id = ?1 AND p.response IS NULL
                         ORDER BY p.created_at DESC LIMIT 1",
        cairn_common::uri::build_node_question_uri,
    )
    .await
}

/// Best-effort: resolve the most-recent pending permission request's URI for an
/// issue, addressed by `(executions.seq, job uri_segment, permission
/// uri_segment)` — the same `permissions/{segment}` path the resource patch
/// accepts. Lets a `needs_authorization` idle / child-attention fact point at
/// the answerable permission segment (e.g. `.../builder/permissions/perm-5`)
/// instead of the bare issue URI, so a handler can go straight to the decision
/// patch with no enumeration read of the collection.
pub async fn pending_permission_uri(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
) -> Option<String> {
    pending_node_detail_uri(
        db,
        project_key,
        number,
        issue_id,
        "SELECT e.seq, j.uri_segment, pr.uri_segment
                         FROM permission_requests pr
                         JOIN runs r ON pr.run_id = r.id
                         JOIN jobs j ON COALESCE(pr.job_id, r.job_id) = j.id
                         JOIN executions e ON j.execution_id = e.id
                         WHERE j.issue_id = ?1 AND pr.status = 'pending'
                           AND pr.uri_segment IS NOT NULL
                         ORDER BY pr.created_at DESC LIMIT 1",
        cairn_common::uri::build_node_permission_uri,
    )
    .await
}

/// Best-effort: resolve the blocked node's detail URI for an issue.
///
/// A blocked node is either a gated agent **job** (resolve to its gated-artifact
/// URI) or a blocked **action_run** — notably a `pr` node awaiting merge/close,
/// whose detail URI is the bare node `cairn://p/PROJ/N/EXEC/pr` (no artifact
/// name: the PR *is* the node's content). Picks the most-recently-created
/// blocked owner across both tables, addressing by `(executions.seq,
/// uri_segment)` — the same key the node-tree emits and the read resolver
/// accepts (CAIRN-1222).
pub async fn blocked_node_artifact_uri(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
) -> Option<String> {
    let issue_id = issue_id.to_string();
    let project_key = project_key.to_string();
    // (seq, segment, output_name, is_action)
    let resolved: Option<(i64, String, Option<String>, bool)> = db
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                // Most-recent blocked agent job, with its latest artifact name.
                let mut job_rows = conn
                    .query(
                        "SELECT e.seq, j.uri_segment, a.output_name, j.created_at
                         FROM jobs j
                         JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN artifacts a ON a.job_id = j.id
                         WHERE j.issue_id = ?1 AND j.status = 'blocked'
                         ORDER BY j.created_at DESC, a.version DESC LIMIT 1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                let job = match job_rows.next().await? {
                    Some(row) => Some((row.i64(0)?, row.text(1)?, row.opt_text(2)?, row.i64(3)?)),
                    None => None,
                };
                drop(job_rows);

                // Most-recent blocked action_run (e.g. a `pr` node).
                let mut action_rows = conn
                    .query(
                        "SELECT e.seq, ar.uri_segment, ar.created_at
                         FROM action_runs ar
                         JOIN executions e ON ar.execution_id = e.id
                         WHERE ar.issue_id = ?1 AND ar.status = 'blocked'
                           AND ar.uri_segment IS NOT NULL
                         ORDER BY ar.created_at DESC LIMIT 1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                let action = match action_rows.next().await? {
                    Some(row) => Some((row.i64(0)?, row.text(1)?, row.i64(2)?)),
                    None => None,
                };
                drop(action_rows);

                // Pick the most-recently-created blocked owner across both.
                let result = match (job, action) {
                    (Some((jseq, jseg, joutput, jcreated)), Some((aseq, aseg, acreated))) => {
                        if acreated >= jcreated {
                            Some((aseq, aseg, None, true))
                        } else {
                            Some((jseq, jseg, joutput, false))
                        }
                    }
                    (Some((jseq, jseg, joutput, _)), None) => Some((jseq, jseg, joutput, false)),
                    (None, Some((aseq, aseg, _))) => Some((aseq, aseg, None, true)),
                    (None, None) => None,
                };
                Ok::<_, DbError>(result)
            })
        })
        .await
        .ok()
        .flatten();
    resolved.map(|(seq, segment, output_name, is_action)| {
        if is_action {
            // Bare node URI — the pr action node has no artifact name.
            cairn_common::uri::build_node_uri(&project_key, number, seq as i32, &segment)
        } else {
            cairn_common::uri::build_node_artifact_uri_named(
                &project_key,
                number,
                seq as i32,
                &segment,
                output_name.as_deref(),
            )
        }
    })
}

/// An open PR work product for an issue, resolved to the producing node's `/pr`
/// artifact URI plus the `merge_requests` row's `updated_at`.
///
/// `updated_at` lets cursor-gated callers (the `watch` catch-up read) decide
/// whether the PR work is newer than what the driver has already seen.
#[derive(Debug, Clone)]
pub struct OpenPrWork {
    pub detail_uri: String,
    pub updated_at: i64,
}

/// Best-effort: resolve the open PR work product for an issue.
///
/// An open `merge_requests` row *is* a PR the driver may need to act on, even
/// when the attention projection is still `None` (GitHub mergeability / check /
/// review state is unknown right after the PR is opened). Prefers the PR linked
/// to `job_hint` — the builder whose turn just ended — so the URI points at that
/// builder's `/pr`; otherwise takes the most-recently-updated open PR. Resolves
/// the producing node's `/pr` node-artifact URI from the producing job's latest
/// artifact, falling back to the generic `/artifact` alias when that job has no
/// artifact row yet.
pub async fn open_pr_work_for_issue(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    job_hint: Option<&str>,
) -> Option<OpenPrWork> {
    let issue_id = issue_id.to_string();
    // Empty sentinel never matches a real job id, so a `None` hint simply leaves
    // ordering to `updated_at` (the CASE evaluates to 1 for every row).
    let job_hint = job_hint.unwrap_or("").to_string();
    let resolved: Option<(i64, String, Option<String>, i64)> = db
        .read(|conn| {
            let issue_id = issue_id.clone();
            let job_hint = job_hint.clone();
            Box::pin(async move {
                // No artifact_type filter: the stored type is the producing
                // node's schema *name* (e.g. "pr"), not a fixed string, so the
                // producing job's latest artifact (its `/pr`) is the right one.
                // Mirrors `blocked_node_artifact_uri`'s untyped join.
                let mut rows = conn
                    .query(
                        "SELECT e.seq, j.uri_segment, a.output_name, mr.updated_at
                         FROM merge_requests mr
                         JOIN jobs j ON mr.job_id = j.id
                         JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN artifacts a ON a.job_id = j.id
                         WHERE mr.issue_id = ?1 AND mr.status = 'open'
                         ORDER BY CASE WHEN mr.job_id = ?2 THEN 0 ELSE 1 END,
                                  mr.updated_at DESC, a.version DESC
                         LIMIT 1",
                        params![issue_id.as_str(), job_hint.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.i64(0)?,
                        row.text(1)?,
                        row.opt_text(2)?,
                        row.i64(3)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .ok()
        .flatten();
    resolved.map(|(seq, node, output_name, updated_at)| OpenPrWork {
        detail_uri: cairn_common::uri::build_node_artifact_uri_named(
            project_key,
            number,
            seq as i32,
            &node,
            output_name.as_deref(),
        ),
        updated_at,
    })
}

/// A typed idle fact plus the cursor (`updated_at`) to stamp on the event the
/// caller emits.
///
/// The cursor matters for the open-PR none-attention case: it is the
/// `merge_request`'s `updated_at` (PR-creation time), strictly fresher than any
/// pre-PR `issue.updated_at`. That keeps the live wake past a watcher's cursor
/// gate even when the post-create refresh + recompute was skipped or failed
/// (the refresh is deliberately non-fatal, and `gh`'s auth differs from Cairn's
/// GitHub App, so a successful `gh pr create` can be paired with a failing
/// refresh). It also matches the cursor the watch catch-up read uses for the
/// same fact, so the live and catch-up paths never disagree. For every other
/// case the cursor is the issue projection's `updated_at`.
#[derive(Debug, Clone)]
pub struct IdleFact {
    pub fact: AttentionFact,
    pub updated_at: i64,
}

/// Build the typed idle fact (and its cursor) for an issue whose agent just went
/// idle (a turn ended) or that hit a boundary wake. Returns `None` when there is
/// nothing for the driver to act on.
///
/// Ordering, highest priority first:
/// 1. terminal status -> `Resolved` (cursor: issue `updated_at`)
/// 2. non-`None` attention -> `AgentIdleWithWork` with the attention-specific
///    detail URI (question / pending permission / blocked artifact), falling
///    back to the issue URI when none resolves (cursor: issue `updated_at` — a
///    non-None attention always implies a recompute bumped it)
/// 3. otherwise -> `None` (idle, nothing pending)
///
/// `job_hint` biases the open-PR lookup toward a specific producing job (the
/// builder whose turn just terminalized) so the detail URI points at its `/pr`.
pub async fn idle_fact_for_issue(
    db: &LocalDb,
    issue_id: &str,
    ctx: &IssueAttentionContext,
    job_hint: Option<&str>,
) -> Option<IdleFact> {
    if ctx.status.is_terminal() {
        return Some(IdleFact {
            fact: AttentionFact::Resolved {
                escalate: false,
                final_status: ctx.status.clone(),
            },
            updated_at: ctx.updated_at,
        });
    }
    let issue_uri = ctx.issue_uri();
    if ctx.attention != IssueAttention::None {
        // Attention-specific gate URI: a pending question or a blocked pre-PR
        // artifact. When neither resolves — e.g. the gate is a PR already
        // awaiting review, whose producing job is complete rather than blocked
        // — fall back to the open-PR work product so the detail URI points at
        // the producing `/pr` rather than the bare issue.
        let detail_uri = match ctx.attention {
            IssueAttention::NeedsInput => {
                pending_question_uri(db, &ctx.project_key, ctx.number, issue_id).await
            }
            IssueAttention::NeedsAuthorization => {
                pending_permission_uri(db, &ctx.project_key, ctx.number, issue_id).await
            }
            IssueAttention::NeedsApproval => {
                blocked_node_artifact_uri(db, &ctx.project_key, ctx.number, issue_id).await
            }
            _ => None,
        };
        let detail_uri = match detail_uri {
            Some(uri) => uri,
            None => open_pr_work_for_issue(db, &ctx.project_key, ctx.number, issue_id, job_hint)
                .await
                .map(|work| work.detail_uri)
                .unwrap_or_else(|| issue_uri.clone()),
        };
        return Some(IdleFact {
            fact: AttentionFact::AgentIdleWithWork {
                escalate: false,
                detail_uri,
            },
            updated_at: ctx.updated_at,
        });
    }
    // attention == None: not actionable by projection, but a freshly-opened PR
    // (its GitHub mergeability/check/review state still unknown, so the
    // projection deliberately stays None) is real work the driver must act on.
    // Carry the merge_request's updated_at — strictly fresher than a stale
    // issue.updated_at when the post-create recompute was skipped or failed.
    if let Some(work) =
        open_pr_work_for_issue(db, &ctx.project_key, ctx.number, issue_id, job_hint).await
    {
        return Some(IdleFact {
            fact: AttentionFact::AgentIdleWithWork {
                escalate: false,
                detail_uri: work.detail_uri,
            },
            updated_at: work.updated_at,
        });
    }
    None
}

/// Read an issue's `(project_key, number, attention, status, updated_at)` in
/// one query, used by emit sites that hold only an `issue_id`.
pub async fn read_issue_for_attention(
    db: &LocalDb,
    issue_id: &str,
) -> Result<IssueAttentionContext, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number, i.attention, i.status, i.updated_at
                     FROM issues i JOIN projects p ON i.project_id = p.id
                     WHERE i.id = ?1 LIMIT 1",
                    params![issue_id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::internal("issue not found"))?;
            let project_key = row.text(0)?;
            let number = row.i64(1)? as i32;
            let attention = row
                .text(2)?
                .parse::<IssueAttention>()
                .unwrap_or(IssueAttention::None);
            let status = row
                .text(3)?
                .parse::<IssueStatus>()
                .unwrap_or(IssueStatus::Backlog);
            let updated_at = row.opt_i64(4)?.unwrap_or(0);
            Ok::<_, DbError>(IssueAttentionContext {
                project_key,
                number,
                attention,
                status,
                updated_at,
            })
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Snapshot of the bits of an issue's projection an emit site needs.
#[derive(Debug, Clone)]
pub struct IssueAttentionContext {
    pub project_key: String,
    pub number: i32,
    pub attention: IssueAttention,
    pub status: IssueStatus,
    pub updated_at: i64,
}

impl IssueAttentionContext {
    pub fn issue_uri(&self) -> String {
        cairn_common::uri::build_issue_uri(&self.project_key, self.number)
    }
}

/// Resolve a `(issue_id, IssueAttentionContext)` from `(project_key, number)`.
/// Used by emit sites that hold URI coordinates but not the internal id.
pub async fn lookup_issue_for_attention_by_key(
    db: &LocalDb,
    project_key: &str,
    number: i32,
) -> Result<(String, IssueAttentionContext), String> {
    let project_key = project_key.to_uppercase();
    db.read(|conn| {
        let project_key = project_key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.id, p.key, i.number, i.attention, i.status, i.updated_at
                     FROM issues i JOIN projects p ON i.project_id = p.id
                     WHERE p.key = ?1 AND i.number = ?2 LIMIT 1",
                    params![project_key.as_str(), number],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::internal("issue not found"))?;
            let issue_id = row.text(0)?;
            let project_key = row.text(1)?;
            let number = row.i64(2)? as i32;
            let attention = row
                .text(3)?
                .parse::<IssueAttention>()
                .unwrap_or(IssueAttention::None);
            let status = row
                .text(4)?
                .parse::<IssueStatus>()
                .unwrap_or(IssueStatus::Backlog);
            let updated_at = row.opt_i64(5)?.unwrap_or(0);
            Ok::<_, DbError>((
                issue_id,
                IssueAttentionContext {
                    project_key,
                    number,
                    attention,
                    status,
                    updated_at,
                },
            ))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attention_fact_urgency_uses_escalation_only_as_interrupt_alias() {
        let default = AttentionFact::Question {
            escalate: false,
            detail_uri: "q".to_string(),
            content: QuestionContent { questions: vec![] },
        };
        let escalated = AttentionFact::Resolved {
            escalate: true,
            final_status: IssueStatus::Complete,
        };
        assert_eq!(default.urgency(), DeliveryUrgency::Queue);
        assert_eq!(escalated.urgency(), DeliveryUrgency::Interrupt);
    }

    #[test]
    fn attention_fact_kinds_declare_delivery_urgency() {
        assert_eq!(
            (AttentionFact::Question {
                escalate: false,
                detail_uri: "q".to_string(),
                content: QuestionContent { questions: vec![] },
            })
            .urgency(),
            DeliveryUrgency::Queue
        );
        assert_eq!(
            (AttentionFact::Permission {
                escalate: false,
                detail_uri: "p".to_string(),
                content: PermissionContent {
                    tool_name: "tool".to_string(),
                    tool_use_id: "tu".to_string(),
                    input: serde_json::json!({}),
                },
            })
            .urgency(),
            DeliveryUrgency::Queue
        );
        assert_eq!(
            (AttentionFact::ArtifactWritten {
                escalate: false,
                detail_uri: "a".to_string(),
                content: ArtifactSummary {
                    output_name: "pr".to_string(),
                    version: 1,
                    confirmed: false,
                    title: None,
                    summary: None,
                    artifact_type: "create-pr".to_string(),
                },
            })
            .urgency(),
            DeliveryUrgency::Queue
        );
        assert_eq!(
            (AttentionFact::AgentIdleWithWork {
                escalate: false,
                detail_uri: "idle".to_string(),
            })
            .urgency(),
            DeliveryUrgency::Queue
        );
        assert_eq!(
            (AttentionFact::PrStateChange {
                escalate: false,
                detail_uri: "pr".to_string(),
                content: PrStateContent {
                    number: 1,
                    state: "open".to_string(),
                    mergeable: None,
                    additions: None,
                    deletions: None,
                    title: None,
                },
            })
            .urgency(),
            DeliveryUrgency::Queue
        );
        assert_eq!(
            (AttentionFact::ExternalMessageReply {
                escalate: false,
                detail_uri: "messages".to_string(),
                message_id: "m".to_string(),
                content: ExternalMessageReplyContent {
                    sender: "builder".to_string(),
                    body: "done".to_string(),
                },
            })
            .urgency(),
            DeliveryUrgency::Steer
        );
        assert_eq!(
            (AttentionFact::Resolved {
                escalate: false,
                final_status: IssueStatus::Complete,
            })
            .urgency(),
            DeliveryUrgency::Queue
        );
    }

    #[test]
    fn fact_keys_distinguish_distinct_facts_and_collapse_same_kind_uri() {
        let q = AttentionFact::Question {
            escalate: false,
            detail_uri: "cairn://p/CAIRN/1/1/planner/questions/q-1".into(),
            content: QuestionContent { questions: vec![] },
        };
        let same = AttentionFact::Question {
            escalate: true,
            detail_uri: "cairn://p/CAIRN/1/1/planner/questions/q-1".into(),
            content: QuestionContent { questions: vec![] },
        };
        let other = AttentionFact::Question {
            escalate: false,
            detail_uri: "cairn://p/CAIRN/1/1/planner/questions/q-2".into(),
            content: QuestionContent { questions: vec![] },
        };
        let idle = AttentionFact::AgentIdleWithWork {
            escalate: false,
            detail_uri: "cairn://p/CAIRN/1/1/planner/questions/q-1".into(),
        };
        assert_eq!(q.key(), same.key());
        assert_ne!(q.key(), other.key());
        assert_ne!(q.key(), idle.key());
    }

    #[test]
    fn event_renders_resolved_with_terminal_status() {
        let event = AttentionEvent {
            issue_id: "i-1".into(),
            issue_uri: "cairn://p/CAIRN/1".into(),
            fact: AttentionFact::Resolved {
                escalate: false,
                final_status: IssueStatus::Merged,
            },
            attention: IssueAttention::None,
            status: IssueStatus::Merged,
            updated_at: 42,
        };
        let json = event_to_watch_json(&event);
        assert_eq!(json["status"], "resolved");
        assert_eq!(json["issue_status"], "merged");
        assert_eq!(json["updated_at"], 42);
        assert_eq!(json["issue_uri"], "cairn://p/CAIRN/1");
        assert_eq!(json["fact"]["kind"], "resolved");
    }

    #[test]
    fn event_renders_actionable_with_detail_uri_and_fact_block() {
        let event = AttentionEvent {
            issue_id: "i-1".into(),
            issue_uri: "cairn://p/CAIRN/1".into(),
            fact: AttentionFact::Question {
                escalate: false,
                detail_uri: "cairn://p/CAIRN/1/1/planner/questions/q-1".into(),
                content: QuestionContent { questions: vec![] },
            },
            attention: IssueAttention::NeedsInput,
            status: IssueStatus::Active,
            updated_at: 5,
        };
        let json = event_to_watch_json(&event);
        assert_eq!(json["status"], "actionable");
        assert_eq!(json["attention"], "needs_input");
        assert_eq!(
            json["detail_uri"],
            "cairn://p/CAIRN/1/1/planner/questions/q-1"
        );
        assert_eq!(json["fact"]["kind"], "question");
    }

    #[test]
    fn external_message_reply_watch_json_includes_inline_body() {
        let event = AttentionEvent {
            issue_id: "issue-1".to_string(),
            issue_uri: "cairn://p/CAIRN/1209".to_string(),
            attention: IssueAttention::None,
            status: IssueStatus::Active,
            updated_at: 1700000001,
            fact: AttentionFact::ExternalMessageReply {
                escalate: false,
                detail_uri: "cairn://p/CAIRN/1209/messages".to_string(),
                message_id: "msg-1".to_string(),
                content: ExternalMessageReplyContent {
                    sender: "cairn://p/CAIRN/1209/1/builder".to_string(),
                    body: "done".to_string(),
                },
            },
        };

        let json = event_to_watch_json(&event);
        assert_eq!(json["status"], "actionable");
        assert_eq!(json["attention"], "none");
        assert_eq!(json["detail_uri"], "cairn://p/CAIRN/1209/messages");
        assert_eq!(json["fact"]["kind"], "external_message_reply");
        assert_eq!(json["fact"]["message_id"], "msg-1");
        assert_eq!(
            json["fact"]["content"]["sender"],
            "cairn://p/CAIRN/1209/1/builder"
        );
        assert_eq!(json["fact"]["content"]["body"], "done");
    }
}
