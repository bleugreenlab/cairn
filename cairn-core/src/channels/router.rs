use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cairn_common::uri::{
    build_node_permission_uri, build_node_question_uri, parse_uri, CairnResource,
};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ledger, render_text_floor, AskOption, ChannelProvider, InboundEvent, OperatorPresence,
    OutboundAsk, OutboundInitiator, OutboundMessage, ResolvedQuestionMessage,
};
use crate::routes::{ChannelSubmission, Presence, RouteContext, RouteFact};
use crate::{
    mcp::handlers::{
        permission::{resolve_permission_request, PermissionDecision, PermissionScope},
        planning::answer_prompt_id,
    },
    models::ChannelRouteConfig,
    orchestrator::Orchestrator,
    storage::{LocalDb, RowExt},
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const BACKLOG_SEAL_RETRY_INTERVAL: Duration = Duration::from_secs(60);
/// One sweep DELIVERS at most this many gates, so a burst of new asks cannot
/// monopolize the outbound path; the rest go out on the next tick.
///
/// It is deliberately not a bound on the query. A claimed gate still matches the
/// loaders' predicates forever -- an unanswered prompt stays unanswered once its
/// node has moved on -- so a `LIMIT` would decide admission by `created_at`
/// ordering, and that column is second-precision: a sealed backlog sharing one
/// second with a newly raised ask could fill the window on every sweep and the
/// live ask would never be offered at all. Admission is by identity instead.
const SWEEP_LIMIT: usize = 100;
const ATTENTION_GRACE: Duration = Duration::from_secs(3 * 60);
const FOLLOW_POLL_LIMIT: usize = 10;
/// How much of a target's title a poll option carries before it is elided. A
/// Messages balloon renders an option as a wrapped block, so a full thread title
/// pushes the rest of the list off the screen.
const FOLLOW_POLL_TITLE_LIMIT: usize = 64;
/// The durable binding prefix for a follow poll's ledger row. The literal value
/// names the poll, not the entity it lists: polls issued by earlier sessions are
/// standing control surfaces still bound by this prefix.
const FOLLOW_POLL_PREFIX: &str = "threads:";

#[derive(Debug, Deserialize)]
struct StoredQuestion {
    question: String,
    #[serde(default)]
    options: Vec<StoredOption>,
}

#[derive(Debug, Deserialize, Serialize)]
struct FollowPollOptions {
    labels: Vec<String>,
    bindings: HashMap<String, String>,
}

fn follow_poll_options(record: &ledger::OutboundRecord) -> Option<FollowPollOptions> {
    let json = record.options_json.as_deref()?;
    serde_json::from_str(json).ok().or_else(|| {
        let bindings: HashMap<String, String> = serde_json::from_str(json).ok()?;
        let labels = record
            .rendered_text
            .lines()
            .filter_map(|line| {
                let (number, label) = line.split_once(". ")?;
                number.parse::<usize>().ok()?;
                bindings.contains_key(label).then(|| label.to_string())
            })
            .collect();
        Some(FollowPollOptions { labels, bindings })
    })
}

fn follow_poll_answer(record: &ledger::OutboundRecord, text: &str) -> String {
    let trimmed = text.trim();
    let Some(index) = super::imessage::parse_reply_number(trimmed, usize::MAX) else {
        return trimmed.to_string();
    };
    follow_poll_options(record)
        .and_then(|options| options.labels.get(index).cloned())
        .unwrap_or_else(|| trimmed.to_string())
}

/// What this channel can follow and converse with.
///
/// The vocabulary is deliberately wider than either entity: a first-class thread
/// is what the operator polls for and what the phone is a window onto, while an
/// issue node stays addressable for a run the operator wants to watch. Both
/// travel one pipeline — poll, follow, live edge, event tap, inbound routing —
/// rather than two that would drift apart.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FollowTarget {
    Thread { project: String, name: String },
    Issue { project: String, number: i32 },
}

impl FollowTarget {
    /// The syntactic reading of a follow URI. A numeric address is an issue here;
    /// [`ChannelRouter::resolve_target`] is the resolver that also asks the
    /// database whether a thread migration vacated that number.
    fn parse(uri: &str) -> Result<Self, String> {
        match parse_uri(uri) {
            Some(CairnResource::Thread {
                project,
                name,
                path,
            }) if path.is_empty() => Ok(Self::Thread { project, name }),
            Some(CairnResource::Issue { project, number }) => Ok(Self::Issue { project, number }),
            _ => Err(format!("not a followable Cairn URI: {uri}")),
        }
    }

    fn project(&self) -> &str {
        match self {
            Self::Thread { project, .. } | Self::Issue { project, .. } => project,
        }
    }

    fn uri(&self) -> String {
        match self {
            Self::Thread { project, name } => format!("cairn://p/{project}/{name}"),
            Self::Issue { project, number } => format!("cairn://p/{project}/{number}"),
        }
    }

    /// How the operator names this target in an `unfollow` command: a thread by
    /// its name, an issue by its number.
    fn selector(&self) -> String {
        match self {
            Self::Thread { name, .. } => name.clone(),
            Self::Issue { number, .. } => number.to_string(),
        }
    }
}

/// The two collections a poll command offers as follow targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollKind {
    Threads,
    Issues,
}

impl PollKind {
    fn caption(self) -> &'static str {
        match self {
            Self::Threads => "Follow threads",
            Self::Issues => "Follow issues",
        }
    }

    fn nothing_active(self) -> &'static str {
        match self {
            Self::Threads => "No active threads right now.",
            Self::Issues => "No active issues right now.",
        }
    }

    fn failure_prefix(self) -> &'static str {
        match self {
            Self::Threads => "Could not list active threads",
            Self::Issues => "Could not list active issues",
        }
    }
}

/// One assistant event from a followed target, with everything the route fact
/// built from it needs.
struct FollowedEvent {
    rowid: i64,
    data: String,
    context: String,
    job_id: String,
    project_id: String,
    repo_path: String,
}

/// A candidate follow target, before labelling decides how much of its address
/// the operator needs to see.
struct PollCandidate {
    updated_at: i64,
    project: String,
    head: String,
    title: Option<String>,
    uri: String,
}

/// The candidates a poll can show, labelled and bound, paired with how many
/// there were in total across every database.
///
/// Already-followed targets sort ahead of the rest. The ten slots span every
/// open database, and this poll is the only surface that manages follows, so a
/// followed target pushed past the limit by busier projects would be streaming
/// to the phone with no way to turn it off.
///
/// A label IS the poll's binding key: a selection arrives as its option text and
/// resolves through `bindings`, so two candidates may never share one or the map
/// silently drops one target and toggles the other in its place. Thread names
/// and issue numbers are unique within a project but not across projects, and
/// this poll spans every open database, so a poll carrying more than one project
/// qualifies every label with its project key.
fn poll_targets(
    mut candidates: Vec<PollCandidate>,
    followed: &HashSet<String>,
) -> (Vec<(String, String)>, usize) {
    candidates.sort_by_key(|candidate| {
        (
            !followed.contains(&candidate.uri),
            std::cmp::Reverse(candidate.updated_at),
        )
    });
    let total = candidates.len();
    candidates.truncate(FOLLOW_POLL_LIMIT);
    let qualify = candidates
        .iter()
        .map(|candidate| candidate.project.as_str())
        .collect::<HashSet<_>>()
        .len()
        > 1;
    (
        candidates
            .iter()
            .map(|candidate| {
                let head = if qualify {
                    format!("{}/{}", candidate.project, candidate.head)
                } else {
                    candidate.head.clone()
                };
                (
                    poll_label(&head, candidate.title.as_deref()),
                    candidate.uri.clone(),
                )
            })
            .collect(),
        total,
    )
}

/// One label, elided to a width a poll balloon can show.
fn poll_label(head: &str, title: Option<&str>) -> String {
    let Some(title) = title.map(str::trim).filter(|title| !title.is_empty()) else {
        return head.to_string();
    };
    let elided = if title.chars().count() > FOLLOW_POLL_TITLE_LIMIT {
        format!(
            "{}…",
            title
                .chars()
                .take(FOLLOW_POLL_TITLE_LIMIT)
                .collect::<String>()
                .trim_end()
        )
    } else {
        title.to_string()
    };
    format!("{head} · {elided}")
}

#[derive(Default)]
struct ReviewGates {
    gates: Vec<Gate>,
    expired_dangling: usize,
}

/// The poll a bare command asks for, matched as an exact case-insensitive word.
/// `threads` means threads: the operator's muscle memory is the word, and after
/// the thread cutover the word means the entity.
fn poll_command(text: &str) -> Option<PollKind> {
    match text.trim().to_ascii_lowercase().as_str() {
        "threads" | "/threads" => Some(PollKind::Threads),
        "issues" | "/issues" => Some(PollKind::Issues),
        _ => None,
    }
}

/// `unfollow` alone targets whatever the reply is bound to; `unfollow <selector>`
/// names a thread or an issue number among the current follows.
fn unfollow_selector(text: &str) -> Option<Option<String>> {
    let mut words = text.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("unfollow") {
        return None;
    }

    match (words.next(), words.next()) {
        (None, None) => Some(None),
        (Some(selector), None) => Some(Some(selector.to_string())),
        _ => None,
    }
}

/// A command the operator typed is answered synchronously, so a delivery that
/// never reached the wire has to say WHY. The ledger already recorded the
/// provider's own error against the intent; repeating a generic failure instead
/// is what left a bare "native thread poll delivery failed" on the phone with no
/// way to tell a transient send from a broken bridge.
fn require_command_delivery(delivered: bool, last_error: Option<String>) -> Result<(), String> {
    delivered.then_some(()).ok_or_else(|| {
        last_error.unwrap_or_else(|| "the provider made no delivery attempt".to_string())
    })
}

fn assistant_text(data: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(data)
        .ok()?
        .get("content")?
        .as_str()
        .map(str::to_string)
        .filter(|text| !text.is_empty())
}

async fn cleanup_resolved_question(
    db: &LocalDb,
    provider: &dyn ChannelProvider,
    record: &ledger::OutboundRecord,
    receipt: &str,
) -> Result<bool, String> {
    if !ledger::claim_question_cleanup(db, &record.id, chrono::Utc::now().timestamp_millis())
        .await?
    {
        return Ok(false);
    }

    cleanup_claimed_question(provider, record, receipt).await;
    Ok(true)
}

async fn cleanup_claimed_question(
    provider: &dyn ChannelProvider,
    record: &ledger::OutboundRecord,
    receipt: &str,
) {
    let (Some(provider_guid), Some(sent_at)) = (&record.provider_guid, record.sent_at) else {
        return;
    };
    let message = ResolvedQuestionMessage {
        conversation: record.conversation.clone(),
        provider_guid: provider_guid.clone(),
        caption_guid: record.caption_guid.clone(),
        sent_at,
        receipt: receipt.to_string(),
    };
    if let Err(error) = provider.cleanup_question(&message).await {
        log::warn!(
            "channel could not clean up resolved question {}: {error}",
            record.binding_ref
        );
    }
}

fn review_notice(project: &str, number: i32, title: &str, content_ref: &str) -> String {
    format!("{project}/{number} review ready — {title}\n{content_ref}")
}

impl Gate {
    fn is_presence_aware(&self) -> bool {
        self.initiated_by.is_presence_aware()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AttentionTiming {
    Defer,
    Send,
}

fn attention_timing(
    presence: OperatorPresence,
    now: Instant,
    deadline: Instant,
) -> AttentionTiming {
    if presence == OperatorPresence::Present && now < deadline {
        AttentionTiming::Defer
    } else {
        AttentionTiming::Send
    }
}

struct DeferredAttention {
    id: String,
    gate: Gate,
    deadline: Instant,
}

fn cancel_resolved_attention(
    deferred: &mut HashMap<String, DeferredAttention>,
    live: &HashSet<String>,
    snapshot_complete: bool,
) -> Vec<String> {
    if !snapshot_complete {
        return Vec::new();
    }
    let cancelled = deferred
        .iter()
        .filter(|(binding, _)| !live.contains(*binding))
        .map(|(_, attention)| attention.id.clone())
        .collect::<Vec<_>>();
    deferred.retain(|binding, _| live.contains(binding));
    cancelled
}

#[derive(Debug, Deserialize)]
struct StoredOption {
    label: String,
    description: Option<String>,
}

/// The gate identities this session has already claimed.
///
/// The ledger is the durable authority; this is the session's cache of it, so a
/// sweep decides what is new by identity instead of re-issuing a fence write for
/// every gate it has already sealed. Without it, a workspace carrying a large
/// permanently-open backlog would pay one no-op write per gate every five
/// seconds, forever.
#[derive(Default)]
struct ClaimSet(Mutex<HashSet<String>>);

impl ClaimSet {
    fn holds(&self, binding_ref: &str) -> bool {
        self.0
            .lock()
            .expect("channel claim set poisoned")
            .contains(binding_ref)
    }

    fn claim(&self, binding_ref: &str) {
        self.0
            .lock()
            .expect("channel claim set poisoned")
            .insert(binding_ref.to_string());
    }
}

#[derive(Debug, Clone)]
struct Gate {
    kind: &'static str,
    initiated_by: OutboundInitiator,
    binding_ref: String,
    job_id: Option<String>,
    context: String,
    ask: OutboundAsk,
}

pub struct ChannelRouter {
    orch: Orchestrator,
    provider: Arc<dyn ChannelProvider>,
    provider_id: &'static str,
    destination: String,
    route: ChannelRouteConfig,
    claims: ClaimSet,
    deferred_attention: Mutex<HashMap<String, DeferredAttention>>,
}

impl ChannelRouter {
    pub fn new_for_provider(
        orch: Orchestrator,
        provider: Arc<dyn ChannelProvider>,
        provider_id: &'static str,
        destination: String,
        route: ChannelRouteConfig,
    ) -> Self {
        Self {
            orch,
            provider,
            provider_id,
            destination,
            route,
            claims: ClaimSet::default(),
            deferred_attention: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub fn new(
        orch: Orchestrator,
        provider: Arc<dyn ChannelProvider>,
        config: crate::models::IMessageChannelConfig,
    ) -> Self {
        Self::new_for_provider(orch, provider, "imessage", config.to, config.route)
    }

    async fn submit_gate(
        &self,
        submission: ChannelSubmission,
        presence: OperatorPresence,
        now: Instant,
    ) -> Result<bool, String> {
        let initiated_by = match submission.initiated_by.as_deref() {
            Some("operator_subscription") => OutboundInitiator::OperatorSubscription,
            _ => OutboundInitiator::CairnPush,
        };
        self.deliver_or_defer(
            Gate {
                kind: "route",
                initiated_by,
                binding_ref: submission.binding_ref,
                job_id: submission.job_id,
                context: submission.context,
                ask: OutboundAsk::Notify {
                    text: submission.text,
                },
            },
            presence,
            now,
        )
        .await
    }

    async fn submit_route(
        &self,
        submission: ChannelSubmission,
        presence: OperatorPresence,
        now: Instant,
    ) -> Result<(), String> {
        let binding_ref = submission.binding_ref.clone();
        self.submit_gate(submission.clone(), presence, now).await?;
        let intent = ledger::get_by_binding(self.ledger(), self.provider_id, "route", &binding_ref)
            .await?
            .ok_or("channel router did not accept the route intent")?;
        match intent.status.as_str() {
            "pending" => {
                let json = serde_json::to_string(&submission).map_err(|error| error.to_string())?;
                ledger::set_pending_route_submission(self.ledger(), &intent.id, &json).await?;
                Ok(())
            }
            "sent" => {
                crate::routes::record_channel_outcome(
                    &self.orch,
                    &submission,
                    Ok(format!("channel_outbound:{}", intent.id)),
                )
                .await
            }
            "failed" => {
                crate::routes::record_channel_outcome(
                    &self.orch,
                    &submission,
                    Err(intent
                        .last_error
                        .unwrap_or_else(|| "channel delivery failed".into())),
                )
                .await
            }
            status => Err(format!("route intent reached unexpected status {status}")),
        }
    }

    async fn recover_pending_routes(
        &self,
        presence: OperatorPresence,
        now: Instant,
    ) -> Result<(), String> {
        for record in ledger::list_unresolved(self.ledger(), self.provider_id).await? {
            if record.kind != "route" || record.status != "pending" {
                continue;
            }
            let json = record.options_json.as_deref().ok_or_else(|| {
                format!(
                    "pending route intent {} has no durable submission",
                    record.id
                )
            })?;
            let submission: ChannelSubmission =
                serde_json::from_str(json).map_err(|error| error.to_string())?;
            let initiated_by = match submission.initiated_by.as_deref() {
                Some("operator_subscription") => OutboundInitiator::OperatorSubscription,
                _ => OutboundInitiator::CairnPush,
            };
            let gate = Gate {
                kind: "route",
                initiated_by,
                binding_ref: submission.binding_ref.clone(),
                job_id: submission.job_id.clone(),
                context: submission.context.clone(),
                ask: OutboundAsk::Notify {
                    text: submission.text.clone(),
                },
            };
            let elapsed_ms = chrono::Utc::now()
                .timestamp_millis()
                .saturating_sub(record.created_at)
                .max(0) as u64;
            let remaining = ATTENTION_GRACE.saturating_sub(Duration::from_millis(elapsed_ms));
            self.deferred_attention
                .lock()
                .expect("deferred attention set poisoned")
                .insert(
                    gate.binding_ref.clone(),
                    DeferredAttention {
                        id: record.id.clone(),
                        gate: gate.clone(),
                        deadline: now + remaining,
                    },
                );
            self.deliver_or_defer(gate, presence, now).await?;
            let updated = ledger::get_by_binding(
                self.ledger(),
                self.provider_id,
                "route",
                &submission.binding_ref,
            )
            .await?
            .ok_or("recovered route intent disappeared")?;
            match updated.status.as_str() {
                "pending" => {}
                "sent" => {
                    crate::routes::record_channel_outcome(
                        &self.orch,
                        &submission,
                        Ok(format!("channel_outbound:{}", updated.id)),
                    )
                    .await?;
                }
                "failed" => {
                    crate::routes::record_channel_outcome(
                        &self.orch,
                        &submission,
                        Err(updated
                            .last_error
                            .unwrap_or_else(|| "channel delivery failed".into())),
                    )
                    .await?;
                }
                status => {
                    return Err(format!(
                        "recovered route reached unexpected status {status}"
                    ))
                }
            }
        }
        Ok(())
    }

    /// Every delivery intent lives in the private database. Channel state is tied
    /// to this runner's provider process and personal messaging account, so
    /// `channel_outbound` is private-lineage and a team replica does not carry it
    /// at all -- reaching across every open database for a ledger row is how one
    /// un-migrated replica used to abort inbound routing for all of them.
    fn ledger(&self) -> &LocalDb {
        &self.orch.db.local
    }

    /// Draws the line between the backlog and the live asks this session will
    /// carry, before the first sweep can deliver anything.
    ///
    /// The line is drawn by IDENTITY, not by time. Every gate that already exists
    /// is fenced into the ledger and then expired, so the ordinary delivery fence
    /// -- the thing that already made delivery once-only -- is what suppresses the
    /// backlog. A clock cannot do this job: `created_at` is second-precision at
    /// the source, so a watermark taken at startup cannot tell an ask raised in
    /// the moment before it from one raised in the moment after.
    ///
    /// Every gate kind is fenced regardless of the route flags, so turning a route
    /// on later cannot dump its accumulated backlog onto the operator's phone.
    /// This is a safety PREREQUISITE, not best-effort work, so it reports failure
    /// rather than logging and letting a sweep proceed. A snapshot that fenced
    /// half a backlog and then hit a transient ledger error leaves the remainder
    /// looking live, and the first successful sweep would text all of it -- the
    /// original incident, reproduced. The caller retries until this succeeds.
    ///
    /// Retrying is safe: claiming is `INSERT OR IGNORE` and expiry is an
    /// idempotent `UPDATE`, so a second pass completes a partial first one.
    async fn draw_the_session_line(&self) -> Result<(), String> {
        let mut fenced = 0;
        let mut expired_dangling = 0;
        for db in self.orch.db.all_dbs().await {
            let result = self.fence_existing_gates(&db).await?;
            fenced += result.0;
            expired_dangling += result.1;
        }
        for follow in ledger::list_follows(self.ledger(), self.provider_id).await? {
            // A URI that cannot PARSE is permanently unusable: skip it, with a
            // note, rather than parking the whole channel behind one dangling
            // follow forever. Everything past this point is a database error,
            // which is transient and must propagate: a cursor left unrebased
            // because a read failed is a cursor the first successful sweep
            // delivers a whole session's backlog from.
            let Ok(parsed) = FollowTarget::parse(&follow.uri) else {
                log::warn!("channel skipped unusable followed URI {}", follow.uri);
                continue;
            };
            let target = self.resolve_target(&follow.uri).await?;
            // A follow recorded under an address its target no longer answers to
            // — a number a thread migration vacated — becomes a second identity
            // for one conversation the moment the poll offers the canonical form.
            // Startup is where that is settled, once, before anything reads it.
            if parsed != target {
                ledger::canonicalize_follow(
                    self.ledger(),
                    self.provider_id,
                    &follow.uri,
                    &target.uri(),
                )
                .await?;
            }
            let live_edge = self.live_edge(&target).await?;
            ledger::advance_follow_cursor(
                self.ledger(),
                self.provider_id,
                &target.uri(),
                live_edge,
            )
            .await?;
        }
        let expired = ledger::expire_undelivered(
            self.ledger(),
            self.provider_id,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;
        if fenced > 0 || expired > 0 || expired_dangling > 0 {
            log::info!(
                "channel sealed the pre-session backlog: {fenced} gate(s) fenced, {expired} intent(s) expired, {expired_dangling} dangling review(s) expired"
            );
        }
        Ok(())
    }

    async fn follow_poll_selection(
        &self,
        record: &ledger::OutboundRecord,
        selected_label: &str,
    ) -> Result<(), String> {
        let options = follow_poll_options(record)
            .ok_or_else(|| "follow poll has no option bindings".to_string())?;
        let uri = options
            .bindings
            .get(selected_label)
            .ok_or_else(|| format!("unknown follow poll option: {selected_label}"))?;
        if ledger::is_target_followed(self.ledger(), self.provider_id, uri).await? {
            self.unfollow(uri).await
        } else {
            self.follow(uri).await
        }
    }

    /// Record a follow under the target's CANONICAL URI, whatever address the
    /// operator reached it by. That URI is the follow's identity everywhere
    /// downstream — the ledger's key, the poll's checkmark, and the prefix of the
    /// binding ref that makes stream delivery once-only — so storing an alias
    /// would make one conversation into two.
    async fn follow(&self, uri: &str) -> Result<(), String> {
        let now = chrono::Utc::now().timestamp_millis();
        let target = self.resolve_target(uri).await?;
        let canonical = target.uri();
        let live_edge = self.live_edge(&target).await?;
        ledger::follow_target(self.ledger(), self.provider_id, &canonical, now, live_edge).await?;
        ledger::set_focus(self.ledger(), self.provider_id, &canonical, now).await
    }

    async fn unfollow(&self, uri: &str) -> Result<(), String> {
        ledger::unfollow_target(self.ledger(), self.provider_id, uri).await?;
        for update in ledger::list_unresolved(self.ledger(), self.provider_id).await? {
            if update.kind == "review"
                && update.binding_ref.starts_with(&format!("{uri}:event:"))
                && ledger::claim_outbound_cleanup(
                    self.ledger(),
                    &update.id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?
            {
                cleanup_claimed_question(self.provider.as_ref(), &update, "✓ unfollowed").await;
            }
        }
        Ok(())
    }

    async fn send_follow_poll(&self, conversation: &str, kind: PollKind) -> Result<(), String> {
        // Unlike an answered one-shot question, a follow poll is a standing
        // control surface. Its GUID binding remains live for the lifetime of the
        // durable ledger row, including after newer polls are issued.
        let followed = ledger::list_follows(self.ledger(), self.provider_id)
            .await?
            .into_iter()
            .map(|follow| follow.uri)
            .collect::<HashSet<_>>();
        let (mut targets, total) = match kind {
            PollKind::Threads => self.active_threads(&followed).await?,
            PollKind::Issues => self.active_issues(&followed).await?,
        };
        if targets.is_empty() {
            return self.send_notice(conversation, kind.nothing_active()).await;
        }
        for (label, uri) in &mut targets {
            if followed.contains(uri) {
                *label = format!("✓ {label}");
            }
        }
        let caption = if total > targets.len() {
            format!(
                "{} (showing {} most recent of {total})",
                kind.caption(),
                targets.len()
            )
        } else {
            kind.caption().to_string()
        };
        let binding_ref = format!("{FOLLOW_POLL_PREFIX}{}", Uuid::new_v4());
        let gate = Gate {
            kind: "question",
            initiated_by: OutboundInitiator::OperatorInbound,
            binding_ref: binding_ref.clone(),
            job_id: None,
            context: String::new(),
            ask: OutboundAsk::Question {
                prompt_id: binding_ref.clone(),
                question_index: 0,
                text: caption,
                options: targets
                    .iter()
                    .map(|(label, _)| AskOption {
                        label: label.clone(),
                        description: None,
                    })
                    .collect(),
            },
        };
        let Some(id) = claim_gate_for_provider(
            &self.claims,
            self.ledger(),
            self.provider_id,
            conversation,
            "poll",
            &gate,
        )
        .await?
        else {
            return Ok(());
        };
        let delivered = self.send_claimed(id.clone(), gate).await?;
        require_command_delivery(
            delivered,
            ledger::get_by_binding(self.ledger(), self.provider_id, "question", &binding_ref)
                .await?
                .and_then(|record| record.last_error),
        )?;
        let bindings: HashMap<String, String> = targets.iter().cloned().collect();
        let options = FollowPollOptions {
            labels: targets.into_iter().map(|(label, _)| label).collect(),
            bindings,
        };
        ledger::update_options(
            self.ledger(),
            &id,
            &serde_json::to_string(&options).map_err(|error| error.to_string())?,
        )
        .await?;
        Ok(())
    }

    /// Every active first-class thread, addressed by name.
    async fn active_threads(
        &self,
        followed: &HashSet<String>,
    ) -> Result<(Vec<(String, String)>, usize), String> {
        let mut rows = Vec::new();
        for db in self.orch.db.all_dbs().await {
            let mut found = db
                .query_all(
                    "SELECT p.key, t.name, t.updated_at FROM threads t JOIN projects p ON p.id = t.project_id WHERE LOWER(t.status) = 'active' ORDER BY t.updated_at DESC",
                    (),
                    |row| {
                        Ok(PollCandidate {
                            updated_at: row.i64(2)?,
                            project: row.text(0)?,
                            head: row.text(1)?,
                            // A thread's name IS its label, so there is nothing
                            // for the elision to shorten; issues still carry a
                            // title and still take the `Some` arm.
                            title: None,
                            uri: format!("cairn://p/{}/{}", row.text(0)?, row.text(1)?),
                        })
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            rows.append(&mut found);
        }
        Ok(poll_targets(rows, followed))
    }

    /// Every active issue, addressed by number. The operator's follow vocabulary
    /// covers issue nodes too; only the poll that offers them is separate.
    async fn active_issues(
        &self,
        followed: &HashSet<String>,
    ) -> Result<(Vec<(String, String)>, usize), String> {
        let mut rows = Vec::new();
        for db in self.orch.db.all_dbs().await {
            let mut found = db
                .query_all(
                    "SELECT p.key, i.number, i.title, i.updated_at FROM issues i JOIN projects p ON p.id = i.project_id WHERE LOWER(i.status) = 'active' ORDER BY i.updated_at DESC",
                    (),
                    |row| {
                        Ok(PollCandidate {
                            updated_at: row.i64(3)?,
                            project: row.text(0)?,
                            head: row.i64(1)?.to_string(),
                            title: Some(row.text(2)?),
                            uri: format!("cairn://p/{}/{}", row.text(0)?, row.i64(1)?),
                        })
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            rows.append(&mut found);
        }
        Ok(poll_targets(rows, followed))
    }

    /// Resolve a follow URI to its target, mapping a number a thread migration
    /// vacated onto that thread. `channels.defaultThread` and every follow
    /// recorded before the cutover still carry a numeric address, and the
    /// canonical alias resolver is the single place that knows which numbers a
    /// thread now answers to.
    async fn resolve_target(&self, uri: &str) -> Result<FollowTarget, String> {
        let target = FollowTarget::parse(uri)?;
        let FollowTarget::Issue { project, .. } = &target else {
            return Ok(target);
        };
        let db = self.orch.db.for_project(project).await;
        let alias_uri = uri.to_string();
        let alias = db
            .read(move |conn| {
                let alias_uri = alias_uri.clone();
                Box::pin(async move {
                    crate::threads::resolve_parent_thread_uri_conn(conn, &alias_uri).await
                })
            })
            .await
            .map_err(|error| error.to_string())?;
        Ok(match alias {
            Some((_, _, name)) => FollowTarget::Thread {
                project: project.clone(),
                name,
            },
            None => target,
        })
    }

    /// The newest event a target's session has produced. A new follow starts here
    /// so neither a restart nor a re-follow replays history.
    async fn live_edge(&self, target: &FollowTarget) -> Result<i64, String> {
        let db = self.orch.db.for_project(target.project()).await;
        let edge = match target {
            // A thread's session is addressed by its SHAPE, never by "some job of
            // this thread": the sub-agent tasks it spawns and the pre-cutover
            // thread-issue's jobs carry its id too, and they are newer.
            FollowTarget::Thread { project, name } => {
                db.query_opt_i64(
                    format!(
                        "SELECT COALESCE(MAX(e.rowid), 0) FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN threads t ON t.id = j.thread_id JOIN projects p ON p.id = t.project_id WHERE p.key = ?1 AND t.name = ?2 AND {}",
                        crate::threads::SESSION_JOB_SHAPE
                    ),
                    params![cairn_common::uri::canonical_project(project), name.clone()],
                )
                .await
            }
            FollowTarget::Issue { project, number } => {
                db.query_opt_i64(
                    "SELECT COALESCE(MAX(e.rowid), 0) FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE p.key = ?1 AND i.number = ?2",
                    params![cairn_common::uri::canonical_project(project), *number],
                )
                .await
            }
        };
        edge.map(|edge| edge.unwrap_or(0))
            .map_err(|error| error.to_string())
    }

    /// The assistant text a followed target has produced since `cursor`, carrying
    /// the context each event needs to become a route fact.
    async fn followed_events(
        &self,
        target: &FollowTarget,
        cursor: i64,
    ) -> Result<Vec<FollowedEvent>, String> {
        let db = self.orch.db.for_project(target.project()).await;
        match target {
            FollowTarget::Thread { project, name } => db
                .query_all(
                    format!(
                        "SELECT e.rowid, e.data, p.key, t.name, j.id, p.id, p.repo_path FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN threads t ON t.id = j.thread_id JOIN projects p ON p.id = t.project_id WHERE p.key = ?1 AND t.name = ?2 AND {} AND e.rowid > ?3 AND e.event_type = 'assistant' ORDER BY e.rowid",
                        crate::threads::SESSION_JOB_SHAPE
                    ),
                    params![cairn_common::uri::canonical_project(project), name.clone(), cursor],
                    |row| {
                        Ok(FollowedEvent {
                            rowid: row.i64(0)?,
                            data: row.text(1)?,
                            // A thread's one identifier, written the way its URI
                            // and its pane header write it. An issue, which has
                            // a number AND a title, still reads "KEY-N Title".
                            context: format!("{}/{}", row.text(2)?, row.text(3)?),
                            job_id: row.text(4)?,
                            project_id: row.text(5)?,
                            repo_path: row.text(6)?,
                        })
                    },
                )
                .await,
            FollowTarget::Issue { project, number } => db
                .query_all(
                    "SELECT e.rowid, e.data, p.key, i.number, i.title, j.id, p.id, p.repo_path FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE p.key = ?1 AND i.number = ?2 AND e.rowid > ?3 AND e.event_type = 'assistant' ORDER BY e.rowid",
                    params![cairn_common::uri::canonical_project(project), *number, cursor],
                    |row| {
                        Ok(FollowedEvent {
                            rowid: row.i64(0)?,
                            data: row.text(1)?,
                            context: format!("{}/{} {}", row.text(2)?.to_lowercase(), row.i64(3)?, row.text(4)?),
                            job_id: row.text(5)?,
                            project_id: row.text(6)?,
                            repo_path: row.text(7)?,
                        })
                    },
                )
                .await,
        }
        .map_err(|error| error.to_string())
    }

    /// Deliver operator text into a target's session. A thread takes the same
    /// append the desktop composer takes — the message becomes visible in the
    /// thread and its session is steered, warm or cold. An issue node takes the
    /// ordinary queue-at-send job path.
    async fn route_to_target(&self, target: &FollowTarget, text: &str) -> Result<(), String> {
        let db = self.orch.db.for_project(target.project()).await;
        match target {
            FollowTarget::Thread { project, name } => {
                let thread_id = db
                    .query_opt(
                        "SELECT t.id FROM threads t JOIN projects p ON p.id = t.project_id WHERE p.key = ?1 AND t.name = ?2",
                        params![cairn_common::uri::canonical_project(project), name.clone()],
                        |row| row.text(0),
                    )
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("{project} has no thread named {name}"))?;
                crate::messages::delivery::append_thread_message(
                    &self.orch, &db, &thread_id, None, "operator", text,
                )
                .await
                .map(|_| ())
            }
            FollowTarget::Issue { project, number } => {
                let job_id = db.query_opt(
                    "SELECT j.id FROM jobs j JOIN runs r ON r.job_id = j.id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE p.key = ?1 AND i.number = ?2 AND j.parent_job_id IS NULL ORDER BY r.created_at DESC LIMIT 1",
                    params![cairn_common::uri::canonical_project(project), *number],
                    |row| row.text(0),
                ).await.map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("{} has no addressable node", target.uri()))?;
                crate::execution::jobs::continue_job_or_enqueue(
                    &self.orch,
                    &job_id,
                    Some(text),
                    None,
                    None,
                )
                .map(|_| ())
            }
        }
    }

    async fn finish_question(
        &self,
        record: &ledger::OutboundRecord,
        receipt: &str,
    ) -> Result<(), String> {
        cleanup_resolved_question(self.ledger(), self.provider.as_ref(), record, receipt).await?;
        Ok(())
    }

    /// Claims every gate open in one database right now, across every kind
    /// regardless of the route flags, so turning a route on later cannot dump its
    /// accumulated backlog onto the phone.
    async fn fence_existing_gates(&self, db: &LocalDb) -> Result<(usize, usize), String> {
        let mut gates = load_questions(db).await?;
        gates.extend(load_permissions(db).await?);
        let reviews = load_reviews(db).await?;
        gates.extend(reviews.gates);
        let mut fenced = 0;
        for gate in gates {
            if self.claim(&gate).await?.is_some() {
                fenced += 1;
            }
        }
        Ok((fenced, reviews.expired_dangling))
    }

    /// Claims a gate for this channel. Returns the new intent's id the first time
    /// the channel sees a gate, and `None` once an earlier sweep or an earlier
    /// session has already claimed it.
    async fn claim(&self, gate: &Gate) -> Result<Option<String>, String> {
        claim_gate_for_provider(
            &self.claims,
            self.ledger(),
            self.provider_id,
            &self.destination,
            self.rendering_for(&gate.ask),
            gate,
        )
        .await
    }

    fn rendering_for(&self, ask: &OutboundAsk) -> &'static str {
        if self.provider.capabilities().structured_asks
            && !matches!(ask, OutboundAsk::Notify { .. })
        {
            "poll"
        } else {
            "text"
        }
    }

    pub async fn sweep(&self) {
        if self.destination.trim().is_empty() {
            return;
        }
        if let Err(error) = self.sweep_live_gates().await {
            log::warn!("channel gate sweep failed: {error}");
        }
        if let Err(error) = self.sweep_followed_updates().await {
            log::warn!("channel followed-target update sweep failed: {error}");
        }
    }

    async fn sweep_followed_updates(&self) -> Result<(), String> {
        let follows = ledger::list_follows(self.ledger(), self.provider_id).await?;
        let presence = super::operator_presence(Some(self.provider.as_ref())).await;
        let now = Instant::now();
        for follow in follows {
            // One unresolvable follow must not silence every other one, so a
            // failure here is reported and skipped rather than aborting the sweep.
            let target = match self.resolve_target(&follow.uri).await {
                Ok(target) => target,
                Err(error) => {
                    log::warn!("channel skipped followed {}: {error}", follow.uri);
                    continue;
                }
            };
            let events = match self.followed_events(&target, follow.cursor_rowid).await {
                Ok(events) => events,
                Err(error) => {
                    log::warn!(
                        "channel could not read updates for followed {}: {error}",
                        follow.uri
                    );
                    continue;
                }
            };
            for event in events {
                if let Some(text) = assistant_text(&event.data) {
                    let detail_uri = format!("{}:event:{}", follow.uri, event.rowid);
                    let fields = std::collections::BTreeMap::from([
                        (
                            "project".into(),
                            serde_json::Value::String(target.project().to_string()),
                        ),
                        (
                            "threadUri".into(),
                            serde_json::Value::String(follow.uri.clone()),
                        ),
                        (
                            "detailUri".into(),
                            serde_json::Value::String(detail_uri.clone()),
                        ),
                        ("text".into(), serde_json::Value::String(text)),
                        ("context".into(), serde_json::Value::String(event.context)),
                        ("jobId".into(), serde_json::Value::String(event.job_id)),
                    ]);
                    let fact = RouteFact {
                        source: "thread_stream".into(),
                        identity: detail_uri,
                        summary: fields
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        fields,
                        route_provenance: None,
                    };
                    match crate::routes::dispatch(
                        &self.orch,
                        fact,
                        if presence == OperatorPresence::Present {
                            Presence::Active
                        } else {
                            Presence::Away
                        },
                        RouteContext {
                            project_id: Some(&event.project_id),
                            project_path: Some(std::path::Path::new(&event.repo_path)),
                        },
                    )
                    .await
                    {
                        Ok(submissions) => {
                            for submission in submissions {
                                if let Err(error) =
                                    self.submit_route(submission, presence, now).await
                                {
                                    log::warn!(
                                        "route channel submission could not be recorded: {error}"
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            log::warn!("route dispatch failed for followed target event: {error}")
                        }
                    }
                }
                ledger::advance_follow_cursor(
                    self.ledger(),
                    self.provider_id,
                    &follow.uri,
                    event.rowid,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn sweep_live_gates(&self) -> Result<(), String> {
        let mut gates = Vec::new();
        let mut snapshot_complete = true;
        for db in self.orch.db.all_dbs().await {
            match self.load_routed_gates(&db).await {
                Ok(mut db_gates) => gates.append(&mut db_gates),
                Err(error) => {
                    snapshot_complete = false;
                    log::warn!("channel skipped one project database during gate sweep: {error}");
                }
            }
        }
        let live: HashSet<_> = gates.iter().map(|gate| gate.binding_ref.clone()).collect();
        let presence = if gates.iter().any(Gate::is_presence_aware) {
            super::operator_presence(Some(self.provider.as_ref())).await
        } else {
            OperatorPresence::Away
        };
        let now = Instant::now();
        self.recover_pending_routes(presence, now).await?;
        let mut delivered = 0;
        for gate in gates {
            if delivered == SWEEP_LIMIT {
                break;
            }
            if self.deliver_or_defer(gate, presence, now).await? {
                delivered += 1;
            }
        }
        let cancelled = cancel_resolved_attention(
            &mut self
                .deferred_attention
                .lock()
                .expect("deferred attention set poisoned"),
            &live,
            snapshot_complete,
        );
        for id in cancelled {
            ledger::mark_expired(self.ledger(), &id, chrono::Utc::now().timestamp_millis()).await?;
        }
        if snapshot_complete {
            for record in ledger::list_unresolved(self.ledger(), self.provider_id).await? {
                if record.kind == "question"
                    && record.status == "sent"
                    && !live.contains(&record.binding_ref)
                {
                    self.finish_question(&record, "✓ question closed in Cairn")
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn load_routed_gates(&self, db: &LocalDb) -> Result<Vec<Gate>, String> {
        let mut gates = Vec::new();
        if self.route.question {
            gates.extend(load_questions(db).await?);
        }
        if self.route.permission {
            gates.extend(load_permissions(db).await?);
        }
        if self.route.review {
            gates.extend(load_reviews(db).await?.gates);
        }
        Ok(gates)
    }

    async fn deliver_or_defer(
        &self,
        gate: Gate,
        presence: OperatorPresence,
        now: Instant,
    ) -> Result<bool, String> {
        let deferred = self
            .deferred_attention
            .lock()
            .expect("deferred attention set poisoned")
            .remove(&gate.binding_ref);
        if let Some(deferred) = deferred {
            if attention_timing(presence, now, deferred.deadline) == AttentionTiming::Defer {
                self.deferred_attention
                    .lock()
                    .expect("deferred attention set poisoned")
                    .insert(gate.binding_ref.clone(), deferred);
                return Ok(false);
            }
            return self.send_claimed(deferred.id, deferred.gate).await;
        }
        if let Some(record) = ledger::get_by_binding(
            self.ledger(),
            self.provider_id,
            gate.kind,
            &gate.binding_ref,
        )
        .await?
        {
            return match record.status.as_str() {
                // A failed intent may already have crossed the provider boundary.
                // Retrying it can duplicate an iMessage, so failure is terminal.
                "failed" => Ok(false),
                "expired" => Ok(true),
                _ => Ok(false),
            };
        }
        let Some(id) = self.claim(&gate).await? else {
            return Ok(false);
        };
        if gate.is_presence_aware() && presence == OperatorPresence::Present {
            self.deferred_attention
                .lock()
                .expect("deferred attention set poisoned")
                .insert(
                    gate.binding_ref.clone(),
                    DeferredAttention {
                        id,
                        gate,
                        deadline: now + ATTENTION_GRACE,
                    },
                );
            return Ok(false);
        }
        self.send_claimed(id, gate).await
    }

    /// Reports whether this call put something on the wire, so a sweep's batch
    /// bounds sends rather than gates examined.
    async fn send_claimed(&self, id: String, gate: Gate) -> Result<bool, String> {
        if !ledger::begin_delivery(self.ledger(), &id).await? {
            return Ok(false);
        }
        let options_json = match &gate.ask {
            OutboundAsk::Question { options, .. } => Some(
                serde_json::to_string(
                    &options
                        .iter()
                        .map(|option| option.label.as_str())
                        .collect::<Vec<_>>(),
                )
                .map_err(|error| error.to_string())?,
            ),
            _ => None,
        };
        let message = OutboundMessage {
            intent_id: id.clone(),
            conversation: self.destination.clone(),
            initiated_by: gate.initiated_by,
            ask: gate.ask,
            context_header: gate.context,
        };
        match self.provider.send(&message).await {
            Ok(sent) => {
                log::info!(
                    "channel delivered {} {} as {}",
                    gate.kind,
                    gate.binding_ref,
                    sent.primary_guid
                );
                ledger::mark_sent(
                    self.ledger(),
                    &id,
                    &sent.primary_guid,
                    sent.caption_guid.as_deref(),
                    options_json.as_deref(),
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            }
            Err(error) => {
                log::warn!(
                    "channel delivery of {} {} failed: {error}",
                    gate.kind,
                    gate.binding_ref
                );
                ledger::mark_failed(self.ledger(), &id, &error).await?;
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn handle_inbound(&self, event: InboundEvent) -> Result<(), String> {
        match event {
            InboundEvent::Selection {
                bound_guid,
                sender,
                option_text,
                selected,
                ..
            } => {
                self.resolve_selection(&bound_guid, &sender, &option_text, selected)
                    .await
            }
            InboundEvent::Selections {
                bound_guid,
                sender,
                changes,
            } => {
                for change in changes {
                    self.resolve_selection(
                        &bound_guid,
                        &sender,
                        &change.option_text,
                        change.selected,
                    )
                    .await?;
                }
                Ok(())
            }
            InboundEvent::Reply {
                bound_guid,
                sender,
                text,
            } => self.resolve_bound(&bound_guid, &sender, &text).await,
            InboundEvent::Bare { sender, text } => self.resolve_bare(&sender, &text).await,
        }
    }

    async fn resolve_bound(&self, guid: &str, sender: &str, text: &str) -> Result<(), String> {
        if let Some(record) =
            ledger::get_by_provider_guid(self.ledger(), self.provider_id, guid).await?
        {
            if record.kind == "review" {
                if let Some(requested) = unfollow_selector(text) {
                    let bound_uri = record.binding_ref.split(":event:").next();
                    let bound_project = bound_uri
                        .and_then(parse_uri)
                        .and_then(|uri| uri.project().map(str::to_string));
                    let followed_uri = match requested {
                        None => bound_uri.map(str::to_string),
                        // A selector names a target the way the operator saw it:
                        // a thread by name, an issue by number.
                        Some(selector) => ledger::list_follows(self.ledger(), self.provider_id)
                            .await?
                            .into_iter()
                            .find(|follow| {
                                FollowTarget::parse(&follow.uri).is_ok_and(|target| {
                                    target.selector().eq_ignore_ascii_case(&selector)
                                        && Some(target.project()) == bound_project.as_deref()
                                })
                            })
                            .map(|follow| follow.uri),
                    };
                    let Some(followed_uri) = followed_uri else {
                        return self.send_notice(sender, "That follow was not found.").await;
                    };
                    let changed =
                        ledger::is_target_followed(self.ledger(), self.provider_id, &followed_uri)
                            .await?;
                    if changed {
                        self.unfollow(&followed_uri).await?;
                    }
                    let confirmation = if changed {
                        format!("Unfollowed {followed_uri}.")
                    } else {
                        format!("{followed_uri} was already unfollowed.")
                    };
                    self.send_notice(sender, &confirmation).await?;
                    return Ok(());
                }
            }
            return self.resolve_record(record, text).await;
        }
        if poll_command(text).is_some() {
            return self.resolve_bare(sender, text).await;
        }
        self.store_unsolicited(Some(guid), sender, text).await
    }

    async fn resolve_selection(
        &self,
        guid: &str,
        sender: &str,
        text: &str,
        selected: bool,
    ) -> Result<(), String> {
        let Some(record) =
            ledger::get_by_provider_guid(self.ledger(), self.provider_id, guid).await?
        else {
            return self.store_unsolicited(Some(guid), sender, text).await;
        };
        if record.binding_ref.starts_with(FOLLOW_POLL_PREFIX) {
            let options = follow_poll_options(&record)
                .ok_or_else(|| "follow poll has no option bindings".to_string())?;
            if let Some(uri) = options.bindings.get(text) {
                if selected {
                    self.follow(uri).await?;
                } else {
                    self.unfollow(uri).await?;
                }
            }
            return Ok(());
        }
        if selected {
            self.resolve_record(record, text).await
        } else {
            Ok(())
        }
    }

    async fn resolve_bare(&self, sender: &str, text: &str) -> Result<(), String> {
        if let Some(kind) = poll_command(text) {
            if let Err(error) = self.send_follow_poll(sender, kind).await {
                log::warn!("channel could not answer the {kind:?} command: {error}");
                self.send_notice(sender, &format!("{}: {error}", kind.failure_prefix()))
                    .await?;
            }
            return Ok(());
        }
        if !text.trim_start().starts_with('/') {
            let focused = ledger::get_focus(self.ledger(), self.provider_id)
                .await?
                .unwrap_or_else(|| {
                    crate::config::settings::load_settings(&self.orch.config_dir)
                        .channels
                        .default_thread
                });
            let target = self.resolve_target(&focused).await?;
            return self.route_to_target(&target, text).await;
        }
        let mut matches = ledger::list_unresolved(self.ledger(), self.provider_id)
            .await?
            .into_iter()
            .filter(|record| {
                record.status == "sent"
                    && super::imessage::normalize_handle(&record.conversation)
                        == super::imessage::normalize_handle(sender)
            })
            .collect::<Vec<_>>();
        if matches.len() == 1 {
            let record = matches.pop().expect("one match");
            return self.resolve_record(record, text).await;
        }
        if matches.len() > 1 {
            return self.send_notice(sender, "I found more than one active ask. Please reply to the specific message you want to answer.").await;
        }
        self.store_unsolicited(None, sender, text).await
    }

    async fn resolve_record(
        &self,
        record: ledger::OutboundRecord,
        text: &str,
    ) -> Result<(), String> {
        if record.status == "resolved" {
            return Ok(());
        }
        match record.kind.as_str() {
            "question" => {
                if record.binding_ref.starts_with(FOLLOW_POLL_PREFIX) {
                    let selected_label = follow_poll_answer(&record, text);
                    return self.follow_poll_selection(&record, &selected_label).await;
                }
                let answer = question_answer(&record, text);
                let won_answer_claim = ledger::claim_question_answer(
                    self.ledger(),
                    &record.id,
                    &answer,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                if won_answer_claim {
                    cleanup_claimed_question(
                        self.provider.as_ref(),
                        &record,
                        &format!("✓ answered: {answer}"),
                    )
                    .await;
                } else if !ledger::record_answer_after_cleanup_claim(
                    self.ledger(),
                    &record.id,
                    &answer,
                )
                .await?
                {
                    return Ok(());
                }
                let (prompt_id, _) = record
                    .binding_ref
                    .rsplit_once(':')
                    .ok_or_else(|| format!("invalid question binding: {}", record.binding_ref))?;
                let question_count = prompt_question_count(&self.orch, prompt_id).await?;
                let answers =
                    ledger::answered_for_prompt(self.ledger(), self.provider_id, prompt_id).await?;
                if answers.len() == question_count {
                    let response = if question_count == 1 {
                        answers[0].1.clone()
                    } else {
                        answers
                            .into_iter()
                            .map(|(index, answer)| format!("Question {}: {}", index + 1, answer))
                            .collect::<Vec<_>>()
                            .join("\n")
                    };
                    answer_prompt_id(&self.orch, prompt_id, response).await?;
                }
            }
            "permission" => {
                let decision = parse_permission(text)?;
                // A chat reply answers with the narrowest containment scope
                // available: there is no picker in a channel message, and
                // inferring a broader one from a one-word reply would grant more
                // than the replier said. It carries no operator capability
                // either — a message in a room is not an authenticated operator
                // at the desktop prompt — so an authority allow arriving here is
                // refused and the prompt stays pending.
                resolve_permission_request(
                    &self.orch,
                    &record.binding_ref,
                    crate::mcp::handlers::permission::PermissionAnswer::from_surface(
                        decision,
                        crate::mcp::handlers::permission::AnswerSurface::ChannelReply,
                    )
                    .with_containment_scope(PermissionScope::Once),
                )
                .await?;
                ledger::mark_resolved(
                    self.ledger(),
                    &record.id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            }
            "review" => {
                let job_id = record
                    .job_id
                    .as_deref()
                    .ok_or_else(|| "review delivery has no recipient job".to_string())?;
                crate::execution::jobs::continue_job_or_enqueue(
                    &self.orch,
                    job_id,
                    Some(text),
                    None,
                    None,
                )?;
                ledger::mark_resolved(
                    self.ledger(),
                    &record.id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            }
            other => return Err(format!("unknown channel outbound kind: {other}")),
        }
        Ok(())
    }

    async fn store_unsolicited(
        &self,
        guid: Option<&str>,
        sender: &str,
        text: &str,
    ) -> Result<(), String> {
        let db = &self.orch.db.local;
        let id = Uuid::new_v4().to_string();
        ledger::insert_inbound(
            db,
            &ledger::InboundRecord {
                id: id.clone(),
                channel: self.provider_id.into(),
                provider_guid: guid.map(str::to_string),
                sender: sender.into(),
                text: text.into(),
                received_at: chrono::Utc::now().timestamp_millis(),
                acknowledged_at: None,
            },
        )
        .await?;
        self.send_notice(sender, "No active ask — your message is visible in Cairn.")
            .await?;
        ledger::mark_inbound_acknowledged(db, &id, chrono::Utc::now().timestamp_millis()).await?;
        let _ = self.orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table":"channel_inbound","action":"insert"}),
        );
        Ok(())
    }

    async fn send_notice(&self, conversation: &str, text: &str) -> Result<(), String> {
        self.provider
            .send(&OutboundMessage {
                intent_id: Uuid::new_v4().to_string(),
                conversation: conversation.into(),
                initiated_by: OutboundInitiator::OperatorInbound,
                ask: OutboundAsk::Notify { text: text.into() },
                context_header: "[Cairn]".into(),
            })
            .await
            .map(|_| ())
    }
}

pub fn spawn(
    orch: Orchestrator,
    provider: Arc<dyn ChannelProvider>,
    provider_id: &'static str,
    destination: String,
    route: ChannelRouteConfig,
) -> Vec<tokio::task::JoinHandle<()>> {
    let router = Arc::new(ChannelRouter::new_for_provider(
        orch,
        provider.clone(),
        provider_id,
        destination,
        route,
    ));
    let (route_tx, mut route_rx) = tokio::sync::mpsc::unbounded_channel();
    super::route_submission_slot()
        .lock()
        .expect("route submission slot poisoned")
        .insert(provider_id, route_tx);
    let route_router = router.clone();
    let route_task = tokio::spawn(async move {
        while let Some(submission) = route_rx.recv().await {
            let presence = super::operator_presence_status().await.presence;
            if let Err(error) = route_router
                .submit_route(submission, presence, Instant::now())
                .await
            {
                log::warn!("route channel submission failed: {error}");
            }
        }
    });
    let sweep = router.clone();
    let sweep_task = tokio::spawn(async move {
        // The channel is a live tap: this session carries the asks raised from
        // here on, and the backlog that predates it belongs to the app, not the
        // phone. Sealing it off COMPLETELY is a precondition for sweeping at all,
        // so a failure parks the sweep rather than letting it text the backlog.
        while let Err(error) = sweep.draw_the_session_line().await {
            super::set_router_blocker(provider_id, Some(error.clone()));
            log::warn!(
                "channel cannot seal the pre-session backlog, so it is not sweeping yet: {error}"
            );
            // A retry must resnapshot the complete backlog to preserve the safety
            // boundary. Pace that expensive work separately from ordinary sweeps.
            tokio::time::sleep(BACKLOG_SEAL_RETRY_INTERVAL).await;
        }
        super::set_router_blocker(provider_id, None);
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = super::wait_for_presence_change() => {},
            }
            sweep.sweep().await;
        }
    });
    let inbound = router.clone();
    let inbound_task = tokio::spawn(async move {
        let mut events = provider.subscribe();
        while let Some(event) = events.recv().await {
            if let Err(error) = inbound.handle_inbound(event).await {
                log::warn!("channel inbound routing failed: {error}");
            }
        }
    });
    vec![sweep_task, inbound_task, route_task]
}

/// Every question gate currently open, newest first. Deliberately unlimited: a
/// `LIMIT` here would decide admission by `created_at` ordering, which cannot
/// separate gates raised within the same second. The router bounds DELIVERIES.
async fn load_questions(db: &LocalDb) -> Result<Vec<Gate>, String> {
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query("SELECT p.id, p.questions, COALESCE(p.job_id, r.job_id), COALESCE(j.node_name, j.uri_segment, 'agent'), pr.key, i.number, e.seq, j.uri_segment, p.uri_segment FROM prompts p JOIN runs r ON r.id=p.run_id LEFT JOIN jobs j ON j.id=COALESCE(p.job_id,r.job_id) LEFT JOIN issues i ON i.id=COALESCE(j.issue_id,r.issue_id) LEFT JOIN projects pr ON pr.id=i.project_id LEFT JOIN executions e ON e.id=j.execution_id WHERE p.response IS NULL AND COALESCE(i.status,'open') NOT IN ('merged','closed','failed') ORDER BY p.created_at DESC", ()).await?;
        let mut gates = Vec::new();
        while let Some(row) = rows.next().await? {
            let prompt_id = row.text(0)?; let questions_json = row.text(1)?; let job_id = row.opt_text(2)?; let context = format!("[Cairn · {}]", row.text(3)?);
            let question_uri = match (row.opt_text(4)?, row.opt_i64(5)?, row.opt_i64(6)?, row.opt_text(7)?, row.opt_text(8)?) {
                (Some(project), Some(number), Some(exec_seq), Some(node), Some(segment)) => Some(build_node_question_uri(&project, number as i32, exec_seq as i32, &node, &segment)),
                _ => None,
            };
            let questions: Vec<StoredQuestion> = serde_json::from_str(&questions_json).map_err(|e| crate::storage::DbError::Row(e.to_string()))?;
            for (index, question) in questions.into_iter().enumerate() {
                let text = match &question_uri { Some(uri) => format!("{}\n\n{}", question.question, uri), None => question.question };
                gates.push(Gate { kind: "question", initiated_by: OutboundInitiator::CairnPush, binding_ref: format!("{prompt_id}:{index}"), job_id: job_id.clone(), context: context.clone(), ask: OutboundAsk::Question { prompt_id: prompt_id.clone(), question_index: index, text, options: question.options.into_iter().map(|o| AskOption { label:o.label, description:o.description }).collect() } });
            }
        }
        Ok(gates)
    })).await.map_err(|e| e.to_string())
}

async fn load_permissions(db: &LocalDb) -> Result<Vec<Gate>, String> {
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query("SELECT req.id, req.tool_name, req.tool_input, COALESCE(req.job_id,r.job_id), COALESCE(j.node_name,j.uri_segment,'agent'), p.key, i.number, e.seq, j.uri_segment, req.uri_segment FROM permission_requests req JOIN runs r ON r.id=req.run_id LEFT JOIN jobs j ON j.id=COALESCE(req.job_id,r.job_id) LEFT JOIN issues i ON i.id=COALESCE(j.issue_id,r.issue_id) LEFT JOIN projects p ON p.id=i.project_id LEFT JOIN executions e ON e.id=j.execution_id WHERE req.status='pending' AND COALESCE(i.status,'open') NOT IN ('merged','closed','failed') ORDER BY req.created_at DESC", ()).await?;
        let mut gates=Vec::new(); while let Some(row)=rows.next().await? { let id=row.text(0)?; let tool=row.text(1)?; let input=row.text(2)?; let uri=match(row.opt_text(5)?,row.opt_i64(6)?,row.opt_i64(7)?,row.opt_text(8)?,row.opt_text(9)?){(Some(project),Some(number),Some(exec_seq),Some(node),Some(segment))=>Some(build_node_permission_uri(&project,number as i32,exec_seq as i32,&node,&segment)),_=>None}; let summary=match uri{Some(uri)=>format!("Allow {tool}?\n{input}\n\n{uri}"),None=>format!("Allow {tool}?\n{input}")}; gates.push(Gate { kind:"permission", initiated_by: OutboundInitiator::CairnPush, binding_ref:id.clone(), job_id:row.opt_text(3)?, context:format!("[Cairn · {}]",row.text(4)?), ask:OutboundAsk::Permission { request_id:id, summary } }); } Ok(gates)
    })).await.map_err(|e| e.to_string())
}

async fn load_reviews(db: &LocalDb) -> Result<ReviewGates, String> {
    let pushes = db.read(|conn| Box::pin(async move {
        let mut rows=conn.query("SELECT id,recipient,content_ref,wake,boundary,\"key\",created_at,delivered_event_id FROM attention_pushes WHERE delivered_event_id IS NULL AND \"key\" LIKE 'review:%' ORDER BY created_at DESC",()).await?; let mut out=Vec::new(); while let Some(row)=rows.next().await? { out.push(crate::orchestrator::attention_push::Push { id:row.text(0)?,recipient:row.text(1)?,content_ref:row.text(2)?,wake:crate::orchestrator::attention_push::Wake::from_db(&row.text(3)?).unwrap(),boundary:crate::orchestrator::attention_push::Boundary::from_db(&row.text(4)?).unwrap(),key:row.text(5)?,created_at:row.i64(6)?,delivered_event_id:row.opt_text(7)? }); } Ok(out)
    })).await.map_err(|e| e.to_string())?;
    let mut result = ReviewGates::default();
    for push in pushes {
        if crate::orchestrator::attention_push::lazy_resolve_live(db, &push)
            .await
            .map_err(|e| e.to_string())?
        {
            let parsed = parse_uri(&push.content_ref);
            let reference = parsed
                .as_ref()
                .and_then(|parsed| Some((parsed.project()?.to_string(), parsed.issue_number()?)));
            let Some((project, number)) = reference else {
                crate::orchestrator::attention_push::delete_pending_by_id(db, &push.id)
                    .await
                    .map_err(|error| error.to_string())?;
                result.expired_dangling += 1;
                log::warn!(
                    "channel expired dangling review {} because its reference is invalid: {}",
                    push.id,
                    push.content_ref
                );
                continue;
            };
            let title = db
                .query_opt_text(
                    "SELECT i.title FROM issues i JOIN projects p ON p.id=i.project_id WHERE upper(p.key)=upper(?1) AND i.number=?2",
                    (project.clone(), number),
                )
                .await
                .map_err(|error| error.to_string())?;
            let Some(title) = title else {
                crate::orchestrator::attention_push::delete_pending_by_id(db, &push.id)
                    .await
                    .map_err(|error| error.to_string())?;
                result.expired_dangling += 1;
                log::warn!(
                    "channel expired dangling review {} because issue {project}/{number} no longer exists",
                    push.id
                );
                continue;
            };
            result.gates.push(Gate {
                kind: "review",
                initiated_by: OutboundInitiator::CairnPush,
                // Bind the external once-only fence to the semantic review fact,
                // never to the randomly generated queue-row UUID.
                binding_ref: push.key.clone(),
                job_id: Some(push.recipient),
                context: String::new(),
                ask: OutboundAsk::Notify {
                    text: review_notice(&project, number, &title, &push.content_ref),
                },
            });
        }
    }
    Ok(result)
}

/// Claims a gate for delivery. Returns the new intent's id the first time the
/// channel sees the gate, and `None` once an earlier sweep -- or the snapshot
/// that sealed off the backlog when this session opened -- already claimed it.
/// This claim is the channel's whole once-only guarantee, and the only thing
/// that decides whether a gate is backlog or live.
#[cfg(test)]
async fn claim_gate(
    claims: &ClaimSet,
    ledger: &LocalDb,
    conversation: &str,
    rendering: &'static str,
    gate: &Gate,
) -> Result<Option<String>, String> {
    claim_gate_for_provider(claims, ledger, CHANNEL, conversation, rendering, gate).await
}

async fn claim_gate_for_provider(
    claims: &ClaimSet,
    ledger: &LocalDb,
    provider_id: &'static str,
    conversation: &str,
    rendering: &'static str,
    gate: &Gate,
) -> Result<Option<String>, String> {
    if claims.holds(&gate.binding_ref) {
        return Ok(None);
    }
    let id = Uuid::new_v4().to_string();
    let rendered = render_text_floor(&gate.ask);
    let inserted = ledger::insert_intent(
        ledger,
        &ledger::NewOutbound {
            id: &id,
            channel: provider_id,
            kind: gate.kind,
            binding_ref: &gate.binding_ref,
            conversation,
            job_id: gate.job_id.as_deref(),
            rendered_text: &rendered,
            rendering,
            created_at: chrono::Utc::now().timestamp_millis(),
        },
    )
    .await?;
    // Claimed either way: a gate the ledger already fenced in an earlier session
    // is just as much this session's business to leave alone.
    claims.claim(&gate.binding_ref);
    Ok(inserted.then_some(id))
}

/// A prompt lives in whichever database holds its project, so the count that
/// decides when a multi-question ask is complete is searched across all of them.
/// This is the one lookup on the reply path that is NOT private-ledger state.
async fn prompt_question_count(orch: &Orchestrator, prompt_id: &str) -> Result<usize, String> {
    for db in orch.db.all_dbs().await {
        let json = db
            .query_opt_text(
                "SELECT questions FROM prompts WHERE id=?1",
                params![prompt_id.to_string()],
            )
            .await
            .map_err(|e| e.to_string())?;
        if let Some(json) = json {
            return serde_json::from_str::<Vec<serde_json::Value>>(&json)
                .map(|v| v.len())
                .map_err(|e| e.to_string());
        }
    }
    Err(format!("prompt not found: {prompt_id}"))
}

fn question_answer(record: &ledger::OutboundRecord, text: &str) -> String {
    let trimmed = text.trim();
    let Some(index) = super::imessage::parse_reply_number(trimmed, usize::MAX) else {
        return trimmed.to_string();
    };
    let options: Vec<String> = record
        .options_json
        .as_deref()
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_default();
    options
        .get(index)
        .cloned()
        .unwrap_or_else(|| trimmed.to_string())
}
fn parse_permission(text: &str) -> Result<PermissionDecision, String> {
    match text.trim().to_ascii_lowercase().as_str() {
        "1" | "yes" | "y" | "approve" | "allow" => Ok(PermissionDecision::Allow),
        "2" | "no" | "n" | "deny" | "denied" => Ok(PermissionDecision::Deny),
        _ => Err("Reply Approve or Deny to resolve this permission request.".into()),
    }
}

#[cfg(test)]
const CHANNEL: &str = "imessage";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::ledger::expire_undelivered;
    use crate::models::IMessageChannelConfig;
    use crate::storage::migrated_test_db;

    #[tokio::test]
    async fn dangling_review_references_expire_individually_without_aborting_the_batch() {
        let db = migrated_test_db("channel-router-dangling-reviews.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('reviewer-issue', 'p', 1, 'Reviewer', 'active', 1, 1);
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('reviewer', 'p', 'reviewer-issue', 'running', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr-live', 'reviewer', 'p', 'reviewer-issue', 'Live review', 'feature', 'main', 'open', 1, 1);
             INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at)
             VALUES
               ('missing-issue', 'reviewer', 'cairn://p/CAIRN/1790/1/reviewer/create-pr', 'wake', 'event', 'review:missing', 1),
               ('invalid-ref', 'reviewer', 'not-a-cairn-uri', 'wake', 'event', 'review:invalid', 2),
               ('live-review', 'reviewer', 'cairn://p/CAIRN/1/1/reviewer/create-pr', 'wake', 'event', 'review:live', 3);",
        )
        .await
        .unwrap();

        let reviews = load_reviews(&db).await.unwrap();

        assert_eq!(
            reviews
                .gates
                .iter()
                .map(|gate| gate.binding_ref.as_str())
                .collect::<Vec<_>>(),
            vec!["review:live"],
            "a dangling predecessor never prevents the valid sibling from reaching the seal"
        );
        assert_eq!(reviews.expired_dangling, 2);
        assert_eq!(
            db.query_one(
                "SELECT COUNT(*) FROM attention_pushes WHERE delivered_event_id IS NULL AND key LIKE 'review:%'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap(),
            1,
            "only the live sibling remains; dangling rows are never reconsidered"
        );
    }

    // Operator presence is process-global, and a review gate is presence-aware:
    // a sibling test pinning presence Active would make this sweep defer its
    // delivery instead of sending it. Joining that serial group is what keeps
    // the assertion about this gate rather than about test interleaving.
    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn pending_review_across_many_sweeps_delivers_one_push_and_one_text() {
        let (orch, db) = route_test_orchestrator("review-many-sweeps.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('issue', 'p', 3727, 'Frontend minimal slice', 'active', 1, 1);
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('reviewer', 'p', 'issue', 'running', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr', 'reviewer', 'p', 'issue', 'Review', 'feature', 'main', 'open', 1, 1);
             INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at, fingerprint)
               VALUES('push', 'reviewer', 'cairn://p/CAIRN/3727/1/builder/pr', 'wake', 'event',
                      'review:cairn://p/CAIRN/3727', 1, 'sha:stable');",
        )
        .await
        .unwrap();
        let provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Away),
        });
        let router = ChannelRouter::new(
            orch,
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );

        router.sweep_live_gates().await.unwrap();
        for revision in 2..=24 {
            db.execute("DELETE FROM attention_pushes", ())
                .await
                .unwrap();
            db.execute(
                "INSERT INTO attention_pushes
                   (id, recipient, content_ref, wake, boundary, key, created_at, fingerprint)
                 VALUES(?1, 'reviewer', 'cairn://p/CAIRN/3727/1/builder/pr', 'wake', 'event',
                        'review:cairn://p/CAIRN/3727', ?2, ?3)",
                params![
                    format!("push-{revision}"),
                    revision,
                    format!("sha:variant-{revision}")
                ],
            )
            .await
            .unwrap();
            router.sweep_live_gates().await.unwrap();
        }

        assert_eq!(
            provider.sends.lock().unwrap().len(),
            1,
            "new row UUIDs and fingerprints for one review-ready fact never cross the channel fence"
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM attention_pushes", ())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
                .await
                .unwrap(),
            Some(1)
        );
    }

    #[test]
    fn poll_commands_are_exact_case_insensitive_bare_words() {
        assert_eq!(poll_command("threads"), Some(PollKind::Threads));
        assert_eq!(poll_command("  Threads\n"), Some(PollKind::Threads));
        assert_eq!(poll_command("/threads"), Some(PollKind::Threads));
        assert_eq!(poll_command("issues"), Some(PollKind::Issues));
        assert_eq!(poll_command("/Issues"), Some(PollKind::Issues));
        assert_eq!(poll_command("threads please"), None);
    }

    #[test]
    fn a_follow_uri_reads_as_a_thread_name_or_an_issue_number() {
        assert_eq!(
            FollowTarget::parse("cairn://p/cairn/settings-ui").unwrap(),
            FollowTarget::Thread {
                project: "CAIRN".into(),
                name: "settings-ui".into()
            }
        );
        assert_eq!(
            FollowTarget::parse("cairn://p/CAIRN/3404").unwrap(),
            FollowTarget::Issue {
                project: "CAIRN".into(),
                number: 3404
            }
        );
        assert_eq!(
            FollowTarget::parse("cairn://p/CAIRN/settings-ui")
                .unwrap()
                .uri(),
            "cairn://p/CAIRN/settings-ui"
        );
        assert!(FollowTarget::parse("not-a-uri").is_err());
    }

    #[test]
    fn unfollow_names_a_target_the_way_the_operator_saw_it() {
        assert_eq!(unfollow_selector("unfollow"), Some(None));
        assert_eq!(
            unfollow_selector(" UnFollow 3404 "),
            Some(Some("3404".into()))
        );
        assert_eq!(
            unfollow_selector("unfollow settings-ui"),
            Some(Some("settings-ui".into()))
        );
        assert_eq!(unfollow_selector("unfollow 3404 please"), None);
        assert_eq!(unfollow_selector("follow 3404"), None);

        let bound_project = parse_uri("cairn://p/CAIRN/settings-ui")
            .and_then(|uri| uri.project().map(str::to_string));
        let follows = [
            "cairn://p/OTHER/settings-ui",
            "cairn://p/CAIRN/3404",
            "cairn://p/CAIRN/settings-ui",
        ];
        let selected = follows.into_iter().find(|uri| {
            FollowTarget::parse(uri).is_ok_and(|target| {
                target.selector().eq_ignore_ascii_case("settings-ui")
                    && Some(target.project()) == bound_project.as_deref()
            })
        });
        assert_eq!(selected, Some("cairn://p/CAIRN/settings-ui"));
    }

    #[test]
    fn a_poll_label_elides_a_title_a_balloon_cannot_show() {
        assert_eq!(poll_label("general", Some("General")), "general · General");
        assert_eq!(poll_label("general", None), "general");
        assert_eq!(poll_label("general", Some("   ")), "general");
        let long = "A".repeat(FOLLOW_POLL_TITLE_LIMIT + 20);
        let label = poll_label("3757", Some(&long));
        assert!(label.ends_with('…'));
        assert_eq!(label.chars().count(), FOLLOW_POLL_TITLE_LIMIT + 8);
    }

    struct RejectingPollProvider {
        sends: Mutex<Vec<OutboundAsk>>,
    }

    #[async_trait::async_trait]
    impl ChannelProvider for RejectingPollProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities {
                structured_asks: true,
                open_options: false,
                edit_in_place: false,
                max_text_len: None,
            }
        }

        async fn send(
            &self,
            message: &OutboundMessage,
        ) -> Result<crate::channels::SentIds, String> {
            self.sends.lock().unwrap().push(message.ask.clone());
            match message.ask {
                OutboundAsk::Question { .. } => Err("poll bridge unavailable".into()),
                OutboundAsk::Notify { .. } => Ok(crate::channels::SentIds {
                    primary_guid: "fallback-guid".into(),
                    caption_guid: None,
                }),
                OutboundAsk::Permission { .. } => unreachable!(),
            }
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Ready
        }
    }

    struct PresentProvider {
        sends: Mutex<Vec<OutboundMessage>>,
        presence_checks: Mutex<usize>,
        presence: Mutex<OperatorPresence>,
    }

    #[async_trait::async_trait]
    impl ChannelProvider for PresentProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities {
                structured_asks: false,
                open_options: false,
                edit_in_place: false,
                max_text_len: None,
            }
        }

        async fn send(
            &self,
            message: &OutboundMessage,
        ) -> Result<crate::channels::SentIds, String> {
            self.sends.lock().unwrap().push(message.clone());
            Ok(crate::channels::SentIds {
                primary_guid: "stream-guid".into(),
                caption_guid: None,
            })
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Ready
        }

        async fn operator_presence(&self) -> OperatorPresence {
            *self.presence_checks.lock().unwrap() += 1;
            *self.presence.lock().unwrap()
        }
    }

    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn bundled_followed_thread_route_matches_presence_and_records_history() {
        super::super::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Active);
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(migrated_test_db("channel-router-follow-presence.db").await);
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('i', 'p', 1, 'Followed work', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e', 'build', 'i', 'p', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('j', 'e', 'i', 'p', 'running', 'builder', 'builder', 1, 1);
             INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at)
               VALUES ('r', 'j', 'i', 'running', 1, 1);
             INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('event-1', 'r', 1, 1, 'assistant', '{\"content\":\"first active update\",\"toolUses\":[]}', 1),
                      ('event-2', 'r', 2, 2, 'assistant', '{\"content\":\"second active update\",\"toolUses\":[]}', 2);",
        )
        .await
        .unwrap();
        ledger::follow_target(&db, CHANNEL, "cairn://p/CAIRN/1", 1, 0)
            .await
            .unwrap();

        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().join("config"),
        )
        .boot_at(0)
        .build();
        let provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Present),
        });
        let router = ChannelRouter::new(
            orch,
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );

        router.sweep_followed_updates().await.unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert!(sends.is_empty());
        }
        assert_eq!(*provider.presence_checks.lock().unwrap(), 1);
        assert!(ledger::get_by_binding(
            &db,
            CHANNEL,
            "route",
            "route:followed-thread-stream:cairn://p/CAIRN/1:event:1",
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            ledger::list_follows(&db, CHANNEL).await.unwrap()[0].cursor_rowid,
            2
        );
        assert!(
            crate::storage::list_route_firings(&db, "workspace", "followed-thread-stream", 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(router.deferred_attention.lock().unwrap().is_empty());

        db.execute(
            "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at) VALUES ('event-away', 'r', 3, 3, 'assistant', '{\"content\":\"away update\",\"toolUses\":[]}', 3)",
            (),
        )
        .await
        .unwrap();
        *provider.presence.lock().unwrap() = OperatorPresence::Away;
        super::super::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Idle);
        let restarted = ChannelRouter::new(
            router.orch.clone(),
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );
        restarted.sweep_followed_updates().await.unwrap();
        let route_intent = ledger::get_by_binding(
            &db,
            CHANNEL,
            "route",
            "route:followed-thread-stream:cairn://p/CAIRN/1:event:3",
        )
        .await
        .unwrap();
        let early_firings =
            crate::storage::list_route_firings(&db, "workspace", "followed-thread-stream", 10)
                .await
                .unwrap();
        let outbound = ledger::list_unresolved(&db, CHANNEL).await.unwrap();
        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(
                sends.len(),
                1,
                "route intent: {route_intent:?}; outbound: {outbound:?}; firings: {early_firings:?}"
            );
            assert!(matches!(
                &sends[0].ask,
                OutboundAsk::Notify { text } if text == "away update"
            ));
        }
        let firings =
            crate::storage::list_route_firings(&db, "workspace", "followed-thread-stream", 10)
                .await
                .unwrap();
        assert_eq!(firings.len(), 1);
        assert_eq!(firings[0].fact_identity, "cairn://p/CAIRN/1:event:3");
        assert_eq!(firings[0].status, "fired");
        super::super::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Auto);
    }

    #[tokio::test]
    async fn failed_native_poll_attempts_text_fallback_without_recording_bindings() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(migrated_test_db("channel-router-poll-fallback.db").await);
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
               VALUES ('t1', 'p', 'general', 'active', 1, 2),
                      ('t2', 'p', 'performance', 'active', 1, 3);",
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().join("config"),
        )
        .boot_at(0)
        .build();
        let provider = Arc::new(RejectingPollProvider {
            sends: Mutex::new(Vec::new()),
        });
        let router = ChannelRouter::new(
            orch,
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );

        router
            .handle_inbound(InboundEvent::Reply {
                bound_guid: "stale-messages-reply-guid".into(),
                sender: "+15551234567".into(),
                text: "Threads".into(),
            })
            .await
            .unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert!(
                matches!(sends.as_slice(), [OutboundAsk::Question { .. }, OutboundAsk::Notify { text }]
                    if text.contains("Could not list active threads")
                        && text.contains("poll bridge unavailable")),
                "the operator is told what the provider actually refused: {sends:?}"
            );
        }
        let poll = ledger::list_unresolved(&db, CHANNEL)
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.binding_ref.starts_with(FOLLOW_POLL_PREFIX))
            .unwrap();
        assert_eq!(poll.status, "failed");
        assert_eq!(poll.options_json, None);
        assert!(
            ledger::list_inbound(&db, CHANNEL, 10).await.unwrap().is_empty(),
            "a command carrying a stale Messages reply GUID must route as a command, not unsolicited text"
        );
    }

    struct RecordingPollProvider {
        sends: Mutex<Vec<OutboundAsk>>,
        next_guid: std::sync::atomic::AtomicUsize,
        cleanups: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ChannelProvider for RecordingPollProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities {
                structured_asks: true,
                open_options: false,
                edit_in_place: false,
                max_text_len: None,
            }
        }

        async fn send(
            &self,
            message: &OutboundMessage,
        ) -> Result<crate::channels::SentIds, String> {
            self.sends.lock().unwrap().push(message.ask.clone());
            let guid = self
                .next_guid
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::channels::SentIds {
                primary_guid: format!("poll-{guid}"),
                caption_guid: None,
            })
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Ready
        }

        async fn cleanup_question(&self, _: &ResolvedQuestionMessage) -> Result<(), String> {
            self.cleanups
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn thread_poll_replies_toggle_forever_and_fresh_polls_show_follow_state() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(migrated_test_db("channel-router-standing-thread-poll.db").await);
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
               VALUES ('t1', 'p', 'general', 'active', 1, 3),
                      ('t2', 'p', 'performance', 'active', 1, 2);",
        )
        .await
        .unwrap();
        assert!(
            ledger::follow_target(&db, CHANNEL, "cairn://p/CAIRN/general", 1, 0)
                .await
                .unwrap()
        );
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().join("config"),
        )
        .boot_at(0)
        .build();
        let provider = Arc::new(RecordingPollProvider {
            sends: Mutex::new(Vec::new()),
            next_guid: std::sync::atomic::AtomicUsize::new(1),
            cleanups: std::sync::atomic::AtomicUsize::new(0),
        });
        let router = ChannelRouter::new(
            orch,
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );

        router
            .send_follow_poll("+15551234567", PollKind::Threads)
            .await
            .unwrap();
        match &provider.sends.lock().unwrap()[0] {
            OutboundAsk::Question { options, .. } => {
                assert_eq!(options[0].label, "✓ general");
                assert_eq!(options[1].label, "performance");
            }
            _ => panic!("threads command must send a poll"),
        }
        let legacy_bindings = serde_json::json!({
            "✓ general": "cairn://p/CAIRN/general",
            "performance": "cairn://p/CAIRN/performance"
        })
        .to_string();
        db.execute(
            "UPDATE channel_outbound SET options_json = ?1 WHERE provider_guid = 'poll-1'",
            params![legacy_bindings],
        )
        .await
        .unwrap();

        router
            .handle_inbound(InboundEvent::Reply {
                bound_guid: "poll-1".into(),
                sender: "+15551234567".into(),
                text: "1".into(),
            })
            .await
            .unwrap();
        assert!(
            !ledger::is_target_followed(&db, CHANNEL, "cairn://p/CAIRN/general")
                .await
                .unwrap()
        );
        router
            .handle_inbound(InboundEvent::Reply {
                bound_guid: "poll-1".into(),
                sender: "+15551234567".into(),
                text: "1".into(),
            })
            .await
            .unwrap();
        assert!(
            ledger::is_target_followed(&db, CHANNEL, "cairn://p/CAIRN/general")
                .await
                .unwrap()
        );

        let native_label = match &provider.sends.lock().unwrap()[0] {
            OutboundAsk::Question { options, .. } => options[0].label.clone(),
            _ => unreachable!(),
        };
        for selected in [false, false] {
            router
                .handle_inbound(InboundEvent::Selection {
                    bound_guid: "poll-1".into(),
                    sender: "+15551234567".into(),
                    option_id: "option-1".into(),
                    option_text: native_label.clone(),
                    selected,
                })
                .await
                .unwrap();
        }
        assert!(
            !ledger::is_target_followed(&db, CHANNEL, "cairn://p/CAIRN/general")
                .await
                .unwrap()
        );
        for selected in [true, true] {
            router
                .handle_inbound(InboundEvent::Selection {
                    bound_guid: "poll-1".into(),
                    sender: "+15551234567".into(),
                    option_id: "option-1".into(),
                    option_text: native_label.clone(),
                    selected,
                })
                .await
                .unwrap();
        }
        assert!(
            ledger::is_target_followed(&db, CHANNEL, "cairn://p/CAIRN/general")
                .await
                .unwrap()
        );

        router
            .send_follow_poll("+15551234567", PollKind::Threads)
            .await
            .unwrap();
        {
            let sends = provider.sends.lock().unwrap();
            assert!(
                matches!(&sends[1], OutboundAsk::Question { options, .. } if options[0].label.starts_with("✓ "))
            );
        }
        let polls = ledger::list_unresolved(&db, CHANNEL)
            .await
            .unwrap()
            .into_iter()
            .filter(|record| record.binding_ref.starts_with(FOLLOW_POLL_PREFIX))
            .collect::<Vec<_>>();
        assert_eq!(polls.len(), 2);
        assert!(polls.iter().all(|poll| poll.status == "sent"));
        assert_eq!(
            provider.cleanups.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "standing controls are never routed through answered-question cleanup"
        );

        ledger::insert_intent(
            &db,
            &ledger::NewOutbound {
                id: "stream-intent",
                channel: CHANNEL,
                kind: "review",
                binding_ref: "cairn://p/CAIRN/general:event:42",
                conversation: "+15551234567",
                job_id: Some("unused-for-unfollow"),
                rendered_text: "stream update",
                rendering: "text",
                created_at: 10,
            },
        )
        .await
        .unwrap();
        ledger::mark_sent(&db, "stream-intent", "stream-guid", None, None, 10)
            .await
            .unwrap();
        router
            .handle_inbound(InboundEvent::Reply {
                bound_guid: "stream-guid".into(),
                sender: "+15551234567".into(),
                text: "unfollow".into(),
            })
            .await
            .unwrap();
        assert!(
            !ledger::is_target_followed(&db, CHANNEL, "cairn://p/CAIRN/general")
                .await
                .unwrap()
        );
        assert!(matches!(
            provider.sends.lock().unwrap().last(),
            Some(OutboundAsk::Notify { text }) if text == "Unfollowed cairn://p/CAIRN/general."
        ));
        let stream = ledger::get_by_provider_guid(&db, CHANNEL, "stream-guid")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream.status, "resolved");
    }

    /// A project with two first-class threads, a live session for the first, a
    /// sub-agent task hanging off that session, and one active issue — the shape
    /// the phone has to tell apart. Returns the session job's id.
    async fn seed_threads_and_an_issue(db: &LocalDb) -> String {
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
               VALUES ('t-general', 'p', 'general', 'active', 1, 9),
                      ('t-perf', 'p', 'performance', 'active', 1, 8),
                      ('t-done', 'p', 'retired', 'closed', 1, 7);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('i-live', 'p', 3757, 'Resource library slice 1', 'active', 1, 6);",
        )
        .await
        .unwrap();
        let session = crate::threads::ensure_thread_session(db, "t-general")
            .await
            .unwrap();
        db.execute_batch(&format!(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('r-session', 'p', '{session}', 'live', 1, 1);
             INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('e-session', 'r-session', 1, 1, 'assistant', '{{\"content\":\"the thread speaking\",\"toolUses\":[]}}', 1);
             INSERT INTO jobs (id, thread_id, parent_job_id, project_id, status, node_name, uri_segment, created_at, updated_at)
               VALUES ('j-task', 't-general', '{session}', 'p', 'complete', 'Survey', 'survey', 9, 9);
             INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('r-task', 'p', 'j-task', 'live', 9, 9);
             INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('e-task', 'r-task', 1, 2, 'assistant', '{{\"content\":\"task chatter\",\"toolUses\":[]}}', 2);"
        ))
        .await
        .unwrap();
        session
    }

    fn test_router(orch: Orchestrator) -> (ChannelRouter, Arc<RecordingPollProvider>) {
        let provider = Arc::new(RecordingPollProvider {
            sends: Mutex::new(Vec::new()),
            next_guid: std::sync::atomic::AtomicUsize::new(1),
            cleanups: std::sync::atomic::AtomicUsize::new(0),
        });
        let router = ChannelRouter::new(
            orch,
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );
        (router, provider)
    }

    /// `threads` offers threads and `issues` offers issues — each labelled and
    /// bound the way its own entity is addressed.
    #[tokio::test]
    async fn each_poll_lists_its_own_entity_with_its_own_uri() {
        let (orch, db) = route_test_orchestrator("channel-router-thread-poll-contents.db").await;
        seed_threads_and_an_issue(&db).await;
        let (router, provider) = test_router(orch);

        let (threads, thread_total) = router.active_threads(&HashSet::new()).await.unwrap();
        assert_eq!(
            threads,
            vec![
                ("general".to_string(), "cairn://p/CAIRN/general".to_string()),
                (
                    "performance".to_string(),
                    "cairn://p/CAIRN/performance".to_string()
                ),
            ],
            "a closed thread is not offered, and a thread is offered as its one name"
        );
        assert_eq!(thread_total, 2);

        let (issues, issue_total) = router.active_issues(&HashSet::new()).await.unwrap();
        assert_eq!(
            issues,
            vec![(
                "3757 · Resource library slice 1".to_string(),
                "cairn://p/CAIRN/3757".to_string()
            )]
        );
        assert_eq!(issue_total, 1);

        router
            .send_follow_poll("+15551234567", PollKind::Issues)
            .await
            .unwrap();
        match &provider.sends.lock().unwrap()[0] {
            OutboundAsk::Question { text, options, .. } => {
                assert_eq!(text, "Follow issues");
                assert_eq!(options[0].label, "3757 · Resource library slice 1");
            }
            other => panic!("the issues command must send a poll, got {other:?}"),
        };
    }

    /// Two projects can each have a thread named `general`, and the poll draws
    /// from every open database. The label is the binding key a selection
    /// arrives as, so an unqualified label would drop one of them and toggle the
    /// other in its place.
    #[tokio::test]
    async fn a_poll_spanning_projects_qualifies_every_label_so_no_binding_collides() {
        let (orch, db) = route_test_orchestrator("channel-router-poll-collision.db").await;
        seed_threads_and_an_issue(&db).await;
        db.execute_batch(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p2', 'w', 'Aggflow', 'AGG', '/tmp/agg', 1, 1);
             INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
               VALUES ('t-agg', 'p2', 'general', 'active', 1, 10);",
        )
        .await
        .unwrap();
        let (router, _provider) = test_router(orch);

        let (targets, _) = router.active_threads(&HashSet::new()).await.unwrap();
        assert_eq!(
            targets,
            vec![
                (
                    "AGG/general".to_string(),
                    "cairn://p/AGG/general".to_string()
                ),
                (
                    "CAIRN/general".to_string(),
                    "cairn://p/CAIRN/general".to_string()
                ),
                (
                    "CAIRN/performance".to_string(),
                    "cairn://p/CAIRN/performance".to_string()
                ),
            ]
        );
        let bindings: HashMap<String, String> = targets.iter().cloned().collect();
        assert_eq!(
            bindings.len(),
            targets.len(),
            "every option keeps its own binding"
        );
    }

    /// A follow reached by a migrated number and the canonical thread URI the
    /// poll offers are two identities for one conversation. Left alone, the poll
    /// shows a streaming thread as unfollowed; following it adds a second row;
    /// and every update then arrives twice under two distinct binding refs.
    #[tokio::test]
    async fn a_follow_recorded_under_a_migrated_number_becomes_canonical_at_startup() {
        let (orch, db) = route_test_orchestrator("channel-router-follow-canonical.db").await;
        seed_threads_and_an_issue(&db).await;
        db.execute(
            "UPDATE threads SET migrated_from_number = 3404 WHERE id = 't-general'",
            (),
        )
        .await
        .unwrap();
        assert!(
            ledger::follow_target(&db, CHANNEL, "cairn://p/CAIRN/3404", 5, 40)
                .await
                .unwrap()
        );
        ledger::set_focus(&db, CHANNEL, "cairn://p/CAIRN/3404", 5)
            .await
            .unwrap();
        let (router, _provider) = test_router(orch);

        router.draw_the_session_line().await.unwrap();

        let follows = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(
            follows.iter().map(|f| f.uri.as_str()).collect::<Vec<_>>(),
            vec!["cairn://p/CAIRN/general"],
            "one target keeps one identity"
        );
        assert_eq!(follows[0].followed_at, 5, "the follow is as old as it was");
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/CAIRN/general"),
            "bare text still routes to the thread the operator last chose"
        );

        // The poll offers the canonical URI, so the checkmark now finds it.
        let followed = ledger::list_follows(&db, CHANNEL)
            .await
            .unwrap()
            .into_iter()
            .map(|follow| follow.uri)
            .collect::<HashSet<_>>();
        let (targets, _) = router.active_threads(&followed).await.unwrap();
        assert!(followed.contains(&targets[0].1));
    }

    /// Merging an alias into a canonical row that already exists must not replay
    /// what one of them already delivered, nor skip what neither has.
    #[tokio::test]
    async fn canonicalizing_onto_an_existing_row_keeps_the_further_cursor() {
        let db = migrated_test_db("channel-ledger-canonicalize-merge.db").await;
        ledger::follow_target(&db, CHANNEL, "cairn://p/CAIRN/3404", 5, 90)
            .await
            .unwrap();
        ledger::follow_target(&db, CHANNEL, "cairn://p/CAIRN/general", 9, 40)
            .await
            .unwrap();

        ledger::canonicalize_follow(
            &db,
            CHANNEL,
            "cairn://p/CAIRN/3404",
            "cairn://p/CAIRN/general",
        )
        .await
        .unwrap();

        let follows = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].uri, "cairn://p/CAIRN/general");
        assert_eq!(
            follows[0].cursor_rowid, 90,
            "nothing already sent is resent"
        );
        assert_eq!(follows[0].followed_at, 5);
    }

    /// The ten slots span every open database, and this poll is the only surface
    /// that manages follows — so a followed target must never be the one the
    /// limit drops.
    #[tokio::test]
    async fn a_followed_target_is_never_crowded_out_of_the_poll_that_manages_it() {
        let (orch, db) = route_test_orchestrator("channel-router-poll-ordering.db").await;
        seed_threads_and_an_issue(&db).await;
        let mut busier = String::new();
        for index in 0..FOLLOW_POLL_LIMIT {
            busier.push_str(&format!(
                "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                   VALUES ('t-busy-{index}', 'p', 'busy-{index}', 'active', 1, {});",
                100 + index
            ));
        }
        db.execute_batch(&busier).await.unwrap();
        let followed = HashSet::from(["cairn://p/CAIRN/performance".to_string()]);
        let (router, _provider) = test_router(orch);

        let (targets, total) = router.active_threads(&followed).await.unwrap();

        assert_eq!(targets.len(), FOLLOW_POLL_LIMIT);
        assert_eq!(total, FOLLOW_POLL_LIMIT + 2);
        assert_eq!(
            targets[0],
            (
                "performance".to_string(),
                "cairn://p/CAIRN/performance".to_string()
            ),
            "the followed thread leads, then the most recently active"
        );
        assert_eq!(targets[1].1, "cairn://p/CAIRN/busy-9");
    }

    /// Following a thread has to resolve through the thread's SESSION job. Its
    /// sub-agent tasks carry the thread's id too and are newer, so a tap that
    /// matched on the thread alone would text the operator a task's chatter and
    /// start every follow past the thread's own last word.
    #[tokio::test]
    async fn a_followed_thread_taps_its_session_and_not_the_tasks_it_spawned() {
        let (orch, db) = route_test_orchestrator("channel-router-thread-tap.db").await;
        seed_threads_and_an_issue(&db).await;
        let (router, _provider) = test_router(orch);

        let target = router
            .resolve_target("cairn://p/CAIRN/general")
            .await
            .unwrap();
        assert_eq!(
            target,
            FollowTarget::Thread {
                project: "CAIRN".into(),
                name: "general".into()
            }
        );
        let session_rowid = db
            .query_one(
                "SELECT rowid FROM events WHERE id = 'e-session'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(router.live_edge(&target).await.unwrap(), session_rowid);

        let events = router.followed_events(&target, 0).await.unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| (event.rowid, event.context.clone()))
                .collect::<Vec<_>>(),
            vec![(session_rowid, "CAIRN/general".to_string())]
        );
        assert!(assistant_text(&events[0].data).as_deref() == Some("the thread speaking"));

        // A fresh follow starts at the live edge, so nothing already said is
        // replayed onto the phone.
        router.follow("cairn://p/CAIRN/general").await.unwrap();
        let follow = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(follow[0].uri, "cairn://p/CAIRN/general");
        assert_eq!(follow[0].cursor_rowid, session_rowid);
        assert!(router
            .followed_events(&target, follow[0].cursor_rowid)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/CAIRN/general"),
            "the newest selection is what bare text routes to"
        );
    }

    /// Inbound text reaches the thread the same way the desktop composer does:
    /// the message becomes visible in the thread and its session is steered —
    /// including a dormant thread, which acquires its session on the way.
    #[tokio::test]
    async fn inbound_text_lands_in_the_followed_threads_transcript() {
        use crate::db::DbState;
        use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(migrated_test_db("channel-router-thread-routing.db").await);
        let session = seed_threads_and_an_issue(&db).await;
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        // Steering a live session legitimately starts one, so the spawner records
        // instead of refusing.
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(
                TestServicesBuilder::new()
                    .with_process(RecordingProcessSpawner::new())
                    .build(),
            ),
            temp.join("config"),
        )
        .build();
        let (router, _provider) = test_router(orch);

        let warm = router
            .resolve_target("cairn://p/CAIRN/general")
            .await
            .unwrap();
        router.route_to_target(&warm, "ship it").await.unwrap();
        assert_eq!(
            db.query_one(
                "SELECT channel_id, sender_name, content FROM messages WHERE channel_type = 'thread'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
            .unwrap(),
            ("t-general".to_string(), "operator".to_string(), "ship it".to_string())
        );
        assert_eq!(
            db.query_opt_text(
                &format!(
                    "SELECT j.id FROM jobs j WHERE j.thread_id = 't-general' AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some(session.as_str()),
            "a warm thread keeps the session it already had"
        );
        assert_eq!(
            db.query_opt_text(
                "SELECT recipient FROM attention_pushes WHERE key LIKE 'direct:%'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some(session.as_str()),
            "the message steers the thread's own session, not a task it spawned"
        );

        let dormant = router
            .resolve_target("cairn://p/CAIRN/performance")
            .await
            .unwrap();
        router
            .route_to_target(&dormant, "and this one")
            .await
            .unwrap();
        assert!(
            db.query_opt_text(
                &format!(
                    "SELECT j.id FROM jobs j WHERE j.thread_id = 't-perf' AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                (),
            )
            .await
            .unwrap()
            .is_some(),
            "a dormant thread acquires its session before the message is visible"
        );

        let missing = FollowTarget::Thread {
            project: "CAIRN".into(),
            name: "no-such-thread".into(),
        };
        assert!(router
            .route_to_target(&missing, "nobody home")
            .await
            .unwrap_err()
            .contains("no thread named"));
    }

    /// The numeric address a thread migration vacated still resolves — that is
    /// what `channels.defaultThread` and every pre-cutover follow carry — while a
    /// number an issue still owns stays an issue.
    #[tokio::test]
    async fn a_migrated_number_resolves_to_its_thread_and_a_live_issue_does_not() {
        let (orch, db) = route_test_orchestrator("channel-router-migrated-follow.db").await;
        seed_threads_and_an_issue(&db).await;
        db.execute(
            "UPDATE threads SET migrated_from_number = 3404 WHERE id = 't-general'",
            (),
        )
        .await
        .unwrap();
        let (router, _provider) = test_router(orch);

        assert_eq!(
            router.resolve_target("cairn://p/CAIRN/3404").await.unwrap(),
            FollowTarget::Thread {
                project: "CAIRN".into(),
                name: "general".into()
            }
        );
        assert_eq!(
            router.resolve_target("cairn://p/CAIRN/3757").await.unwrap(),
            FollowTarget::Issue {
                project: "CAIRN".into(),
                number: 3757
            }
        );
    }

    #[test]
    fn thread_tap_emits_every_assistant_text_segment_and_no_tool_call() {
        let turn = [
            r#"{"content":"first","toolUses":[]}"#,
            r#"{"toolUses":[{"name":"read","input":{}}]}"#,
            r#"{"content":"second","toolUses":[]}"#,
            r#"{"content":"third","toolUses":[]}"#,
        ];
        assert_eq!(
            turn.into_iter()
                .filter_map(assistant_text)
                .collect::<Vec<_>>(),
            vec!["first", "second", "third"]
        );
    }

    async fn seed_run(db: &LocalDb) {
        db.execute(
            "INSERT INTO runs (id, created_at, updated_at) VALUES ('run-1', 100, 100)",
            (),
        )
        .await
        .unwrap();
    }

    async fn seed_prompt(db: &LocalDb, id: &str, created_at: i64) {
        db.execute(
            "INSERT INTO prompts (id, run_id, questions, created_at) VALUES (?1, 'run-1', ?2, ?3)",
            params![
                id,
                "[{\"question\":\"Which path?\",\"options\":[]}]",
                created_at
            ],
        )
        .await
        .unwrap();
    }

    fn route_submission(identity: &str) -> ChannelSubmission {
        ChannelSubmission {
            route_id: "route-test".into(),
            scope_key: "workspace".into(),
            project_id: None,
            fact: RouteFact {
                source: "attention".into(),
                identity: identity.into(),
                fields: std::collections::BTreeMap::new(),
                summary: Some("Went idle with work remaining".into()),
                route_provenance: None,
            },
            transforms_json: None,
            created_at: chrono::Utc::now().timestamp_millis(),
            binding_ref: format!("route:route-test:{identity}"),
            text: "notify".into(),
            context: "[Cairn]".into(),
            job_id: None,
            initiated_by: None,
        }
    }

    async fn route_test_orchestrator(name: &str) -> (Orchestrator, Arc<LocalDb>) {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(migrated_test_db(name).await);
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        (
            Orchestrator::builder(
                Arc::new(DbState::new(db.clone(), search)),
                Arc::new(TestServicesBuilder::new().build()),
                temp.join("config"),
            )
            .build(),
            db,
        )
    }

    #[tokio::test]
    async fn one_fact_observed_across_many_polls_sends_one_text() {
        let (orch, db) = route_test_orchestrator("route-stable-fact.db").await;
        let provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Away),
        });
        let router = ChannelRouter::new(
            orch,
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );

        for _ in 0..24 {
            router
                .submit_route(
                    route_submission("cairn://p/CAIRN/3727:review_ready"),
                    OperatorPresence::Away,
                    Instant::now(),
                )
                .await
                .unwrap();
        }

        assert_eq!(provider.sends.lock().unwrap().len(), 1);
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
                .await
                .unwrap(),
            Some(1),
            "one stable fact owns one durable intent across every poll"
        );
    }

    #[tokio::test]
    async fn failed_route_delivery_records_failed_history_not_fired() {
        let (orch, db) = route_test_orchestrator("route-delivery-failure.db").await;
        let router = ChannelRouter::new(
            orch,
            Arc::new(FailingNotifyProvider),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );
        router
            .submit_route(
                route_submission("failure"),
                OperatorPresence::Away,
                Instant::now(),
            )
            .await
            .unwrap();
        let firing = crate::storage::list_route_firings(&db, "workspace", "route-test", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(firing.status, "failed");
        assert_eq!(firing.error.as_deref(), Some("bridge send failed"));
    }

    #[tokio::test]
    async fn pending_route_recovers_after_router_restart() {
        let (orch, db) = route_test_orchestrator("route-restart-recovery.db").await;
        let first_provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Present),
        });
        let first = ChannelRouter::new(
            orch.clone(),
            first_provider,
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );
        first
            .submit_route(
                route_submission("restart"),
                OperatorPresence::Present,
                Instant::now(),
            )
            .await
            .unwrap();
        assert!(
            crate::storage::list_route_firings(&db, "workspace", "route-test", 1)
                .await
                .unwrap()
                .is_empty()
        );
        drop(first);

        let resumed_provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Away),
        });
        let resumed = ChannelRouter::new(
            orch,
            resumed_provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );
        resumed
            .recover_pending_routes(OperatorPresence::Away, Instant::now())
            .await
            .unwrap();

        assert_eq!(resumed_provider.sends.lock().unwrap().len(), 1);
        let firing = crate::storage::list_route_firings(&db, "workspace", "route-test", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(firing.status, "fired");
    }

    struct FailingNotifyProvider;

    #[async_trait::async_trait]
    impl ChannelProvider for FailingNotifyProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities::default()
        }

        async fn send(
            &self,
            _message: &OutboundMessage,
        ) -> Result<crate::channels::SentIds, String> {
            Err("bridge send failed".into())
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Ready
        }
    }

    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn forcing_idle_flushes_a_gate_deferred_while_inferred_present() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(migrated_test_db("channel-router-forced-idle.db").await);
        seed_run(&db).await;
        seed_prompt(&db, "live", 500).await;
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().join("config"),
        )
        .boot_at(0)
        .build();
        let provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Present),
        });
        let router = ChannelRouter::new(
            orch,
            provider.clone(),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );

        crate::channels::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Auto);
        router.sweep_live_gates().await.unwrap();
        assert!(provider.sends.lock().unwrap().is_empty());
        assert_eq!(router.deferred_attention.lock().unwrap().len(), 1);

        crate::channels::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Idle);
        router.sweep_live_gates().await.unwrap();
        crate::channels::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Auto);

        assert_eq!(provider.sends.lock().unwrap().len(), 1);
        assert!(router.deferred_attention.lock().unwrap().is_empty());
        assert_eq!(*provider.presence_checks.lock().unwrap(), 2);
    }

    /// One sweep's worth of claiming, reporting which gates it won. Drives the
    /// real claim path, so what it reports is exactly what the router would send.
    async fn sweep_questions(claims: &ClaimSet, db: &LocalDb) -> Vec<String> {
        let mut delivered = Vec::new();
        for gate in load_questions(db).await.unwrap() {
            if delivered.len() == SWEEP_LIMIT {
                break;
            }
            if claim_gate(claims, db, "+15551234567", "text", &gate)
                .await
                .unwrap()
                .is_some()
            {
                delivered.push(gate.binding_ref.clone());
            }
        }
        delivered
    }

    /// The acceptance scenario for CAIRN-3434: a restart onto a backlog of open
    /// questions texts none of them, and the next ask raised goes out on its own.
    ///
    /// The line is drawn by identity, and this test is what forces that. A stale
    /// ask and a live one can share a `created_at` exactly -- the column is
    /// second-precision -- so a startup watermark compared against it cannot tell
    /// them apart, and would replay the stale ask on every restart forever.
    #[tokio::test]
    async fn a_backlog_ask_and_a_live_ask_sharing_one_timestamp_stay_separable() {
        let db = migrated_test_db("channel-router-session-line.db").await;
        let claims = ClaimSet::default();
        seed_run(&db).await;
        seed_prompt(&db, "backlog", 500).await;

        // The session opens: claim what already exists, then abandon the claims.
        assert_eq!(sweep_questions(&claims, &db).await, vec!["backlog:0"]);
        assert_eq!(expire_undelivered(&db, CHANNEL, 500).await.unwrap(), 1);

        // A live ask lands in the very same wall-clock second the session opened.
        seed_prompt(&db, "live", 500).await;

        assert_eq!(
            sweep_questions(&claims, &db).await,
            vec!["live:0"],
            "the backlog stays sealed and only the newly raised ask is deliverable"
        );
        assert!(
            sweep_questions(&claims, &db).await.is_empty(),
            "a delivered ask stays claimed, so a later sweep does not text it twice"
        );
    }

    /// The sharpest form of the same collision, and the one an ordering-plus-limit
    /// window cannot survive: a backlog LARGER than one sweep's batch sharing a
    /// single `created_at` second with the ask actually being raised. Ordered
    /// selection may hand back the same sealed rows on every sweep, so the live
    /// gate is never even offered. Identity does not care about the ordering.
    #[tokio::test]
    async fn a_live_ask_is_reachable_when_an_oversized_backlog_shares_its_second() {
        let db = migrated_test_db("channel-router-oversized-backlog.db").await;
        let claims = ClaimSet::default();
        seed_run(&db).await;
        let backlog = SWEEP_LIMIT + 5;
        let values = (0..backlog)
            .map(|index| {
                format!(
                    "('stale-{index}', 'run-1', '[{{\"question\":\"Which path?\",\"options\":[]}}]', 500)"
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        db.execute_batch(&format!(
            "INSERT INTO prompts (id, run_id, questions, created_at) VALUES {values};"
        ))
        .await
        .unwrap();

        // Sealing is unbounded, so it claims the whole backlog and not one batch.
        let mut sealed = 0;
        for gate in load_questions(&db).await.unwrap() {
            if claim_gate(&claims, &db, "+15551234567", "text", &gate)
                .await
                .unwrap()
                .is_some()
            {
                sealed += 1;
            }
        }
        assert_eq!(sealed, backlog);
        assert_eq!(
            expire_undelivered(&db, CHANNEL, 500).await.unwrap() as usize,
            backlog
        );

        // The live ask shares the backlog's exact timestamp and sorts nowhere in
        // particular among 105 identical keys.
        seed_prompt(&db, "live", 500).await;
        assert_eq!(
            sweep_questions(&claims, &db).await,
            vec!["live:0"],
            "a fresh ask is reachable no matter how the engine orders its ties"
        );
    }

    /// Permission gates are sealed by the same claim. The snapshot ignores the
    /// route flags deliberately, so turning a route on later cannot dump its
    /// accumulated backlog onto the phone.
    #[tokio::test]
    async fn permission_gates_are_claimed_by_the_same_fence() {
        let db = migrated_test_db("channel-router-permission-fence.db").await;
        let claims = ClaimSet::default();
        seed_run(&db).await;
        db.execute(
            "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at) VALUES ('req-1', 'run-1', 'req-1', 'Bash', '{}', 'pending', 500)",
            (),
        )
        .await
        .unwrap();

        let gates = load_permissions(&db).await.unwrap();
        assert_eq!(gates.len(), 1);
        assert!(claim_gate(&claims, &db, "+15551234567", "text", &gates[0])
            .await
            .unwrap()
            .is_some());
        assert!(
            claim_gate(&claims, &db, "+15551234567", "text", &gates[0])
                .await
                .unwrap()
                .is_none(),
            "a claimed permission request is never asked for a second time"
        );
    }

    /// A fresh process has an empty claim set, so the durable ledger has to be
    /// what stops a re-send after a restart -- the claim cache must read through
    /// to it, not shadow it.
    #[tokio::test]
    async fn a_restarted_session_still_defers_to_the_durable_ledger() {
        let db = migrated_test_db("channel-router-restart-claims.db").await;
        seed_run(&db).await;
        seed_prompt(&db, "asked", 500).await;
        assert_eq!(
            sweep_questions(&ClaimSet::default(), &db).await,
            vec!["asked:0"]
        );

        assert!(
            sweep_questions(&ClaimSet::default(), &db).await.is_empty(),
            "the ledger, not the in-memory cache, is what makes delivery once-only"
        );
    }

    struct CleanupCountingProvider {
        cleanups: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ChannelProvider for CleanupCountingProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities {
                structured_asks: false,
                open_options: false,
                edit_in_place: false,
                max_text_len: None,
            }
        }

        async fn send(&self, _: &OutboundMessage) -> Result<crate::channels::SentIds, String> {
            unreachable!("cleanup test never sends")
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Ready
        }

        async fn cleanup_question(&self, _: &ResolvedQuestionMessage) -> Result<(), String> {
            self.cleanups
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn sweep_and_inbound_race_keeps_one_cleanup_and_delivers_the_answer() {
        let db = migrated_test_db("channel-router-cleanup-claim.db").await;
        let intent = ledger::NewOutbound {
            id: "intent",
            channel: CHANNEL,
            kind: "question",
            binding_ref: "prompt:0",
            conversation: "+15551234567",
            job_id: None,
            rendered_text: "Which path?",
            rendering: "text",
            created_at: 10,
        };
        assert!(ledger::insert_intent(&db, &intent).await.unwrap());
        assert!(ledger::mark_sent(&db, "intent", "guid", None, None, 11)
            .await
            .unwrap());
        let record = ledger::get_by_binding(&db, CHANNEL, "question", "prompt:0")
            .await
            .unwrap()
            .unwrap();
        let provider = CleanupCountingProvider {
            cleanups: std::sync::atomic::AtomicUsize::new(0),
        };

        let (sweep, answer_delivered) = tokio::join!(
            cleanup_resolved_question(&db, &provider, &record, "✓ closed"),
            async {
                let won = ledger::claim_question_answer(&db, &record.id, "Ship it", 12)
                    .await
                    .unwrap();
                if won {
                    cleanup_claimed_question(&provider, &record, "✓ answered: Ship it").await;
                    true
                } else {
                    ledger::record_answer_after_cleanup_claim(&db, &record.id, "Ship it")
                        .await
                        .unwrap()
                }
            },
        );

        assert!(sweep.unwrap() || answer_delivered);
        assert!(
            answer_delivered,
            "a cleanup claim must not consume the inbound answer"
        );
        assert_eq!(
            provider.cleanups.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            ledger::get_by_binding(&db, CHANNEL, "question", "prompt:0")
                .await
                .unwrap()
                .unwrap()
                .options_json
                .as_deref(),
            Some("{\"answer\":\"Ship it\"}")
        );
    }

    #[test]
    fn permission_words_are_strict() {
        assert_eq!(
            parse_permission("Approve").unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(parse_permission("2").unwrap(), PermissionDecision::Deny);
        assert!(parse_permission("maybe").is_err());
    }

    #[test]
    fn numbered_and_free_text_question_replies_resolve() {
        let mut record = ledger::OutboundRecord {
            id: "intent".into(),
            channel: CHANNEL.into(),
            kind: "question".into(),
            binding_ref: "prompt:0".into(),
            conversation: "+15551234567".into(),
            job_id: None,
            rendered_text: String::new(),
            rendering: "text".into(),
            options_json: Some("[\"Legacy\",\"New\"]".into()),
            status: "sent".into(),
            provider_guid: Some("guid".into()),
            caption_guid: None,
            created_at: 0,
            sent_at: Some(0),
            resolved_at: None,
            last_error: None,
        };
        assert_eq!(question_answer(&record, "2"), "New");
        assert_eq!(
            question_answer(&record, "a different answer"),
            "a different answer"
        );
        record.options_json = None;
        assert_eq!(question_answer(&record, "1"), "1");
    }

    #[test]
    fn present_attention_defers_until_the_deadline() {
        let now = Instant::now();
        assert_eq!(
            attention_timing(OperatorPresence::Present, now, now + Duration::from_secs(1)),
            AttentionTiming::Defer
        );
        assert_eq!(
            attention_timing(OperatorPresence::Present, now, now),
            AttentionTiming::Send
        );
        let review = Gate {
            kind: "review",
            initiated_by: OutboundInitiator::CairnPush,
            binding_ref: "review".into(),
            job_id: None,
            context: String::new(),
            ask: OutboundAsk::Notify {
                text: String::new(),
            },
        };
        assert!(review.is_presence_aware());

        let response = Gate {
            initiated_by: OutboundInitiator::OperatorInbound,
            ..review
        };
        assert!(
            !response.is_presence_aware(),
            "a response to inbound is conversation even when its payload looks like a push"
        );
    }

    #[test]
    fn followed_thread_stream_is_a_presence_aware_subscription_push() {
        let subscription = Gate {
            kind: "review",
            initiated_by: OutboundInitiator::OperatorSubscription,
            binding_ref: "cairn://p/CAIRN/1: event:1".into(),
            job_id: None,
            context: String::new(),
            ask: OutboundAsk::Notify {
                text: "subscribed update".into(),
            },
        };

        assert!(subscription.is_presence_aware());
    }

    #[test]
    fn losing_presence_escalates_before_the_deadline() {
        let now = Instant::now();
        assert_eq!(
            attention_timing(OperatorPresence::Away, now, now + ATTENTION_GRACE),
            AttentionTiming::Send
        );
    }

    #[test]
    fn one_failed_database_cannot_cancel_a_healthy_deferred_question() {
        let gate = Gate {
            kind: "question",
            initiated_by: OutboundInitiator::CairnPush,
            binding_ref: "healthy:0".into(),
            job_id: None,
            context: String::new(),
            ask: OutboundAsk::Question {
                prompt_id: "healthy".into(),
                question_index: 0,
                text: "Which path?".into(),
                options: Vec::new(),
            },
        };
        let mut deferred = HashMap::from([(
            gate.binding_ref.clone(),
            DeferredAttention {
                id: "intent".into(),
                gate,
                deadline: Instant::now() + ATTENTION_GRACE,
            },
        )]);

        assert!(cancel_resolved_attention(&mut deferred, &HashSet::new(), false).is_empty());
        assert!(deferred.contains_key("healthy:0"));
        assert_eq!(
            cancel_resolved_attention(&mut deferred, &HashSet::new(), true),
            vec!["intent"]
        );
        assert!(deferred.is_empty());
    }

    #[test]
    fn review_notice_leads_with_issue_title_and_event_without_boilerplate() {
        let content_ref = "cairn://p/CAIRN/3445/1/builder/artifact";
        assert_eq!(
            review_notice(
                "CAIRN",
                3445,
                "Reap nested Linux process groups when checks stop",
                content_ref,
            ),
            "CAIRN-3445 review ready — Reap nested Linux process groups when checks stop\ncairn://p/CAIRN/3445/1/builder/artifact"
        );
    }
}
