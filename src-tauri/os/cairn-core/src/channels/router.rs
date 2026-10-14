use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cairn_common::uri::{parse_uri, CairnResource};
use cairn_db::turso::{params, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    agent_message, bindings::FollowTarget, ledger, render_text_floor, AskOption, ChannelProvider,
    InboundEvent, OperatorPresence, OutboundAsk, OutboundInitiator, OutboundMessage,
    ResolvedQuestionMessage,
};
use crate::routes::{ChannelSubmission, RouteFact};
use crate::{
    mcp::handlers::permission::PermissionDecision,
    models::{ChannelInboundCapabilities, MessageClassPolicy},
    orchestrator::{generation_cache::GenerationCache, Orchestrator},
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
/// The minimum rest between two sweeps, whatever woke them.
///
/// The timer alone cannot spin once its missed ticks coalesce, but a presence
/// change is an external event: a beacon that flaps, or any future wake source,
/// would otherwise be able to drive sweeps as fast as they complete. A floor
/// well under the cadence costs a live wake almost nothing and makes "this loop
/// cannot become a busy loop" a property of the schedule rather than a property
/// of every caller that might one day notify it.
const SWEEP_FLOOR: Duration = Duration::from_secs(1);
const ATTENTION_GRACE: Duration = Duration::from_secs(3 * 60);
const RETRY_BASE: Duration = Duration::from_secs(5);
const RETRY_CAP: Duration = Duration::from_secs(5 * 60);
const MAX_ATTEMPTS: i64 = 6;
/// How long an undirected Discord route waits for its subject's structural
/// surface to be created before its firing is recorded as failed.
const SURFACE_WAIT: Duration = Duration::from_secs(10 * 60);
const FOLLOW_POLL_LIMIT: usize = 10;
/// How much of a target's title a poll option carries before it is elided. A
/// Messages balloon renders an option as a wrapped block, so a full thread title
/// pushes the rest of the list off the screen.
const FOLLOW_POLL_TITLE_LIMIT: usize = 64;
/// The durable binding prefix for a follow poll's ledger row. Command polls are
/// not backed by a Cairn prompt, so the gate sweep recognizes this namespace and
/// leaves their provider bindings intact until a newer poll of the same kind
/// supersedes them.
const FOLLOW_POLL_PREFIX: &str = "threads:";

/// What a provider's router inbox carries: a route-sink submission, or an
/// agent message the write side has already ledgered as a `pending` intent.
#[derive(Debug)]
pub enum RouterSubmission {
    Route(Box<ChannelSubmission>),
    AgentMessage { intent_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionBoundary {
    Fresh,
    Resumed,
}

async fn seal_gate_for_provider(
    claims: &ClaimSet,
    ledger: &LocalDb,
    provider_id: &'static str,
    conversation: &str,
    rendering: &'static str,
    gate: &Gate,
) -> Result<bool, String> {
    let delivery_key = gate.delivery_key(provider_id, conversation);
    if claims.holds(&delivery_key) {
        return Ok(false);
    }
    let id = Uuid::new_v4().to_string();
    let rendered = render_text_floor(&gate.ask);
    let inserted = ledger::insert_intent_with_status(
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
        "expired",
    )
    .await?;
    claims.claim(&delivery_key);
    Ok(inserted)
}

#[derive(Debug, Deserialize)]
struct StoredQuestion {
    question: String,
    #[serde(default)]
    options: Vec<StoredOption>,
    #[serde(default, rename = "multiSelect")]
    multi_select: bool,
}

fn permission_answer_token(decision: PermissionDecision) -> &'static str {
    match decision {
        PermissionDecision::Allow => "allow",
        PermissionDecision::Deny => "deny",
    }
}

/// The conversation a directed route's gate is keyed and sent under.
///
/// Discord takes the canonical `discord:<guild>/<channel>` address: its provider
/// refuses anything else, and the guild is part of the identity. iMessage stays
/// a bare handle because `IMessageProvider` hands the string to `imsg send --to`
/// and matches chats by normalized handle, neither of which understands the
/// `imessage:` prefix. Telegram's provider accepts either form, but its directed
/// route rows were delivered under the bare chat id; switching the key shape
/// would miss each such row and re-notify every still-observed fact once.
fn route_conversation(address: Option<&crate::channels::ConversationAddress>) -> Option<String> {
    address.map(|address| match address.destination() {
        super::ConversationDestination::IMessage { handle } => handle.clone(),
        super::ConversationDestination::Telegram { chat_id } => chat_id.to_string(),
        super::ConversationDestination::Discord { .. } => address.to_string(),
    })
}

fn starts_with_known_slash_command(text: &str) -> bool {
    text.split_whitespace()
        .next()
        .and_then(|token| token.strip_prefix('/'))
        .and_then(super::commands::command_spec)
        .is_some()
}

fn channel_sender_name(provider_id: &str) -> String {
    format!("operator via {provider_id}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ChannelCommand {
    Threads,
    Issues,
    Focus(String),
    Unfollow(Option<String>),
    Help,
}

fn channel_command(text: &str) -> Option<ChannelCommand> {
    let mut words = text.split_whitespace();
    let token = words.next()?;
    let slash_command = token.strip_prefix('/');
    let name = slash_command.unwrap_or(token);
    // Preserve the existing bare iMessage shortcuts. Help remains slash-only so
    // ordinary conversation containing that word is not captured.
    if slash_command.is_none() && name.eq_ignore_ascii_case("help") {
        return None;
    }

    let spec = super::commands::command_spec(name)?;
    let argument = words.next();
    if words.next().is_some() || (!spec.takes_argument && argument.is_some()) {
        return None;
    }
    match (spec.name, argument) {
        ("threads", None) => Some(ChannelCommand::Threads),
        ("issues", None) => Some(ChannelCommand::Issues),
        ("focus", Some(selector)) => Some(ChannelCommand::Focus(selector.to_string())),
        ("unfollow", selector) => Some(ChannelCommand::Unfollow(selector.map(str::to_string))),
        ("help", None) => Some(ChannelCommand::Help),
        _ => None,
    }
}

fn follow_poll_kind(binding_ref: &str) -> Option<PollKind> {
    let kind = binding_ref
        .strip_prefix(FOLLOW_POLL_PREFIX)?
        .split(':')
        .next()?;
    serde_json::from_value(serde_json::Value::String(kind.to_string())).ok()
}

fn route_binding_target(binding_ref: &str) -> Option<&str> {
    let start = binding_ref.find("cairn://")?;
    binding_ref[start..].split(":event:").next()
}

fn routed_gate_target(uri: &str) -> Option<String> {
    let resource = parse_uri(uri)?;
    match resource {
        CairnResource::Thread { project, name, .. } => {
            Some(cairn_common::uri::build_thread_uri(&project, &name))
        }
        resource => Some(format!(
            "cairn://p/{}/{}",
            resource.project()?,
            resource.issue_number()?
        )),
    }
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

/// The two collections a poll command offers as follow targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
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

    fn binding_prefix(self) -> String {
        let kind = serde_json::to_value(self).expect("poll kind serializes");
        format!(
            "{FOLLOW_POLL_PREFIX}{}:",
            kind.as_str().expect("poll kind serializes as a string")
        )
    }
}

/// Why a sweep is running. It rides in the sweep's log line because a
/// recurrence has to be legible without a profiler: a loop running hot on the
/// timer is a slow sweep, and one running hot on presence is a wake source that
/// has started flapping, and those have different fixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepReason {
    Timer,
    Presence,
}

impl SweepReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Timer => "timer",
            Self::Presence => "presence",
        }
    }
}

/// The sweep loop's clock.
///
/// A sweep polls everything there is, so its cadence is a floor on REST, not a
/// schedule to catch up to. `tokio::time::interval` defaults to
/// `MissedTickBehavior::Burst`, which means precisely the opposite: a sweep that
/// overruns its period leaves a deadline ready for every period it covered, so a
/// twenty-second sweep on a five-second cadence is followed by four more sweeps
/// with no sleep between them. Under the load that made the sweep slow in the
/// first place that condition never clears, and each provider's loop becomes a
/// permanently ready worker — which is what a rebuilt runner sitting at 500% CPU
/// across three enabled channels actually was (CAIRN-4208).
///
/// `Delay` coalesces those missed deadlines into ONE sweep and starts the next
/// period when the overrunning sweep finishes. `Skip` also coalesces, but aligns
/// to the original grid, so a sweep finishing just before a boundary is followed
/// immediately by the next one; under overload that is the case that matters,
/// and rest is what overload needs.
struct SweepSchedule {
    interval: tokio::time::Interval,
    /// Tokio's clock, not the standard one, so the floor honors paused time and
    /// the overload behavior is testable without sleeping through it.
    finished: Option<tokio::time::Instant>,
}

impl SweepSchedule {
    fn new() -> Self {
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            interval,
            finished: None,
        }
    }

    /// Waits for the next sweep and reports what woke it.
    async fn next(&mut self) -> SweepReason {
        let reason = tokio::select! {
            _ = self.interval.tick() => SweepReason::Timer,
            _ = super::wait_for_presence_change() => SweepReason::Presence,
        };
        if let Some(finished) = self.finished {
            tokio::time::sleep_until(finished + SWEEP_FLOOR).await;
        }
        reason
    }

    /// Records that a sweep has finished, which is what the floor is measured
    /// from. A sweep's own duration is never charged against the floor.
    fn finished(&mut self) {
        self.finished = Some(tokio::time::Instant::now());
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
}

/// The jobs whose transcript a followed target streams, with the context every
/// event of that target carries.
///
/// Resolving the TARGET first is the whole shape of the follow read. Both reads
/// below used to drive from `events.rowid > cursor` — an open-ended range over
/// the events primary key — and join outward to discover which target each event
/// belonged to. So a sweep read every event the WORKSPACE had produced since the
/// cursor, joined it four ways, and discarded nearly all of it at the project
/// and issue predicates: the cost followed total event history rather than the
/// followed target's own transcript. On an installation with 1.5M events that
/// was 15-17 seconds per follow, repeated every five seconds by every enabled
/// provider (CAIRN-4208).
struct FollowSource {
    /// Empty when the target has no session yet — a dormant thread, or an issue
    /// that has never run. Both reads then answer without touching `events`.
    job_ids: Vec<String>,
    context: String,
    project_id: String,
}

/// The jobs a followed THREAD streams. A thread's session is addressed by its
/// SHAPE, never by "some job of this thread": the sub-agent tasks it spawns and
/// the pre-cutover thread-issue's jobs carry its id too, and they are newer.
///
/// `CROSS JOIN` is the join ORDER, not a cross product: it pins projects as the
/// outer loop so the thread is found by the `(project_id, name)` key and its
/// jobs by `thread_id`. Left free, the planner drove from `jobs` and scanned it,
/// because on a workspace with no gathered statistics the two orders look alike
/// to it and only one of them is bounded by the followed target. Projects leads
/// because `LOWER(p.key)` can use no index by construction, and a workspace's
/// project list is the one table here that is small on purpose.
fn follow_thread_jobs_sql() -> String {
    format!(
        "SELECT j.id, p.key, t.name, p.id
           FROM projects p
           CROSS JOIN threads t ON t.project_id = p.id
           CROSS JOIN jobs j ON j.thread_id = t.id
          WHERE LOWER(p.key) = ?1 AND t.name = ?2 AND {}",
        crate::threads::SESSION_JOB_SHAPE
    )
}

/// The jobs a followed ISSUE streams: every node of every execution it has run.
const FOLLOW_ISSUE_JOBS_SQL: &str = "SELECT j.id, p.key, i.number, i.title, p.id
   FROM projects p
   CROSS JOIN issues i ON i.project_id = p.id
   CROSS JOIN jobs j ON j.issue_id = i.id
  WHERE LOWER(p.key) = ?1 AND i.number = ?2";

/// The assistant events a resolved target's jobs have produced since the cursor.
/// Built for a given job count so the regression test can plan the shape the
/// sweep runs, at a cardinality that exposes a planner flip.
fn followed_events_sql(jobs: usize) -> String {
    format!(
        "SELECT e.rowid, e.data, r.job_id
           FROM runs r
           JOIN events e ON e.run_id = r.id
          WHERE r.job_id IN ({}) AND e.event_type = 'assistant' AND e.rowid > ?
          ORDER BY e.rowid",
        placeholders(jobs)
    )
}

/// The newest rowid a resolved target's jobs have reached, over EVERY event type
/// — a follow starts at the target's live edge, not at its last assistant turn,
/// so a session mid-turn does not replay the rest of that turn onto the phone.
fn follow_live_edge_sql(jobs: usize) -> String {
    format!(
        "SELECT COALESCE(MAX(e.rowid), 0)
           FROM runs r
           JOIN events e ON e.run_id = r.id
          WHERE r.job_id IN ({})",
        placeholders(jobs)
    )
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
    /// Undelivered review pushes this load examined, whatever became of them.
    /// Logged beside the refresh duration so the backlog's size and the sweep's
    /// cost are readable together: a duration that climbs while this stays flat
    /// means the loader started paying for something other than the backlog.
    scanned: usize,
}

/// The poll a bare command asks for, matched as an exact case-insensitive word.
/// `threads` means threads: the operator's muscle memory is the word, and after
/// the thread cutover the word means the entity.
fn poll_command(text: &str) -> Option<PollKind> {
    match channel_command(text)? {
        ChannelCommand::Threads => Some(PollKind::Threads),
        ChannelCommand::Issues => Some(PollKind::Issues),
        _ => None,
    }
}

fn unfollow_selector(text: &str) -> Option<Option<String>> {
    match channel_command(text)? {
        ChannelCommand::Unfollow(selector) => Some(selector),
        _ => None,
    }
}

fn focus_selector(text: &str) -> Option<String> {
    match channel_command(text)? {
        ChannelCommand::Focus(selector) => Some(selector),
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

pub(crate) fn assistant_text(data: &str) -> Option<String> {
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
    multi_select: bool,
) -> Result<bool, String> {
    if !ledger::claim_question_cleanup(db, &record.id, chrono::Utc::now().timestamp_millis())
        .await?
    {
        return Ok(false);
    }

    cleanup_claimed_question(db, provider, record, receipt, multi_select).await?;
    Ok(true)
}

async fn cleanup_claimed_question(
    db: &LocalDb,
    provider: &dyn ChannelProvider,
    record: &ledger::OutboundRecord,
    receipt: &str,
    multi_select: bool,
) -> Result<(), String> {
    let (Some(provider_guid), Some(sent_at)) = (&record.provider_guid, record.sent_at) else {
        return Ok(());
    };
    let action_ref = if record.kind == "question" {
        record
            .binding_ref
            .rsplit_once(':')
            .map(|(prompt_id, _)| prompt_id)
            .unwrap_or(&record.binding_ref)
    } else {
        &record.binding_ref
    };
    let resolution = ledger::resolution_for_action(db, action_ref).await?;
    let message = ResolvedQuestionMessage {
        conversation: record.conversation.clone(),
        provider_guid: provider_guid.clone(),
        caption_guid: record.caption_guid.clone(),
        sent_at,
        original_text: record.rendered_text.clone(),
        answer: resolution.as_ref().map(|value| value.answer.clone()),
        answering_surface: resolution.map(|value| value.winner_surface),
        multi_select,
        receipt: receipt.to_string(),
    };
    provider.cleanup_question(&message).await.map_err(|error| {
        log::warn!(
            "channel could not clean up resolved question {}: {error}",
            record.binding_ref
        );
        error
    })
}

fn review_notice(project: &str, number: i32, title: &str, content_ref: &str) -> String {
    format!("{project}/{number} review ready — {title}\n{content_ref}")
}

impl Gate {
    fn is_presence_aware(&self) -> bool {
        self.initiated_by.is_presence_aware()
    }

    /// Whether the operator's attention surfaces should skip this ask because a
    /// live agent watcher is already answerable for it (CAIRN-4732).
    ///
    /// `route` gates are excluded by an explicit term rather than by relying on
    /// their ask key being absent. An operator-configured route sink is a
    /// destination the operator chose for themselves, so no question of
    /// answerability applies to it, and saying so here means a later change that
    /// gives route gates a key cannot silently start withholding them. An agent
    /// `message` carries no ask at all, so the same reasoning excludes it.
    fn held_by_agent(&self) -> bool {
        !matches!(self.kind, "route" | agent_message::KIND) && self.held_since.is_some()
    }

    fn message_class(&self) -> i64 {
        match self.ask {
            OutboundAsk::Question { .. } => super::bindings::MESSAGE_CLASS_QUESTION,
            OutboundAsk::Permission { .. } => super::bindings::MESSAGE_CLASS_PERMISSION,
            OutboundAsk::Notify { .. } => super::bindings::MESSAGE_CLASS_NOTIFY,
        }
    }

    fn delivery_key(&self, provider: &str, default_conversation: &str) -> String {
        format!(
            "{provider}\u{0}{}\u{0}{}\u{0}{}",
            self.conversation.as_deref().unwrap_or(default_conversation),
            self.kind,
            self.binding_ref
        )
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

/// What a route submission's gate is keyed and sent under.
enum RouteConversation {
    /// Deliver under this conversation. `None` is the provider's configured
    /// default conversation, which only single-conversation providers have.
    Deliver(Option<String>),
    /// The subject's Discord surface is being created; its structural binding
    /// becomes the conversation once the reconciler installs it.
    Awaiting(String),
    /// No conversation exists or can be made; the firing fails with this reason.
    Unaddressable(String),
}

/// An undirected Discord route waiting for its subject's structural surface.
struct HeldRoute {
    submission: ChannelSubmission,
    target: String,
    deadline: Instant,
}

/// The issue or thread a route's fact is about. On Discord that subject's
/// structural conversation is where an undirected route lands. A thread-stream
/// fact names the followed target; an attention fact names a resource under
/// its issue (the issue itself, or a node or artifact beneath it).
fn route_subject(fact: &RouteFact) -> Option<FollowTarget> {
    ["threadUri", "detailUri"]
        .into_iter()
        .filter_map(|field| fact.fields.get(field)?.as_str())
        .find_map(|uri| match parse_uri(uri)? {
            CairnResource::Thread { project, name, .. } => {
                Some(FollowTarget::Thread { project, name })
            }
            resource => Some(FollowTarget::Issue {
                project: resource.project()?.to_string(),
                number: resource.issue_number()?,
            }),
        })
}

fn cancel_resolved_attention(
    deferred: &mut HashMap<String, DeferredAttention>,
    live: &HashSet<String>,
    snapshot_complete: bool,
) -> Vec<DeferredAttention> {
    if !snapshot_complete {
        return Vec::new();
    }
    // A route has no domain row to resolve and is never in the live gate set,
    // so its absence there says nothing; its deferral ends only on presence or
    // its deadline.
    let settles = |binding: &String, attention: &DeferredAttention| {
        attention.gate.kind != "route" && !live.contains(binding)
    };
    let cancelled = deferred
        .iter()
        .filter(|(binding, attention)| settles(binding, attention))
        .map(|(_, attention)| DeferredAttention {
            id: attention.id.clone(),
            gate: attention.gate.clone(),
            deadline: attention.deadline,
        })
        .collect::<Vec<_>>();
    deferred.retain(|binding, attention| !settles(binding, attention));
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
    conversation: Option<String>,
    target_uri: Option<String>,
    /// The `attention_pushes` key this ask is filed under, when it has one.
    ///
    /// Built with the SAME builder the push creator uses rather than with a
    /// second derivation that happens to agree. If the two strings ever diverge
    /// the answerability lookup finds nothing, every ask looks unheld, and the
    /// whole feature fails open to the old behaviour with nothing to notice it
    /// -- which is why `router_ask_keys_match_the_push_creator` asserts the two
    /// directly rather than trusting the shapes to stay aligned.
    ///
    /// `None` for a gate that names no ask: an operator-inbound poll, or a route
    /// submission. A thread's own ask DOES get a key, and no push is ever filed
    /// under it -- the creator pushes only for an issue-scoped ask, and excludes
    /// the asking node besides -- so it resolves as unheld and reaches the
    /// operator, which is the case this whole feature turns on.
    ask_key: Option<String>,
    /// When a live agent watcher was told about this ask, if one holds it.
    /// Stamped once per gate-source snapshot by [`stamp_agent_holders`].
    held_since: Option<i64>,
}

/// The three gate loads that depend only on the DATABASE.
///
/// A sweep belongs to one provider, but most of what it reads does not: the open
/// questions, the pending permissions, and the review backlog are facts about
/// the workspace, and the provider only decides what to do with them. Every
/// enabled provider nevertheless ran all three on its own five-second loop, so a
/// workspace with three channels configured paid for the same expensive reads
/// three times per tick, on three different threads (CAIRN-4208).
#[derive(Clone, Copy, PartialEq, Eq)]
enum GateKind {
    Question,
    Permission,
    Review,
}

impl GateKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Question => "question",
            Self::Permission => "permission",
            Self::Review => "review",
        }
    }
}

/// One kind's gates as of one database generation, plus the diagnostic counts
/// the refresh log carries.
#[derive(Clone)]
struct GateSourceSnapshot {
    /// The database's mutation generation when this load STARTED. A reader whose
    /// database has moved past it asks for a recomputation; a reader that
    /// arrives while one is in flight joins it instead.
    generation: u64,
    gates: Arc<Vec<Gate>>,
    backlog: usize,
    expired: usize,
}

/// Process-wide, so the sharing is between PROVIDERS and not merely between one
/// provider's own sweeps.
fn gate_sources() -> &'static GenerationCache<GateSourceSnapshot> {
    static SOURCES: std::sync::OnceLock<GenerationCache<GateSourceSnapshot>> =
        std::sync::OnceLock::new();
    SOURCES.get_or_init(|| GenerationCache::new("channel gate source"))
}

async fn load_source_gates(
    db: &LocalDb,
    kind: GateKind,
) -> Result<(Vec<Gate>, usize, usize), String> {
    let (mut gates, backlog, expired) = match kind {
        GateKind::Question => (load_questions(db).await?, 0, 0),
        GateKind::Permission => (load_permissions(db).await?, 0, 0),
        GateKind::Review => {
            let reviews = load_reviews(db).await?;
            (reviews.gates, reviews.scanned, reviews.expired_dangling)
        }
    };
    stamp_agent_holders(db, &mut gates).await?;
    Ok((gates, backlog, expired))
}

/// Ask the one answerability predicate about a whole gate load at once.
///
/// Here rather than inside each loader for two reasons. It keeps the loaders'
/// admission shape intact -- they select the candidate population, this filters
/// it, which is the CAIRN-4480 lesson about gate-shaped queries. And it sits
/// inside `shared_gates`, so however many providers observe one database
/// generation, the predicate runs once for all of them.
async fn stamp_agent_holders(db: &LocalDb, gates: &mut [Gate]) -> Result<(), String> {
    let keys: Vec<&str> = gates
        .iter()
        .filter_map(|gate| gate.ask_key.as_deref())
        .collect();
    if keys.is_empty() {
        return Ok(());
    }
    let held = crate::orchestrator::attention_push::asks_held_by_agent(db, &keys)
        .await
        .map_err(|error| error.to_string())?;
    if held.is_empty() {
        return Ok(());
    }
    for gate in gates {
        let Some(key) = gate.ask_key.as_deref() else {
            continue;
        };
        gate.held_since = held
            .get(&cairn_common::uri::canonicalize_uri_identity(key))
            .copied();
    }
    Ok(())
}

/// One kind's gates for one database, computed once per generation for the whole
/// process however many providers ask for them.
///
/// The freshness rule is "no staler than when this reader asked", not "newer
/// than anything": a reader that joins a flight already running accepts that
/// flight's snapshot, and a reader whose database has advanced past what is
/// published asks for a new one. Sharing therefore engages exactly when it is
/// needed — under load, where sweeps are long and overlap, three providers
/// collapse into one computation — and costs nothing at idle, where sweeps are
/// short, never overlap, and each is a hit anyway.
async fn shared_gates(db: &LocalDb, kind: GateKind) -> Result<GateSourceSnapshot, String> {
    // The key carries the database's INSTANCE as well as its path: a handle
    // reopened at the same path starts its generation again at zero, and a key
    // that could not tell the two apart would hand the new database the old
    // one's gates the moment its counter caught up.
    let key = format!(
        "{}|{}|{}",
        db.instance(),
        db.path().display(),
        kind.as_str()
    );
    let observed = db.mutation_generation();
    // Deciding and advancing under one lock is the point: N providers observing
    // the same advance must produce ONE recomputation, where N unconditional
    // invalidations would each reject the readers already computing.
    gate_sources().invalidate_stale(&key, "database-advanced", |snapshot| {
        snapshot.generation != observed
    });
    let failure = Mutex::new(None);
    let snapshot = gate_sources()
        .get_or_compute(&key, || async {
            // Read the generation BEFORE the load, so a write racing this
            // snapshot forces one follow-up refresh instead of being lost.
            let generation = db.mutation_generation();
            let started = Instant::now();
            match load_source_gates(db, kind).await {
                Ok((gates, backlog, expired)) => {
                    log::info!(
                        "channel gate source kind={} generation={generation} backlog={backlog} gates={} expired={expired} duration_ms={}",
                        kind.as_str(),
                        gates.len(),
                        started.elapsed().as_millis()
                    );
                    Some(GateSourceSnapshot {
                        generation,
                        gates: Arc::new(gates),
                        backlog,
                        expired,
                    })
                }
                Err(problem) => {
                    *failure.lock().expect("gate source failure slot poisoned") = Some(problem);
                    None
                }
            }
        })
        .await;
    snapshot.ok_or_else(|| {
        failure
            .lock()
            .expect("gate source failure slot poisoned")
            .take()
            .unwrap_or_else(|| format!("channel {} gate source load failed", kind.as_str()))
    })
}

#[derive(Default)]
struct GateSnapshot {
    generation: u64,
    binding_generation: String,
    initialized: bool,
    routed: Vec<Gate>,
}

pub struct ChannelRouter {
    orch: Orchestrator,
    provider: Arc<dyn ChannelProvider>,
    provider_id: &'static str,
    destination: String,
    route: MessageClassPolicy,
    inbound_capabilities: ChannelInboundCapabilities,
    claims: ClaimSet,
    deferred_attention: Mutex<HashMap<String, DeferredAttention>>,
    gate_snapshots: Mutex<HashMap<std::path::PathBuf, GateSnapshot>>,
    /// Keyed by binding ref, so a fact re-submitted while its surface is still
    /// being created waits once.
    held_routes: Mutex<HashMap<String, HeldRoute>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InboundCapability {
    Permissions,
    Answers,
    FreeText,
}

fn admits_capability(
    capabilities: ChannelInboundCapabilities,
    capability: InboundCapability,
) -> bool {
    match capability {
        InboundCapability::Permissions => capabilities.permissions,
        InboundCapability::Answers => capabilities.answers,
        InboundCapability::FreeText => capabilities.free_text,
    }
}

fn inbound_parts(event: &InboundEvent) -> (Option<&str>, &str, &str) {
    match event {
        InboundEvent::Selection {
            bound_guid,
            sender,
            option_text,
            ..
        } => (Some(bound_guid), sender, option_text),
        InboundEvent::Selections {
            bound_guid,
            sender,
            changes,
            ..
        } => (
            Some(bound_guid),
            sender,
            changes
                .first()
                .map(|change| change.option_text.as_str())
                .unwrap_or(""),
        ),
        InboundEvent::Reply {
            bound_guid,
            sender,
            text,
            ..
        } => (Some(bound_guid), sender, text),
        InboundEvent::Bare { sender, text, .. } | InboundEvent::Rejected { sender, text, .. } => {
            (None, sender, text)
        }
    }
}

fn inbound_sender_text(event: &InboundEvent) -> (&str, &str) {
    let (_, sender, text) = inbound_parts(event);
    (sender, text)
}

impl ChannelRouter {
    pub fn new_for_provider(
        orch: Orchestrator,
        provider: Arc<dyn ChannelProvider>,
        provider_id: &'static str,
        destination: String,
        route: MessageClassPolicy,
        inbound_capabilities: ChannelInboundCapabilities,
    ) -> Self {
        Self {
            orch,
            provider,
            provider_id,
            destination,
            route,
            inbound_capabilities,
            claims: ClaimSet::default(),
            deferred_attention: Mutex::new(HashMap::new()),
            gate_snapshots: Mutex::new(HashMap::new()),
            held_routes: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub fn new(
        orch: Orchestrator,
        provider: Arc<dyn ChannelProvider>,
        config: crate::models::IMessageChannelConfig,
    ) -> Self {
        Self::new_for_provider(
            orch,
            provider,
            "imessage",
            config.to,
            config.route,
            config.inbound_capabilities,
        )
    }

    async fn submit_gate(
        &self,
        submission: ChannelSubmission,
        conversation: Option<String>,
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
                conversation,
                target_uri: None,
                ask_key: None,
                held_since: None,
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
        let conversation = match self.route_gate_conversation(&submission).await? {
            RouteConversation::Deliver(conversation) => conversation,
            RouteConversation::Awaiting(target) => {
                self.held_routes
                    .lock()
                    .expect("held route set poisoned")
                    .entry(submission.binding_ref.clone())
                    .or_insert(HeldRoute {
                        submission,
                        target,
                        deadline: now + SURFACE_WAIT,
                    });
                return Ok(());
            }
            RouteConversation::Unaddressable(reason) => {
                return crate::routes::record_channel_outcome(&self.orch, &submission, Err(reason))
                    .await;
            }
        };
        let binding_ref = submission.binding_ref.clone();
        self.submit_gate(submission.clone(), conversation.clone(), presence, now)
            .await?;
        let intent = self
            .route_intent(
                conversation.as_deref().unwrap_or(&self.destination),
                &binding_ref,
            )
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

    /// The conversation a route submission's gate is keyed and sent under.
    ///
    /// A directed sink names its own. An undirected sink takes the provider's
    /// default conversation, which iMessage and Telegram have and Discord does
    /// not: a Discord router's destination is its guild, and a guild is never a
    /// conversation. There an undirected route belongs to the structural
    /// conversation of its fact's subject (the issue's thread, the Cairn
    /// thread's channel), where a binding-routed gate for that target lands.
    async fn route_gate_conversation(
        &self,
        submission: &ChannelSubmission,
    ) -> Result<RouteConversation, String> {
        if let Some(conversation) = route_conversation(submission.destination.as_ref()) {
            return Ok(RouteConversation::Deliver(Some(conversation)));
        }
        if self.provider_id != "discord" {
            return Ok(RouteConversation::Deliver(None));
        }
        let Some(subject) = route_subject(&submission.fact) else {
            return Ok(RouteConversation::Unaddressable(format!(
                "Discord has no default conversation and this {} fact names no issue or thread; address the sink to a Discord conversation",
                submission.fact.source
            )));
        };
        let target = subject.uri();
        if let Some(conversation) = self.discord_structural_conversation(&target).await? {
            return Ok(RouteConversation::Deliver(Some(conversation)));
        }
        let db = self.orch.db.for_project(subject.project()).await;
        if self.ensure_discord_surface(&db, &target).await? {
            Ok(RouteConversation::Awaiting(target))
        } else {
            Ok(RouteConversation::Unaddressable(format!(
                "Discord has no default conversation and {target} has no Discord surface to carry this (unknown or already finished); address the sink to a Discord conversation"
            )))
        }
    }

    /// The target's structural conversation in the configured guild, as the
    /// surface reconciler bound it: a canonical `discord:<guild>/<channel>`.
    async fn discord_structural_conversation(
        &self,
        target: &str,
    ) -> Result<Option<String>, String> {
        let guild = self.discord_guild()?;
        Ok(ledger::list_eligible_bindings(
            self.ledger(),
            self.provider_id,
            target,
            super::bindings::MESSAGE_CLASS_NOTIFY,
        )
        .await?
        .into_iter()
        .filter(|binding| {
            binding.binding_kind == super::bindings::BindingKind::Structural.as_str()
        })
        .map(|binding| binding.conversation)
        .find(|conversation| {
            conversation
                .parse::<super::ConversationAddress>()
                .is_ok_and(|address| {
                    matches!(
                        address.destination(),
                        super::ConversationDestination::Discord { guild_id, .. } if *guild_id == guild
                    )
                })
        }))
    }

    fn discord_guild(&self) -> Result<u64, String> {
        self.destination
            .parse::<u64>()
            .map_err(|_| "Discord router destination must be a guild ID".to_string())
    }

    /// Re-resolve every route waiting on a Discord surface: send those whose
    /// structural binding has landed, and fail those that outwaited
    /// [`SURFACE_WAIT`], so a sink that never resolves still leaves a firing.
    async fn retry_held_routes(&self, presence: OperatorPresence, now: Instant) {
        let held = std::mem::take(&mut *self.held_routes.lock().expect("held route set poisoned"));
        for (binding_ref, route) in held {
            let failure = match self.route_gate_conversation(&route.submission).await {
                Ok(RouteConversation::Deliver(_)) => {
                    if let Err(error) = self.submit_route(route.submission, presence, now).await {
                        log::warn!("held route {binding_ref} could not be submitted: {error}");
                    }
                    continue;
                }
                Ok(RouteConversation::Awaiting(_)) if now < route.deadline => None,
                Ok(RouteConversation::Awaiting(target)) => Some(format!(
                    "the Discord surface for {target} was not ready within {} minutes",
                    SURFACE_WAIT.as_secs() / 60
                )),
                Ok(RouteConversation::Unaddressable(reason)) => Some(reason),
                Err(error) => {
                    log::warn!(
                        "held route {binding_ref} for {} could not be resolved: {error}",
                        route.target
                    );
                    None
                }
            };
            match failure {
                Some(reason) => {
                    if let Err(error) = crate::routes::record_channel_outcome(
                        &self.orch,
                        &route.submission,
                        Err(reason),
                    )
                    .await
                    {
                        log::warn!("held route {binding_ref} outcome was not recorded: {error}");
                    }
                }
                None => {
                    self.held_routes
                        .lock()
                        .expect("held route set poisoned")
                        .entry(binding_ref)
                        .or_insert(route);
                }
            }
        }
    }

    /// Retire a pending undirected Discord route keyed by the guild id. Every
    /// such row predates structural route delivery and could never send; it is
    /// history, so it fails representably rather than being re-sent or left to
    /// stall recovery.
    async fn retire_guild_keyed_route(
        &self,
        record: &ledger::OutboundRecord,
        submission: &ChannelSubmission,
    ) -> Result<(), String> {
        let reason = "this undirected Discord route was keyed to the guild id, which is not a conversation; it predates structural route delivery and is not re-sent";
        let now = chrono::Utc::now().timestamp_millis();
        if ledger::begin_delivery(self.ledger(), &record.id, now).await? {
            ledger::mark_failed(self.ledger(), &record.id, reason, now, None).await?;
        }
        crate::routes::record_channel_outcome(&self.orch, submission, Err(reason.into())).await
    }

    /// The route intent a gate wrote, read back under the same conversation it
    /// was keyed by. One binding can own rows under more than one conversation
    /// string (a directed Discord route recorded under its bare channel id before
    /// it carried the canonical address), and the binding alone would pick one
    /// of them arbitrarily.
    async fn route_intent(
        &self,
        conversation: &str,
        binding_ref: &str,
    ) -> Result<Option<ledger::OutboundRecord>, String> {
        ledger::get_by_conversation_binding(
            self.ledger(),
            self.provider_id,
            conversation,
            "route",
            binding_ref,
        )
        .await
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
            let conversation = match route_conversation(submission.destination.as_ref()) {
                Some(conversation) => Some(conversation),
                // An undirected Discord row is keyed by the structural
                // conversation resolved when it was submitted. The row is the
                // delivery's identity, so recovery sends it there rather than
                // re-resolving the subject.
                None if self.provider_id == "discord" => {
                    if record
                        .conversation
                        .parse::<super::ConversationAddress>()
                        .is_err()
                    {
                        self.retire_guild_keyed_route(&record, &submission).await?;
                        continue;
                    }
                    Some(record.conversation.clone())
                }
                None => None,
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
                conversation,
                target_uri: None,
                ask_key: None,
                held_since: None,
            };
            let conversation = gate
                .conversation
                .clone()
                .unwrap_or_else(|| self.destination.clone());
            if record.conversation != conversation {
                // Recorded under an earlier key for the same destination (a
                // directed Discord route keyed by its bare channel id). The send
                // below goes out under this row's id, so the row adopts the key
                // its delivery addresses; otherwise a re-fire would miss it and
                // deliver the fact again. A row already holding that key is the
                // delivery, and this one is retired unsent.
                if self
                    .route_intent(&conversation, &record.binding_ref)
                    .await?
                    .is_some()
                {
                    ledger::mark_expired(
                        self.ledger(),
                        &record.id,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                    continue;
                }
                ledger::adopt_pending_conversation(self.ledger(), &record.id, &conversation)
                    .await?;
            }
            let elapsed_ms = chrono::Utc::now()
                .timestamp_millis()
                .saturating_sub(record.created_at)
                .max(0) as u64;
            let remaining = ATTENTION_GRACE.saturating_sub(Duration::from_millis(elapsed_ms));
            let delivery_key = gate.delivery_key(self.provider_id, &self.destination);
            self.deferred_attention
                .lock()
                .expect("deferred attention set poisoned")
                .insert(
                    delivery_key,
                    DeferredAttention {
                        id: record.id.clone(),
                        gate: gate.clone(),
                        deadline: now + remaining,
                    },
                );
            self.deliver_or_defer(gate, presence, now).await?;
            let updated = ledger::get_by_id(self.ledger(), &record.id)
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

    /// Deliver one ledgered agent message through the ordinary outbound path, so
    /// its claim, retry backoff, `last_error`, and attempt cap are the same as
    /// every other intent's. Reports whether it went on the wire.
    async fn deliver_agent_message(&self, record: ledger::OutboundRecord) -> Result<bool, String> {
        let context = agent_message::sender_home(&record.binding_ref)
            .map(agent_message::context_header)
            .unwrap_or_else(|| "[Cairn]".to_string());
        let gate = Gate {
            kind: agent_message::KIND,
            initiated_by: OutboundInitiator::Agent,
            binding_ref: record.binding_ref,
            job_id: record.job_id,
            context,
            ask: OutboundAsk::Notify {
                text: record.rendered_text,
            },
            conversation: Some(record.conversation),
            target_uri: None,
            ask_key: None,
            held_since: None,
        };
        // An agent gate is not presence aware, so `deliver_or_defer` never
        // consults the presence passed here: the message is never deferred into
        // `deferred_attention`, and so can never be expired by presence
        // cancellation either.
        self.deliver_or_defer(gate, OperatorPresence::Away, Instant::now())
            .await
    }

    /// The inbox hand-off for a freshly enqueued agent message.
    async fn submit_agent_message(&self, intent_id: &str) -> Result<(), String> {
        let Some(record) = ledger::get_by_id(self.ledger(), intent_id).await? else {
            return Err(format!("agent message intent {intent_id} is not ledgered"));
        };
        if record.kind != agent_message::KIND || record.channel != self.provider_id {
            return Err(format!(
                "intent {intent_id} is a {} {} row, not a {} agent message",
                record.channel, record.kind, self.provider_id
            ));
        }
        self.deliver_agent_message(record).await.map(|_| ())
    }

    /// Deliver every agent message this provider owes an attempt: rows the inbox
    /// hand-off never reached (a restart, or no router at enqueue time) and
    /// failed rows whose retry is due. Racing the inbox is safe because
    /// `begin_delivery` / `begin_retry_delivery` admit exactly one sender.
    async fn deliver_pending_agent_messages(&self) -> Result<(), String> {
        let due = ledger::list_due_deliveries(
            self.ledger(),
            self.provider_id,
            agent_message::KIND,
            chrono::Utc::now().timestamp_millis(),
            MAX_ATTEMPTS,
        )
        .await?;
        let mut delivered = 0;
        for record in due {
            if delivered == SWEEP_LIMIT {
                break;
            }
            let id = record.id.clone();
            match self.deliver_agent_message(record).await {
                Ok(true) => delivered += 1,
                Ok(false) => {}
                Err(error) => log::warn!("channel agent message {id} delivery failed: {error}"),
            }
        }
        Ok(())
    }

    async fn question_is_multi_select(
        &self,
        record: &ledger::OutboundRecord,
    ) -> Result<bool, String> {
        if record.kind != "question" {
            return Ok(false);
        }
        let Some((prompt_id, index)) = record.binding_ref.rsplit_once(':') else {
            return Ok(false);
        };
        let Ok(index) = index.parse::<usize>() else {
            return Ok(false);
        };
        for db in self.orch.db.all_dbs().await {
            if let Some(json) = db
                .query_opt_text(
                    "SELECT questions FROM prompts WHERE id=?1",
                    params![prompt_id],
                )
                .await
                .map_err(|error| error.to_string())?
            {
                let questions: Vec<StoredQuestion> =
                    serde_json::from_str(&json).map_err(|error| error.to_string())?;
                return Ok(questions
                    .get(index)
                    .is_some_and(|question| question.multi_select));
            }
        }
        Ok(false)
    }

    async fn followed_uri(&self, selector: &str) -> Result<Option<String>, String> {
        let follows = ledger::list_follows(self.ledger(), self.provider_id).await?;
        if let Some(exact) = follows
            .iter()
            .find(|follow| follow.uri.eq_ignore_ascii_case(selector))
        {
            return Ok(Some(exact.uri.clone()));
        }
        let mut matches = follows.into_iter().filter(|follow| {
            FollowTarget::parse(&follow.uri).is_ok_and(|target| {
                target.selector().eq_ignore_ascii_case(selector)
                    || format!("{}/{}", target.project(), target.selector())
                        .eq_ignore_ascii_case(selector)
            })
        });
        let first = matches.next().map(|follow| follow.uri);
        if first.is_some() && matches.next().is_some() {
            return Err(format!(
                "More than one follow matches {selector}; use project/{selector}."
            ));
        }
        Ok(first)
    }

    async fn follow_or_focus(&self, uri: &str, conversation: &str) -> Result<(), String> {
        let target = self.resolve_target(uri).await?;
        let canonical = target.uri();
        let already_followed =
            ledger::is_target_followed(self.ledger(), self.provider_id, &canonical).await?;
        if already_followed {
            ledger::set_focus(
                self.ledger(),
                self.provider_id,
                &canonical,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        } else {
            self.follow(&canonical).await?;
        }
        let action = if already_followed {
            "Focused"
        } else {
            "Following"
        };
        self.send_notice(
            conversation,
            &format!(
                "{action} {} — loose messages now go here. /threads to switch.",
                target.selector()
            ),
        )
        .await
    }

    async fn unfollow_selector_command(
        &self,
        selector: &str,
        conversation: &str,
    ) -> Result<(), String> {
        let Some(uri) = self.followed_uri(selector).await? else {
            return self
                .send_notice(conversation, "That follow was not found.")
                .await;
        };
        self.unfollow_uri_command(&uri, conversation).await
    }

    async fn unfollow_uri_command(&self, uri: &str, conversation: &str) -> Result<(), String> {
        self.unfollow(uri).await?;
        let target = FollowTarget::parse(uri)?;
        self.send_notice(conversation, &format!("Unfollowed {}.", target.selector()))
            .await
    }

    async fn focus_selector_command(
        &self,
        selector: &str,
        conversation: &str,
    ) -> Result<(), String> {
        let Some(uri) = self.followed_uri(selector).await? else {
            return self
                .send_notice(
                    conversation,
                    "That follow was not found. Use /threads or /issues to follow it first.",
                )
                .await;
        };
        self.follow_or_focus(&uri, conversation).await
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
    /// Retrying is safe: sealing is a single `INSERT OR IGNORE`, so a second pass
    /// completes a partial first one without touching already-owed work.
    async fn draw_the_session_line(&self, boundary: SessionBoundary) -> Result<(), String> {
        if boundary == SessionBoundary::Resumed {
            return Ok(());
        }
        let mut fenced = 0;
        let mut expired_dangling = 0;
        for db in self.orch.db.all_dbs().await {
            let result = self.fence_existing_gates(&db).await?;
            fenced += result.0;
            expired_dangling += result.1;
        }
        if ledger::remove_home_relative_focus(self.ledger(), self.provider_id).await? {
            log::warn!("channel removed unresolvable home-relative focus");
        }
        for follow in ledger::list_follows(self.ledger(), self.provider_id).await? {
            if follow.uri == "cairn:~"
                || follow.uri.starts_with("cairn:~/")
                || follow.uri.starts_with("cairn:@")
            {
                // A home-relative URI only has meaning together with the session
                // that emitted it. Legacy ledger rows retained neither that
                // context nor a stable identity, so guessing would risk routing
                // operator messages to an unrelated session after restart.
                log::warn!(
                    "channel removed unresolvable home-relative followed URI {}",
                    follow.uri
                );
                ledger::unfollow_target(self.ledger(), self.provider_id, &follow.uri).await?;
                continue;
            }
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
        if fenced > 0 || expired_dangling > 0 {
            log::info!(
                "channel sealed the pre-session backlog: {fenced} gate(s) sealed, {expired_dangling} dangling review(s) expired"
            );
        }
        Ok(())
    }

    async fn follow_poll_selection(
        &self,
        record: &ledger::OutboundRecord,
        selected_label: &str,
        conversation: &str,
    ) -> Result<(), String> {
        let options = follow_poll_options(record)
            .ok_or_else(|| "follow poll has no option bindings".to_string())?;
        let uri = options
            .bindings
            .get(selected_label)
            .ok_or_else(|| format!("unknown follow poll option: {selected_label}"))?;
        let uri = self.resolve_bound_target(record, uri).await?;
        self.follow_or_focus(&uri, conversation).await
    }

    /// Resolve a binding while the outbound row still carries the emitting
    /// session's job. Once reduced to a follow row, a home-relative URI has no
    /// context and cannot be recovered safely.
    async fn resolve_bound_target(
        &self,
        record: &ledger::OutboundRecord,
        uri: &str,
    ) -> Result<String, String> {
        let Some(suffix) = uri
            .strip_prefix("cairn:~/")
            .or_else(|| (uri == "cairn:~").then_some(""))
        else {
            return Ok(uri.to_string());
        };
        let job_id = record
            .job_id
            .as_deref()
            .ok_or_else(|| format!("home-relative follow binding has no session context: {uri}"))?;
        for db in self.orch.db.all_dbs().await {
            if let Some(home) = crate::jobs::queries::home_uri_for_job(&db, job_id)
                .await
                .map_err(|error| error.to_string())?
            {
                return if suffix.is_empty() {
                    Ok(home)
                } else {
                    Ok(format!("{}/{suffix}", home.trim_end_matches('/')))
                };
            }
        }
        Err(format!(
            "home-relative follow binding references unknown job {job_id}"
        ))
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
                let _ = cleanup_claimed_question(
                    self.ledger(),
                    self.provider.as_ref(),
                    &update,
                    "✓ unfollowed",
                    false,
                )
                .await;
            }
        }
        Ok(())
    }

    async fn send_follow_poll(&self, conversation: &str, kind: PollKind) -> Result<(), String> {
        // Unlike an answered one-shot question, a follow poll is a standing
        // control surface. Its GUID binding remains live through ordinary gate
        // sweeps and outbound traffic; only a newer poll of the same kind replaces it.
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
        let binding_ref = format!("{}{}", kind.binding_prefix(), Uuid::new_v4());
        let gate = Gate {
            conversation: None,
            target_uri: None,
            kind: "question",
            initiated_by: OutboundInitiator::OperatorInbound,
            binding_ref: binding_ref.clone(),
            job_id: None,
            context: String::new(),
            // A poll Cairn is putting TO the operator. It answers no agent's
            // ask and no push was ever minted for it.
            ask_key: None,
            held_since: None,
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
        let delivered = self.send_claimed(id.clone(), gate, false).await?;
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

        for previous in ledger::list_unresolved(self.ledger(), self.provider_id).await? {
            if previous.id != id
                && previous.status == "sent"
                && previous.conversation == conversation
                && follow_poll_kind(&previous.binding_ref) == Some(kind)
            {
                self.finish_question(&previous, "✓ replaced by a newer command")
                    .await?;
            }
        }
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
                            uri: format!(
                                "cairn://p/{}/{}",
                                cairn_common::uri::canonical_project(row.text(0)?),
                                row.text(1)?
                            ),
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
                            uri: format!(
                                "cairn://p/{}/{}",
                                cairn_common::uri::canonical_project(row.text(0)?),
                                row.i64(1)?
                            ),
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

    /// Resolve a followed target to the jobs that carry its transcript, once,
    /// before either event read runs. See [`FollowSource`] for why the reads are
    /// shaped this way.
    async fn follow_source(&self, target: &FollowTarget) -> Result<FollowSource, String> {
        let db = self.orch.db.for_project(target.project()).await;
        let rows = match target {
            FollowTarget::Thread { project, name } => {
                db.query_all(
                    follow_thread_jobs_sql(),
                    params![cairn_common::uri::canonical_project(project), name.clone()],
                    |row| {
                        Ok((
                            row.text(0)?,
                            // A thread's one identifier, written the way its URI
                            // and its pane header write it. An issue, which has a
                            // number AND a title, still reads "KEY-N Title".
                            format!("{}/{}", row.text(1)?, row.text(2)?),
                            row.text(3)?,
                        ))
                    },
                )
                .await
            }
            FollowTarget::Issue { project, number } => {
                db.query_all(
                    FOLLOW_ISSUE_JOBS_SQL,
                    params![cairn_common::uri::canonical_project(project), *number],
                    |row| {
                        Ok((
                            row.text(0)?,
                            format!(
                                "{}/{} {}",
                                row.text(1)?.to_lowercase(),
                                row.i64(2)?,
                                row.text(3)?
                            ),
                            row.text(4)?,
                        ))
                    },
                )
                .await
            }
        }
        .map_err(|error| error.to_string())?;

        // Every row of either statement describes the same target, so the
        // context and project are read off the first one.
        let Some((_, context, project_id)) = rows.first().cloned() else {
            return Ok(FollowSource {
                job_ids: Vec::new(),
                context: String::new(),
                project_id: String::new(),
            });
        };
        Ok(FollowSource {
            job_ids: rows.into_iter().map(|(job_id, ..)| job_id).collect(),
            context,
            project_id,
        })
    }

    /// The newest event a target's session has produced. A new follow starts here
    /// so neither a restart nor a re-follow replays history.
    async fn live_edge(&self, target: &FollowTarget) -> Result<i64, String> {
        let source = self.follow_source(target).await?;
        if source.job_ids.is_empty() {
            return Ok(0);
        }
        let db = self.orch.db.for_project(target.project()).await;
        let sql = follow_live_edge_sql(source.job_ids.len());
        let binds: Vec<Value> = source.job_ids.into_iter().map(Value::Text).collect();
        db.query_opt_i64(sql, binds)
            .await
            .map(|edge| edge.unwrap_or(0))
            .map_err(|error| error.to_string())
    }

    /// The assistant text a followed target has produced since `cursor`, carrying
    /// the context each event needs to become a route fact.
    async fn followed_events(
        &self,
        target: &FollowTarget,
        cursor: i64,
    ) -> Result<Vec<FollowedEvent>, String> {
        let source = self.follow_source(target).await?;
        if source.job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let db = self.orch.db.for_project(target.project()).await;
        let sql = followed_events_sql(source.job_ids.len());
        let mut binds: Vec<Value> = source
            .job_ids
            .iter()
            .cloned()
            .map(Value::Text)
            .collect::<Vec<_>>();
        binds.push(Value::Integer(cursor));
        let FollowSource {
            context,
            project_id,
            ..
        } = source;
        db.query_all(sql, binds, move |row| {
            Ok(FollowedEvent {
                rowid: row.i64(0)?,
                data: row.text(1)?,
                context: context.clone(),
                job_id: row.text(2)?,
                project_id: project_id.clone(),
            })
        })
        .await
        .map_err(|error| error.to_string())
    }

    /// Deliver operator text into a target's session. A thread takes the same
    /// append the desktop composer takes — the message becomes visible in the
    /// thread and its session is steered, warm or cold. An issue node takes the
    /// ordinary queue-at-send job path.
    async fn route_to_target(&self, target: &FollowTarget, text: &str) -> Result<(), String> {
        let db = self.orch.db.for_project(target.project()).await;
        let sender_name = channel_sender_name(self.provider_id);
        match target {
            FollowTarget::Thread { project, name } => {
                let thread_id = db
                    .query_opt(
                        "SELECT t.id FROM threads t JOIN projects p ON p.id = t.project_id WHERE LOWER(p.key) = ?1 AND t.name = ?2",
                        params![cairn_common::uri::canonical_project(project), name.clone()],
                        |row| row.text(0),
                    )
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("{project} has no thread named {name}"))?;
                crate::messages::delivery::append_thread_message(
                    &self.orch,
                    &db,
                    &thread_id,
                    None,
                    &sender_name,
                    text,
                )
                .await
                .map(|_| ())
            }
            FollowTarget::Issue { project, number } => {
                let job_id = db.query_opt(
                    "SELECT j.id FROM jobs j JOIN runs r ON r.job_id = j.id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE LOWER(p.key) = ?1 AND i.number = ?2 AND j.parent_job_id IS NULL ORDER BY r.created_at DESC LIMIT 1",
                    params![cairn_common::uri::canonical_project(project), *number],
                    |row| row.text(0),
                ).await.map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("{} has no addressable node", target.uri()))?;
                let attributed = format!("[Direct message from {sender_name}] {text}");
                crate::execution::jobs::continue_job_or_enqueue(
                    &self.orch,
                    &job_id,
                    Some(&attributed),
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
        let multi_select = self.question_is_multi_select(record).await?;
        cleanup_resolved_question(
            self.ledger(),
            self.provider.as_ref(),
            record,
            receipt,
            multi_select,
        )
        .await?;
        Ok(())
    }

    /// A gate disappearing from a routed snapshot is only a hint. Confirm the
    /// owning domain row resolved before converting owed delivery into `expired`.
    async fn gate_is_resolved(&self, gate: &Gate) -> Result<bool, String> {
        for db in self.orch.db.all_dbs().await {
            let resolved = match gate.kind {
                "permission" => db
                    .query_opt_i64(
                        "SELECT status != 'pending' FROM permission_requests WHERE id = ?1",
                        (gate.binding_ref.clone(),),
                    )
                    .await
                    .map_err(|error| error.to_string())?,
                "question" => {
                    let prompt_id = gate
                        .binding_ref
                        .rsplit_once(':')
                        .map(|(prompt, _)| prompt)
                        .unwrap_or(&gate.binding_ref);
                    db.query_opt_i64(
                        "SELECT response IS NOT NULL FROM prompts WHERE id = ?1",
                        (prompt_id.to_string(),),
                    )
                    .await
                    .map_err(|error| error.to_string())?
                }
                "review" => db
                    .query_opt_i64(
                        "SELECT retired_at IS NOT NULL OR delivered_event_id IS NOT NULL FROM attention_pushes WHERE key = ?1",
                        (gate.binding_ref.clone(),),
                    )
                    .await
                    .map_err(|error| error.to_string())?,
                _ => None,
            };
            if let Some(resolved) = resolved {
                return Ok(resolved != 0);
            }
        }
        Ok(false)
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
            if seal_gate_for_provider(
                &self.claims,
                self.ledger(),
                self.provider_id,
                gate.conversation.as_deref().unwrap_or(&self.destination),
                self.rendering_for(&gate.ask),
                &gate,
            )
            .await?
            {
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
            gate.conversation.as_deref().unwrap_or(&self.destination),
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
        if let Err(error) = self.deliver_pending_agent_messages().await {
            log::warn!("channel agent message sweep failed: {error}");
        }
        if let Err(error) = self.sweep_followed_updates().await {
            log::warn!("channel followed-target update sweep failed: {error}");
        }
        if let Err(error) = self.sweep_pending_domain_actions().await {
            log::warn!("channel ask domain-action sweep failed: {error}");
        }
        if let Err(error) = self.sweep_pending_cleanup().await {
            log::warn!("channel ask cleanup sweep failed: {error}");
        }
    }

    async fn sweep_pending_domain_actions(&self) -> Result<(), String> {
        const ACTION_LEASE_MS: i64 = 60_000;
        for (action_ref, kind) in ledger::pending_ask_actions(self.ledger()).await? {
            let answers = match ledger::answers_for_action(self.ledger(), &action_ref).await {
                Ok(answers) => answers,
                Err(error) => {
                    log::warn!("channel could not load pending action {action_ref}: {error}");
                    continue;
                }
            };
            if kind == "question" && answers.len() != 1 {
                continue;
            }
            let now = chrono::Utc::now().timestamp_millis();
            let Some(lease) =
                ledger::try_lease_ask_action(self.ledger(), &action_ref, now, ACTION_LEASE_MS)
                    .await?
            else {
                continue;
            };

            // Read before the effect runs: parking a refusal withdraws the
            // winning answer, and the conversation that sent it still has to be
            // told what became of it.
            let winner = ledger::resolution_for_action(self.ledger(), &action_ref)
                .await?
                .ok_or_else(|| format!("{kind} action has no provenance: {action_ref}"))?;

            let result = if kind == "question" {
                let response = answers[0].1.clone();
                crate::mcp::handlers::planning::answer_prompt_id_domain(
                    &self.orch,
                    &action_ref,
                    response,
                    winner.resolution_provenance()?,
                )
                .await
                .map(|_| ())
            } else {
                let answer = answers
                    .first()
                    .map(|(_, answer)| answer.as_str())
                    .ok_or_else(|| format!("permission action has no answer: {action_ref}"));
                match answer.and_then(parse_permission) {
                    Ok(decision) => {
                        let recovered =
                            crate::mcp::handlers::permission::recovered_permission_answer(
                                decision, &winner,
                            )?;
                        crate::mcp::handlers::permission::resolve_permission_request_domain(
                            &self.orch,
                            &action_ref,
                            recovered,
                        )
                        .await
                        .map(|_| ())
                    }
                    Err(error) => Err(error),
                }
            };
            match result {
                Ok(()) => {
                    let receipt_answer = answers
                        .first()
                        .map(|(_, answer)| answer.as_str())
                        .unwrap_or("answered");
                    ledger::finalize_ask_resolution(
                        self.ledger(),
                        &action_ref,
                        &format!("✓ answered: {receipt_answer}"),
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                }
                // What a failure means is the ledger's to say. A refusal the
                // domain will give again is gated on an operator acting rather
                // than on time: re-attempting it re-derives the same refusal
                // from the same stored answer, which is how four permanently
                // unapprovable actions produced ~112,000 'will retry' lines in a
                // day (CAIRN-4364). One line at park time, then silence until a
                // new answer claims the vacated gate.
                Err(error) => {
                    match ledger::record_ask_action_failure(
                        self.ledger(),
                        &lease,
                        &error,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?
                    {
                        ledger::AskActionFailure::Parked => {
                            log::warn!(
                                "channel domain action {action_ref} is parked for the operator: {error}"
                            );
                            self.report_operator_pending(&winner, &error).await;
                        }
                        ledger::AskActionFailure::Retryable => {
                            log::warn!("channel domain action {action_ref} will retry: {error}");
                        }
                        // Another worker owns this attempt's outcome; saying
                        // anything here would be a second voice on one event.
                        ledger::AskActionFailure::Stale => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Tells the conversation that answered a gate that its answer was withdrawn
    /// and the request is waiting for an operator.
    ///
    /// Parking is otherwise silent, and a channel reply is the surface that would
    /// learn nothing: the ledger accepted the answer, and only the domain knew it
    /// could not be applied. A sweep running on a different provider says
    /// nothing rather than addressing a conversation it does not own.
    async fn report_operator_pending(&self, winner: &ledger::AskResolution, refusal: &str) {
        if winner.winner_provider != self.provider_id
            || winner.winner_conversation.trim().is_empty()
        {
            return;
        }
        if let Err(error) = self.send_notice(&winner.winner_conversation, refusal).await {
            log::warn!(
                "channel could not report the operator-pending refusal for {}: {error}",
                winner.action_ref
            );
        }
    }

    async fn sweep_pending_cleanup(&self) -> Result<(), String> {
        for record in ledger::list_cleanup_pending(self.ledger(), self.provider_id).await? {
            let receipt = record
                .options_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                .and_then(|value| {
                    value
                        .get("receipt")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| "✓ answered in another conversation".to_string());
            let multi_select = self.question_is_multi_select(&record).await?;
            match cleanup_claimed_question(
                self.ledger(),
                self.provider.as_ref(),
                &record,
                &receipt,
                multi_select,
            )
            .await
            {
                Ok(()) => {
                    ledger::acknowledge_cleanup(self.ledger(), &record.id).await?;
                }
                Err(error) => {
                    ledger::record_cleanup_failure(self.ledger(), &record.id, &error).await?;
                    log::warn!("channel cleanup deferred for {}: {error}", record.id);
                }
            }
        }
        Ok(())
    }

    async fn sweep_followed_updates(&self) -> Result<(), String> {
        let follows = ledger::list_follows(self.ledger(), self.provider_id).await?;
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
                    let actor_job_id = event.job_id.clone();
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
                        identity: detail_uri.clone(),
                        summary: fields
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        fields,
                        origin: Some(crate::routes::installation_machine_origin(&self.orch)?),
                        route_provenance: None,
                    };
                    // One local insert, then on to the next event: the sweep
                    // never waits on a route. Every following provider's sweep
                    // mints the same utterance; the log keeps one row, and the
                    // route lane fans its stream out to every follower.
                    if let Err(error) = crate::routes::mint(
                        &self.orch,
                        crate::routes::Mint {
                            fact,
                            record_ref: detail_uri,
                            record_id: Some(event.rowid.to_string()),
                            project_id: Some(&event.project_id),
                            presence: crate::routes::log::operator_presence().await,
                            // The thread that said this is its actor, so its own
                            // standing triggers never wake it on its own words.
                            actor_job_id: Some(&actor_job_id),
                        },
                    )
                    .await
                    {
                        log::warn!(
                            "could not mint followed {} event {}: {error}",
                            follow.uri,
                            event.rowid
                        );
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
        let mut live_gates = Vec::new();
        let binding_generation = ledger::binding_generation(self.ledger()).await?;
        for db in self.orch.db.all_dbs().await {
            match self.load_routed_gates(&db, &binding_generation).await {
                Ok((mut db_gates, mut db_live)) => {
                    gates.append(&mut db_gates);
                    live_gates.append(&mut db_live);
                }
                Err(error) => {
                    snapshot_complete = false;
                    log::warn!("channel skipped one project database during gate sweep: {error}");
                }
            }
        }
        let live: HashSet<_> = live_gates
            .iter()
            .map(|gate| gate.delivery_key(self.provider_id, &self.destination))
            .collect();
        // Held and pending routes are presence-aware work outside `gates`, so
        // they need the operator's real presence as much as a live ask does.
        let route_work = !self
            .held_routes
            .lock()
            .expect("held route set poisoned")
            .is_empty()
            || ledger::has_pending_route(self.ledger(), self.provider_id).await?;
        let presence = if route_work || gates.iter().any(Gate::is_presence_aware) {
            super::operator_presence(Some(self.provider.as_ref())).await
        } else {
            OperatorPresence::Away
        };
        let now = Instant::now();
        self.retry_held_routes(presence, now).await;
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
        for attention in cancelled {
            if self.gate_is_resolved(&attention.gate).await? {
                ledger::mark_expired(
                    self.ledger(),
                    &attention.id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            } else if delivered < SWEEP_LIMIT
                && self
                    .send_claimed(attention.id, attention.gate, false)
                    .await?
            {
                delivered += 1;
            }
        }
        if snapshot_complete {
            for record in ledger::list_unresolved(self.ledger(), self.provider_id).await? {
                if record.kind == "question"
                    && record.status == "sent"
                    && !record.binding_ref.starts_with(FOLLOW_POLL_PREFIX)
                    && !live.contains(&record.binding_ref)
                {
                    self.finish_question(&record, "✓ question closed in Cairn")
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Turn each gate into the renderings this provider will actually deliver.
    ///
    /// This is where an ask held by a live agent watcher is withheld from the
    /// operator (CAIRN-4732), because this is the only place that knows whether
    /// a rendering is going to an ATTENTION surface -- the operator's pocket --
    /// or to a STRUCTURAL one. The distinction is the whole rule: an issue's own
    /// Discord thread is the ask's place and keeps carrying it fully answerable,
    /// while a single-conversation provider's default destination and a `follow`
    /// binding are the operator's pocket and do not.
    ///
    /// Withholding never strands an ask. It stays answerable in-app, its
    /// watcher's push is the delivery, and if that watcher dies or the operator
    /// mutes the child the very next sweep finds it unheld and sends it.
    async fn route_gates(&self, db: &LocalDb, gates: Vec<Gate>) -> Result<Vec<Gate>, String> {
        // By ASK, not by rendering. One prompt carrying three questions produces
        // three gates under one key, and that is one thing being held from the
        // operator, not three.
        let mut held: HashMap<&str, i64> = HashMap::new();
        for gate in gates.iter().filter(|gate| gate.held_by_agent()) {
            let (Some(key), Some(since)) = (gate.ask_key.as_deref(), gate.held_since) else {
                continue;
            };
            held.entry(key)
                .and_modify(|held_since| *held_since = (*held_since).min(since))
                .or_insert(since);
        }
        super::set_held_asks(
            self.provider_id,
            held.len() as i64,
            held.values().copied().min(),
        );

        let mut routed = Vec::new();
        for gate in gates {
            let Some(target_uri) = gate.target_uri.as_deref() else {
                // No target means the provider's default conversation, which IS
                // the operator's pocket.
                if !gate.held_by_agent() {
                    routed.push(gate);
                }
                continue;
            };
            let bindings = ledger::list_eligible_bindings(
                self.ledger(),
                self.provider_id,
                target_uri,
                gate.message_class(),
            )
            .await?;
            if bindings.is_empty() {
                if self.provider_id == "discord" {
                    self.ensure_discord_surface(db, target_uri).await?;
                    // Surface reconciliation installs the structural binding. Until
                    // then this event remains unclaimed and is retried by the next
                    // sweep; a guild ID is never a deliverable conversation.
                    continue;
                }
                if !gate.held_by_agent() {
                    routed.push(gate);
                }
                continue;
            }
            routed.extend(
                bindings
                    .into_iter()
                    .filter(|binding| {
                        // A structural surface is the ask's own home and keeps
                        // carrying it; a `follow` binding is the operator's
                        // pocket reached by another route. Deleting this one
                        // term is all it takes to quiet Discord issue threads
                        // too, if the operator ever wants that.
                        !gate.held_by_agent()
                            || binding.binding_kind
                                == super::bindings::BindingKind::Structural.as_str()
                    })
                    .map(|binding| {
                        let mut rendering = gate.clone();
                        rendering.conversation = Some(binding.conversation);
                        rendering
                    }),
            );
        }
        Ok(routed)
    }

    /// Returns `(work, live)`: `work` contains gates whose database changed and
    /// need routing now; `live` is the complete cached identity set used for cleanup.
    /// LocalDb's monotonic mutation generation makes the safety tick query-free at
    /// idle while preserving a full live set for fencing and resolution cleanup.
    async fn load_routed_gates(
        &self,
        db: &LocalDb,
        binding_generation: &str,
    ) -> Result<(Vec<Gate>, Vec<Gate>), String> {
        let path = db.path().to_path_buf();
        let generation = db.mutation_generation();
        if let Some(snapshot) = self
            .gate_snapshots
            .lock()
            .expect("gate snapshot cache poisoned")
            .get(&path)
            .filter(|snapshot| {
                snapshot.initialized
                    && snapshot.generation == generation
                    && snapshot.binding_generation == binding_generation
            })
        {
            let deferred = self
                .deferred_attention
                .lock()
                .expect("deferred attention set poisoned");
            let work = snapshot
                .routed
                .iter()
                .filter(|gate| {
                    let key = gate.delivery_key(self.provider_id, &self.destination);
                    !self.claims.holds(&key) || deferred.contains_key(&key)
                })
                .cloned()
                .collect();
            return Ok((work, snapshot.routed.clone()));
        }

        // Capture generation before querying and store that exact value. A write
        // racing this snapshot advances the database generation beyond the stored
        // value and therefore forces one follow-up refresh instead of being lost.
        // A shared source computed a moment earlier carries an older generation,
        // and taking the OLDER of the two is what keeps that reuse from pinning
        // this router to a snapshot it would otherwise have refreshed.
        let started = Instant::now();
        let mut kinds = Vec::new();
        if self.route.question {
            kinds.push(GateKind::Question);
        }
        if self.route.permission {
            kinds.push(GateKind::Permission);
        }
        if self.route.notify {
            kinds.push(GateKind::Review);
        }
        let mut gates = Vec::new();
        let mut review_pushes = 0;
        let mut review_backlog = 0;
        let mut expired = 0;
        let mut generation = generation;
        for kind in kinds {
            let snapshot = shared_gates(db, kind).await?;
            generation = generation.min(snapshot.generation);
            review_backlog += snapshot.backlog;
            expired += snapshot.expired;
            if kind == GateKind::Review {
                review_pushes = snapshot.gates.len();
            }
            gates.extend(snapshot.gates.iter().cloned());
        }
        let routed = self.route_gates(db, gates).await?;
        // `backlog` is the undelivered review rows this refresh read. It is here
        // next to the duration because the pair is the diagnostic: this refresh
        // costs what the BACKLOG costs, and a duration that grows while the
        // backlog does not means it has started paying for the workspace again.
        log::info!(
            "channel gate refresh reason=generation dbs=1 backlog={} pushes={} gates={} expired={} duration_ms={}",
            review_backlog,
            review_pushes,
            routed.len(),
            expired,
            started.elapsed().as_millis()
        );
        self.gate_snapshots
            .lock()
            .expect("gate snapshot cache poisoned")
            .insert(
                path,
                GateSnapshot {
                    generation,
                    binding_generation: binding_generation.to_string(),
                    initialized: true,
                    routed: routed.clone(),
                },
            );
        Ok((routed.clone(), routed))
    }

    /// Ensure the structural Discord surface for an issue or thread target exists
    /// or is being created. False when the target has none to give: it is not an
    /// issue or thread, is unknown, or has finished.
    async fn ensure_discord_surface(&self, db: &LocalDb, target_uri: &str) -> Result<bool, String> {
        match parse_uri(target_uri) {
            Some(CairnResource::Issue { project, number }) => {
                if !self
                    .ensure_discord_issue_surface(db, target_uri, project, number)
                    .await?
                {
                    return Ok(false);
                }
            }
            Some(CairnResource::Thread {
                project,
                name,
                path,
            }) if path.is_empty() => {
                let guild_id = self.discord_guild()?;
                let active = db
                    .query_opt_i64(
                        "SELECT 1 FROM threads t JOIN projects p ON p.id = t.project_id
                         WHERE upper(p.key) = upper(?1) AND t.name = ?2 AND t.status = 'active'",
                        (project.clone(), name),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                if active.is_none() {
                    return Ok(false);
                }
                super::discord_surfaces::ensure_thread_surface(
                    db,
                    guild_id,
                    &project,
                    target_uri,
                    chrono::Utc::now().timestamp(),
                )
                .await?;
            }
            _ => return Ok(false),
        }
        super::wake_discord_surfaces();
        Ok(true)
    }

    async fn ensure_discord_issue_surface(
        &self,
        db: &LocalDb,
        target_uri: &str,
        project: String,
        number: i32,
    ) -> Result<bool, String> {
        let guild_id = self.discord_guild()?;
        let details = db
            .query_opt(
                "SELECT i.status, t.name FROM issues i
                 JOIN projects p ON p.id = i.project_id
                 LEFT JOIN threads t ON t.id = i.parent_thread_id
                 WHERE upper(p.key) = upper(?1) AND i.number = ?2",
                (project.clone(), number),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        let Some((status, parent_thread)) = details else {
            return Ok(false);
        };
        if matches!(status.as_str(), "complete" | "failed" | "merged" | "closed") {
            return Ok(false);
        }
        let parent_target = parent_thread
            .as_deref()
            .map(|name| format!("cairn://p/{project}/{name}"));
        super::discord_surfaces::ensure_issue_surface(
            db,
            guild_id,
            &project,
            target_uri,
            parent_target.as_deref(),
            chrono::Utc::now().timestamp(),
        )
        .await?;
        Ok(true)
    }

    async fn deliver_or_defer(
        &self,
        gate: Gate,
        presence: OperatorPresence,
        now: Instant,
    ) -> Result<bool, String> {
        let delivery_key = gate.delivery_key(self.provider_id, &self.destination);
        let deferred = self
            .deferred_attention
            .lock()
            .expect("deferred attention set poisoned")
            .remove(&delivery_key);
        if let Some(deferred) = deferred {
            if attention_timing(presence, now, deferred.deadline) == AttentionTiming::Defer {
                self.deferred_attention
                    .lock()
                    .expect("deferred attention set poisoned")
                    .insert(delivery_key.clone(), deferred);
                return Ok(false);
            }
            return self.send_claimed(deferred.id, deferred.gate, false).await;
        }
        let conversation = gate.conversation.as_deref().unwrap_or(&self.destination);
        if let Some(record) = ledger::get_by_conversation_binding(
            self.ledger(),
            self.provider_id,
            conversation,
            gate.kind,
            &gate.binding_ref,
        )
        .await?
        {
            return match record.status.as_str() {
                "pending" => {
                    if gate.is_presence_aware() && presence == OperatorPresence::Present {
                        let age_ms = chrono::Utc::now()
                            .timestamp_millis()
                            .saturating_sub(record.created_at)
                            as u64;
                        if let Some(remaining) =
                            ATTENTION_GRACE.checked_sub(Duration::from_millis(age_ms))
                        {
                            self.deferred_attention
                                .lock()
                                .expect("deferred attention set poisoned")
                                .insert(
                                    delivery_key,
                                    DeferredAttention {
                                        id: record.id,
                                        gate,
                                        deadline: now + remaining,
                                    },
                                );
                            return Ok(false);
                        }
                    }
                    self.send_claimed(record.id, gate, false).await
                }
                "failed"
                    if record.attempts < MAX_ATTEMPTS
                        && record
                            .next_attempt_at
                            .is_some_and(|at| at <= chrono::Utc::now().timestamp_millis()) =>
                {
                    self.send_claimed(record.id, gate, true).await
                }
                "failed" => Ok(false),
                "expired" => Ok(true),
                "sent" | "resolved" => Ok(false),
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
                    delivery_key,
                    DeferredAttention {
                        id,
                        gate,
                        deadline: now + ATTENTION_GRACE,
                    },
                );
            return Ok(false);
        }
        self.send_claimed(id, gate, false).await
    }

    /// Reports whether this call put something on the wire, so a sweep's batch
    /// bounds sends rather than gates examined.
    async fn send_claimed(&self, id: String, gate: Gate, retry: bool) -> Result<bool, String> {
        // An outage spends nothing: an unavailable provider is never called, so a
        // message cannot exhaust its attempts unseen while the provider is down.
        // A degraded provider is running and is called, and its failures spend.
        if matches!(
            self.provider.health(),
            super::ChannelHealth::Unavailable { .. }
        ) {
            return Ok(false);
        }
        let now = chrono::Utc::now().timestamp_millis();
        let claimed = if retry {
            ledger::begin_retry_delivery(self.ledger(), &id, now, MAX_ATTEMPTS).await?
        } else {
            ledger::begin_delivery(self.ledger(), &id, now).await?
        };
        if !claimed {
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
            conversation: gate
                .conversation
                .clone()
                .unwrap_or_else(|| self.destination.clone()),
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
                let current = ledger::get_by_id(self.ledger(), &id)
                    .await?
                    .ok_or_else(|| format!("delivery intent disappeared: {id}"))?;
                let attempt = current.attempts + 1;
                let shift = attempt.saturating_sub(1).clamp(0, 16) as u32;
                let backoff = RETRY_BASE
                    .checked_mul(1u32 << shift)
                    .unwrap_or(RETRY_CAP)
                    .min(RETRY_CAP);
                let failed_at = chrono::Utc::now().timestamp_millis();
                ledger::mark_failed(
                    self.ledger(),
                    &id,
                    &error,
                    failed_at,
                    Some(failed_at + backoff.as_millis() as i64),
                )
                .await?;
                if attempt >= MAX_ATTEMPTS {
                    log::warn!(
                        "channel delivery terminally failed kind={} binding_ref={} conversation={} attempts={attempt}: {error}",
                        gate.kind,
                        gate.binding_ref,
                        message.conversation,
                    );
                }
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn handle_inbound(&self, event: InboundEvent) -> Result<(), String> {
        let conversation = match &event {
            InboundEvent::Selection { conversation, .. }
            | InboundEvent::Selections { conversation, .. }
            | InboundEvent::Reply { conversation, .. }
            | InboundEvent::Bare { conversation, .. }
            | InboundEvent::Rejected { conversation, .. } => conversation.clone(),
        };
        if matches!(event, InboundEvent::Rejected { .. }) {
            let (sender, text) = inbound_sender_text(&event);
            return self
                .store_rejected(None, sender, text, "allowlist", false)
                .await;
        }
        let (guid, sender, text) = inbound_parts(&event);
        let bound = if let Some(guid) = guid {
            ledger::get_by_provider_guid(self.ledger(), self.provider_id, guid).await?
        } else {
            None
        };
        let capability = if bound
            .as_ref()
            .is_some_and(|record| record.kind == "permission")
        {
            InboundCapability::Permissions
        } else if bound.as_ref().is_some_and(|record| {
            record.binding_ref.starts_with(FOLLOW_POLL_PREFIX)
                || matches!(record.kind.as_str(), "question" | "review")
        }) || channel_command(text).is_some()
            || poll_command(text).is_some()
            || starts_with_known_slash_command(text)
        {
            InboundCapability::Answers
        } else if guid.is_none() {
            let unresolved = ledger::list_unresolved(self.ledger(), self.provider_id)
                .await?
                .into_iter()
                .filter(|record| {
                    record.status == "sent"
                        && super::address::normalize_imessage_handle(&record.conversation)
                            == super::address::normalize_imessage_handle(sender)
                })
                .collect::<Vec<_>>();
            if unresolved.len() == 1 {
                if unresolved[0].kind == "permission" {
                    InboundCapability::Permissions
                } else {
                    InboundCapability::Answers
                }
            } else {
                InboundCapability::FreeText
            }
        } else {
            InboundCapability::FreeText
        };
        if !admits_capability(self.inbound_capabilities, capability) {
            self.store_rejected(guid, sender, text, "policy", false)
                .await?;
            let notice = match capability {
                InboundCapability::Permissions => {
                    "Permission answers are disabled for this channel."
                }
                InboundCapability::Answers => {
                    "Answers and channel controls are disabled for this channel."
                }
                InboundCapability::FreeText => "Free-text messages are disabled for this channel.",
            };
            return self.send_notice(&conversation, notice).await;
        }
        if self.provider_id == "discord" {
            let target_uri =
                ledger::lookup_conversation_target(self.ledger(), self.provider_id, &conversation)
                    .await?;
            match &event {
                InboundEvent::Bare { sender, text, .. } => {
                    let Some(target_uri) = target_uri else {
                        self.store_rejected(None, sender, text, "unbound_conversation", false)
                            .await?;
                        return self
                            .send_notice(
                                &conversation,
                                "This Discord channel is not bound to a Cairn conversation.",
                            )
                            .await;
                    };
                    let target = self.resolve_target(&target_uri).await?;
                    return self.route_to_target(&target, text).await;
                }
                InboundEvent::Reply {
                    bound_guid,
                    sender,
                    text,
                    ..
                } if ledger::get_by_provider_guid(self.ledger(), self.provider_id, bound_guid)
                    .await?
                    .is_none() =>
                {
                    let Some(target_uri) = target_uri else {
                        self.store_rejected(
                            Some(bound_guid),
                            sender,
                            text,
                            "unbound_conversation",
                            false,
                        )
                        .await?;
                        return self
                            .send_notice(
                                &conversation,
                                "This Discord channel is not bound to a Cairn conversation.",
                            )
                            .await;
                    };
                    let target = self.resolve_target(&target_uri).await?;
                    return self.route_to_target(&target, text).await;
                }
                _ => {}
            }
        }
        match event {
            InboundEvent::Selection {
                bound_guid,
                sender,
                option_text,
                selected,
                ..
            } => {
                self.resolve_selection(&bound_guid, &conversation, &sender, &option_text, selected)
                    .await
            }
            InboundEvent::Selections {
                bound_guid,
                sender,
                changes,
                ..
            } => {
                for change in changes {
                    self.resolve_selection(
                        &bound_guid,
                        &conversation,
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
                ..
            } => {
                self.resolve_bound(&bound_guid, &conversation, &sender, &text)
                    .await
            }
            InboundEvent::Bare { sender, text, .. } => {
                self.resolve_bare(&conversation, &sender, &text).await
            }
            InboundEvent::Rejected { .. } => {
                unreachable!("rejected inbound handled by policy gate")
            }
        }
    }

    async fn resolve_bound(
        &self,
        guid: &str,
        conversation: &str,
        sender: &str,
        text: &str,
    ) -> Result<(), String> {
        if text.trim_start().starts_with('/') {
            return self.resolve_bare(conversation, sender, text).await;
        }
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
                        return self
                            .send_notice(conversation, "That follow was not found.")
                            .await;
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
                    self.send_notice(conversation, &confirmation).await?;
                    return Ok(());
                }
            }
            if record.kind == "route" {
                let target_uri = route_binding_target(&record.binding_ref).ok_or_else(|| {
                    format!("route reply has no target binding: {}", record.binding_ref)
                })?;
                let target = self.resolve_target(target_uri).await?;
                return self.route_to_target(&target, text).await;
            }
            // A reply to an agent's message goes back to whoever spoke. A thread
            // (or a thread's task) answers as its thread; an issue node or task
            // has no transcript of its own to receive into, so the reply reaches
            // the issue's root job via `route_to_target` -- a reply to a
            // sub-node's message lands on its issue, by design.
            if record.kind == agent_message::KIND {
                let target_uri = agent_message::sender_home(&record.binding_ref)
                    .and_then(routed_gate_target)
                    .ok_or_else(|| {
                        format!("message reply has no sender home: {}", record.binding_ref)
                    })?;
                let target = self.resolve_target(&target_uri).await?;
                return self.route_to_target(&target, text).await;
            }
            return self.resolve_record(record, sender, text).await;
        }
        if poll_command(text).is_some() || text.trim_start().starts_with('/') {
            return self.resolve_bare(conversation, sender, text).await;
        }
        self.store_unsolicited(Some(guid), conversation, sender, text)
            .await
    }

    async fn resolve_selection(
        &self,
        guid: &str,
        conversation: &str,
        sender: &str,
        text: &str,
        selected: bool,
    ) -> Result<(), String> {
        let Some(record) =
            ledger::get_by_provider_guid(self.ledger(), self.provider_id, guid).await?
        else {
            return self
                .store_unsolicited(Some(guid), conversation, sender, text)
                .await;
        };
        if record.binding_ref.starts_with(FOLLOW_POLL_PREFIX) {
            let options = follow_poll_options(&record)
                .ok_or_else(|| "follow poll has no option bindings".to_string())?;
            if let Some(uri) = options.bindings.get(text) {
                if selected {
                    self.follow_or_focus(uri, &record.conversation).await?;
                } else {
                    self.unfollow(uri).await?;
                    let target = FollowTarget::parse(uri)?;
                    self.send_notice(
                        &record.conversation,
                        &format!("Unfollowed {}.", target.selector()),
                    )
                    .await?;
                }
            }
            return Ok(());
        }
        if selected {
            self.resolve_record(record, sender, text).await
        } else {
            Ok(())
        }
    }

    async fn resolve_bare(
        &self,
        conversation: &str,
        sender: &str,
        text: &str,
    ) -> Result<(), String> {
        if let Some(kind) = poll_command(text) {
            if let Err(error) = self.send_follow_poll(conversation, kind).await {
                log::warn!("channel could not answer the {kind:?} command: {error}");
                self.send_notice(conversation, &format!("{}: {error}", kind.failure_prefix()))
                    .await?;
            }
            return Ok(());
        }
        if let Some(selector) = unfollow_selector(text) {
            return match selector {
                Some(selector) => {
                    self.unfollow_selector_command(&selector, conversation)
                        .await
                }
                None => {
                    let Some(focused) = ledger::get_focus(self.ledger(), self.provider_id).await?
                    else {
                        return self
                            .send_notice(conversation, "There is no focused follow to unfollow.")
                            .await;
                    };
                    self.unfollow_uri_command(&focused, conversation).await
                }
            };
        }
        if let Some(selector) = focus_selector(text) {
            return self.focus_selector_command(&selector, conversation).await;
        }
        if matches!(channel_command(text), Some(ChannelCommand::Help)) {
            return self.send_notice(conversation, "Commands: /threads, /issues, /focus <name>, /unfollow [name], /help. Without a name, /unfollow stops the current focus. Reply to a pushed update to message that thread directly.").await;
        }
        if starts_with_known_slash_command(text) {
            return self
                .send_notice(
                    conversation,
                    "Invalid command usage. Use /help for channel commands.",
                )
                .await;
        }
        let mut matches = ledger::list_unresolved(self.ledger(), self.provider_id)
            .await?
            .into_iter()
            .filter(|record| {
                record.status == "sent"
                    && super::address::normalize_imessage_handle(&record.conversation)
                        == super::address::normalize_imessage_handle(sender)
            })
            .collect::<Vec<_>>();
        if matches.len() == 1 {
            let record = matches.pop().expect("one match");
            return self.resolve_record(record, sender, text).await;
        }
        if matches.len() > 1 {
            return self.send_notice(conversation, "I found more than one active ask. Please reply to the specific message you want to answer.").await;
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
        self.send_notice(
            conversation,
            "Unknown command. Use /help for channel commands.",
        )
        .await
    }

    async fn resolve_record(
        &self,
        record: ledger::OutboundRecord,
        sender: &str,
        text: &str,
    ) -> Result<(), String> {
        if record.status == "resolved" {
            return Ok(());
        }
        match record.kind.as_str() {
            "question" => {
                if record.binding_ref.starts_with(FOLLOW_POLL_PREFIX) {
                    let selected_label = follow_poll_answer(&record, text);
                    return self
                        .follow_poll_selection(&record, &selected_label, &record.conversation)
                        .await;
                }
                let answer = question_answer(&record, text);
                let (prompt_id, _) = record
                    .binding_ref
                    .rsplit_once(':')
                    .ok_or_else(|| format!("invalid question binding: {}", record.binding_ref))?;
                if let Some(winner) =
                    ledger::resolution_for_action(self.ledger(), prompt_id).await?
                {
                    self.send_notice(
                        &record.conversation,
                        &format!(
                            "Already answered: {} via {}",
                            winner.answer, winner.winner_surface
                        ),
                    )
                    .await?;
                    return Ok(());
                }
                ledger::claim_question_answer(
                    self.ledger(),
                    &record.id,
                    &answer,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                let answers = ledger::staged_answers_for_prompt(
                    self.ledger(),
                    self.provider_id,
                    &record.conversation,
                    prompt_id,
                )
                .await?;
                if answers.len() != prompt_question_count(&self.orch, prompt_id).await? {
                    return Ok(());
                }
                let response = if answers.len() == 1 {
                    answers[0].1.clone()
                } else {
                    answers
                        .iter()
                        .map(|(index, answer)| format!("Question {}: {}", index + 1, answer))
                        .collect::<Vec<_>>()
                        .join("\n")
                };
                let claim = ledger::claim_ask_resolution(
                    self.ledger(),
                    prompt_id,
                    &response,
                    cairn_common::identity::AppearanceTransport::ChannelReply,
                    Some(self.provider_id),
                    Some(&record.conversation),
                    Some(sender),
                    "question",
                    prompt_id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                self.sweep_pending_domain_actions().await?;
                self.sweep_pending_cleanup().await?;
                if let ledger::AskClaim::Existing(winner) = claim {
                    self.send_notice(
                        &record.conversation,
                        &format!(
                            "Already answered: {} via {}",
                            winner.answer, winner.winner_surface
                        ),
                    )
                    .await?;
                }
            }
            "permission" => {
                let answer = permission_answer_token(parse_permission(text)?);
                let claim = ledger::claim_ask_resolution(
                    self.ledger(),
                    &record.binding_ref,
                    answer,
                    cairn_common::identity::AppearanceTransport::ChannelReply,
                    Some(self.provider_id),
                    Some(&record.conversation),
                    Some(sender),
                    "permission",
                    &record.binding_ref,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                self.sweep_pending_domain_actions().await?;
                self.sweep_pending_cleanup().await?;
                if let ledger::AskClaim::Existing(winner) = claim {
                    self.send_notice(
                        &record.conversation,
                        &format!(
                            "Already answered: {} via {}",
                            winner.answer, winner.winner_surface
                        ),
                    )
                    .await?;
                }
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

    async fn store_rejected(
        &self,
        guid: Option<&str>,
        sender: &str,
        text: &str,
        reason: &str,
        acknowledged: bool,
    ) -> Result<(), String> {
        let db = &self.orch.db.local;
        let id = Uuid::new_v4().to_string();
        ledger::insert_inbound(
            db,
            &ledger::InboundRecord {
                id,
                channel: self.provider_id.into(),
                provider_guid: guid.map(str::to_string),
                sender: sender.into(),
                text: text.into(),
                received_at: chrono::Utc::now().timestamp_millis(),
                rejection_reason: Some(reason.into()),
                acknowledged_at: acknowledged.then(|| chrono::Utc::now().timestamp_millis()),
            },
        )
        .await?;
        let _ = self.orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table":"channel_inbound","action":"insert"}),
        );
        Ok(())
    }

    async fn store_unsolicited(
        &self,
        guid: Option<&str>,
        conversation: &str,
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
                rejection_reason: Some("unmatched".into()),
                acknowledged_at: None,
            },
        )
        .await?;
        self.send_notice(
            conversation,
            "No active ask — your message is visible in Cairn.",
        )
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
    route: MessageClassPolicy,
    inbound_capabilities: ChannelInboundCapabilities,
    boundary: SessionBoundary,
) -> Vec<tokio::task::JoinHandle<()>> {
    let router = Arc::new(ChannelRouter::new_for_provider(
        orch,
        provider.clone(),
        provider_id,
        destination,
        route,
        inbound_capabilities,
    ));
    let (route_tx, mut route_rx) = tokio::sync::mpsc::unbounded_channel();
    super::route_submission_slot()
        .lock()
        .expect("route submission slot poisoned")
        .insert(provider_id, route_tx);
    let route_router = router.clone();
    let route_task = tokio::spawn(async move {
        while let Some(submission) = route_rx.recv().await {
            match submission {
                RouterSubmission::Route(submission) => {
                    let presence = super::operator_presence_status().await.presence;
                    if let Err(error) = route_router
                        .submit_route(*submission, presence, Instant::now())
                        .await
                    {
                        log::warn!("route channel submission failed: {error}");
                    }
                }
                RouterSubmission::AgentMessage { intent_id } => {
                    if let Err(error) = route_router.submit_agent_message(&intent_id).await {
                        log::warn!("agent message channel submission failed: {error}");
                    }
                }
            }
        }
    });
    let sweep = router.clone();
    let sweep_task = tokio::spawn(async move {
        // The channel is a live tap: this session carries the asks raised from
        // here on, and the backlog that predates it belongs to the app, not the
        // phone. Sealing it off COMPLETELY is a precondition for sweeping at all,
        // so a failure parks the sweep rather than letting it text the backlog.
        while let Err(error) = sweep.draw_the_session_line(boundary).await {
            super::set_router_blocker(provider_id, Some(error.clone()));
            log::warn!(
                "channel cannot seal the pre-session backlog, so it is not sweeping yet: {error}"
            );
            // A retry must resnapshot the complete backlog to preserve the safety
            // boundary. Pace that expensive work separately from ordinary sweeps.
            tokio::time::sleep(BACKLOG_SEAL_RETRY_INTERVAL).await;
        }
        super::set_router_blocker(provider_id, None);
        let mut schedule = SweepSchedule::new();
        loop {
            let reason = schedule.next().await;
            let started = Instant::now();
            sweep.sweep().await;
            schedule.finished();
            // A sweep's duration next to what woke it is the pair that makes a
            // recurrence diagnosable from the log alone: whether the cadence is
            // being outrun, and by which provider.
            log::info!(
                "channel sweep provider={provider_id} reason={} duration_ms={}",
                reason.as_str(),
                started.elapsed().as_millis()
            );
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
const QUESTION_GATES_SQL: &str = "SELECT p.id, p.questions, COALESCE(p.job_id, r.job_id),
                     COALESCE(j.node_name, j.uri_segment, 'agent'), p.uri_segment,
                     proj.key, i.number, COALESCE(e.seq, 1), j.uri_segment
                FROM prompts p
                JOIN runs r ON r.id = p.run_id
                LEFT JOIN jobs j ON j.id = COALESCE(p.job_id, r.job_id)
                LEFT JOIN issues i ON i.id = COALESCE(j.issue_id, r.issue_id)
                LEFT JOIN projects proj ON proj.id = i.project_id
                LEFT JOIN executions e ON e.id = j.execution_id
               WHERE p.response IS NULL
                 AND COALESCE(i.status, 'open') NOT IN ('merged', 'closed', 'failed')
               ORDER BY p.created_at DESC";

async fn load_questions(db: &LocalDb) -> Result<Vec<Gate>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(QUESTION_GATES_SQL, ()).await?;
            let mut gates = Vec::new();
            while let Some(row) = rows.next().await? {
                let prompt_id = row.text(0)?;
                let questions_json = row.text(1)?;
                let job_id = row.opt_text(2)?;
                let context = format!("[Cairn · {}]", row.text(3)?);
                let segment = row.opt_text(4)?;
                // The push creator's own builder, given the creator's own
                // inputs. Appending `/questions/{segment}` to the job's HOME URI
                // agrees with it for a top-level node and disagrees for a
                // sub-agent task, whose home nests under its parent while its
                // question URI does not -- so deriving it this way is what makes
                // the ask key match the push by construction rather than by
                // coincidence. A job with no issue (a thread's session) has no
                // node URI to build and mints no watcher push either, so it
                // keeps the home-relative form.
                let question_uri = match (
                    row.opt_text(5)?,
                    row.opt_i64(6)?,
                    row.opt_text(8)?,
                    segment.as_deref(),
                ) {
                    (Some(project), Some(number), Some(node), Some(segment)) => {
                        Some(cairn_common::uri::build_node_question_uri(
                            &project,
                            number as i32,
                            row.i64(7)? as i32,
                            &node,
                            segment,
                        ))
                    }
                    _ => match (job_id.as_deref(), segment.as_deref()) {
                        (Some(job_id), Some(segment)) => {
                            crate::jobs::queries::home_uri_for_job_conn(conn, job_id)
                                .await?
                                .map(|home| format!("{home}/questions/{segment}"))
                        }
                        _ => None,
                    },
                };
                // Every gate from one prompt renders one option of one ask, and
                // the watchers were told about it once, under one key.
                let ask_key = question_uri.as_deref().map(|uri| format!("question:{uri}"));
                let target_uri = question_uri.as_deref().and_then(routed_gate_target);
                let questions: Vec<StoredQuestion> = serde_json::from_str(&questions_json)
                    .map_err(|e| crate::storage::DbError::Row(e.to_string()))?;
                for (index, question) in questions.into_iter().enumerate() {
                    let text = match &question_uri {
                        Some(uri) => format!("{}\n\n{}", question.question, uri),
                        None => question.question,
                    };
                    gates.push(Gate {
                        conversation: None,
                        target_uri: target_uri.clone(),
                        kind: "question",
                        initiated_by: OutboundInitiator::CairnPush,
                        binding_ref: format!("{prompt_id}:{index}"),
                        job_id: job_id.clone(),
                        context: context.clone(),
                        ask_key: ask_key.clone(),
                        held_since: None,
                        ask: OutboundAsk::Question {
                            prompt_id: prompt_id.clone(),
                            question_index: index,
                            text,
                            options: question
                                .options
                                .into_iter()
                                .map(|o| AskOption {
                                    label: o.label,
                                    description: o.description,
                                })
                                .collect(),
                        },
                    });
                }
            }
            Ok(gates)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

const PERMISSION_GATES_SQL: &str = "SELECT req.id, req.tool_name, req.tool_input,
                     COALESCE(req.job_id, r.job_id),
                     COALESCE(j.node_name, j.uri_segment, 'agent'), req.uri_segment,
                     proj.key, i.number, COALESCE(e.seq, 1), j.uri_segment,
                     parent.uri_segment
                FROM permission_requests req
                JOIN runs r ON r.id = req.run_id
                LEFT JOIN jobs j ON j.id = COALESCE(req.job_id, r.job_id)
                LEFT JOIN jobs parent ON parent.id = j.parent_job_id
                LEFT JOIN issues i ON i.id = COALESCE(j.issue_id, r.issue_id)
                LEFT JOIN projects proj ON proj.id = i.project_id
                LEFT JOIN executions e ON e.id = j.execution_id
               WHERE req.status = 'pending'
                 AND COALESCE(i.status, 'open') NOT IN ('merged', 'closed', 'failed')
               ORDER BY req.created_at DESC";

async fn load_permissions(db: &LocalDb) -> Result<Vec<Gate>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(PERMISSION_GATES_SQL, ()).await?;
            let mut gates = Vec::new();
            while let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                let tool = row.text(1)?;
                let input = row.text(2)?;
                let job_id = row.opt_text(3)?;
                let segment = row.opt_text(5)?;
                // The push creator's builders, as in `load_questions` and for
                // the same reason. A sub-agent task nests its permission under
                // the parent node so the URI resolves (issue #143); reproducing
                // that choice here rather than inferring it from the home URI is
                // what keeps the two derivations from drifting apart.
                let uri = match (
                    row.opt_text(6)?,
                    row.opt_i64(7)?,
                    row.opt_text(9)?,
                    segment.as_deref(),
                ) {
                    (Some(project), Some(number), Some(node), Some(segment)) => {
                        let number = number as i32;
                        let seq = row.i64(8)? as i32;
                        Some(match row.opt_text(10)? {
                            Some(parent) => cairn_common::uri::build_task_permission_uri(
                                &project, number, seq, &parent, &node, segment,
                            ),
                            None => cairn_common::uri::build_node_permission_uri(
                                &project, number, seq, &node, segment,
                            ),
                        })
                    }
                    _ => match (job_id.as_deref(), segment.as_deref()) {
                        (Some(job_id), Some(segment)) => {
                            crate::jobs::queries::home_uri_for_job_conn(conn, job_id)
                                .await?
                                .map(|home| format!("{home}/permissions/{segment}"))
                        }
                        _ => None,
                    },
                };
                let summary = permission_ask_body(&tool, &input, uri.as_deref());
                let target_uri = uri.as_deref().and_then(routed_gate_target);
                let ask_key = uri.as_deref().map(|uri| format!("permission:{uri}"));
                gates.push(Gate {
                    conversation: None,
                    target_uri,
                    kind: "permission",
                    initiated_by: OutboundInitiator::CairnPush,
                    binding_ref: id.clone(),
                    job_id,
                    context: format!("[Cairn · {}]", row.text(4)?),
                    ask_key,
                    held_since: None,
                    ask: OutboundAsk::Permission {
                        request_id: id,
                        summary,
                    },
                });
            }
            Ok(gates)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

fn permission_ask_body(tool: &str, tool_input: &str, permission_uri: Option<&str>) -> String {
    let parsed = serde_json::from_str::<serde_json::Value>(tool_input).ok();
    let field = |name| {
        parsed
            .as_ref()
            .and_then(|value| value.get(name))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let mut sections = match (field("summary"), field("descriptor")) {
        (Some(summary), Some(descriptor)) => vec![summary.to_string(), descriptor.to_string()],
        (Some(summary), None) => vec![summary.to_string()],
        (None, Some(descriptor)) => vec![descriptor.to_string()],
        (None, None) => vec![format!("Allow {tool}?"), tool_input.to_string()],
    };
    if let Some(uri) = permission_uri {
        sections.push(uri.to_string());
    }
    sections.join("\n\n")
}

/// The outstanding review OBLIGATIONS, which is not the same as the undelivered
/// review queue.
///
/// Named so the regression test can plan it. This statement reads ONE table: the
/// issue each push is about is recovered from the push's own canonical URI by
/// [`review_subject`], not by asking the database to rediscover it. See
/// [`load_review_issue_titles`] for what that replaced and why.
///
/// The population is `retired_at IS NULL` and deliberately NOT
/// `delivered_event_id IS NULL`. Delivery is a fact about the watcher's queue --
/// a live parent drains its review push seconds after it is raised -- whereas
/// retirement is the obligation itself ending. Scoping to undelivered rows meant
/// that once a parent drained a review, the router could never see it again: if
/// that parent then completed, failed, or its thread closed with the review
/// still unresolved, nothing would ever reach the operator and the ask was
/// silently stranded. That was invisible while every review was texted
/// immediately, and became reachable the moment held asks began to be withheld.
///
/// The wider population is only affordable because this loader RETIRES what it
/// proves resolved, delivered rows included ([`RetirementScope::IncludingDelivered`]).
/// Nothing else ever retires a delivered row, so without that the set would grow
/// with the workspace's whole review history and every provider would re-resolve
/// it every five seconds.
const REVIEW_BACKLOG_SQL: &str = "SELECT ap.id, ap.recipient, ap.content_ref, ap.\"key\",
                      ap.wake, ap.boundary, ap.created_at, ap.delivered_event_id
               FROM attention_pushes ap
              WHERE ap.retired_at IS NULL
                AND ap.\"key\" LIKE 'review:%'
              ORDER BY ap.created_at DESC, ap.id DESC";

/// The issue a review push is about, read out of the push's own content ref.
///
/// This is the same `(project key, issue number)` pair
/// [`crate::orchestrator::attention_push`] derives to decide the push's
/// liveness, from the same canonical URI through the same parser, so the two
/// halves of one sweep cannot disagree about which issue a push names.
fn review_subject(content_ref: &str) -> Option<(String, i32)> {
    let parsed = parse_uri(content_ref)?;
    let project = parsed.project().map(cairn_common::uri::canonical_project)?;
    Some((project, parsed.issue_number()?))
}

/// `?, ?, ?` for an `IN (...)` list of `n` bound values.
fn placeholders(n: usize) -> String {
    (0..n).map(|_| "?").collect::<Vec<_>>().join(", ")
}

/// The statement behind [`load_review_issue_titles`], built for a given batch
/// size. Split out so the regression test can plan the shape the loader runs.
fn review_issue_titles_sql(numbers: usize, keys: usize) -> String {
    format!(
        "SELECT p.key, i.number, i.title
           FROM issues i JOIN projects p ON p.id = i.project_id
          WHERE i.number IN ({}) AND lower(p.key) IN ({})",
        placeholders(numbers),
        placeholders(keys),
    )
}

/// Titles for the issues a review backlog names, in one batched statement.
///
/// Shaped like the first step of `attention_push::resolve_subjects`, for the
/// same reason: the numbers drive `idx_issues_number` (migration 0195), so the
/// work is proportional to the BACKLOG rather than to the workspace. Keying by
/// the `(project, number)` pair is what keeps a number that exists in several
/// projects from cross-resolving.
///
/// The loader used to ask SQL to recover the issue instead, by joining every
/// undelivered push against every project and every issue under `lower(...)
/// LIKE ...` predicates no index can serve. That is a cross product of the
/// workspace re-executed by every provider's five-second sweep; on an
/// installation with a few hundred queued reviews and a few thousand issues it
/// held three cores permanently and churned gigabytes through the allocator
/// building the concatenated strings it compared (CAIRN-4194).
async fn load_review_issue_titles(
    db: &LocalDb,
    subjects: &[(String, i32)],
) -> Result<HashMap<(String, i32), String>, String> {
    let numbers: Vec<i64> = subjects
        .iter()
        .map(|(_, number)| *number as i64)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let keys: Vec<String> = subjects
        .iter()
        .map(|(project, _)| project.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if numbers.is_empty() || keys.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = review_issue_titles_sql(numbers.len(), keys.len());
    db.read(move |conn| {
        Box::pin(async move {
            let mut binds: Vec<Value> = numbers.into_iter().map(Value::Integer).collect();
            binds.extend(keys.into_iter().map(Value::Text));
            let mut rows = conn.query(&sql, binds).await?;
            let mut titles = HashMap::new();
            while let Some(row) = rows.next().await? {
                titles.insert(
                    (
                        cairn_common::uri::canonical_project(row.text(0)?),
                        row.i64(1)? as i32,
                    ),
                    row.text(2)?,
                );
            }
            Ok(titles)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_reviews(db: &LocalDb) -> Result<ReviewGates, String> {
    use crate::orchestrator::attention_push::{Boundary, Push, RetirementScope, Wake};

    // One statement for the whole backlog. Liveness is NOT decided here. It comes
    // from `attention_push::resolve_verdict_refs`, which is the one place that
    // predicate lives — this query used to carry its own copy as correlated
    // EXISTS arms, and two copies of a delivery predicate drift apart silently.
    let rows: Vec<Push> = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(REVIEW_BACKLOG_SQL, ()).await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(Push {
                        id: row.text(0)?,
                        recipient: row.text(1)?,
                        content_ref: row.text(2)?,
                        key: row.text(3)?,
                        wake: Wake::from_db(&row.text(4)?).unwrap_or(Wake::Wake),
                        boundary: Boundary::from_db(&row.text(5)?).unwrap_or(Boundary::Event),
                        created_at: row.i64(6)?,
                        delivered_event_id: row.opt_text(7)?,
                    });
                }
                Ok(out)
            })
        })
        .await
        .map_err(|error| error.to_string())?;

    // The issues those refs name, in one batched lookup keyed by the pair.
    let subjects: Vec<(String, i32)> = rows
        .iter()
        .filter_map(|row| review_subject(&row.content_ref))
        .collect();
    let titles = load_review_issue_titles(db, &subjects).await?;

    // Expire what does not resolve, then resolve liveness for everything that
    // survives in ONE batched call rather than a query per push.
    let mut result = ReviewGates {
        scanned: rows.len(),
        ..Default::default()
    };
    let mut resolvable = Vec::with_capacity(rows.len());
    let mut dangling: Vec<Push> = Vec::new();
    for row in rows {
        let Some((project, number, title)) =
            review_subject(&row.content_ref).and_then(|(project, number)| {
                titles
                    .get(&(project.clone(), number))
                    .map(|title| (project, number, title.clone()))
            })
        else {
            result.expired_dangling += 1;
            log::warn!(
                "channel expired dangling review {} because its semantic key does not resolve: {}",
                row.id,
                row.key
            );
            // Dismissal is a pending-only act, so a delivered dangling row has to
            // be RETIRED instead of deleted. Leaving it alone is not an option
            // now that the population is every unretired review: a lost subject
            // would turn each delivered watcher row into permanent sweep work,
            // which is the unbounded-history shape this loader exists to avoid.
            if row.delivered_event_id.is_some() {
                dangling.push(row);
            } else {
                crate::orchestrator::attention_push::delete_pending_by_id(db, &row.id)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            continue;
        };
        resolvable.push((row, project, number, title));
    }

    // A subject that no longer exists cannot have an outstanding obligation, so
    // this is a resolution the loader PROVED rather than an absence of evidence
    // -- the same conclusion the pending branch above expresses by deleting the
    // row outright, recorded instead of discarded.
    if !dangling.is_empty() {
        let gone = vec![
            crate::orchestrator::attention_push::Verdict::Terminal(
                crate::orchestrator::attention_push::RetirementReason::ReviewResolved,
            );
            dangling.len()
        ];
        crate::orchestrator::attention_push::retire_resolved(
            db,
            &dangling,
            &gone,
            RetirementScope::IncludingDelivered,
        )
        .await
        .map_err(|error| error.to_string())?;
    }

    let refs: Vec<(&str, &str)> = resolvable
        .iter()
        .map(|(row, _, _, _)| (row.key.as_str(), row.content_ref.as_str()))
        .collect();
    let verdicts = crate::orchestrator::attention_push::resolve_verdict_refs(db, &refs)
        .await
        .map_err(|error| error.to_string())?;

    // Retire what this pass PROVED resolved, delivered rows included. Nothing
    // else retires a delivered row, so without this the obligation population
    // would grow with the workspace's whole review history and every provider
    // would re-resolve all of it every five seconds (CAIRN-4207's shape).
    let observed: Vec<Push> = resolvable
        .iter()
        .map(|(row, _, _, _)| row.clone())
        .collect();
    crate::orchestrator::attention_push::retire_resolved(
        db,
        &observed,
        &verdicts,
        RetirementScope::IncludingDelivered,
    )
    .await
    .map_err(|error| error.to_string())?;

    // One gate per ASK, not per row. An issue with two watchers has two push
    // rows under one `review:` key, and they are one review to deliver and one
    // ask to report as held.
    let mut seen: HashSet<String> = HashSet::new();
    for ((row, project, number, title), verdict) in resolvable.into_iter().zip(verdicts) {
        if !verdict.is_live() || !seen.insert(row.key.clone()) {
            continue;
        }
        result.gates.push(Gate {
            conversation: None,
            target_uri: Some(format!("cairn://p/{project}/{number}")),
            kind: "review",
            initiated_by: OutboundInitiator::CairnPush,
            // A review gate is read straight out of the push table, so its
            // binding ref already IS the key and no derivation can drift.
            ask_key: Some(row.key.clone()),
            held_since: None,
            binding_ref: row.key,
            job_id: Some(row.recipient),
            context: String::new(),
            ask: OutboundAsk::Notify {
                text: review_notice(&project, number, &title, &row.content_ref),
            },
        });
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
    let delivery_key = gate.delivery_key(provider_id, conversation);
    if claims.holds(&delivery_key) {
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
    claims.claim(&delivery_key);
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
    use crate::models::IMessageChannelConfig;
    use crate::storage::migrated_test_db;

    #[tokio::test]
    async fn legacy_ownerless_job_questions_load_without_a_uri() {
        let db = migrated_test_db("channel-router-ownerless-job-question.db").await;
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at)
               VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO jobs (id, project_id, agent_config_id, status, uri_segment, created_at, updated_at)
               VALUES ('legacy-manager', 'p', 'manager', 'failed', 'manager', 1, 1);
             INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('legacy-run', 'p', 'legacy-manager', 'failed', 1, 1);
             INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, created_at)
               VALUES ('legacy-prompt', 'legacy-run', 'legacy-manager',
                       '[{\"question\":\"Choose?\",\"options\":[]}]', 'q-1', 1);",
        )
        .await
        .unwrap();

        let questions = load_questions(&db).await.unwrap();

        assert_eq!(questions.len(), 1);
        assert_eq!(questions[0].target_uri, None);
        let OutboundAsk::Question { text, .. } = &questions[0].ask else {
            panic!("expected question gate")
        };
        assert_eq!(text, "Choose?");
    }

    #[tokio::test]
    async fn thread_gates_carry_canonical_thread_targets() {
        let db = migrated_test_db("channel-router-thread-gates.db").await;
        let fixture = crate::test_support::seed_thread_session(&db).await;
        db.execute_batch(&format!(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('thread-run', '{}', '{}', 'running', 1, 1);
             INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, created_at)
               VALUES ('thread-prompt', 'thread-run', '{}', '[{{\"question\":\"Choose?\",\"options\":[]}}]', 'ask-1', 1);
             INSERT INTO permission_requests (id, run_id, job_id, tool_use_id, tool_name, tool_input, status, uri_segment, created_at)
               VALUES ('thread-permission', 'thread-run', '{}', 'tool-1', 'Bash', '{{}}', 'pending', 'permit-1', 1);",
            fixture.project_id,
            fixture.session_job_id,
            fixture.session_job_id,
            fixture.session_job_id,
        ))
        .await
        .unwrap();

        let questions = load_questions(&db).await.unwrap();
        assert_eq!(questions.len(), 1);
        assert_eq!(
            questions[0].target_uri.as_deref(),
            Some("cairn://p/thread-project/design")
        );
        let OutboundAsk::Question { text, .. } = &questions[0].ask else {
            panic!("expected question gate")
        };
        assert!(text.contains("cairn://p/thread-project/design/questions/ask-1"));

        let permissions = load_permissions(&db).await.unwrap();
        assert_eq!(permissions.len(), 1);
        assert_eq!(
            permissions[0].target_uri.as_deref(),
            Some("cairn://p/thread-project/design")
        );
        let OutboundAsk::Permission { summary, .. } = &permissions[0].ask else {
            panic!("expected permission gate")
        };
        assert!(summary.contains("cairn://p/thread-project/design/permissions/permit-1"));
    }

    /// One issue carrying an ask from each job shape that can raise one: a
    /// top-level node, and a sub-agent task beneath it.
    async fn seed_ask_shapes(db: &LocalDb) {
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at)
               VALUES ('w','W',1,1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','w','Proj','proj','/tmp/proj',1,1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('i','p',2,'Child','active',1,1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e','recipe','i','p','running',1,1);
             INSERT INTO jobs (id, project_id, issue_id, execution_id, node_name, uri_segment, agent_config_id, status, created_at, updated_at)
               VALUES ('node-job','p','i','e','Builder','builder','build','running',1,1);
             INSERT INTO jobs (id, parent_job_id, project_id, issue_id, execution_id, node_name, uri_segment, agent_config_id, status, created_at, updated_at)
               VALUES ('task-job','node-job','p','i','e','Probe','probe','build','running',1,1);
             INSERT INTO jobs (id, project_id, issue_id, status, created_at, updated_at)
               VALUES ('watcher','p','i','running',1,1);
             INSERT INTO runs (id, project_id, job_id, issue_id, created_at, updated_at)
               VALUES ('node-run','p','node-job','i',1,1);
             INSERT INTO runs (id, project_id, job_id, issue_id, created_at, updated_at)
               VALUES ('task-run','p','task-job','i',1,1);
             INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, created_at)
               VALUES ('node-prompt','node-run','node-job','[{\"question\":\"Node?\",\"options\":[]}]','q-1',2);
             INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, created_at)
               VALUES ('task-prompt','task-run','task-job','[{\"question\":\"Task?\",\"options\":[]}]','q-2',1);
             INSERT INTO permission_requests (id, run_id, job_id, tool_use_id, tool_name, tool_input, status, uri_segment, created_at)
               VALUES ('node-perm','node-run','node-job','tu-1','Bash','{}','pending','perm-1',2);
             INSERT INTO permission_requests (id, run_id, job_id, tool_use_id, tool_name, tool_input, status, uri_segment, created_at)
               VALUES ('task-perm','task-run','task-job','tu-2','Bash','{}','pending','perm-2',1);",
        )
        .await
        .unwrap();
    }

    const NODE_QUESTION_KEY: &str = "question:cairn://p/proj/2/1/builder/questions/q-1";

    fn key_of(gates: &[Gate], binding_ref: &str) -> String {
        gates
            .iter()
            .find(|gate| gate.binding_ref.starts_with(binding_ref))
            .unwrap_or_else(|| panic!("no gate for {binding_ref}"))
            .ask_key
            .clone()
            .unwrap_or_else(|| panic!("{binding_ref} carried no ask key"))
    }

    /// The router derives an ask's push key; the push creator derives the push.
    /// Nothing forces those two strings to agree, and if they ever stop agreeing
    /// the answerability lookup matches nothing, every ask reads as unheld, and
    /// the feature silently reverts to texting the operator about everything --
    /// while every other test in this file still passes. So assert the exact
    /// strings, against the creator's own builders, for every job shape.
    ///
    /// The task shapes are why this is not a formality. A sub-agent task's
    /// question URI is FLAT (`.../{task}/questions/{q}`, from the asking job's
    /// own segment) while its permission URI NESTS under the parent node
    /// (`.../{node}/task/{task}/permissions/{p}`, issue #143). Deriving either
    /// by appending to the job's home URI would have produced a key no push
    /// could ever match.
    #[tokio::test]
    async fn router_ask_keys_match_the_push_creator() {
        let db = migrated_test_db("channel-router-ask-key-parity.db").await;
        seed_ask_shapes(&db).await;

        let questions = load_questions(&db).await.unwrap();
        assert_eq!(
            key_of(&questions, "node-prompt"),
            format!(
                "question:{}",
                cairn_common::uri::build_node_question_uri("proj", 2, 1, "builder", "q-1")
            )
        );
        assert_eq!(key_of(&questions, "node-prompt"), NODE_QUESTION_KEY);
        assert_eq!(
            key_of(&questions, "task-prompt"),
            format!(
                "question:{}",
                cairn_common::uri::build_node_question_uri("proj", 2, 1, "probe", "q-2")
            )
        );

        let permissions = load_permissions(&db).await.unwrap();
        assert_eq!(
            key_of(&permissions, "node-perm"),
            format!(
                "permission:{}",
                cairn_common::uri::build_node_permission_uri("proj", 2, 1, "builder", "perm-1")
            )
        );
        assert_eq!(
            key_of(&permissions, "task-perm"),
            format!(
                "permission:{}",
                cairn_common::uri::build_task_permission_uri(
                    "proj", 2, 1, "builder", "probe", "perm-2"
                )
            )
        );
    }

    /// A thread's own ask is the case the operator must always hear about. It
    /// gets a key like any other, and no push is ever filed under it, so it
    /// resolves as unheld by absence rather than by a special case.
    #[tokio::test]
    async fn a_threads_own_ask_is_held_by_nobody() {
        let db = migrated_test_db("channel-router-thread-own-ask.db").await;
        let fixture = crate::test_support::seed_thread_session(&db).await;
        db.execute_batch(&format!(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('thread-run', '{}', '{}', 'running', 1, 1);
             INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, created_at)
               VALUES ('thread-prompt','thread-run','{}','[{{\"question\":\"Choose?\",\"options\":[]}}]','ask-1',1);",
            fixture.project_id, fixture.session_job_id, fixture.session_job_id,
        ))
        .await
        .unwrap();

        let mut questions = load_questions(&db).await.unwrap();
        assert_eq!(
            key_of(&questions, "thread-prompt"),
            "question:cairn://p/thread-project/design/questions/ask-1"
        );
        stamp_agent_holders(&db, &mut questions).await.unwrap();
        assert!(
            questions.iter().all(|gate| !gate.held_by_agent()),
            "nothing was told about a thread's own question, so the operator gets it"
        );
    }

    /// Tell `recipient` about the node question, the way `push_to_issue_watchers`
    /// does when an issue has a parent thread or a coordinating job.
    async fn tell_watcher(db: &LocalDb, recipient: &str) -> String {
        crate::orchestrator::attention_push::push(
            db,
            recipient,
            "cairn://p/proj/2/1/builder/questions/q-1",
            crate::orchestrator::attention_push::Wake::Wake,
            crate::orchestrator::attention_push::Boundary::Event,
            NODE_QUESTION_KEY,
        )
        .await
        .unwrap()
        .0
    }

    async fn operator_carries_the_node_question(
        router: &ChannelRouter,
        db: &LocalDb,
        generation: &str,
    ) -> bool {
        let (routed, _) = router.load_routed_gates(db, generation).await.unwrap();
        routed
            .iter()
            .any(|gate| gate.ask_key.as_deref() == Some(NODE_QUESTION_KEY))
    }

    /// The rule this issue exists for, and every way out of it.
    ///
    /// An ask reaches the operator's pocket only while no live agent watcher is
    /// answerable for it. Withholding never strands anything: the moment the
    /// holder dies, or the operator mutes the child by hand, the next sweep
    /// finds the ask unheld and sends it.
    #[tokio::test]
    async fn the_operator_hears_a_child_ask_only_when_no_live_agent_holds_it() {
        let (orch, db) = route_test_orchestrator("channel-router-held-ask.db").await;
        seed_ask_shapes(&db).await;
        let (router, _provider) = test_router(orch);

        assert!(
            operator_carries_the_node_question(&router, &db, "g0").await,
            "an orphan issue's ask has no agent answerable for it"
        );

        let push_id = tell_watcher(&db, "watcher").await;
        assert!(
            !operator_carries_the_node_question(&router, &db, "g1").await,
            "a live watcher was told, so its in-app attention IS the delivery"
        );

        // Drain it, the way a live parent does within seconds of being told. The
        // holder is unchanged -- being told is what made it the holder -- but
        // this is the state the died-watcher case actually arrives in, so a test
        // that skipped it would not exercise the production failure.
        crate::orchestrator::attention_push::stamp_delivered(&db, &[push_id], "ev-1")
            .await
            .unwrap();
        assert!(
            !operator_carries_the_node_question(&router, &db, "g2").await,
            "draining a push does not release the ask"
        );

        db.execute("UPDATE jobs SET status='complete' WHERE id='watcher'", ())
            .await
            .unwrap();
        assert!(
            operator_carries_the_node_question(&router, &db, "g3").await,
            "a watcher that finished without answering must not strand the ask"
        );
    }

    /// The same died-watcher guarantee for REVIEWS, which reach the router
    /// through the push table rather than through a live domain row and so are
    /// the only ask kind that could lose its candidacy by being delivered.
    ///
    /// A question stays loadable while `prompts.response IS NULL` whatever
    /// happens to any push. A review's only trace IS the push, so scoping the
    /// backlog to undelivered rows meant a drained review vanished from the
    /// router entirely: withheld while the parent was alive, then invisible
    /// forever once it died. That is the stranding this whole rule forbids, and
    /// it is unreachable without stamping the push delivered first.
    #[tokio::test]
    async fn a_drained_review_still_reaches_the_operator_when_its_watcher_dies() {
        let (orch, db) = route_test_orchestrator("channel-router-drained-review.db").await;
        seed_ask_shapes(&db).await;
        db.execute_batch(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                status, opened_at, updated_at)
               VALUES('mr','node-job','p','i','Review','feature','main','open',1,1);",
        )
        .await
        .unwrap();
        const REVIEW: &str = "review:cairn://p/proj/2";
        let (push_id, _) = crate::orchestrator::attention_push::push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/builder/create-pr",
            crate::orchestrator::attention_push::Wake::Wake,
            crate::orchestrator::attention_push::Boundary::Event,
            REVIEW,
        )
        .await
        .unwrap();

        // iMessage's default policy excludes notify, so a review would never be
        // loaded at all; this is the policy an operator who wants review
        // notices runs.
        let router = ChannelRouter::new_for_provider(
            orch,
            Arc::new(RecordingPollProvider::new()),
            "imessage",
            "+15551234567".into(),
            MessageClassPolicy {
                question: false,
                permission: false,
                notify: true,
            },
            ChannelInboundCapabilities::default(),
        );
        let carries = |generation: &'static str| {
            let router = &router;
            let db = &db;
            async move {
                let (routed, _) = router.load_routed_gates(db, generation).await.unwrap();
                routed
                    .iter()
                    .any(|gate| gate.ask_key.as_deref() == Some(REVIEW))
            }
        };

        assert!(!carries("g0").await, "a live watcher holds the review");

        crate::orchestrator::attention_push::stamp_delivered(&db, &[push_id], "ev-1")
            .await
            .unwrap();
        assert!(
            !carries("g1").await,
            "draining it does not release it either"
        );

        db.execute("UPDATE jobs SET status='complete' WHERE id='watcher'", ())
            .await
            .unwrap();
        assert!(
            carries("g2").await,
            "a review whose watcher died unresolved must still reach the operator"
        );
    }

    /// A subject that goes away must take its delivered rows with it.
    ///
    /// The population is now every unretired review, so anything that stays in it
    /// forever is permanent work on a five-second sweep. A dangling PENDING row
    /// is deleted, as it always was; a delivered one cannot be (dismissal is a
    /// pending-only act) and has to be retired instead, or losing one issue
    /// leaves a row per watcher being re-read for the life of the workspace.
    #[tokio::test]
    async fn a_delivered_review_whose_subject_vanishes_leaves_the_sweep() {
        let db = migrated_test_db("channel-router-dangling-review.db").await;
        seed_ask_shapes(&db).await;
        let (push_id, _) = crate::orchestrator::attention_push::push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/builder/create-pr",
            crate::orchestrator::attention_push::Wake::Wake,
            crate::orchestrator::attention_push::Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        crate::orchestrator::attention_push::stamp_delivered(&db, &[push_id], "ev-1")
            .await
            .unwrap();

        assert_eq!(review_backlog_len(&db).await, 1);

        // The subject stops resolving, the way a deleted issue does.
        db.execute("UPDATE issues SET number=9999 WHERE id='i'", ())
            .await
            .unwrap();
        let reviews = load_reviews(&db).await.unwrap();
        assert!(reviews.gates.is_empty());
        assert_eq!(reviews.expired_dangling, 1);
        assert_eq!(
            review_backlog_len(&db).await,
            0,
            "a delivered row with no subject must be retired, not re-read forever"
        );
    }

    /// How many rows the review sweep would carry on its next pass.
    async fn review_backlog_len(db: &LocalDb) -> usize {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(REVIEW_BACKLOG_SQL, ()).await?;
                let mut carried = 0usize;
                while rows.next().await?.is_some() {
                    carried += 1;
                }
                Ok(carried)
            })
        })
        .await
        .unwrap()
    }

    /// Muting a child is the thread role's explicit hand-off gesture: it means
    /// "this one is in the user's hands", so it has to flip delivery and not
    /// only wakes.
    #[tokio::test]
    async fn muting_a_child_hands_its_asks_back_to_the_operator() {
        let (orch, db) = route_test_orchestrator("channel-router-muted-child.db").await;
        seed_ask_shapes(&db).await;
        let (router, _provider) = test_router(orch);

        tell_watcher(&db, "watcher").await;
        assert!(!operator_carries_the_node_question(&router, &db, "g0").await);

        db.execute(
            "INSERT INTO wake_subscriptions
               (id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at)
             VALUES('sub','watcher','issue','cairn://p/proj/2','muted','agent',1,1)",
            (),
        )
        .await
        .unwrap();
        assert!(
            operator_carries_the_node_question(&router, &db, "g1").await,
            "the mute is read live, so it releases an ask raised before it"
        );
    }

    /// Withholding is about the operator's ATTENTION, not about the ask. Per the
    /// CAIRN-4006 dual-post ruling an issue's own Discord thread is the ask's
    /// own place rather than the operator's pocket, so it keeps carrying a
    /// child's ask -- fully answerable in situ -- at the same moment iMessage is
    /// being spared it.
    #[tokio::test]
    async fn a_structural_surface_carries_the_ask_the_operator_is_spared() {
        let (orch, db) = route_test_orchestrator("channel-router-structural-held.db").await;
        seed_ask_shapes(&db).await;
        tell_watcher(&db, "watcher").await;

        let (imessage, _provider) = test_router(orch.clone());
        assert!(
            !operator_carries_the_node_question(&imessage, &db, "g0").await,
            "the operator's pocket is spared an ask a live agent holds"
        );

        let discord = ChannelRouter::new_for_provider(
            orch,
            Arc::new(RecordingPollProvider::new()),
            "discord",
            "9".into(),
            MessageClassPolicy::default(),
            ChannelInboundCapabilities::default(),
        );
        ledger::bind_target(
            discord.ledger(),
            "discord",
            "discord:9/1",
            "cairn://p/proj/2",
            crate::channels::bindings::BindingKind::Structural,
            crate::channels::bindings::MESSAGE_CLASSES_ALL,
            1,
            0,
        )
        .await
        .unwrap();

        let (routed, _) = discord.load_routed_gates(&db, "g0").await.unwrap();
        let carried: Vec<_> = routed
            .iter()
            .filter(|gate| gate.ask_key.as_deref() == Some(NODE_QUESTION_KEY))
            .collect();
        assert_eq!(
            carried.len(),
            1,
            "the issue's own thread is where the ask lives, held or not"
        );
        assert!(carried[0].conversation.is_some());
    }

    /// An operator-configured route sink is a destination the operator chose, so
    /// no question of agent answerability applies to it. Guarded by an explicit
    /// term rather than by route gates happening to carry no ask key.
    #[tokio::test]
    async fn a_route_submission_is_never_withheld() {
        let held_ask = Gate {
            conversation: None,
            target_uri: None,
            kind: "route",
            initiated_by: OutboundInitiator::CairnPush,
            binding_ref: "route:1".into(),
            job_id: None,
            context: String::new(),
            ask_key: Some(NODE_QUESTION_KEY.into()),
            held_since: Some(1),
            ask: OutboundAsk::Notify {
                text: "routed".into(),
            },
        };
        assert!(!held_ask.held_by_agent());

        let question = Gate {
            kind: "question",
            ..held_ask
        };
        assert!(question.held_by_agent());
    }

    /// The rendered plan for a statement, one node per line.
    async fn query_plan(db: &LocalDb, sql: &str, binds: Vec<Value>) -> String {
        let sql = format!("EXPLAIN QUERY PLAN {sql}");
        db.read(move |conn| {
            Box::pin(async move {
                let mut rows = conn.query(&sql, binds).await?;
                let mut plan = String::new();
                while let Some(row) = rows.next().await? {
                    plan.push_str(&row.text(3)?);
                    plan.push('\n');
                }
                Ok(plan)
            })
        })
        .await
        .unwrap()
    }

    /// Every channel provider re-reads the review backlog on a five-second
    /// sweep, so what that read costs has to follow the BACKLOG. It used to
    /// follow the workspace: the loader asked SQL to recover each push's issue by
    /// joining every undelivered push against every project and every issue under
    /// `lower(...) LIKE ...` predicates no index can serve, and a rebuilt runner
    /// with a few hundred queued reviews and a few thousand issues sat at three
    /// cores permanently (CAIRN-4194).
    ///
    /// The plan is what pins this, because the regression is invisible to every
    /// cheaper seam: the old shape was also two statements, in two read
    /// transactions, returning one row per push. Only its plan said that reading
    /// a backlog meant walking the workspace.
    #[tokio::test]
    async fn review_backlog_statements_never_walk_the_workspace() {
        let db = migrated_test_db("channel-router-review-plan.db").await;

        let backlog = query_plan(&db, REVIEW_BACKLOG_SQL, Vec::new()).await;
        assert!(
            !backlog.contains("projects") && !backlog.contains("issues"),
            "the backlog statement reads the push table alone; a plan that reaches \
             the workspace is the cross product coming back:\n{backlog}"
        );

        let titles = query_plan(
            &db,
            &review_issue_titles_sql(1, 1),
            vec![Value::Integer(1), Value::Text("cairn".into())],
        )
        .await;
        assert!(
            titles.contains("idx_issues_number"),
            "the batched lookup is driven by the backlog's issue numbers \
             (migration 0195), not by a scan:\n{titles}"
        );
    }

    /// The follow sweep re-reads every followed target every five seconds, so
    /// what that read costs has to follow the TARGET. It used to follow the
    /// workspace: both statements drove from `events.rowid > cursor`, an
    /// open-ended range over the events primary key, and joined outward to
    /// discover which target each event belonged to — so a sweep read every event
    /// the workspace had produced since the cursor and discarded nearly all of it
    /// at the project and issue predicates. On a 1.5M-row `events` table that was
    /// 15-17 seconds per follow (CAIRN-4208).
    ///
    /// The plan is what pins this, for the same reason it pins the review backlog
    /// next door: the old shape returned the same rows this one does, in the same
    /// number of statements. Only its plan said that reading one thread's last
    /// word meant reading the whole workspace's history.
    ///
    /// The job list is planned at a realistic width rather than at one, because
    /// an `IN` list of a single value is exactly the cardinality at which a
    /// planner will pick a different path than it picks in production.
    #[tokio::test]
    async fn followed_target_statements_never_walk_the_event_table() {
        let db = migrated_test_db("channel-router-follow-plan.db").await;
        const JOBS: usize = 8;
        let job_ids: Vec<Value> = (0..JOBS).map(|n| Value::Text(format!("job-{n}"))).collect();

        let issue_jobs = query_plan(
            &db,
            FOLLOW_ISSUE_JOBS_SQL,
            vec![Value::Text("cairn".into()), Value::Integer(4208)],
        )
        .await;
        assert!(
            issue_jobs.contains("idx_issues_project_number")
                && issue_jobs.contains("idx_jobs_issue_id"),
            "a followed issue is found by its (project, number) key and reaches \
             its jobs through issues(id):\n{issue_jobs}"
        );
        let thread_jobs = query_plan(
            &db,
            &follow_thread_jobs_sql(),
            vec![
                Value::Text("cairn".into()),
                Value::Text("performance".into()),
            ],
        )
        .await;
        assert!(
            thread_jobs.contains("SEARCH t USING INDEX")
                && thread_jobs.contains("idx_jobs_thread_id"),
            "a followed thread is found by its (project, name) key and reaches \
             its session job through threads(id):\n{thread_jobs}"
        );
        for plan in [&issue_jobs, &thread_jobs] {
            assert!(
                !plan.contains("events"),
                "resolving WHICH jobs a target owns must not touch the event \
                 table at all:\n{plan}"
            );
            assert!(
                !plan.contains("SCAN jobs")
                    && !plan.contains("SCAN issues")
                    && !plan.contains("SCAN threads"),
                "only projects may be scanned here — it is the one table in this \
                 join that a workspace keeps small, and LOWER(p.key) can use no \
                 index by construction. Everything else is reached by key:\n{plan}"
            );
        }

        let mut event_binds = job_ids.clone();
        event_binds.push(Value::Integer(0));
        let events = query_plan(&db, &followed_events_sql(JOBS), event_binds).await;
        let edge = query_plan(&db, &follow_live_edge_sql(JOBS), job_ids).await;
        for plan in [&events, &edge] {
            assert!(
                plan.contains("SEARCH r USING INDEX idx_runs_job_id"),
                "the resolved jobs drive the read:\n{plan}"
            );
            assert!(
                plan.contains("SEARCH e USING INDEX idx_events"),
                "and each run seeks its own events:\n{plan}"
            );
            assert!(
                !plan.contains("SCAN e") && !plan.contains("e USING INTEGER PRIMARY KEY"),
                "driving from the events primary key is the workspace walk \
                 coming back: it reads every event the workspace produced after \
                 the cursor and joins outward to find out whose it was:\n{plan}"
            );
        }
    }

    /// The measurement behind the rewrite, kept runnable because a migrated
    /// empty database cannot show it: every plan here is cheap at zero rows, and
    /// what changed is which table's SIZE the read is proportional to.
    ///
    /// Ignored by default because it builds a live-sized `events` table (the
    /// installation this was diagnosed on had 1,477,400 rows). Run it with
    /// `cargo test -p cairn-core --lib measure_follow_read -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "measurement: seeds a live-sized events table"]
    async fn measure_follow_read_at_live_scale() {
        const RUNS: usize = 300;
        const EVENTS_PER_RUN: usize = 5_000;
        const BATCH: usize = 500;
        // The followed target's own transcript. A follow is a TAP: the target
        // says something occasionally, and the workspace goes on producing
        // between its turns.
        const TARGET_EVENTS: usize = 200;
        const TARGET_NEW_EVENTS: usize = 5;

        let db = migrated_test_db("channel-router-follow-scale.db").await;
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);",
        )
        .await
        .unwrap();
        for run in 0..RUNS {
            db.execute_batch(&format!(
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                   VALUES ('i-{run}', 'p', {run}, 'Issue {run}', 'active', 1, 1);
                 INSERT INTO jobs (id, issue_id, project_id, status, uri_segment, created_at, updated_at)
                   VALUES ('j-{run}', 'i-{run}', 'p', 'complete', 'builder', 1, 1);
                 INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                   VALUES ('r-{run}', 'p', 'j-{run}', 'complete', 1, 1);"
            ))
            .await
            .unwrap();
        }

        let insert_events = |run: usize, from: usize, count: usize| {
            let values: Vec<String> = (from..from + count)
                .map(|n| {
                    format!(
                        "('e-{run}-{n}', 'r-{run}', {n}, {n}, 'assistant', '{{\"content\":\"turn {n}\",\"toolUses\":[]}}', {n})"
                    )
                })
                .collect();
            format!(
                "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at) VALUES {};",
                values.join(", ")
            )
        };

        // The target speaks first, and the follow cursor stops there.
        db.execute_batch(&insert_events(0, 0, TARGET_EVENTS))
            .await
            .unwrap();
        let cursor = db
            .query_one("SELECT MAX(rowid) FROM events", (), |row| row.i64(0))
            .await
            .unwrap();

        // Then the rest of the workspace produces, at length, while the followed
        // target says nothing. Every one of these events sits between the follow
        // cursor and the end of the primary key, and the old statement read all
        // of them on every sweep to discover that none of them were the
        // target's.
        let seeding = Instant::now();
        for run in 1..RUNS {
            for chunk in (0..EVENTS_PER_RUN).step_by(BATCH) {
                db.execute_batch(&insert_events(run, chunk, BATCH))
                    .await
                    .unwrap();
            }
        }
        let total = TARGET_EVENTS + (RUNS - 1) * EVENTS_PER_RUN;
        println!("seeded {total} events in {:?}", seeding.elapsed());

        // And then the target speaks again: the sweep that has something to say.
        db.execute_batch(&insert_events(0, TARGET_EVENTS, TARGET_NEW_EVENTS))
            .await
            .unwrap();

        let old = Instant::now();
        let old_rows = db
            .query_all(
                "SELECT e.rowid, e.data, p.key, i.number, i.title, j.id, p.id, p.repo_path FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE LOWER(p.key) = ?1 AND i.number = ?2 AND e.rowid > ?3 AND e.event_type = 'assistant' ORDER BY e.rowid",
                params!["cairn", 0_i64, cursor],
                |row| row.i64(0),
            )
            .await
            .unwrap();
        let old = old.elapsed();

        let new = Instant::now();
        let jobs: Vec<String> = db
            .query_all(FOLLOW_ISSUE_JOBS_SQL, params!["cairn", 0_i64], |row| {
                row.text(0)
            })
            .await
            .unwrap();
        let mut binds: Vec<Value> = jobs.iter().cloned().map(Value::Text).collect();
        binds.push(Value::Integer(cursor));
        let new_rows = db
            .query_all(&followed_events_sql(jobs.len()), binds, |row| row.i64(0))
            .await
            .unwrap();
        let new = new.elapsed();

        println!(
            "events={total} rows={} old={old:?} new={new:?}",
            new_rows.len()
        );
        assert_eq!(old_rows, new_rows, "both shapes return the same events");
        assert_eq!(new_rows.len(), TARGET_NEW_EVENTS);
        assert!(
            new * 20 < old,
            "the target-driven read must not be within an order of magnitude of \
             the workspace walk: old={old:?} new={new:?}"
        );
    }

    /// A sweep that overruns its cadence is followed by ONE sweep, not by one
    /// for every deadline it covered.
    ///
    /// This is the difference between a slow sweep and a saturated core. With
    /// tokio's default `Burst`, a twenty-second sweep on a five-second cadence
    /// leaves four deadlines ready at once, so the loop runs four more sweeps
    /// back to back with no sleep at all — and while the condition that made the
    /// sweep slow persists, it never sleeps again. Three enabled providers is
    /// then three permanently ready workers, which is what a rebuilt runner
    /// sitting at 500% CPU was (CAIRN-4208).
    ///
    /// Paused time is what makes this assertable: the property is about WHEN the
    /// loop is allowed to run, so it is measured on the clock rather than
    /// inferred from a duration.
    /// The presence wake is process-global, so a permit some other test left
    /// behind arrives here as a spurious wake. Draining it is part of arranging
    /// a schedule test, not incidental cleanup.
    async fn drain_presence_wakes() {
        while tokio::time::timeout(
            Duration::from_millis(1),
            super::super::wait_for_presence_change(),
        )
        .await
        .is_ok()
        {}
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial(operator_presence)]
    async fn a_sweep_that_overruns_its_cadence_is_followed_by_one_sweep() {
        drain_presence_wakes().await;
        let mut schedule = SweepSchedule::new();
        assert_eq!(schedule.next().await, SweepReason::Timer);

        // A sweep four cadences long.
        tokio::time::advance(SWEEP_INTERVAL * 4).await;
        let overran = tokio::time::Instant::now();
        schedule.finished();

        let mut starts = Vec::new();
        for _ in 0..3 {
            assert_eq!(schedule.next().await, SweepReason::Timer);
            starts.push(overran.elapsed());
            schedule.finished();
        }

        assert!(
            starts[0] <= SWEEP_FLOOR,
            "the missed deadlines coalesce into one sweep, which runs now: {starts:?}"
        );
        assert!(
            starts[1] >= SWEEP_INTERVAL && starts[2] >= SWEEP_INTERVAL * 2,
            "and the loop returns to its cadence rather than working off a \
             backlog of deadlines: under Burst these are three sweeps with no \
             sleep between them: {starts:?}"
        );
    }

    /// A presence change is an external event, so the floor holds for it too: a
    /// wake source that flaps can advance a sweep, but it cannot spin the loop.
    #[tokio::test(start_paused = true)]
    #[serial_test::serial(operator_presence)]
    async fn a_burst_of_presence_changes_buys_one_sweep_and_cannot_spin() {
        use crate::channels::OperatorPresenceMode;

        drain_presence_wakes().await;
        let mut schedule = SweepSchedule::new();
        assert_eq!(schedule.next().await, SweepReason::Timer);
        schedule.finished();

        for mode in [
            OperatorPresenceMode::Idle,
            OperatorPresenceMode::Active,
            OperatorPresenceMode::Auto,
            OperatorPresenceMode::Idle,
        ] {
            super::super::set_operator_presence_mode(mode);
        }
        let started = tokio::time::Instant::now();
        assert_eq!(schedule.next().await, SweepReason::Presence);
        assert!(
            started.elapsed() >= SWEEP_FLOOR,
            "a live wake still rests the floor"
        );
        schedule.finished();

        // The changes that arrived while the first sweep ran are one wake, not
        // four: what follows is the ordinary cadence.
        let started = tokio::time::Instant::now();
        assert_eq!(schedule.next().await, SweepReason::Timer);
        assert!(started.elapsed() >= SWEEP_FLOOR);
        super::super::set_operator_presence_mode(OperatorPresenceMode::Auto);
    }

    /// Every enabled provider sweeps on its own five-second loop, but what the
    /// sweep READS is a fact about the workspace, not about the provider. Three
    /// channels configured used to mean the same expensive backlog reads three
    /// times per tick on three threads; they are now computed once per database
    /// generation for the whole process, and a provider that arrives at an
    /// unchanged generation performs no database work at all.
    #[tokio::test]
    async fn a_second_providers_gate_source_costs_no_database_reads() {
        let db = migrated_test_db("channel-router-gate-source-sharing.db").await;
        seed_run(&db).await;
        seed_prompt(&db, "prompt-1", 100).await;

        let before = db.read_transaction_count();
        let first = shared_gates(&db, GateKind::Question).await.unwrap();
        assert!(
            db.read_transaction_count() > before,
            "the first provider to ask pays for the load"
        );
        assert_eq!(first.gates.len(), 1);

        let shared = db.read_transaction_count();
        let second = shared_gates(&db, GateKind::Question).await.unwrap();
        assert_eq!(
            db.read_transaction_count(),
            shared,
            "the next provider's sweep reads nothing: this is the multiplication \
             by enabled providers that the profile showed"
        );
        assert_eq!(second.gates.len(), 1);
        assert_eq!(second.generation, first.generation);

        // A write is what makes a shared snapshot stale, and it costs exactly one
        // recomputation however many providers observe it.
        seed_prompt(&db, "prompt-2", 101).await;
        let stale = db.read_transaction_count();
        let refreshed = shared_gates(&db, GateKind::Question).await.unwrap();
        assert!(db.read_transaction_count() > stale);
        assert_eq!(refreshed.gates.len(), 2, "and it sees the new ask");
        assert_ne!(refreshed.generation, first.generation);

        let after = db.read_transaction_count();
        assert_eq!(
            shared_gates(&db, GateKind::Question)
                .await
                .unwrap()
                .gates
                .len(),
            2
        );
        assert_eq!(db.read_transaction_count(), after);
    }

    /// A database reopened at the same path is a DIFFERENT database as far as
    /// the shared snapshot is concerned.
    ///
    /// The mutation generation is per handle and starts at zero, while the cache
    /// is a process-wide static that outlives any handle. Keyed by path and
    /// generation alone, a reopened database would be served the previous
    /// handle's gates the moment its counter reached the same number — delivering
    /// asks that are already resolved, or silently withholding new ones, until
    /// some unrelated write happened to move the counter off the collision.
    #[tokio::test]
    async fn a_reopened_database_does_not_inherit_the_previous_handles_gates() {
        let seed = migrated_test_db("channel-router-gate-source-reopen.db").await;
        seed_run(&seed).await;
        seed_prompt(&seed, "prompt-1", 100).await;
        let path = seed.path().to_path_buf();

        // A fresh handle publishes at generation zero, which is exactly where
        // every later handle at this path also begins.
        let first = LocalDb::open(&path).await.unwrap();
        assert_eq!(first.mutation_generation(), 0);
        assert_eq!(
            shared_gates(&first, GateKind::Question)
                .await
                .unwrap()
                .gates
                .len(),
            1
        );
        drop(first);

        seed.execute("DELETE FROM prompts", ()).await.unwrap();
        let reopened = LocalDb::open(&path).await.unwrap();
        assert_eq!(reopened.mutation_generation(), 0, "the collision");
        assert!(
            shared_gates(&reopened, GateKind::Question)
                .await
                .unwrap()
                .gates
                .is_empty(),
            "the reopened database is read, not inherited"
        );
    }

    /// Issue numbers repeat across projects, so the lookup is keyed by the PAIR.
    /// Resolving on the number alone would hand a push the wrong project's title
    /// and point its notice at the wrong issue.
    #[tokio::test]
    async fn a_review_resolves_against_its_own_project_when_numbers_repeat() {
        let db = migrated_test_db("channel-router-review-cross-project.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p-cairn', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1),
                     ('p-atlas', 'w', 'Atlas', 'atlas', '/tmp/atlas', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('cairn-7', 'p-cairn', 7, 'Cairn seven', 'active', 1, 1),
                     ('atlas-7', 'p-atlas', 7, 'Atlas seven', 'active', 1, 1);
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('cairn-job', 'p-cairn', 'cairn-7', 'running', 1, 1),
                     ('atlas-job', 'p-atlas', 'atlas-7', 'running', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('cairn-mr', 'cairn-job', 'p-cairn', 'cairn-7', 'C', 'f', 'main', 'open', 1, 1),
                     ('atlas-mr', 'atlas-job', 'p-atlas', 'atlas-7', 'A', 'f', 'main', 'open', 1, 1);
             INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at)
             VALUES
               ('cairn-push', 'cairn-job', 'cairn://p/cairn/7/1/builder/create-pr', 'wake', 'event',
                'review:cairn://p/cairn/7', 1),
               ('atlas-push', 'atlas-job', 'cairn://p/atlas/7/1/builder/create-pr', 'wake', 'event',
                'review:cairn://p/atlas/7', 2);",
        )
        .await
        .unwrap();

        let reviews = load_reviews(&db).await.unwrap();

        assert_eq!(reviews.scanned, 2);
        assert_eq!(reviews.expired_dangling, 0);
        let mut notices: Vec<String> = reviews
            .gates
            .iter()
            .map(|gate| match &gate.ask {
                OutboundAsk::Notify { text } => text.clone(),
                other => panic!("a review gate notifies, got {other:?}"),
            })
            .collect();
        notices.sort();
        assert_eq!(
            notices,
            vec![
                "atlas/7 review ready \u{2014} Atlas seven\ncairn://p/atlas/7/1/builder/create-pr",
                "cairn/7 review ready \u{2014} Cairn seven\ncairn://p/cairn/7/1/builder/create-pr",
            ],
            "each push carries its own project's issue"
        );
    }

    #[tokio::test]
    async fn dangling_review_references_expire_individually_without_aborting_the_batch() {
        let db = migrated_test_db("channel-router-dangling-reviews.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
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
               ('missing-issue', 'reviewer', 'cairn://p/cairn/1790/1/reviewer/create-pr', 'wake', 'event', 'review:missing', 1),
               ('invalid-ref', 'reviewer', 'not-a-cairn-uri', 'wake', 'event', 'review:invalid', 2),
               ('live-review', 'reviewer', 'cairn://p/cairn/1/1/reviewer/create-pr', 'wake', 'event', 'review:live', 3);",
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

    #[tokio::test]
    async fn unchanged_review_backlog_is_generation_bounded() {
        let (orch, db) = route_test_orchestrator("review-generation-bounded.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('issue', 'p', 1, 'Review', 'active', 1, 1);
             -- Complete rather than running: this test is about generation
             -- bounding, and a LIVE recipient would correctly withhold the
             -- notice as an ask an agent still holds (CAIRN-4732), leaving
             -- nothing for the generation fence to bound.
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('reviewer', 'p', 'issue', 'complete', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr', 'reviewer', 'p', 'issue', 'Review', 'feature', 'main', 'open', 1, 1);
             INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at)
               VALUES('push', 'reviewer', 'cairn://p/cairn/1/1/reviewer/create-pr', 'wake', 'event',
                      'review:cairn://p/cairn/1', 1);",
        )
        .await
        .unwrap();
        let provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Away),
        });
        let router = ChannelRouter::new_for_provider(
            orch,
            provider,
            "imessage",
            "+15551234567".into(),
            MessageClassPolicy {
                question: false,
                permission: false,
                notify: true,
            },
            ChannelInboundCapabilities::default(),
        );

        let (first_work, first_live) = router.load_routed_gates(&db, "").await.unwrap();
        let (pending_work, _) = router.load_routed_gates(&db, "").await.unwrap();
        assert_eq!((first_work.len(), first_live.len()), (1, 1));
        assert_eq!(
            pending_work.len(),
            1,
            "unclaimed cached work cannot be stranded"
        );
        let delivery_key = first_work[0].delivery_key(router.provider_id, &router.destination);
        router.claims.claim(&delivery_key);
        let (idle_work, idle_live) = router.load_routed_gates(&db, "").await.unwrap();
        assert!(
            idle_work.is_empty(),
            "a fenced unchanged generation performs no work"
        );
        assert_eq!(
            idle_live.len(),
            1,
            "the cached live set still protects route fencing"
        );

        db.execute(
            "UPDATE issues SET updated_at=updated_at+1 WHERE id='issue'",
            (),
        )
        .await
        .unwrap();
        let (invalidated_work, _) = router.load_routed_gates(&db, "").await.unwrap();
        let (settled_work, _) = router.load_routed_gates(&db, "").await.unwrap();
        assert_eq!(
            invalidated_work.len(),
            1,
            "one mutation causes one bounded refresh"
        );
        assert!(
            settled_work.is_empty(),
            "the refreshed generation settles immediately"
        );
    }

    // Operator presence is process-global, and a review gate is presence-aware:
    // a sibling test pinning presence Active would make this sweep defer its
    // delivery instead of sending it. Joining that serial group is what keeps
    // the assertion about this gate rather than about test interleaving.
    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn pending_review_across_many_sweeps_stays_off_imessage_notify_policy() {
        let (orch, db) = route_test_orchestrator("review-many-sweeps.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('issue', 'p', 3727, 'Frontend minimal slice', 'active', 1, 1);
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('reviewer', 'p', 'issue', 'running', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr', 'reviewer', 'p', 'issue', 'Review', 'feature', 'main', 'open', 1, 1);
             INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at, fingerprint)
               VALUES('push', 'reviewer', 'cairn://p/cairn/3727/1/builder/pr', 'wake', 'event',
                      'review:cairn://p/cairn/3727', 1, 'sha:stable');",
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
                 VALUES(?1, 'reviewer', 'cairn://p/cairn/3727/1/builder/pr', 'wake', 'event',
                        'review:cairn://p/cairn/3727', ?2, ?3)",
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
            0,
            "ordinary review notifications stay off the iMessage attention surface"
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
            Some(0)
        );
    }

    #[test]
    fn shared_channel_attribution_names_the_allowlisted_operator_and_provider() {
        assert_eq!(channel_sender_name("telegram"), "operator via telegram");
        assert_eq!(channel_sender_name("imessage"), "operator via imessage");
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
    fn registered_commands_and_parser_stay_in_agreement() {
        for spec in super::super::commands::CHANNEL_COMMANDS {
            let text = if spec.takes_argument {
                format!("/{} target", spec.name)
            } else {
                format!("/{}", spec.name)
            };
            assert!(
                channel_command(&text).is_some(),
                "registered command did not parse: {text}"
            );
            assert!(starts_with_known_slash_command(&text));
        }

        assert!(channel_command("/not-registered").is_none());
        assert!(!starts_with_known_slash_command("/not-registered"));
    }

    #[test]
    fn a_follow_uri_reads_as_a_thread_name_or_an_issue_number() {
        assert_eq!(
            FollowTarget::parse("cairn://p/cairn/settings-ui").unwrap(),
            FollowTarget::Thread {
                project: "cairn".into(),
                name: "settings-ui".into()
            }
        );
        assert_eq!(
            FollowTarget::parse("cairn://p/cairn/3404").unwrap(),
            FollowTarget::Issue {
                project: "cairn".into(),
                number: 3404
            }
        );
        assert_eq!(
            FollowTarget::parse("cairn://p/cairn/settings-ui")
                .unwrap()
                .uri(),
            "cairn://p/cairn/settings-ui"
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

        let bound_project = parse_uri("cairn://p/cairn/settings-ui")
            .and_then(|uri| uri.project().map(str::to_string));
        let follows = [
            "cairn://p/OTHER/settings-ui",
            "cairn://p/cairn/3404",
            "cairn://p/cairn/settings-ui",
        ];
        let selected = follows.into_iter().find(|uri| {
            FollowTarget::parse(uri).is_ok_and(|target| {
                target.selector().eq_ignore_ascii_case("settings-ui")
                    && Some(target.project()) == bound_project.as_deref()
            })
        });
        assert_eq!(selected, Some("cairn://p/cairn/settings-ui"));
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
                state_cards: false,
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
                state_cards: false,
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
               VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
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
        ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/1", 1, 0)
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
        drain_route_lane_into(&router, OperatorPresence::Present).await;

        {
            let sends = provider.sends.lock().unwrap();
            assert!(sends.is_empty());
        }
        // The sweep minted both utterances and matched nothing: the bundled
        // route only streams while the operator is away.
        assert_eq!(
            db.query_text(
                "SELECT group_concat(route_state, ',') FROM (SELECT route_state FROM fact_log WHERE source = 'thread_stream' ORDER BY id)",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("unmatched,unmatched")
        );
        assert!(ledger::get_by_binding(
            &db,
            CHANNEL,
            "route",
            "route:followed-thread-stream:cairn://p/cairn/1:event:1",
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
        drain_route_lane_into(&restarted, OperatorPresence::Away).await;
        let route_intent = ledger::get_by_binding(
            &db,
            CHANNEL,
            "route",
            "route:followed-thread-stream:cairn://p/cairn/1:event:3",
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
        assert_eq!(firings[0].fact_identity, "cairn://p/cairn/1:event:3");
        assert_eq!(firings[0].status, "fired");
        assert!(
            firings[0].fact_log_id.is_some(),
            "the firing joins its fact log row"
        );
        super::super::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Auto);
    }

    const FOLLOWED_FIXTURE: &str = "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
         INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('i', 'p', 1, 'Followed work', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('e', 'build', 'i', 'p', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
           VALUES ('j', 'e', 'i', 'p', 'running', 'builder', 'builder', 1, 1);
         INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at)
           VALUES ('r', 'j', 'i', 'running', 1, 1);
         INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
           VALUES ('event-1', 'r', 1, 1, 'assistant', '{\"content\":\"first update\",\"toolUses\":[]}', 1),
                  ('event-2', 'r', 2, 2, 'assistant', '{\"content\":\"second update\",\"toolUses\":[]}', 2);";

    async fn followed_orchestrator(
        temp: &std::path::Path,
        name: &str,
        routes: &[(&str, &str)],
    ) -> (Orchestrator, Arc<LocalDb>) {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        let config = temp.join("config");
        std::fs::create_dir_all(config.join("routes")).unwrap();
        for (id, body) in routes {
            std::fs::write(config.join(format!("routes/{id}.yaml")), body).unwrap();
        }
        let db = Arc::new(migrated_test_db(name).await);
        db.execute_batch(FOLLOWED_FIXTURE).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config,
        )
        .boot_at(0)
        .build();
        (orch, db)
    }

    /// Mint one utterance of `cairn://p/cairn/1` the way a following
    /// provider's sweep does.
    async fn mint_utterance(orch: &Orchestrator, db: &LocalDb, event: &str) {
        let rowid = db
            .query_opt_i64(
                "SELECT rowid FROM events WHERE id = ?1",
                (event.to_string(),),
            )
            .await
            .unwrap()
            .unwrap();
        let detail = format!("cairn://p/cairn/1:event:{rowid}");
        crate::routes::mint(
            orch,
            crate::routes::Mint {
                fact: RouteFact {
                    source: "thread_stream".into(),
                    identity: detail.clone(),
                    fields: std::collections::BTreeMap::from([
                        ("project".into(), serde_json::json!("cairn")),
                        ("threadUri".into(), serde_json::json!("cairn://p/cairn/1")),
                        ("detailUri".into(), serde_json::json!(detail)),
                        ("context".into(), serde_json::json!("cairn/1 Followed work")),
                        ("jobId".into(), serde_json::json!("j")),
                    ]),
                    origin: None,
                    summary: Some("update".into()),
                    route_provenance: None,
                },
                record_ref: detail,
                record_id: Some(rowid.to_string()),
                project_id: Some("p"),
                presence: crate::routes::Presence::Away,
                actor_job_id: None,
            },
        )
        .await
        .unwrap();
    }

    const STREAM_ROUTE: &str = "name: Stream\ndescription: every utterance to its followers\nnodes:\n  - id: in\n    type: trigger\n    when: { fact: thread_stream }\n  - id: out\n    type: sink\n    sink: { kind: channel, register: notify }\nedges:\n  - { from: in, to: out }\n";

    /// A route that runs out the route lane's bound must not cost the routes
    /// before it on the same row their deliveries.
    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn a_timed_out_row_still_hands_over_what_earlier_routes_prepared() {
        let temp = tempfile::tempdir().unwrap();
        let hang = format!(
            "name: Hang\ndescription: never answers\nnodes:\n  - id: in\n    type: trigger\n    when: {{ fact: thread_stream }}\n  - id: wait\n    type: response\n    response: {}\n  - id: out\n    type: sink\n    sink: {{ kind: message, target: 'cairn://p/cairn/1' }}\nedges:\n  - {{ from: in, to: wait }}\n  - {{ from: wait, to: out }}\n",
            crate::routes::HANGING_RESPONSE
        );
        let (orch, db) = followed_orchestrator(
            temp.path(),
            "channel-router-timeout-handover.db",
            &[("a-deliver", STREAM_ROUTE), ("b-hang", &hang)],
        )
        .await;
        ledger::follow_target(&db, "imessage", "cairn://p/cairn/1", 1, 0)
            .await
            .unwrap();
        mint_utterance(&orch, &db, "event-1").await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        super::super::route_submission_slot()
            .lock()
            .unwrap()
            .insert("imessage", tx);
        crate::routes::drain_fact_log_within(&orch, std::time::Duration::from_millis(200)).await;
        super::super::route_submission_slot()
            .lock()
            .unwrap()
            .remove("imessage");

        let mut delivered = 0;
        while let Ok(submission) = rx.try_recv() {
            if let RouterSubmission::Route(submission) = submission {
                if submission.route_id == "a-deliver" {
                    delivered += 1;
                }
            }
        }
        assert_eq!(
            delivered, 1,
            "the finished route's delivery reaches its router once"
        );
        let row = crate::storage::get_fact(&db, 1).await.unwrap().unwrap();
        assert_eq!(
            (row.route_state.as_str(), row.route_note.as_deref()),
            ("failed", Some("timeout"))
        );
    }

    /// One utterance followed on several providers is one log row, and the
    /// route lane hands each live follower its own copy -- and nobody else.
    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn one_utterance_is_one_row_and_one_delivery_per_live_follower() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = followed_orchestrator(
            temp.path(),
            "channel-router-two-followers.db",
            &[("stream", STREAM_ROUTE)],
        )
        .await;
        let target = "cairn://p/cairn/1";
        ledger::follow_target(&db, "imessage", target, 1, 0)
            .await
            .unwrap();
        ledger::follow_target(&db, "telegram", target, 1, 0)
            .await
            .unwrap();
        // Discord began following long after the utterance was said.
        ledger::follow_target(&db, "discord", target, 10_000_000, 0)
            .await
            .unwrap();

        // Two providers' sweeps observe the same utterance.
        mint_utterance(&orch, &db, "event-1").await;
        mint_utterance(&orch, &db, "event-1").await;
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM fact_log", ())
                .await
                .unwrap(),
            Some(1)
        );
        let row = crate::storage::get_fact(&db, 1).await.unwrap().unwrap();
        let stored: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&row.fields_json).unwrap();
        assert!(
            !stored.contains_key("text") && !row.fields_json.contains("first update"),
            "the utterance stays in the transcript: {}",
            row.fields_json
        );
        let hydrated = crate::routes::log::hydrate(&orch, &row).await.unwrap();
        assert_eq!(hydrated.fact.fields["text"], "first update");

        let providers = ["imessage", "telegram", "discord"];
        let mut receivers = Vec::new();
        for provider in providers {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            super::super::route_submission_slot()
                .lock()
                .unwrap()
                .insert(provider, tx);
            receivers.push(rx);
        }
        crate::routes::drain_fact_log(&orch).await;
        for provider in providers {
            super::super::route_submission_slot()
                .lock()
                .unwrap()
                .remove(provider);
        }
        let received: Vec<Vec<Option<String>>> = receivers
            .iter_mut()
            .map(|rx| {
                let mut addressed = Vec::new();
                while let Ok(RouterSubmission::Route(submission)) = rx.try_recv() {
                    if submission.route_id == "stream" {
                        addressed.push(submission.provider);
                    }
                }
                addressed
            })
            .collect();
        assert_eq!(
            received,
            vec![
                vec![Some("imessage".to_string())],
                vec![Some("telegram".to_string())],
                vec![],
            ],
            "each live follower receives exactly its own copy"
        );

        for provider in providers {
            ledger::unfollow_target(&db, provider, target)
                .await
                .unwrap();
        }
        mint_utterance(&orch, &db, "event-2").await;
        crate::routes::drain_fact_log(&orch).await;
        let firings = crate::storage::list_route_firings(&db, "workspace", "stream", 10)
            .await
            .unwrap();
        assert_eq!(
            (
                firings[0].status.as_str(),
                firings[0].drop_reason.as_deref()
            ),
            ("dropped", Some("no_follower"))
        );
    }

    /// The channel sweep only mints: a route whose response never answers
    /// cannot hold it up (cairn/4480).
    #[tokio::test]
    async fn the_follow_sweep_never_waits_on_a_route() {
        let temp = tempfile::tempdir().unwrap();
        let hang = "name: Hang\ndescription: never answers\nnodes:\n  - id: in\n    type: trigger\n    when: { fact: thread_stream }\n  - id: wait\n    type: response\n    response: test-hang\n  - id: out\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/1' }\nedges:\n  - { from: in, to: wait }\n  - { from: wait, to: out }\n";
        let (orch, db) = followed_orchestrator(
            temp.path(),
            "channel-router-hung-route.db",
            &[("hang", hang)],
        )
        .await;
        ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/1", 1, 0)
            .await
            .unwrap();
        let router = ChannelRouter::new(
            orch,
            Arc::new(PresentProvider {
                sends: Mutex::new(Vec::new()),
                presence_checks: Mutex::new(0),
                presence: Mutex::new(OperatorPresence::Away),
            }),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );
        tokio::time::timeout(Duration::from_secs(1), router.sweep_followed_updates())
            .await
            .expect("the sweep returns without waiting on routes")
            .unwrap();
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM fact_log WHERE route_state = 'pending'",
                ()
            )
            .await
            .unwrap(),
            Some(2),
            "both utterances are minted and left for the worker"
        );
    }

    /// A followed thread's utterance is minted as that thread's own row, so a
    /// standing trigger it matches wakes every other thread holding it but
    /// never the thread that said it -- which would otherwise wake itself on
    /// each reply that cites what it watches.
    #[tokio::test]
    async fn a_thread_is_never_woken_by_its_own_followed_utterance() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) =
            followed_orchestrator(temp.path(), "channel-router-own-utterance.db", &[]).await;
        db.execute_batch(
            "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
               VALUES ('t-speaker', 'p', 'speaker', 'active', 1, 1),
                      ('t-listener', 'p', 'listener', 'active', 1, 1);
             INSERT INTO jobs (id, thread_id, project_id, status, uri_segment, created_at, updated_at)
               VALUES ('speaker', 't-speaker', 'p', 'running', 'thread', 1, 1),
                      ('listener', 't-listener', 'p', 'running', 'thread', 1, 1);
             INSERT INTO runs (id, job_id, status, created_at, updated_at)
               VALUES ('speaker-run', 'speaker', 'running', 1, 1);
             INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('event-speaker', 'speaker-run', 1, 3, 'assistant', '{\"content\":\"see cairn://posts/7\",\"toolUses\":[]}', 3);",
        )
        .await
        .unwrap();
        let target = "cairn://p/cairn/speaker";
        let clause: Vec<crate::routes::TriggerClause> = serde_json::from_value(
            serde_json::json!([{ "fact": "thread_stream", "threadUri": target }]),
        )
        .unwrap();
        for job in ["speaker", "listener"] {
            let clause = clause.clone();
            db.write(move |conn| {
                let clause = clause.clone();
                Box::pin(async move {
                    crate::orchestrator::wakes::rebuild_derived_thread_subscriptions_conn(
                        conn, job, &clause,
                    )
                    .await
                })
            })
            .await
            .unwrap();
        }
        ledger::follow_target(&db, CHANNEL, target, 1, 0)
            .await
            .unwrap();
        let router = ChannelRouter::new(
            orch,
            Arc::new(PresentProvider {
                sends: Mutex::new(Vec::new()),
                presence_checks: Mutex::new(0),
                presence: Mutex::new(OperatorPresence::Away),
            }),
            IMessageChannelConfig {
                enabled: true,
                to: "+15551234567".into(),
                ..Default::default()
            },
        );

        router.sweep_followed_updates().await.unwrap();
        crate::routes::drain_fact_log(&router.orch).await;

        let (actor, wakes) = db
            .query_one(
                "SELECT actor_job_id, wakes_json FROM fact_log WHERE source = 'thread_stream'",
                (),
                |row| {
                    Ok((
                        crate::storage::RowExt::opt_text(row, 0)?,
                        crate::storage::RowExt::opt_text(row, 1)?,
                    ))
                },
            )
            .await
            .unwrap();
        let woken: Vec<String> =
            serde_json::from_str::<Vec<serde_json::Value>>(wakes.as_deref().unwrap_or("[]"))
                .unwrap()
                .iter()
                .filter_map(|wake| wake["job"].as_str().map(str::to_owned))
                .collect();
        assert_eq!(
            woken,
            vec!["listener".to_string()],
            "the other thread wakes; the speaker does not wake itself"
        );
        assert_eq!(
            actor.as_deref(),
            Some("speaker"),
            "the utterance's row names the job that said it"
        );
    }

    /// Run the fact log's lanes, then hand this router every channel submission
    /// the route lane addressed to its provider, the way its route task does.
    async fn drain_route_lane_into(router: &ChannelRouter, presence: OperatorPresence) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        super::super::route_submission_slot()
            .lock()
            .unwrap()
            .insert(router.provider_id, tx);
        crate::routes::drain_fact_log(&router.orch).await;
        super::super::route_submission_slot()
            .lock()
            .unwrap()
            .remove(router.provider_id);
        while let Ok(submission) = rx.try_recv() {
            let RouterSubmission::Route(submission) = submission else {
                continue;
            };
            router
                .submit_route(*submission, presence, Instant::now())
                .await
                .unwrap();
        }
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
               VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
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
                conversation: "imessage:+15551234567".into(),
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
        deliveries: Mutex<Vec<OutboundMessage>>,
        next_guid: std::sync::atomic::AtomicUsize,
        cleanups: std::sync::atomic::AtomicUsize,
    }

    impl RecordingPollProvider {
        fn new() -> Self {
            Self {
                sends: Mutex::new(Vec::new()),
                deliveries: Mutex::new(Vec::new()),
                next_guid: std::sync::atomic::AtomicUsize::new(1),
                cleanups: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChannelProvider for RecordingPollProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities {
                state_cards: false,
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
            self.deliveries.lock().unwrap().push(message.clone());
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

        async fn cleanup_question(&self, _message: &ResolvedQuestionMessage) -> Result<(), String> {
            self.cleanups
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn follow_poll_selection_focuses_on_tap_and_only_deselection_unfollows() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(migrated_test_db("channel-router-standing-thread-poll.db").await);
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
               VALUES ('t1', 'p', 'general', 'active', 1, 3),
                      ('t2', 'p', 'performance', 'active', 1, 2);",
        )
        .await
        .unwrap();
        assert!(
            ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/general", 1, 0)
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
        let provider = Arc::new(RecordingPollProvider::new());
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
            "✓ general": "cairn://p/cairn/general",
            "performance": "cairn://p/cairn/performance"
        })
        .to_string();
        db.execute(
            "UPDATE channel_outbound SET options_json = ?1 WHERE provider_guid = 'poll-1'",
            params![legacy_bindings],
        )
        .await
        .unwrap();

        router
            .send_notice("+15551234567", "intervening routed update")
            .await
            .unwrap();
        router.sweep_live_gates().await.unwrap();
        assert_eq!(
            provider.cleanups.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "ordinary traffic and the next gate sweep must not clear a command poll"
        );

        router
            .handle_inbound(InboundEvent::Reply {
                conversation: "imessage:+15551234567".into(),
                bound_guid: "poll-1".into(),
                sender: "+15551234567".into(),
                text: "1".into(),
            })
            .await
            .unwrap();
        assert!(
            ledger::is_target_followed(&db, CHANNEL, "cairn://p/cairn/general")
                .await
                .unwrap()
        );
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/cairn/general"),
            "tap-only providers focus an existing follow instead of toggling it off"
        );
        router
            .handle_inbound(InboundEvent::Reply {
                conversation: "imessage:+15551234567".into(),
                bound_guid: "poll-1".into(),
                sender: "+15551234567".into(),
                text: "1".into(),
            })
            .await
            .unwrap();
        assert!(
            ledger::is_target_followed(&db, CHANNEL, "cairn://p/cairn/general")
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
                    conversation: "imessage:+15551234567".into(),
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
            !ledger::is_target_followed(&db, CHANNEL, "cairn://p/cairn/general")
                .await
                .unwrap()
        );
        for selected in [true, true] {
            router
                .handle_inbound(InboundEvent::Selection {
                    conversation: "imessage:+15551234567".into(),
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
            ledger::is_target_followed(&db, CHANNEL, "cairn://p/cairn/general")
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
                matches!(sends.iter().rev().find(|ask| matches!(ask, OutboundAsk::Question { .. })), Some(OutboundAsk::Question { options, .. }) if options[0].label.starts_with("✓ "))
            );
        }
        let polls = ledger::list_unresolved(&db, CHANNEL)
            .await
            .unwrap()
            .into_iter()
            .filter(|record| record.binding_ref.starts_with(FOLLOW_POLL_PREFIX))
            .collect::<Vec<_>>();
        assert_eq!(polls.len(), 1);
        assert!(polls.iter().all(|poll| poll.status == "sent"));
        assert_eq!(
            provider.cleanups.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a newer /threads replaces the old keyboard after the new poll is delivered"
        );

        ledger::insert_intent(
            &db,
            &ledger::NewOutbound {
                id: "stream-intent",
                channel: CHANNEL,
                kind: "review",
                binding_ref: "cairn://p/cairn/general:event:42",
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
                conversation: "imessage:+15551234567".into(),
                bound_guid: "stream-guid".into(),
                sender: "+15551234567".into(),
                text: "unfollow".into(),
            })
            .await
            .unwrap();
        assert!(
            !ledger::is_target_followed(&db, CHANNEL, "cairn://p/cairn/general")
                .await
                .unwrap()
        );
        assert!(matches!(
            provider.sends.lock().unwrap().last(),
            Some(OutboundAsk::Notify { text }) if text == "Unfollowed cairn://p/cairn/general."
        ));
        let stream = ledger::get_by_provider_guid(&db, CHANNEL, "stream-guid")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream.status, "resolved");
    }

    #[tokio::test]
    async fn slash_commands_switch_focus_and_unfollow_without_provider_deselection() {
        let (orch, db) = route_test_orchestrator("channel-router-slash-controls.db").await;
        seed_threads_and_an_issue(&db).await;
        ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/general", 1, 0)
            .await
            .unwrap();
        ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/performance", 2, 0)
            .await
            .unwrap();
        ledger::set_focus(&db, CHANNEL, "cairn://p/cairn/performance", 2)
            .await
            .unwrap();
        let (router, provider) = test_router(orch);

        ledger::insert_intent(
            &db,
            &ledger::NewOutbound {
                id: "bound-update",
                channel: CHANNEL,
                kind: "route",
                binding_ref: "route:follow:cairn://p/cairn/performance:event:9",
                conversation: "operator",
                job_id: None,
                rendered_text: "update",
                rendering: "text",
                created_at: 3,
            },
        )
        .await
        .unwrap();
        ledger::mark_sent(&db, "bound-update", "bound-guid", None, None, 3)
            .await
            .unwrap();

        for (id, guid, created_at) in [
            ("active-ask-one", "ask-guid-one", 4),
            ("active-ask-two", "ask-guid-two", 5),
        ] {
            ledger::insert_intent(
                &db,
                &ledger::NewOutbound {
                    id,
                    channel: CHANNEL,
                    kind: "question",
                    binding_ref: id,
                    conversation: "operator",
                    job_id: None,
                    rendered_text: "question",
                    rendering: "text",
                    created_at,
                },
            )
            .await
            .unwrap();
            ledger::mark_sent(&db, id, guid, None, None, created_at)
                .await
                .unwrap();
        }

        router
            .resolve_bound("bound-guid", "operator", "operator", "/focus general")
            .await
            .unwrap();
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/cairn/general")
        );
        assert!(matches!(
            provider.sends.lock().unwrap().last(),
            Some(OutboundAsk::Notify { text }) if text.starts_with("Focused general")
        ));

        let messages_before_commands = db
            .query_one("SELECT COUNT(*) FROM messages", (), |row| row.i64(0))
            .await
            .unwrap();
        router
            .resolve_bare("operator", "operator", "/unfollow")
            .await
            .unwrap();
        assert!(
            !ledger::is_target_followed(&db, CHANNEL, "cairn://p/cairn/general")
                .await
                .unwrap()
        );
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/cairn/performance"),
            "unfollowing the focus moves loose-message routing to a remaining follow"
        );
        assert!(matches!(
            provider.sends.lock().unwrap().last(),
            Some(OutboundAsk::Notify { text }) if text == "Unfollowed general."
        ));
        router
            .resolve_bare("operator", "operator", "/help")
            .await
            .unwrap();
        assert!(matches!(
            provider.sends.lock().unwrap().last(),
            Some(OutboundAsk::Notify { text })
                if text.contains("/unfollow [name]")
                    && text.contains("stops the current focus")
        ));
        router
            .resolve_bare("operator", "operator", "/focus general extra")
            .await
            .unwrap();
        assert!(matches!(
            provider.sends.lock().unwrap().last(),
            Some(OutboundAsk::Notify { text }) if text.starts_with("Invalid command usage.")
        ));
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM messages", (), |row| row.i64(0))
                .await
                .unwrap(),
            messages_before_commands,
            "known loose commands are neither ask answers nor routed chat text"
        );

        router
            .resolve_bare("operator", "operator", "ambiguous answer")
            .await
            .unwrap();
        assert!(matches!(
            provider.sends.lock().unwrap().last(),
            Some(OutboundAsk::Notify { text }) if text.starts_with("I found more than one active ask.")
        ));
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM messages", (), |row| row.i64(0))
                .await
                .unwrap(),
            messages_before_commands,
            "an ambiguous free-text answer is not routed to the focused thread"
        );
    }

    #[tokio::test]
    async fn replying_to_a_followed_update_routes_with_each_providers_attribution() {
        for (provider_id, db_name) in [
            ("imessage", "channel-router-imessage-route-reply.db"),
            ("telegram", "channel-router-telegram-route-reply.db"),
        ] {
            let (orch, db) = route_test_orchestrator(db_name).await;
            seed_threads_and_an_issue(&db).await;
            let provider = Arc::new(RecordingPollProvider::new());
            let router = ChannelRouter::new_for_provider(
                orch,
                provider,
                provider_id,
                "operator".into(),
                MessageClassPolicy::default(),
                ChannelInboundCapabilities {
                    permissions: true,
                    answers: true,
                    free_text: true,
                },
            );
            ledger::insert_intent(
                &db,
                &ledger::NewOutbound {
                    id: "route-update",
                    channel: provider_id,
                    kind: "route",
                    binding_ref: "route:follow:cairn://p/cairn/general:event:42",
                    conversation: "operator",
                    job_id: None,
                    rendered_text: "thread update",
                    rendering: "text",
                    created_at: 10,
                },
            )
            .await
            .unwrap();
            ledger::mark_sent(&db, "route-update", "route-guid", None, None, 10)
                .await
                .unwrap();

            router
                .resolve_bound("route-guid", "operator", "operator", "channel reply")
                .await
                .unwrap();

            for channel_type in ["thread", "direct"] {
                assert_eq!(
                    db.query_one(
                        "SELECT sender_name, content FROM messages WHERE channel_type = ?1 ORDER BY created_at DESC LIMIT 1",
                        params![channel_type],
                        |row| Ok((row.text(0)?, row.text(1)?)),
                    )
                    .await
                    .unwrap(),
                    (
                        format!("operator via {provider_id}"),
                        "channel reply".to_string()
                    ),
                    "{channel_type} delivery must preserve channel attribution"
                );
            }
        }
    }

    /// A project with two first-class threads, a live session for the first, a
    /// sub-agent task hanging off that session, and one active issue — the shape
    /// the phone has to tell apart. Returns the session job's id.
    async fn seed_threads_and_an_issue(db: &LocalDb) -> String {
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
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
        let provider = Arc::new(RecordingPollProvider::new());
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

    #[tokio::test]
    async fn capability_rejection_notice_returns_to_the_arriving_discord_conversation() {
        let (orch, _db) = route_test_orchestrator("channel-router-discord-bounded-notice.db").await;
        let provider = Arc::new(RecordingPollProvider::new());
        let router = ChannelRouter::new_for_provider(
            orch,
            provider.clone(),
            "discord",
            "discord-user-snowflake".into(),
            MessageClassPolicy::default(),
            ChannelInboundCapabilities {
                permissions: true,
                answers: true,
                free_text: false,
            },
        );

        router
            .handle_inbound(InboundEvent::Bare {
                conversation: "discord:42/7".into(),
                sender: "discord-user-snowflake".into(),
                text: "route this".into(),
            })
            .await
            .unwrap();

        let deliveries = provider.deliveries.lock().unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].conversation, "discord:42/7");
        assert!(matches!(
            &deliveries[0].ask,
            OutboundAsk::Notify { text } if text.contains("Free-text messages are disabled")
        ));
    }

    #[tokio::test]
    async fn unbound_discord_bare_and_stale_reply_reject_at_the_arriving_conversation() {
        let (orch, db) = route_test_orchestrator("channel-router-discord-unbound-inbound.db").await;
        seed_threads_and_an_issue(&db).await;
        ledger::follow_target(&db, "discord", "cairn://p/cairn/general", 1, 0)
            .await
            .unwrap();
        ledger::set_focus(&db, "discord", "cairn://p/cairn/general", 2)
            .await
            .unwrap();
        let provider = Arc::new(RecordingPollProvider::new());
        let router = ChannelRouter::new_for_provider(
            orch,
            provider.clone(),
            "discord",
            "discord:42/99".into(),
            MessageClassPolicy::default(),
            ChannelInboundCapabilities {
                permissions: true,
                answers: true,
                free_text: true,
            },
        );

        for event in [
            InboundEvent::Bare {
                conversation: "discord:42/7".into(),
                sender: "operator".into(),
                text: "loose input".into(),
            },
            InboundEvent::Reply {
                conversation: "discord:42/8".into(),
                bound_guid: "stale-guid".into(),
                sender: "operator".into(),
                text: "stale reply".into(),
            },
            InboundEvent::Selection {
                conversation: "discord:42/9".into(),
                bound_guid: "stale-component".into(),
                sender: "operator".into(),
                option_id: "approve".into(),
                option_text: "Approve".into(),
                selected: true,
            },
        ] {
            router.handle_inbound(event).await.unwrap();
        }

        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM messages", (), |row| row.i64(0))
                .await
                .unwrap(),
            0,
            "unbound Discord input must not fall through to the legacy global focus"
        );
        let deliveries = provider.deliveries.lock().unwrap();
        assert_eq!(
            deliveries
                .iter()
                .map(|message| message.conversation.as_str())
                .collect::<Vec<_>>(),
            vec!["discord:42/7", "discord:42/8", "discord:42/9"]
        );
        assert!(deliveries[..2].iter().all(|message| matches!(
            &message.ask,
            OutboundAsk::Notify { text } if text.contains("not bound")
        )));
        assert!(matches!(
            &deliveries[2].ask,
            OutboundAsk::Notify { text } if text.contains("No active ask")
        ));
    }

    #[test]
    fn capabilities_are_independently_admitted() {
        let capabilities = ChannelInboundCapabilities {
            permissions: true,
            answers: false,
            free_text: true,
        };
        assert!(admits_capability(
            capabilities,
            InboundCapability::Permissions
        ));
        assert!(!admits_capability(capabilities, InboundCapability::Answers));
        assert!(admits_capability(capabilities, InboundCapability::FreeText));
    }

    #[test]
    fn inbound_capabilities_migrate_legacy_forms_and_round_trip_exactly() {
        for (legacy, expected) in [
            ("open", (true, true, true)),
            ("bounded", (true, true, false)),
            ("outbound_only", (false, false, false)),
        ] {
            let config: crate::models::TelegramChannelConfig =
                serde_json::from_value(serde_json::json!({
                    "inboundPolicy": legacy
                }))
                .unwrap();
            assert_eq!(
                (
                    config.inbound_capabilities.permissions,
                    config.inbound_capabilities.answers,
                    config.inbound_capabilities.free_text,
                ),
                expected
            );
        }
        let omitted: crate::models::TelegramChannelConfig =
            serde_json::from_value(serde_json::json!({
                "enabled": true,
                "chatId": "1",
                "allowFrom": ["2"],
                "route": {}
            }))
            .unwrap();
        assert_eq!(
            omitted.inbound_capabilities,
            ChannelInboundCapabilities {
                permissions: true,
                answers: true,
                free_text: true,
            }
        );
        let exact = crate::models::TelegramChannelConfig {
            inbound_capabilities: ChannelInboundCapabilities {
                permissions: false,
                answers: true,
                free_text: true,
            },
            ..Default::default()
        };
        let json = serde_json::to_value(&exact).unwrap();
        assert_eq!(
            json["inboundCapabilities"],
            serde_json::json!({
                "permissions": false,
                "answers": true,
                "freeText": true
            })
        );
        assert_eq!(
            serde_json::from_value::<crate::models::TelegramChannelConfig>(json).unwrap(),
            exact
        );
    }

    #[tokio::test]
    async fn resumed_session_preserves_backlog_and_follow_cursor() {
        let (orch, db) = route_test_orchestrator("channel-router-resumed-session.db").await;
        seed_run(&db).await;
        seed_prompt(&db, "during-outage", 500).await;
        db.execute_batch(
            "INSERT INTO channel_conversation_binding
               (provider, conversation, target_uri, binding_kind, message_classes, followed_at, cursor_rowid, selected_at)
             VALUES ('imessage', 'imessage:legacy', 'cairn:~/', 'follow', 7, 4, 30, 4);",
        )
        .await
        .unwrap();
        let (router, _provider) = test_router(orch);

        router
            .draw_the_session_line(SessionBoundary::Resumed)
            .await
            .unwrap();

        let follows = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].cursor_rowid, 30);
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
                .await
                .unwrap(),
            Some(0),
            "asks raised during a short gateway outage remain live"
        );
    }

    #[tokio::test]
    async fn startup_drops_an_orphaned_home_relative_focus() {
        let (orch, db) = route_test_orchestrator("channel-router-relative-focus.db").await;
        db.execute(
            "INSERT INTO channel_conversation_binding
               (provider, conversation, target_uri, binding_kind, message_classes, followed_at, selected_at)
             VALUES ('imessage', 'imessage:legacy', 'cairn:~/', 'follow', 7, 4, 4)",
            (),
        )
        .await
        .unwrap();
        let (router, _provider) = test_router(orch);

        router
            .draw_the_session_line(SessionBoundary::Fresh)
            .await
            .unwrap();

        assert_eq!(ledger::get_focus(&db, CHANNEL).await.unwrap(), None);
    }

    #[tokio::test]
    async fn startup_drops_a_home_relative_follow_and_its_focus() {
        let (orch, db) = route_test_orchestrator("channel-router-relative-follow.db").await;
        db.execute_batch(
            "INSERT INTO channel_conversation_binding
               (provider, conversation, target_uri, binding_kind, message_classes, followed_at, cursor_rowid, selected_at)
             VALUES ('imessage', 'imessage:legacy', 'cairn:~/', 'follow', 7, 4, 30, 4);",
        )
        .await
        .unwrap();
        let (router, _provider) = test_router(orch);

        router
            .draw_the_session_line(SessionBoundary::Fresh)
            .await
            .unwrap();

        assert!(ledger::list_follows(&db, CHANNEL).await.unwrap().is_empty());
        assert_eq!(ledger::get_focus(&db, CHANNEL).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_session_relative_poll_binding_persists_its_canonical_thread_uri() {
        let (orch, db) = route_test_orchestrator("channel-router-relative-binding.db").await;
        let session = seed_threads_and_an_issue(&db).await;
        let (router, _provider) = test_router(orch);
        let options = FollowPollOptions {
            labels: vec!["general".to_string()],
            bindings: HashMap::from([("general".to_string(), "cairn:~/".to_string())]),
        };
        let record = ledger::OutboundRecord {
            id: "relative-poll".to_string(),
            channel: CHANNEL.to_string(),
            kind: "question".to_string(),
            binding_ref: format!("{FOLLOW_POLL_PREFIX}relative"),
            conversation: "+15551234567".to_string(),
            job_id: Some(session),
            rendered_text: "Follow threads\n1. general".to_string(),
            rendering: "poll".to_string(),
            options_json: Some(serde_json::to_string(&options).unwrap()),
            status: "sent".to_string(),
            provider_guid: Some("poll-guid".to_string()),
            caption_guid: None,
            created_at: 1,
            sent_at: Some(1),
            resolved_at: None,
            last_error: None,
            attempts: 0,
            next_attempt_at: None,
            last_attempt_at: None,
        };

        assert_eq!(
            router
                .resolve_bound_target(&record, "cairn:~/task/example")
                .await
                .unwrap(),
            "cairn://p/cairn/general/task/example"
        );

        router
            .follow_poll_selection(&record, "general", &record.conversation)
            .await
            .unwrap();

        let follows = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].uri, "cairn://p/cairn/general");
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/cairn/general")
        );
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
                ("general".to_string(), "cairn://p/cairn/general".to_string()),
                (
                    "performance".to_string(),
                    "cairn://p/cairn/performance".to_string()
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
                "cairn://p/cairn/3757".to_string()
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
                    "cairn://p/agg/general".to_string()
                ),
                (
                    "cairn/general".to_string(),
                    "cairn://p/cairn/general".to_string()
                ),
                (
                    "cairn/performance".to_string(),
                    "cairn://p/cairn/performance".to_string()
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
            ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/3404", 5, 40)
                .await
                .unwrap()
        );
        ledger::set_focus(&db, CHANNEL, "cairn://p/cairn/3404", 5)
            .await
            .unwrap();
        let (router, _provider) = test_router(orch);

        router
            .draw_the_session_line(SessionBoundary::Fresh)
            .await
            .unwrap();

        let follows = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(
            follows.iter().map(|f| f.uri.as_str()).collect::<Vec<_>>(),
            vec!["cairn://p/cairn/general"],
            "one target keeps one identity"
        );
        assert_eq!(follows[0].followed_at, 5, "the follow is as old as it was");
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/cairn/general"),
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
        ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/3404", 5, 90)
            .await
            .unwrap();
        ledger::follow_target(&db, CHANNEL, "cairn://p/cairn/general", 9, 40)
            .await
            .unwrap();

        ledger::canonicalize_follow(
            &db,
            CHANNEL,
            "cairn://p/cairn/3404",
            "cairn://p/cairn/general",
        )
        .await
        .unwrap();

        let follows = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].uri, "cairn://p/cairn/general");
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
        let followed = HashSet::from(["cairn://p/cairn/performance".to_string()]);
        let (router, _provider) = test_router(orch);

        let (targets, total) = router.active_threads(&followed).await.unwrap();

        assert_eq!(targets.len(), FOLLOW_POLL_LIMIT);
        assert_eq!(total, FOLLOW_POLL_LIMIT + 2);
        assert_eq!(
            targets[0],
            (
                "performance".to_string(),
                "cairn://p/cairn/performance".to_string()
            ),
            "the followed thread leads, then the most recently active"
        );
        assert_eq!(targets[1].1, "cairn://p/cairn/busy-9");
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
            .resolve_target("cairn://p/cairn/general")
            .await
            .unwrap();
        assert_eq!(
            target,
            FollowTarget::Thread {
                project: "cairn".into(),
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
            vec![(session_rowid, "cairn/general".to_string())]
        );
        assert!(assistant_text(&events[0].data).as_deref() == Some("the thread speaking"));

        // A fresh follow starts at the live edge, so nothing already said is
        // replayed onto the phone.
        router.follow("cairn://p/cairn/general").await.unwrap();
        let follow = ledger::list_follows(&db, CHANNEL).await.unwrap();
        assert_eq!(follow[0].uri, "cairn://p/cairn/general");
        assert_eq!(follow[0].cursor_rowid, session_rowid);
        assert!(router
            .followed_events(&target, follow[0].cursor_rowid)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            ledger::get_focus(&db, CHANNEL).await.unwrap().as_deref(),
            Some("cairn://p/cairn/general"),
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
            .resolve_target("cairn://p/cairn/general")
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
            (
                "t-general".to_string(),
                "operator via imessage".to_string(),
                "ship it".to_string()
            )
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
            .resolve_target("cairn://p/cairn/performance")
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
            project: "cairn".into(),
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
            router.resolve_target("cairn://p/cairn/3404").await.unwrap(),
            FollowTarget::Thread {
                project: "cairn".into(),
                name: "general".into()
            }
        );
        assert_eq!(
            router.resolve_target("cairn://p/cairn/3757").await.unwrap(),
            FollowTarget::Issue {
                project: "cairn".into(),
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
                origin: None,
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
            destination: None,
            fact_log_id: None,
            provider: None,
        }
    }

    async fn route_test_orchestrator(name: &str) -> (Orchestrator, Arc<LocalDb>) {
        use crate::db::DbState;
        use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(migrated_test_db(name).await);
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        (
            Orchestrator::builder(
                Arc::new(DbState::new(db.clone(), search)),
                Arc::new(
                    TestServicesBuilder::new()
                        .with_process(RecordingProcessSpawner::new().clone())
                        .build(),
                ),
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
                    route_submission("cairn://p/cairn/3727:review_ready"),
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
    async fn directed_route_uses_addressed_conversation_in_provider_and_ledger() {
        let (orch, db) = route_test_orchestrator("route-directed-conversation.db").await;
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
                to: "default@example.com".into(),
                ..Default::default()
            },
        );
        let mut submission = route_submission("directed");
        submission.destination = Some("imessage:TARGET@example.com".parse().unwrap());
        router
            .submit_route(submission, OperatorPresence::Away, Instant::now())
            .await
            .unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].conversation, "target@example.com");
        }
        let outbound = ledger::get_by_binding(&db, CHANNEL, "route", "route:route-test:directed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outbound.conversation, "target@example.com");
        assert_eq!(outbound.status, "sent");
    }

    /// Accepts exactly what `DiscordProvider` accepts: it resolves every send
    /// through the real provider's address check, so a conversation the live
    /// provider would refuse fails here the same way.
    struct DiscordAddressProvider {
        guild: u64,
        sends: Mutex<Vec<OutboundMessage>>,
    }

    #[async_trait::async_trait]
    impl ChannelProvider for DiscordAddressProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities::default()
        }

        async fn send(
            &self,
            message: &OutboundMessage,
        ) -> Result<crate::channels::SentIds, String> {
            cairn_channels::discord::discord_channel_in_guild(&message.conversation, self.guild)?;
            self.sends.lock().unwrap().push(message.clone());
            Ok(crate::channels::SentIds {
                primary_guid: "discord-message".into(),
                caption_guid: None,
            })
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Ready
        }
    }

    fn discord_route_router(
        orch: Orchestrator,
        provider: Arc<DiscordAddressProvider>,
    ) -> ChannelRouter {
        ChannelRouter::new_for_provider(
            orch,
            provider,
            "discord",
            // A Discord router's default destination is its guild id, which is
            // never itself deliverable; a directed route must not fall back to it.
            "1".into(),
            MessageClassPolicy::default(),
            ChannelInboundCapabilities::default(),
        )
    }

    #[tokio::test]
    async fn directed_discord_route_sends_to_the_canonical_conversation_address() {
        let (orch, db) = route_test_orchestrator("route-directed-discord.db").await;
        let provider = Arc::new(DiscordAddressProvider {
            guild: 1,
            sends: Mutex::new(Vec::new()),
        });
        let router = discord_route_router(orch, provider.clone());
        let mut submission = route_submission("directed");
        submission.destination = Some("discord:1/99".parse().unwrap());
        router
            .submit_route(submission, OperatorPresence::Away, Instant::now())
            .await
            .unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].conversation, "discord:1/99");
        }
        let outbound = ledger::get_by_binding(&db, "discord", "route", "route:route-test:directed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outbound.conversation, "discord:1/99");
        assert_eq!(outbound.status, "sent", "{:?}", outbound.last_error);
    }

    /// A directed Discord route recorded before routes carried the canonical
    /// address sits under the bare channel id, failed, for the same binding. Its
    /// re-fire lands on a new canonical row, and the route's history must report
    /// that row's delivery rather than the stranded one's failure.
    #[tokio::test]
    async fn stranded_bare_discord_route_row_does_not_shadow_the_canonical_delivery() {
        let (orch, db) = route_test_orchestrator("route-directed-discord-legacy.db").await;
        ledger::insert_intent(
            &db,
            &ledger::NewOutbound {
                id: "legacy-bare",
                channel: "discord",
                kind: "route",
                binding_ref: "route:route-test:directed",
                conversation: "99",
                job_id: None,
                rendered_text: "notify",
                rendering: "text",
                created_at: 1,
            },
        )
        .await
        .unwrap();
        assert!(ledger::begin_delivery(&db, "legacy-bare", 2).await.unwrap());
        ledger::mark_failed(
            &db,
            "legacy-bare",
            "conversation address `99` must begin with a provider followed by ':'",
            2,
            None,
        )
        .await
        .unwrap();
        let provider = Arc::new(DiscordAddressProvider {
            guild: 1,
            sends: Mutex::new(Vec::new()),
        });
        let router = discord_route_router(orch, provider.clone());
        let mut submission = route_submission("directed");
        submission.destination = Some("discord:1/99".parse().unwrap());
        router
            .submit_route(submission, OperatorPresence::Away, Instant::now())
            .await
            .unwrap();

        assert_eq!(provider.sends.lock().unwrap().len(), 1);
        let firing = crate::storage::list_route_firings(&db, "workspace", "route-test", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(firing.status, "fired", "{:?}", firing.error);
    }

    /// A directed Discord route left pending before routes carried the canonical
    /// address: keyed by the bare channel id, its durable submission naming the
    /// canonical conversation.
    async fn seed_legacy_pending_discord_route(db: &LocalDb, id: &str) -> ChannelSubmission {
        let mut submission = route_submission("directed");
        submission.destination = Some("discord:1/99".parse().unwrap());
        ledger::insert_intent(
            db,
            &ledger::NewOutbound {
                id,
                channel: "discord",
                kind: "route",
                binding_ref: &submission.binding_ref,
                conversation: "99",
                job_id: None,
                rendered_text: "notify",
                rendering: "text",
                created_at: 1,
            },
        )
        .await
        .unwrap();
        ledger::set_pending_route_submission(db, id, &serde_json::to_string(&submission).unwrap())
            .await
            .unwrap();
        submission
    }

    async fn discord_route_rows(db: &LocalDb) -> Option<i64> {
        db.query_opt_i64(
            "SELECT COUNT(*) FROM channel_outbound WHERE channel = 'discord' AND kind = 'route'",
            (),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn legacy_pending_discord_route_recovers_under_one_canonical_identity() {
        let (orch, db) = route_test_orchestrator("route-discord-legacy-pending.db").await;
        let submission = seed_legacy_pending_discord_route(&db, "legacy-pending").await;
        let provider = Arc::new(DiscordAddressProvider {
            guild: 1,
            sends: Mutex::new(Vec::new()),
        });
        let router = discord_route_router(orch, provider.clone());

        router
            .recover_pending_routes(OperatorPresence::Away, Instant::now())
            .await
            .unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].conversation, "discord:1/99");
        }
        let row = ledger::get_by_id(&db, "legacy-pending")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.conversation, "discord:1/99");
        assert_eq!(row.status, "sent");
        let firing = crate::storage::list_route_firings(&db, "workspace", "route-test", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(firing.status, "fired", "{:?}", firing.error);

        // The same fact observed again finds the recovered row and stays quiet.
        router
            .submit_route(submission, OperatorPresence::Away, Instant::now())
            .await
            .unwrap();
        assert_eq!(provider.sends.lock().unwrap().len(), 1);
        assert_eq!(discord_route_rows(&db).await, Some(1));
    }

    #[tokio::test]
    async fn legacy_pending_discord_route_yields_to_an_existing_canonical_row() {
        let (orch, db) = route_test_orchestrator("route-discord-legacy-collision.db").await;
        seed_legacy_pending_discord_route(&db, "legacy-pending").await;
        ledger::insert_intent(
            &db,
            &ledger::NewOutbound {
                id: "canonical",
                channel: "discord",
                kind: "route",
                binding_ref: "route:route-test:directed",
                conversation: "discord:1/99",
                job_id: None,
                rendered_text: "notify",
                rendering: "text",
                created_at: 2,
            },
        )
        .await
        .unwrap();
        assert!(ledger::begin_delivery(&db, "canonical", 3).await.unwrap());
        ledger::mark_sent(&db, "canonical", "canonical-message", None, None, 3)
            .await
            .unwrap();
        let provider = Arc::new(DiscordAddressProvider {
            guild: 1,
            sends: Mutex::new(Vec::new()),
        });
        let router = discord_route_router(orch, provider.clone());

        router
            .recover_pending_routes(OperatorPresence::Away, Instant::now())
            .await
            .unwrap();

        assert!(provider.sends.lock().unwrap().is_empty());
        let legacy = ledger::get_by_id(&db, "legacy-pending")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(legacy.status, "expired");
        assert_eq!(legacy.conversation, "99");
    }

    #[tokio::test]
    async fn legacy_pending_discord_route_waits_out_an_unavailable_provider() {
        let (orch, db) = route_test_orchestrator("route-discord-legacy-unavailable.db").await;
        seed_legacy_pending_discord_route(&db, "legacy-pending").await;
        let provider = Arc::new(UnavailableProvider {
            sends: std::sync::atomic::AtomicUsize::new(0),
        });
        let router = ChannelRouter::new_for_provider(
            orch,
            provider.clone(),
            "discord",
            "1".into(),
            MessageClassPolicy::default(),
            ChannelInboundCapabilities::default(),
        );

        for _ in 0..2 {
            router
                .recover_pending_routes(OperatorPresence::Away, Instant::now())
                .await
                .unwrap();
        }

        assert_eq!(provider.sends.load(std::sync::atomic::Ordering::SeqCst), 0);
        let row = ledger::get_by_id(&db, "legacy-pending")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "pending");
        assert_eq!(row.conversation, "discord:1/99");
        assert_eq!(discord_route_rows(&db).await, Some(1));
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

    struct UnavailableProvider {
        sends: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ChannelProvider for UnavailableProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities::default()
        }

        async fn send(
            &self,
            _message: &OutboundMessage,
        ) -> Result<crate::channels::SentIds, String> {
            self.sends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("an unavailable provider must not be called")
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Unavailable {
                reason: "executor offline".into(),
            }
        }
    }

    #[tokio::test]
    async fn unavailable_provider_leaves_new_intent_pending_without_sending() {
        let (orch, db) = route_test_orchestrator("route-unavailable-pending.db").await;
        let provider = Arc::new(UnavailableProvider {
            sends: std::sync::atomic::AtomicUsize::new(0),
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
        let gate = Gate {
            kind: "route",
            initiated_by: OutboundInitiator::CairnPush,
            binding_ref: "route:unavailable".into(),
            job_id: None,
            context: "[Cairn]".into(),
            ask: OutboundAsk::Notify {
                text: "queued".into(),
            },
            conversation: None,
            target_uri: None,
            ask_key: None,
            held_since: None,
        };

        assert!(!router
            .deliver_or_defer(gate, OperatorPresence::Away, Instant::now())
            .await
            .unwrap());
        assert_eq!(provider.sends.load(std::sync::atomic::Ordering::SeqCst), 0);
        let record = ledger::get_by_binding(&db, CHANNEL, "route", "route:unavailable")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.status, "pending");
        assert_eq!(record.last_error, None);
        assert_eq!(record.next_attempt_at, None);
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

    fn agent_message_router(
        orch: Orchestrator,
        provider: Arc<dyn ChannelProvider>,
        provider_id: &'static str,
    ) -> ChannelRouter {
        ChannelRouter::new_for_provider(
            orch,
            provider,
            provider_id,
            "operator".into(),
            MessageClassPolicy::default(),
            ChannelInboundCapabilities {
                permissions: true,
                answers: true,
                free_text: true,
            },
        )
    }

    async fn enqueue_agent_message(db: &LocalDb, conversation: &str, home: &str) -> String {
        let address: crate::channels::ConversationAddress = conversation.parse().unwrap();
        agent_message::enqueue(db, &address, home, Some("j-sender"), "hello from Cairn")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_agent_message_is_sent_once_and_a_reply_reaches_its_sender() {
        for (provider_id, conversation, home, db_name) in [
            (
                "discord",
                "discord:1/2",
                "cairn://p/cairn/general",
                "agent-message-discord.db",
            ),
            (
                "telegram",
                "telegram:77",
                "cairn://p/cairn/general",
                "agent-message-telegram.db",
            ),
            (
                "discord",
                "discord:1/2",
                "cairn://p/cairn/3757/1/builder",
                "agent-message-node.db",
            ),
        ] {
            let (orch, db) = route_test_orchestrator(db_name).await;
            seed_threads_and_an_issue(&db).await;
            db.execute_batch(
                "INSERT INTO jobs (id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at)
                   VALUES ('j-issue', 'i-live', 'p', 'complete', 'builder', 'builder', 9, 9);
                 INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                   VALUES ('r-issue', 'p', 'j-issue', 'complete', 9, 9);",
            )
            .await
            .unwrap();
            let provider = Arc::new(RecordingPollProvider::new());
            let router = agent_message_router(orch, provider.clone(), provider_id);

            let id = enqueue_agent_message(&db, conversation, home).await;
            router.deliver_pending_agent_messages().await.unwrap();
            // A sent message is no longer owed, so a later sweep sends nothing.
            router.deliver_pending_agent_messages().await.unwrap();

            let deliveries = provider.deliveries.lock().unwrap().clone();
            assert_eq!(deliveries.len(), 1, "{db_name}");
            let sent = &deliveries[0];
            assert_eq!(sent.initiated_by, OutboundInitiator::Agent);
            assert_eq!(sent.conversation, conversation);
            assert_eq!(sent.context_header, agent_message::context_header(home));
            assert!(
                matches!(&sent.ask, OutboundAsk::Notify { text } if text == "hello from Cairn")
            );
            let record = ledger::get_by_id(&db, &id).await.unwrap().unwrap();
            assert_eq!(record.kind, "message");
            assert_eq!(record.status, "sent");
            assert_eq!(record.conversation, conversation);
            assert_eq!(record.job_id.as_deref(), Some("j-sender"));
            let guid = record.provider_guid.unwrap();

            router
                .resolve_bound(&guid, conversation, "operator", "operator reply")
                .await
                .unwrap();
            if home == "cairn://p/cairn/general" {
                assert_eq!(sent.context_header, "[Cairn · cairn/general]");
                assert_eq!(
                    db.query_one(
                        "SELECT sender_name, content FROM messages WHERE channel_type = 'thread' ORDER BY created_at DESC LIMIT 1",
                        (),
                        |row| Ok((row.text(0)?, row.text(1)?)),
                    )
                    .await
                    .unwrap(),
                    (
                        format!("operator via {provider_id}"),
                        "operator reply".to_string()
                    ),
                    "{db_name}: the reply lands in the sending thread"
                );
            } else {
                assert_eq!(sent.context_header, "[Cairn · cairn/3757 builder]");
                assert_eq!(
                    db.query_opt_text(
                        "SELECT content FROM queued_messages WHERE job_id = 'j-issue'",
                        (),
                    )
                    .await
                    .unwrap(),
                    Some(format!(
                        "[Direct message from operator via {provider_id}] operator reply"
                    )),
                    "{db_name}: a node's reply reaches its issue's root job"
                );
            }
        }
    }

    #[test]
    fn a_message_reply_targets_the_senders_thread_or_issue() {
        for (home, target) in [
            ("cairn://p/cairn/general", "cairn://p/cairn/general"),
            (
                "cairn://p/cairn/general/task/survey",
                "cairn://p/cairn/general",
            ),
            ("cairn://p/cairn/3757/1/builder", "cairn://p/cairn/3757"),
            (
                "cairn://p/cairn/3757/1/builder/task/review",
                "cairn://p/cairn/3757",
            ),
        ] {
            let binding = agent_message::binding_ref("intent", home);
            assert_eq!(
                agent_message::sender_home(&binding).and_then(routed_gate_target),
                Some(target.to_string()),
                "{home}"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn an_agent_message_sends_while_the_operator_is_present() {
        crate::channels::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Active);
        let (orch, db) = route_test_orchestrator("agent-message-present.db").await;
        let provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Present),
        });
        let router = agent_message_router(orch, provider.clone(), "discord");

        let id = enqueue_agent_message(&db, "discord:1/2", "cairn://p/cairn/general").await;
        router.submit_agent_message(&id).await.unwrap();

        assert_eq!(provider.sends.lock().unwrap().len(), 1);
        assert!(router.deferred_attention.lock().unwrap().is_empty());
        let record = ledger::get_by_id(&db, &id).await.unwrap().unwrap();
        assert_eq!(record.status, "sent");
        crate::channels::set_operator_presence_mode(crate::channels::OperatorPresenceMode::Auto);
    }

    #[tokio::test]
    async fn a_failed_agent_message_retries_on_the_shared_backoff() {
        let (orch, db) = route_test_orchestrator("agent-message-retry.db").await;
        let router = agent_message_router(orch, Arc::new(FailingNotifyProvider), "discord");

        let id = enqueue_agent_message(&db, "discord:1/2", "cairn://p/cairn/general").await;
        router.deliver_pending_agent_messages().await.unwrap();
        let failed = ledger::get_by_id(&db, &id).await.unwrap().unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.last_error.as_deref(), Some("bridge send failed"));
        assert_eq!(failed.attempts, 1);
        assert!(failed.next_attempt_at.is_some());
        assert_eq!(
            ledger::latest_send_error(&db, "discord").await.unwrap(),
            Some("bridge send failed".to_string()),
            "the failure surfaces as the provider's last_send_error"
        );

        // Not yet due: the sweep leaves it alone.
        router.deliver_pending_agent_messages().await.unwrap();
        assert_eq!(
            ledger::get_by_id(&db, &id).await.unwrap().unwrap().attempts,
            1
        );

        db.execute(
            "UPDATE channel_outbound SET next_attempt_at = 0 WHERE id = ?1",
            (id.clone(),),
        )
        .await
        .unwrap();
        router.deliver_pending_agent_messages().await.unwrap();
        let retried = ledger::get_by_id(&db, &id).await.unwrap().unwrap();
        assert_eq!(retried.status, "failed");
        assert_eq!(retried.attempts, 2);
    }

    struct DegradedFailingProvider;

    #[async_trait::async_trait]
    impl ChannelProvider for DegradedFailingProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities::default()
        }

        async fn send(
            &self,
            _message: &OutboundMessage,
        ) -> Result<crate::channels::SentIds, String> {
            Err("Discord thread is locked".into())
        }

        fn subscribe(&self) -> tokio::sync::mpsc::Receiver<InboundEvent> {
            tokio::sync::mpsc::channel(1).1
        }

        fn health(&self) -> crate::channels::ChannelHealth {
            crate::channels::ChannelHealth::Degraded {
                reason: "Discord bot is missing required guild permissions: MANAGE_MESSAGES".into(),
            }
        }
    }

    #[tokio::test]
    async fn a_degraded_provider_spends_attempts_and_an_outage_spends_none() {
        let (orch, db) = route_test_orchestrator("agent-message-degraded-spend.db").await;
        let id = enqueue_agent_message(&db, "discord:1/2", "cairn://p/cairn/general").await;
        async fn attempts(db: &LocalDb, id: &str) -> i64 {
            ledger::get_by_id(db, id).await.unwrap().unwrap().attempts
        }
        async fn make_due(db: &LocalDb, id: &str) {
            db.execute(
                "UPDATE channel_outbound SET next_attempt_at = 0 WHERE id = ?1",
                (id,),
            )
            .await
            .unwrap();
        }
        let degraded =
            agent_message_router(orch.clone(), Arc::new(DegradedFailingProvider), "discord");

        degraded.deliver_pending_agent_messages().await.unwrap();
        assert_eq!(
            attempts(&db, &id).await,
            1,
            "a degraded provider was called"
        );

        // An outage while the retry is due: never called, nothing spent.
        make_due(&db, &id).await;
        let offline = agent_message_router(
            orch,
            Arc::new(UnavailableProvider {
                sends: std::sync::atomic::AtomicUsize::new(0),
            }),
            "discord",
        );
        offline.deliver_pending_agent_messages().await.unwrap();
        let held = ledger::get_by_id(&db, &id).await.unwrap().unwrap();
        assert_eq!(held.attempts, 1);
        assert_eq!(
            held.next_attempt_at,
            Some(0),
            "the due retry is left for later"
        );

        for spent in 2..=MAX_ATTEMPTS {
            make_due(&db, &id).await;
            degraded.deliver_pending_agent_messages().await.unwrap();
            assert_eq!(attempts(&db, &id).await, spent);
        }
        make_due(&db, &id).await;
        degraded.deliver_pending_agent_messages().await.unwrap();
        assert_eq!(
            attempts(&db, &id).await,
            MAX_ATTEMPTS,
            "an exhausted message is not retried again"
        );
        assert_eq!(
            crate::channels::outbound_status(&db, "discord").await,
            (
                Some("Discord thread is locked".to_string()),
                ledger::UndeliveredCounts {
                    pending: 0,
                    failed: 1
                }
            )
        );

        // A later successful send on the provider retires the exhausted failure
        // from status; the row keeps its error.
        let later = chrono::Utc::now().timestamp_millis() + 1;
        assert!(ledger::insert_intent(
            &db,
            &ledger::NewOutbound {
                id: "later",
                channel: "discord",
                kind: "review",
                binding_ref: "review:later",
                conversation: "discord:1/3",
                job_id: None,
                rendered_text: "Ready for review",
                rendering: "text",
                created_at: later,
            },
        )
        .await
        .unwrap());
        assert!(
            ledger::mark_sent(&db, "later", "later-guid", None, None, later)
                .await
                .unwrap()
        );
        assert_eq!(
            crate::channels::outbound_status(&db, "discord").await,
            (None, ledger::UndeliveredCounts::default())
        );
        assert_eq!(
            ledger::get_by_id(&db, &id)
                .await
                .unwrap()
                .unwrap()
                .last_error
                .as_deref(),
            Some("Discord thread is locked")
        );
    }

    #[tokio::test]
    async fn a_pending_agent_message_is_delivered_by_a_restarted_router() {
        let (orch, db) = route_test_orchestrator("agent-message-restart.db").await;
        // Enqueued with no router to receive the hand-off.
        let id = enqueue_agent_message(&db, "discord:1/2", "cairn://p/cairn/general").await;

        let provider = Arc::new(PresentProvider {
            sends: Mutex::new(Vec::new()),
            presence_checks: Mutex::new(0),
            presence: Mutex::new(OperatorPresence::Away),
        });
        let router = agent_message_router(orch, provider.clone(), "discord");
        // Route recovery neither claims nor trips over a message row.
        router
            .recover_pending_routes(OperatorPresence::Away, Instant::now())
            .await
            .unwrap();
        assert!(provider.sends.lock().unwrap().is_empty());
        assert_eq!(
            ledger::get_by_id(&db, &id).await.unwrap().unwrap().status,
            "pending"
        );

        router.deliver_pending_agent_messages().await.unwrap();
        assert_eq!(provider.sends.lock().unwrap().len(), 1);
        assert_eq!(
            ledger::get_by_id(&db, &id).await.unwrap().unwrap().status,
            "sent"
        );
    }

    #[test]
    fn an_agent_message_is_never_held_by_an_agent() {
        let gate = |kind: &'static str| Gate {
            kind,
            initiated_by: OutboundInitiator::Agent,
            binding_ref: "message:x:cairn://p/cairn/general".into(),
            job_id: None,
            context: "[Cairn]".into(),
            ask: OutboundAsk::Notify { text: "hi".into() },
            conversation: Some("discord:1/2".into()),
            target_uri: None,
            ask_key: None,
            held_since: Some(1),
        };
        assert!(!gate(agent_message::KIND).held_by_agent());
        assert!(!gate("route").held_by_agent());
        assert!(gate("question").held_by_agent());
        assert!(!gate(agent_message::KIND).is_presence_aware());
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

        // The session opens: seal what already exists directly into `expired`.
        for gate in load_questions(&db).await.unwrap() {
            assert!(
                seal_gate_for_provider(&claims, &db, CHANNEL, "+15551234567", "text", &gate,)
                    .await
                    .unwrap()
            );
        }

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
            if seal_gate_for_provider(&claims, &db, CHANNEL, "+15551234567", "text", &gate)
                .await
                .unwrap()
            {
                sealed += 1;
            }
        }
        assert_eq!(sealed, backlog);

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
        multi_select: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl ChannelProvider for CleanupCountingProvider {
        fn capabilities(&self) -> crate::channels::ChannelCapabilities {
            crate::channels::ChannelCapabilities {
                state_cards: false,
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

        async fn cleanup_question(&self, message: &ResolvedQuestionMessage) -> Result<(), String> {
            self.cleanups
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.multi_select
                .store(message.multi_select, std::sync::atomic::Ordering::SeqCst);
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
            multi_select: std::sync::atomic::AtomicBool::new(false),
        };

        let (sweep, answer_delivered) = tokio::join!(
            cleanup_resolved_question(&db, &provider, &record, "✓ closed", false),
            async {
                let won = ledger::claim_question_answer(&db, &record.id, "Ship it", 12)
                    .await
                    .unwrap();
                if won {
                    cleanup_claimed_question(&db, &provider, &record, "✓ answered: Ship it", false)
                        .await
                        .unwrap();
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

    #[tokio::test]
    async fn finish_question_carries_durable_multi_select_to_provider_cleanup() {
        let (orch, project_db) =
            route_test_orchestrator("channel-router-multi-select-cleanup.db").await;
        seed_run(&project_db).await;
        project_db.execute("INSERT INTO prompts (id, run_id, questions, created_at) VALUES ('multi', 'run-1', ?1, 1)", (r#"[{"question":"Choose","options":[],"multiSelect":true}]"#,)).await.unwrap();
        let provider = Arc::new(CleanupCountingProvider {
            cleanups: std::sync::atomic::AtomicUsize::new(0),
            multi_select: std::sync::atomic::AtomicBool::new(false),
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
        ledger::insert_intent(
            router.ledger(),
            &ledger::NewOutbound {
                id: "multi-intent",
                channel: CHANNEL,
                kind: "question",
                binding_ref: "multi:0",
                conversation: "+15551234567",
                job_id: None,
                rendered_text: "Choose",
                rendering: "poll",
                created_at: 1,
            },
        )
        .await
        .unwrap();
        ledger::mark_sent(router.ledger(), "multi-intent", "guid", None, None, 2)
            .await
            .unwrap();
        let record = ledger::get_by_binding(router.ledger(), CHANNEL, "question", "multi:0")
            .await
            .unwrap()
            .unwrap();

        router.finish_question(&record, "✓ answered").await.unwrap();

        assert!(provider
            .multi_select
            .load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn directed_route_addresses_resolve_to_the_conversation_each_provider_sends_to() {
        let imessage: crate::channels::ConversationAddress =
            "imessage:USER@example.com".parse().unwrap();
        let telegram: crate::channels::ConversationAddress = "telegram:-123".parse().unwrap();
        let discord: crate::channels::ConversationAddress = "discord:1/2".parse().unwrap();
        assert_eq!(
            route_conversation(Some(&imessage)).as_deref(),
            Some("user@example.com")
        );
        assert_eq!(route_conversation(Some(&telegram)).as_deref(), Some("-123"));
        assert_eq!(
            route_conversation(Some(&discord)).as_deref(),
            Some("discord:1/2")
        );
        assert_eq!(route_conversation(None), None);
    }

    #[tokio::test]
    async fn non_channel_permission_winner_recovers_after_an_independent_lease() {
        let db = migrated_test_db("channel-router-non-channel-permission-recovery.db").await;
        let claim = ledger::claim_ask_resolution(
            &db,
            "permission-recovery",
            "Deny",
            cairn_common::identity::AppearanceTransport::ResourcePatch,
            None,
            None,
            Some("cairn://p/cairn/4008/1/builder"),
            "permission",
            "permission-recovery",
            10,
        )
        .await
        .unwrap();
        assert!(matches!(claim, ledger::AskClaim::Won(_)));

        let first = ledger::try_lease_ask_action(&db, "permission-recovery", 11, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(
            ledger::try_lease_ask_action(&db, "permission-recovery", 11, 1)
                .await
                .unwrap()
                .is_none(),
            "an active worker owns the first lease"
        );
        let second = ledger::try_lease_ask_action(&db, "permission-recovery", 13, 1)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.token, second.token);

        let winner = ledger::resolution_for_action(&db, "permission-recovery")
            .await
            .unwrap()
            .unwrap();
        let answer = crate::mcp::handlers::permission::recovered_permission_answer(
            PermissionDecision::Deny,
            &winner,
        )
        .unwrap();
        let (_, transport, surface, actor) = answer.resolution_provenance().unwrap();
        assert_eq!(
            transport,
            cairn_common::identity::AppearanceTransport::ResourcePatch
        );
        assert_eq!(surface, "resource_patch");
        assert_eq!(actor.as_deref(), Some("cairn://p/cairn/4008/1/builder"));
        assert_eq!(answer.channel_provenance(), (None, None));
    }

    #[test]
    fn permission_body_prefers_human_fields_and_keeps_the_link_separate() {
        assert_eq!(
            permission_ask_body(
                "Bash",
                r#"{"summary":"command blocked by the sandbox","descriptor":"git checkout main","command":"ignored"}"#,
                Some("cairn://p/cairn/4080/1/builder/permissions/ask")
            ),
            "command blocked by the sandbox\n\ngit checkout main\n\ncairn://p/cairn/4080/1/builder/permissions/ask"
        );
    }

    #[test]
    fn permission_body_falls_back_to_raw_input_without_human_fields() {
        assert_eq!(
            permission_ask_body("Bash", r#"{"command":"git status"}"#, None),
            "Allow Bash?\n\n{\"command\":\"git status\"}"
        );
    }

    #[tokio::test]
    async fn permission_poll_vote_and_numbered_reply_resolve_once_with_channel_provenance() {
        let (orch, db) = route_test_orchestrator("channel-permission-poll-race.db").await;
        let (router, _) = test_router(orch);
        let intent = ledger::NewOutbound {
            id: "permission-intent",
            channel: CHANNEL,
            kind: "permission",
            binding_ref: "permission-request",
            conversation: "imessage:+15551234567",
            job_id: None,
            rendered_text: "Allow this command?",
            rendering: "poll",
            created_at: 10,
        };
        assert!(ledger::insert_intent(&db, &intent).await.unwrap());
        assert!(
            ledger::mark_sent(&db, intent.id, "permission-poll", None, None, 11)
                .await
                .unwrap()
        );

        let _ = tokio::join!(
            router.handle_inbound(InboundEvent::Selection {
                conversation: "imessage:+15551234567".into(),
                bound_guid: "permission-poll".into(),
                sender: "+15551234567".into(),
                option_id: "approve-option".into(),
                option_text: "Approve".into(),
                selected: true,
            }),
            router.handle_inbound(InboundEvent::Reply {
                conversation: "imessage:+15551234567".into(),
                bound_guid: "permission-poll".into(),
                sender: "+15551234567".into(),
                text: "2".into(),
            })
        );

        assert_eq!(db.query_one("SELECT COUNT(*) FROM channel_ask_resolution WHERE binding_ref = 'permission-request'", (), |row| row.i64(0)).await.unwrap(), 1);
        let winner = ledger::resolution_for_action(&db, "permission-request")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(winner.answer.as_str(), "allow" | "deny"));
        assert_eq!(winner.winner_surface, "channel_reply");
        assert_eq!(winner.winner_provider, "imessage");
        assert_eq!(winner.winner_conversation, "imessage:+15551234567");
        assert_eq!(winner.winner_actor, "+15551234567");
        assert!(db.query_one("SELECT resolved_at FROM channel_ask_resolution WHERE binding_ref = 'permission-request'", (), |row| row.i64(0)).await.unwrap() > 0);
    }

    /// A domain effect refused because only an operator may approve it is
    /// attempted ONCE, and the answer that earned the refusal is withdrawn.
    ///
    /// The refusal says nothing about this moment. The stored answer came from a
    /// surface that is not allowed to give it, so every later attempt re-derives
    /// the same refusal from the same row — four such actions logged ~112,000
    /// 'will retry' lines in a day (CAIRN-4364). The quieter half of the bug is
    /// worse: a channel reply claims the gate before anything asks whether that
    /// surface may decide, so an unwithdrawn refused answer keeps replaying for
    /// the operator whose approval the prompt is waiting for.
    #[tokio::test]
    async fn an_operator_pending_refusal_parks_the_action_after_one_attempt() {
        let (orch, db) = route_test_orchestrator("channel-router-operator-pending-park.db").await;
        seed_run(&db).await;
        let escape = crate::mcp::handlers::fence::Crossing::shell_command(
            "command blocked by the executor sandbox".to_string(),
            "cat ~/.cairn/operator_auth_secret",
        )
        .stored_tool_input_for_test();
        db.execute(
            "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at) VALUES ('req-1', 'run-1', 'tool-1', 'run', ?1, 'pending', 500)",
            params![escape],
        )
        .await
        .unwrap();
        ledger::claim_ask_resolution(
            &db,
            "req-1",
            "allow",
            cairn_common::identity::AppearanceTransport::ChannelReply,
            Some(CHANNEL),
            Some("imessage:+15551234567"),
            Some("+15551234567"),
            "permission",
            "req-1",
            10,
        )
        .await
        .unwrap();

        let (router, provider) = test_router(orch);
        for _ in 0..3 {
            router.sweep_pending_domain_actions().await.unwrap();
        }

        assert_eq!(
            db.query_one(
                "SELECT attempt_count FROM channel_ask_action WHERE action_ref = 'req-1'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap(),
            1,
            "an answer only an operator can lift is attempted once, not once per sweep"
        );
        assert_eq!(
            db.query_one(
                "SELECT parked_at IS NOT NULL FROM channel_ask_action WHERE action_ref = 'req-1'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap(),
            1,
            "the action records that it is waiting for a person"
        );
        assert_eq!(
            db.query_one(
                "SELECT COUNT(*) FROM channel_ask_resolution WHERE action_ref = 'req-1'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap(),
            0,
            "the refused answer is withdrawn, leaving the gate open for the operator"
        );
        assert_eq!(
            db.query_opt_text(
                "SELECT status FROM permission_requests WHERE id = 'req-1'",
                ()
            )
            .await
            .unwrap()
            .as_deref(),
            Some("pending"),
        );

        let deliveries = provider.deliveries.lock().unwrap();
        assert_eq!(
            deliveries.len(),
            1,
            "the conversation that answered is told once, not once per sweep"
        );
        let OutboundAsk::Notify { text } = &deliveries[0].ask else {
            panic!("expected a notice back to the answering conversation")
        };
        assert!(
            text.contains("still pending for the operator"),
            "unexpected notice: {text}"
        );
    }

    #[test]
    fn permission_words_are_strict() {
        for spelling in ["1", "yes", "y", "approve", "allow", "Approve"] {
            assert_eq!(
                permission_answer_token(parse_permission(spelling).unwrap()),
                "allow"
            );
        }
        for spelling in ["2", "no", "n", "deny", "denied", "Deny"] {
            assert_eq!(
                permission_answer_token(parse_permission(spelling).unwrap()),
                "deny"
            );
        }
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
            attempts: 0,
            next_attempt_at: None,
            last_attempt_at: None,
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
            conversation: None,
            target_uri: None,
            kind: "review",
            initiated_by: OutboundInitiator::CairnPush,
            binding_ref: "review".into(),
            job_id: None,
            context: String::new(),
            ask_key: None,
            held_since: None,
            ask: OutboundAsk::Notify {
                text: String::new(),
            },
        };
        assert!(review.is_presence_aware());

        let response = Gate {
            conversation: None,
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
            conversation: None,
            target_uri: None,
            kind: "review",
            initiated_by: OutboundInitiator::OperatorSubscription,
            binding_ref: "cairn://p/cairn/1: event:1".into(),
            job_id: None,
            context: String::new(),
            ask_key: None,
            held_since: None,
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
            conversation: None,
            target_uri: None,
            kind: "question",
            initiated_by: OutboundInitiator::CairnPush,
            binding_ref: "healthy:0".into(),
            job_id: None,
            context: String::new(),
            ask_key: None,
            held_since: None,
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
            cancel_resolved_attention(&mut deferred, &HashSet::new(), true)
                .into_iter()
                .map(|attention| attention.id)
                .collect::<Vec<_>>(),
            vec!["intent"]
        );
        assert!(deferred.is_empty());
    }

    #[test]
    fn review_notice_leads_with_issue_title_and_event_without_boilerplate() {
        let content_ref = "cairn://p/cairn/3445/1/builder/artifact";
        assert_eq!(
            review_notice(
                "cairn",
                3445,
                "Reap nested Linux process groups when checks stop",
                content_ref,
            ),
            "cairn/3445 review ready — Reap nested Linux process groups when checks stop\ncairn://p/cairn/3445/1/builder/artifact"
        );
    }

    fn guild_discord_router(orch: Orchestrator) -> (ChannelRouter, Arc<DiscordAddressProvider>) {
        let provider = Arc::new(DiscordAddressProvider {
            guild: 1,
            sends: Mutex::new(Vec::new()),
        });
        (discord_route_router(orch, provider.clone()), provider)
    }

    /// An undirected (`destination: Subscriptions`) route submission whose fact
    /// names its subject in `field`.
    fn undirected_route_submission(identity: &str, field: &str, uri: &str) -> ChannelSubmission {
        let mut submission = route_submission(identity);
        submission
            .fact
            .fields
            .insert(field.into(), serde_json::Value::String(uri.into()));
        submission
    }

    async fn bind_structural(db: &LocalDb, conversation: &str, target: &str) {
        ledger::bind_target(
            db,
            "discord",
            conversation,
            target,
            super::super::bindings::BindingKind::Structural,
            super::super::bindings::MESSAGE_CLASSES_ALL,
            1,
            0,
        )
        .await
        .unwrap();
    }

    async fn route_firing_for(db: &LocalDb, identity: &str) -> crate::storage::RouteFiringRecord {
        crate::storage::list_route_firings(db, "workspace", "route-test", 50)
            .await
            .unwrap()
            .into_iter()
            .find(|firing| firing.fact_identity == identity)
            .unwrap_or_else(|| panic!("no firing recorded for {identity}"))
    }

    async fn discord_route_row_count(db: &LocalDb) -> i64 {
        db.query_opt_i64(
            "SELECT COUNT(*) FROM channel_outbound WHERE channel = 'discord' AND kind = 'route'",
            (),
        )
        .await
        .unwrap()
        .unwrap_or(0)
    }

    #[tokio::test]
    async fn undirected_discord_route_delivers_to_its_issue_structural_conversation() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-issue.db").await;
        bind_structural(&db, "discord:1/77", "cairn://p/cairn/42").await;
        let (router, provider) = guild_discord_router(orch);

        router
            .submit_route(
                undirected_route_submission(
                    "issue-subject",
                    "detailUri",
                    "cairn://p/cairn/42/1/builder/create-pr",
                ),
                OperatorPresence::Away,
                Instant::now(),
            )
            .await
            .unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].conversation, "discord:1/77");
        }
        let outbound =
            ledger::get_by_binding(&db, "discord", "route", "route:route-test:issue-subject")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(outbound.conversation, "discord:1/77");
        assert_eq!(outbound.status, "sent", "{:?}", outbound.last_error);
        assert_eq!(route_firing_for(&db, "issue-subject").await.status, "fired");
    }

    #[tokio::test]
    async fn undirected_discord_route_delivers_to_its_thread_channel() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-thread.db").await;
        bind_structural(&db, "discord:1/88", "cairn://p/cairn/channels").await;
        let (router, provider) = guild_discord_router(orch);

        router
            .submit_route(
                undirected_route_submission(
                    "thread-subject",
                    "threadUri",
                    "cairn://p/cairn/channels",
                ),
                OperatorPresence::Away,
                Instant::now(),
            )
            .await
            .unwrap();

        let sends = provider.sends.lock().unwrap();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].conversation, "discord:1/88");
    }

    #[tokio::test]
    async fn undirected_discord_route_without_a_subject_records_a_failed_firing_and_no_row() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-nowhere.db").await;
        let (router, provider) = guild_discord_router(orch);

        router
            .submit_route(
                route_submission("no-subject"),
                OperatorPresence::Away,
                Instant::now(),
            )
            .await
            .unwrap();

        assert!(provider.sends.lock().unwrap().is_empty());
        let firing = route_firing_for(&db, "no-subject").await;
        assert_eq!(firing.status, "failed");
        let error = firing.error.unwrap_or_default();
        assert!(
            error.contains("Discord has no default conversation"),
            "{error}"
        );
        assert_eq!(
            discord_route_row_count(&db).await,
            0,
            "an unaddressable sink owes no delivery, so it writes no outbound row"
        );
    }

    async fn seed_route_subject_issue(db: &LocalDb, number: i32, status: &str) {
        db.execute_batch(&format!(
            "INSERT OR IGNORE INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT OR IGNORE INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('issue-{number}','p',{number},'Target','{status}',1,1);"
        ))
        .await
        .unwrap();
    }

    async fn issue_surface_rows(db: &LocalDb, target: &str) -> i64 {
        db.query_opt_i64(
            "SELECT COUNT(*) FROM discord_surface WHERE surface_kind = 'issue_thread' AND target_uri = ?1",
            (target.to_string(),),
        )
        .await
        .unwrap()
        .unwrap_or(0)
    }

    /// The subject has no Discord thread yet: the route asks for one, waits with
    /// no outbound row, and delivers there once the reconciler binds it.
    #[tokio::test]
    async fn undirected_discord_route_waits_for_its_subject_surface_then_delivers() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-await.db").await;
        seed_route_subject_issue(&db, 42, "active").await;
        let (router, provider) = guild_discord_router(orch);
        let submission = undirected_route_submission("awaits", "detailUri", "cairn://p/cairn/42");
        let start = Instant::now();

        for _ in 0..3 {
            router
                .submit_route(submission.clone(), OperatorPresence::Away, start)
                .await
                .unwrap();
        }
        assert!(provider.sends.lock().unwrap().is_empty());
        assert_eq!(discord_route_row_count(&db).await, 0);
        assert_eq!(issue_surface_rows(&db, "cairn://p/cairn/42").await, 1);
        assert_eq!(router.held_routes.lock().unwrap().len(), 1);

        // Still being created: the next sweep keeps waiting.
        router
            .retry_held_routes(OperatorPresence::Away, start + Duration::from_secs(5))
            .await;
        assert_eq!(router.held_routes.lock().unwrap().len(), 1);

        bind_structural(&db, "discord:1/77", "cairn://p/cairn/42").await;
        router
            .retry_held_routes(OperatorPresence::Away, start + Duration::from_secs(10))
            .await;

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].conversation, "discord:1/77");
        }
        assert!(router.held_routes.lock().unwrap().is_empty());
        let outbound = ledger::get_by_binding(&db, "discord", "route", "route:route-test:awaits")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outbound.conversation, "discord:1/77");
        assert_eq!(outbound.status, "sent");
        assert_eq!(route_firing_for(&db, "awaits").await.status, "fired");
    }

    #[tokio::test]
    async fn undirected_discord_route_whose_surface_never_lands_records_a_failed_firing() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-timeout.db").await;
        seed_route_subject_issue(&db, 42, "active").await;
        let (router, provider) = guild_discord_router(orch);
        let start = Instant::now();
        router
            .submit_route(
                undirected_route_submission("times-out", "detailUri", "cairn://p/cairn/42"),
                OperatorPresence::Away,
                start,
            )
            .await
            .unwrap();

        router
            .retry_held_routes(OperatorPresence::Away, start + SURFACE_WAIT)
            .await;

        assert!(provider.sends.lock().unwrap().is_empty());
        assert!(router.held_routes.lock().unwrap().is_empty());
        assert_eq!(discord_route_row_count(&db).await, 0);
        let firing = route_firing_for(&db, "times-out").await;
        assert_eq!(firing.status, "failed");
        let error = firing.error.unwrap_or_default();
        assert!(error.contains("was not ready within"), "{error}");
    }

    #[tokio::test]
    async fn undirected_discord_route_on_a_finished_issue_fails_without_a_surface() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-closed.db").await;
        seed_route_subject_issue(&db, 43, "closed").await;
        let (router, provider) = guild_discord_router(orch);
        router
            .submit_route(
                undirected_route_submission("closed", "detailUri", "cairn://p/cairn/43"),
                OperatorPresence::Away,
                Instant::now(),
            )
            .await
            .unwrap();

        assert!(provider.sends.lock().unwrap().is_empty());
        assert!(router.held_routes.lock().unwrap().is_empty());
        assert_eq!(issue_surface_rows(&db, "cairn://p/cairn/43").await, 0);
        assert_eq!(discord_route_row_count(&db).await, 0);
        let firing = route_firing_for(&db, "closed").await;
        assert_eq!(firing.status, "failed");
        let error = firing.error.unwrap_or_default();
        assert!(
            error.contains("Discord has no default conversation"),
            "{error}"
        );
    }

    /// A structural binding left behind in another guild is not this router's
    /// conversation; the provider would refuse it.
    #[tokio::test]
    async fn structural_binding_in_another_guild_is_not_the_route_conversation() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-guild.db").await;
        bind_structural(&db, "discord:2/55", "cairn://p/cairn/42").await;
        bind_structural(&db, "discord:1/77", "cairn://p/cairn/42").await;
        let (router, provider) = guild_discord_router(orch);
        router
            .submit_route(
                undirected_route_submission("guild", "detailUri", "cairn://p/cairn/42"),
                OperatorPresence::Away,
                Instant::now(),
            )
            .await
            .unwrap();

        let sends = provider.sends.lock().unwrap();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].conversation, "discord:1/77");
    }

    #[test]
    fn route_subject_reduces_fact_uris_to_their_issue_or_thread() {
        let fact = |field: &str, uri: &str| RouteFact {
            source: "attention".into(),
            identity: "subject".into(),
            fields: std::collections::BTreeMap::from([(
                field.to_string(),
                serde_json::Value::String(uri.into()),
            )]),
            origin: None,
            summary: None,
            route_provenance: None,
        };
        let issue = FollowTarget::Issue {
            project: "cairn".into(),
            number: 42,
        };
        assert_eq!(
            route_subject(&fact("detailUri", "cairn://p/cairn/42")),
            Some(issue.clone())
        );
        assert_eq!(
            route_subject(&fact("detailUri", "cairn://p/cairn/42/1/builder/create-pr")),
            Some(issue)
        );
        assert_eq!(
            route_subject(&fact("threadUri", "cairn://p/cairn/channels")),
            Some(FollowTarget::Thread {
                project: "cairn".into(),
                name: "channels".into()
            })
        );
        assert_eq!(route_subject(&fact("detailUri", "cairn://posts")), None);
        assert_eq!(route_subject(&fact("url", "cairn://p/cairn/42")), None);
    }

    /// A held route whose surface lands while the operator is present is
    /// deferred like any route, and stays deferred across sweeps with no live
    /// asks until the operator goes idle.
    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn held_route_respects_operator_presence_through_the_sweep() {
        use crate::channels::{set_operator_presence_mode, OperatorPresenceMode};

        let (orch, db) = route_test_orchestrator("route-undirected-discord-presence.db").await;
        seed_route_subject_issue(&db, 42, "active").await;
        let (router, provider) = guild_discord_router(orch);
        set_operator_presence_mode(OperatorPresenceMode::Active);

        router
            .submit_route(
                undirected_route_submission("present", "detailUri", "cairn://p/cairn/42"),
                OperatorPresence::Present,
                Instant::now(),
            )
            .await
            .unwrap();
        bind_structural(&db, "discord:1/77", "cairn://p/cairn/42").await;

        for _ in 0..2 {
            router.sweep_live_gates().await.unwrap();
            assert!(provider.sends.lock().unwrap().is_empty());
            let row = ledger::get_by_binding(&db, "discord", "route", "route:route-test:present")
                .await
                .unwrap()
                .expect("the landed route owes a durable delivery");
            assert_eq!(row.conversation, "discord:1/77");
            assert_eq!(row.status, "pending");
        }

        set_operator_presence_mode(OperatorPresenceMode::Idle);
        router.sweep_live_gates().await.unwrap();
        set_operator_presence_mode(OperatorPresenceMode::Auto);

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].conversation, "discord:1/77");
        }
        let row = ledger::get_by_binding(&db, "discord", "route", "route:route-test:present")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "sent");
        assert_eq!(route_firing_for(&db, "present").await.status, "fired");
    }

    /// A pending undirected route row keyed by the guild id (as every such row
    /// was before routes resolved a structural conversation) is retired as a
    /// representable failure, and a structural pending row after it still
    /// recovers under the conversation it was keyed by.
    #[tokio::test]
    async fn guild_keyed_pending_route_fails_without_blocking_recovery() {
        let (orch, db) = route_test_orchestrator("route-undirected-discord-recovery.db").await;
        let seed = |id: &'static str,
                    conversation: &'static str,
                    submission: ChannelSubmission,
                    created_at: i64| {
            let db = db.clone();
            async move {
                ledger::insert_intent(
                    &db,
                    &ledger::NewOutbound {
                        id,
                        channel: "discord",
                        kind: "route",
                        binding_ref: &submission.binding_ref,
                        conversation,
                        job_id: None,
                        rendered_text: "notify",
                        rendering: "text",
                        created_at,
                    },
                )
                .await
                .unwrap();
                ledger::set_pending_route_submission(
                    &db,
                    id,
                    &serde_json::to_string(&submission).unwrap(),
                )
                .await
                .unwrap();
            }
        };
        seed("guild-keyed", "1", route_submission("guild-keyed"), 1).await;
        seed(
            "structural",
            "discord:1/77",
            undirected_route_submission("structural", "detailUri", "cairn://p/cairn/42"),
            2,
        )
        .await;
        let (router, provider) = guild_discord_router(orch);

        router
            .recover_pending_routes(OperatorPresence::Away, Instant::now())
            .await
            .unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].conversation, "discord:1/77");
        }
        let legacy = ledger::get_by_id(&db, "guild-keyed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(legacy.status, "failed");
        let firing = route_firing_for(&db, "guild-keyed").await;
        assert_eq!(firing.status, "failed");
        assert!(
            firing
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("guild id"),
            "{:?}",
            firing.error
        );
        let structural = ledger::get_by_id(&db, "structural").await.unwrap().unwrap();
        assert_eq!(structural.status, "sent", "{:?}", structural.last_error);
        assert_eq!(route_firing_for(&db, "structural").await.status, "fired");
    }
}
