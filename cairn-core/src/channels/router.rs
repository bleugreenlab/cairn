use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cairn_common::uri::{build_node_permission_uri, build_node_question_uri, parse_uri};
use cairn_db::turso::{params, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    bindings::FollowTarget, ledger, render_text_floor, AskOption, ChannelProvider, InboundEvent,
    OperatorPresence, OutboundAsk, OutboundInitiator, OutboundMessage, ResolvedQuestionMessage,
};
use crate::routes::{ChannelSubmission, Presence, RouteContext, RouteFact};
use crate::{
    mcp::handlers::permission::PermissionDecision,
    models::{ChannelInboundCapabilities, MessageClassPolicy},
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
/// The durable binding prefix for a follow poll's ledger row. Command polls are
/// not backed by a Cairn prompt, so the gate sweep recognizes this namespace and
/// leaves their provider bindings intact until a newer poll of the same kind
/// supersedes them.
const FOLLOW_POLL_PREFIX: &str = "threads:";

#[derive(Debug, Deserialize)]
struct StoredQuestion {
    question: String,
    #[serde(default)]
    options: Vec<StoredOption>,
}

fn permission_answer_token(decision: PermissionDecision) -> &'static str {
    match decision {
        PermissionDecision::Allow => "allow",
        PermissionDecision::Deny => "deny",
    }
}

fn route_conversation(address: Option<&crate::channels::ConversationAddress>) -> Option<String> {
    address.map(|address| match address.destination() {
        super::ConversationDestination::IMessage { handle } => handle.clone(),
        super::ConversationDestination::Telegram { chat_id } => chat_id.to_string(),
        super::ConversationDestination::Discord { channel_id, .. } => channel_id.to_string(),
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
    Some(format!(
        "cairn://p/{}/{}",
        resource.project()?,
        resource.issue_number()?
    ))
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

    cleanup_claimed_question(provider, record, receipt).await?;
    Ok(true)
}

async fn cleanup_claimed_question(
    provider: &dyn ChannelProvider,
    record: &ledger::OutboundRecord,
    receipt: &str,
) -> Result<(), String> {
    let (Some(provider_guid), Some(sent_at)) = (&record.provider_guid, record.sent_at) else {
        return Ok(());
    };
    let message = ResolvedQuestionMessage {
        conversation: record.conversation.clone(),
        provider_guid: provider_guid.clone(),
        caption_guid: record.caption_guid.clone(),
        sent_at,
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
    conversation: Option<String>,
    target_uri: Option<String>,
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
        presence: OperatorPresence,
        now: Instant,
    ) -> Result<bool, String> {
        let initiated_by = match submission.initiated_by.as_deref() {
            Some("operator_subscription") => OutboundInitiator::OperatorSubscription,
            _ => OutboundInitiator::CairnPush,
        };
        let conversation = route_conversation(submission.destination.as_ref());
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
                conversation: route_conversation(submission.destination.as_ref()),
                target_uri: None,
            };
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
        if ledger::remove_home_relative_focus(self.ledger(), self.provider_id).await? {
            log::warn!("channel removed unresolvable home-relative focus");
        }
        for follow in ledger::list_follows(self.ledger(), self.provider_id).await? {
            if follow.uri == "cairn:~" || follow.uri.starts_with("cairn:~/") {
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
                let _ =
                    cleanup_claimed_question(self.provider.as_ref(), &update, "✓ unfollowed").await;
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
                        "SELECT COALESCE(MAX(e.rowid), 0) FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN threads t ON t.id = j.thread_id JOIN projects p ON p.id = t.project_id WHERE LOWER(p.key) = ?1 AND t.name = ?2 AND {}",
                        crate::threads::SESSION_JOB_SHAPE
                    ),
                    params![cairn_common::uri::canonical_project(project), name.clone()],
                )
                .await
            }
            FollowTarget::Issue { project, number } => {
                db.query_opt_i64(
                    "SELECT COALESCE(MAX(e.rowid), 0) FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE LOWER(p.key) = ?1 AND i.number = ?2",
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
                        "SELECT e.rowid, e.data, p.key, t.name, j.id, p.id, p.repo_path FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN threads t ON t.id = j.thread_id JOIN projects p ON p.id = t.project_id WHERE LOWER(p.key) = ?1 AND t.name = ?2 AND {} AND e.rowid > ?3 AND e.event_type = 'assistant' ORDER BY e.rowid",
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
                    "SELECT e.rowid, e.data, p.key, i.number, i.title, j.id, p.id, p.repo_path FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE LOWER(p.key) = ?1 AND i.number = ?2 AND e.rowid > ?3 AND e.event_type = 'assistant' ORDER BY e.rowid",
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

            let result = if kind == "question" {
                let response = answers[0].1.clone();
                let winner = ledger::resolution_for_action(self.ledger(), &action_ref)
                    .await?
                    .ok_or_else(|| format!("question action has no provenance: {action_ref}"))?;
                crate::mcp::handlers::planning::answer_prompt_id_domain(
                    &self.orch,
                    &action_ref,
                    response,
                    winner.resolution_provenance()?,
                )
                .await
                .map(|_| ())
            } else {
                let provenance = ledger::resolution_for_action(self.ledger(), &action_ref)
                    .await?
                    .ok_or_else(|| format!("permission action has no provenance: {action_ref}"))?;
                let answer = answers
                    .first()
                    .map(|(_, answer)| answer.as_str())
                    .ok_or_else(|| format!("permission action has no answer: {action_ref}"));
                match answer.and_then(parse_permission) {
                    Ok(decision) => {
                        let recovered =
                            crate::mcp::handlers::permission::recovered_permission_answer(
                                decision,
                                &provenance,
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
                Err(error) => {
                    ledger::release_ask_action(self.ledger(), &lease, &error).await?;
                    log::warn!("channel domain action {action_ref} will retry: {error}");
                }
            }
        }
        Ok(())
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
            match cleanup_claimed_question(self.provider.as_ref(), &record, &receipt).await {
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
                        origin: Some(crate::routes::installation_machine_origin(&self.orch)?),
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

    async fn route_gates(&self, db: &LocalDb, gates: Vec<Gate>) -> Result<Vec<Gate>, String> {
        let mut routed = Vec::new();
        for gate in gates {
            let Some(target_uri) = gate.target_uri.as_deref() else {
                routed.push(gate);
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
                    self.ensure_discord_issue_surface(db, target_uri).await?;
                    // Surface reconciliation installs the structural binding. Until
                    // then this event remains unclaimed and is retried by the next
                    // sweep; a guild ID is never a deliverable conversation.
                    continue;
                }
                routed.push(gate);
                continue;
            }
            routed.extend(bindings.into_iter().map(|binding| {
                let mut rendering = gate.clone();
                rendering.conversation = Some(binding.conversation);
                rendering
            }));
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
        let started = Instant::now();
        let mut gates = Vec::new();
        if self.route.question {
            gates.extend(load_questions(db).await?);
        }
        if self.route.permission {
            gates.extend(load_permissions(db).await?);
        }
        let mut review_pushes = 0;
        let mut review_backlog = 0;
        let mut expired = 0;
        if self.route.notify {
            let reviews = load_reviews(db).await?;
            review_pushes = reviews.gates.len();
            review_backlog = reviews.scanned;
            expired = reviews.expired_dangling;
            gates.extend(reviews.gates);
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

    async fn ensure_discord_issue_surface(
        &self,
        db: &LocalDb,
        target_uri: &str,
    ) -> Result<(), String> {
        let Some(cairn_common::uri::CairnResource::Issue { project, number }) =
            parse_uri(target_uri)
        else {
            return Ok(());
        };
        let guild_id = self
            .destination
            .parse::<u64>()
            .map_err(|_| "Discord router destination must be a guild ID".to_string())?;
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
            return Ok(());
        };
        if matches!(status.as_str(), "complete" | "failed" | "merged" | "closed") {
            return Ok(());
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
        super::wake_discord_surfaces();
        Ok(())
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
            return self.send_claimed(deferred.id, deferred.gate).await;
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
                    delivery_key,
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
                ledger::mark_failed(self.ledger(), &id, &error).await?;
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
                        && super::imessage::normalize_handle(&record.conversation)
                            == super::imessage::normalize_handle(sender)
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
                    && super::imessage::normalize_handle(&record.conversation)
                        == super::imessage::normalize_handle(sender)
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
                gates.push(Gate { conversation: None, target_uri: question_uri.as_deref().and_then(routed_gate_target), kind: "question", initiated_by: OutboundInitiator::CairnPush, binding_ref: format!("{prompt_id}:{index}"), job_id: job_id.clone(), context: context.clone(), ask: OutboundAsk::Question { prompt_id: prompt_id.clone(), question_index: index, text, options: question.options.into_iter().map(|o| AskOption { label:o.label, description:o.description }).collect() } });
            }
        }
        Ok(gates)
    })).await.map_err(|e| e.to_string())
}

async fn load_permissions(db: &LocalDb) -> Result<Vec<Gate>, String> {
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query("SELECT req.id, req.tool_name, req.tool_input, COALESCE(req.job_id,r.job_id), COALESCE(j.node_name,j.uri_segment,'agent'), p.key, i.number, e.seq, j.uri_segment, req.uri_segment FROM permission_requests req JOIN runs r ON r.id=req.run_id LEFT JOIN jobs j ON j.id=COALESCE(req.job_id,r.job_id) LEFT JOIN issues i ON i.id=COALESCE(j.issue_id,r.issue_id) LEFT JOIN projects p ON p.id=i.project_id LEFT JOIN executions e ON e.id=j.execution_id WHERE req.status='pending' AND COALESCE(i.status,'open') NOT IN ('merged','closed','failed') ORDER BY req.created_at DESC", ()).await?;
        let mut gates=Vec::new(); while let Some(row)=rows.next().await? { let id=row.text(0)?; let tool=row.text(1)?; let input=row.text(2)?; let uri=match(row.opt_text(5)?,row.opt_i64(6)?,row.opt_i64(7)?,row.opt_text(8)?,row.opt_text(9)?){(Some(project),Some(number),Some(exec_seq),Some(node),Some(segment))=>Some(build_node_permission_uri(&project,number as i32,exec_seq as i32,&node,&segment)),_=>None}; let summary=permission_ask_body(&tool, &input, uri.as_deref()); let target_uri = uri.as_deref().and_then(routed_gate_target); gates.push(Gate { conversation: None, target_uri, kind:"permission", initiated_by: OutboundInitiator::CairnPush, binding_ref:id.clone(), job_id:row.opt_text(3)?, context:format!("[Cairn · {}]",row.text(4)?), ask:OutboundAsk::Permission { request_id:id, summary } }); } Ok(gates)
    })).await.map_err(|e| e.to_string())
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

/// The undelivered review backlog, and only that.
///
/// Named so the regression test can plan it. This statement reads ONE table: the
/// issue each push is about is recovered from the push's own canonical URI by
/// [`review_subject`], not by asking the database to rediscover it. See
/// [`load_review_issue_titles`] for what that replaced and why.
const REVIEW_BACKLOG_SQL: &str = "SELECT ap.id, ap.recipient, ap.content_ref, ap.\"key\"
               FROM attention_pushes ap
              WHERE ap.delivered_event_id IS NULL AND ap.retired_at IS NULL
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
    struct ReviewRow {
        id: String,
        recipient: String,
        content_ref: String,
        key: String,
    }

    // One statement for the whole backlog. Liveness is NOT decided here. It comes
    // from `attention_push::resolve_live_refs`, which is the one place that
    // predicate lives — this query used to carry its own copy as correlated
    // EXISTS arms, and two copies of a delivery predicate drift apart silently.
    let rows = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(REVIEW_BACKLOG_SQL, ()).await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(ReviewRow {
                        id: row.text(0)?,
                        recipient: row.text(1)?,
                        content_ref: row.text(2)?,
                        key: row.text(3)?,
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
    for row in rows {
        let Some((project, number, title)) =
            review_subject(&row.content_ref).and_then(|(project, number)| {
                titles
                    .get(&(project.clone(), number))
                    .map(|title| (project, number, title.clone()))
            })
        else {
            crate::orchestrator::attention_push::delete_pending_by_id(db, &row.id)
                .await
                .map_err(|error| error.to_string())?;
            result.expired_dangling += 1;
            log::warn!(
                "channel expired dangling review {} because its semantic key does not resolve: {}",
                row.id,
                row.key
            );
            continue;
        };
        resolvable.push((row, project, number, title));
    }

    let refs: Vec<(&str, &str)> = resolvable
        .iter()
        .map(|(row, _, _, _)| (row.key.as_str(), row.content_ref.as_str()))
        .collect();
    let live = crate::orchestrator::attention_push::resolve_live_refs(db, &refs)
        .await
        .map_err(|error| error.to_string())?;

    for ((row, project, number, title), live) in resolvable.into_iter().zip(live) {
        if !live {
            continue;
        }
        result.gates.push(Gate {
            conversation: None,
            target_uri: Some(format!("cairn://p/{project}/{number}")),
            kind: "review",
            initiated_by: OutboundInitiator::CairnPush,
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
    use crate::channels::ledger::expire_undelivered;
    use crate::models::IMessageChannelConfig;
    use crate::storage::migrated_test_db;

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
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('reviewer', 'p', 'issue', 'running', 1, 1);
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

        {
            let sends = provider.sends.lock().unwrap();
            assert!(sends.is_empty());
        }
        assert_eq!(*provider.presence_checks.lock().unwrap(), 1);
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

        async fn cleanup_question(&self, _: &ResolvedQuestionMessage) -> Result<(), String> {
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
        let provider = Arc::new(RecordingPollProvider {
            sends: Mutex::new(Vec::new()),
            deliveries: Mutex::new(Vec::new()),
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
            let provider = Arc::new(RecordingPollProvider {
                sends: Mutex::new(Vec::new()),
                deliveries: Mutex::new(Vec::new()),
                next_guid: std::sync::atomic::AtomicUsize::new(1),
                cleanups: std::sync::atomic::AtomicUsize::new(0),
            });
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
        let provider = Arc::new(RecordingPollProvider {
            sends: Mutex::new(Vec::new()),
            deliveries: Mutex::new(Vec::new()),
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

    #[tokio::test]
    async fn capability_rejection_notice_returns_to_the_arriving_discord_conversation() {
        let (orch, _db) = route_test_orchestrator("channel-router-discord-bounded-notice.db").await;
        let provider = Arc::new(RecordingPollProvider {
            sends: Mutex::new(Vec::new()),
            deliveries: Mutex::new(Vec::new()),
            next_guid: std::sync::atomic::AtomicUsize::new(1),
            cleanups: std::sync::atomic::AtomicUsize::new(0),
        });
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
        let provider = Arc::new(RecordingPollProvider {
            sends: Mutex::new(Vec::new()),
            deliveries: Mutex::new(Vec::new()),
            next_guid: std::sync::atomic::AtomicUsize::new(1),
            cleanups: std::sync::atomic::AtomicUsize::new(0),
        });
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

        router.draw_the_session_line().await.unwrap();

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

        router.draw_the_session_line().await.unwrap();

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

        router.draw_the_session_line().await.unwrap();

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
                    cleanup_claimed_question(&provider, &record, "✓ answered: Ship it")
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

    #[test]
    fn directed_route_addresses_resolve_to_provider_native_conversations() {
        let imessage: crate::channels::ConversationAddress =
            "imessage:USER@example.com".parse().unwrap();
        let telegram: crate::channels::ConversationAddress = "telegram:-123".parse().unwrap();
        let discord: crate::channels::ConversationAddress = "discord:1/2".parse().unwrap();
        assert_eq!(
            route_conversation(Some(&imessage)).as_deref(),
            Some("user@example.com")
        );
        assert_eq!(route_conversation(Some(&telegram)).as_deref(), Some("-123"));
        assert_eq!(route_conversation(Some(&discord)).as_deref(), Some("2"));
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
}
