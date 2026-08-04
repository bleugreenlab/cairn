use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cairn_common::uri::{build_node_permission_uri, build_node_question_uri, parse_uri};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ledger, render_text_floor, AskOption, ChannelProvider, InboundEvent, OperatorPresence,
    OutboundAsk, OutboundInitiator, OutboundMessage, ResolvedQuestionMessage,
};
use crate::{
    mcp::handlers::{
        permission::{resolve_permission_request, PermissionDecision, PermissionScope},
        planning::answer_prompt_id,
    },
    models::IMessageChannelConfig,
    orchestrator::Orchestrator,
    storage::{LocalDb, RowExt},
};

const CHANNEL: &str = "imessage";
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
const THREAD_POLL_LIMIT: usize = 10;
const THREAD_POLL_PREFIX: &str = "threads:";

#[derive(Debug, Deserialize)]
struct StoredQuestion {
    question: String,
    #[serde(default)]
    options: Vec<StoredOption>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ThreadPollOptions {
    labels: Vec<String>,
    bindings: HashMap<String, String>,
}

fn thread_poll_options(record: &ledger::OutboundRecord) -> Option<ThreadPollOptions> {
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
        Some(ThreadPollOptions { labels, bindings })
    })
}

fn thread_poll_answer(record: &ledger::OutboundRecord, text: &str) -> String {
    let trimmed = text.trim();
    let Some(index) = super::imessage::parse_reply_number(trimmed, usize::MAX) else {
        return trimmed.to_string();
    };
    thread_poll_options(record)
        .and_then(|options| options.labels.get(index).cloned())
        .unwrap_or_else(|| trimmed.to_string())
}

#[derive(Default)]
struct ReviewGates {
    gates: Vec<Gate>,
    expired_dangling: usize,
}

fn is_threads_command(text: &str) -> bool {
    text.trim().eq_ignore_ascii_case("threads")
}

fn require_command_delivery(delivered: bool) -> Result<(), String> {
    delivered
        .then_some(())
        .ok_or_else(|| "native thread poll delivery failed".to_string())
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
    if !ledger::claim_question_cleanup(db, &record.id, chrono::Utc::now().timestamp()).await? {
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
    format!("{project}-{number} review ready — {title}\n{content_ref}")
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
    config: IMessageChannelConfig,
    claims: ClaimSet,
    deferred_attention: Mutex<HashMap<String, DeferredAttention>>,
}

impl ChannelRouter {
    pub fn new(
        orch: Orchestrator,
        provider: Arc<dyn ChannelProvider>,
        config: IMessageChannelConfig,
    ) -> Self {
        Self {
            orch,
            provider,
            config,
            claims: ClaimSet::default(),
            deferred_attention: Mutex::new(HashMap::new()),
        }
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
        for follow in ledger::list_thread_follows(self.ledger(), CHANNEL).await? {
            let live_edge = self.thread_live_edge(&follow.thread_uri).await?;
            ledger::advance_thread_cursor(self.ledger(), CHANNEL, &follow.thread_uri, live_edge)
                .await?;
        }
        let expired =
            ledger::expire_undelivered(self.ledger(), CHANNEL, chrono::Utc::now().timestamp())
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
        let options = thread_poll_options(record)
            .ok_or_else(|| "thread poll has no option bindings".to_string())?;
        let thread_uri = options
            .bindings
            .get(selected_label)
            .ok_or_else(|| format!("unknown thread poll option: {selected_label}"))?;
        if ledger::is_thread_followed(self.ledger(), CHANNEL, thread_uri).await? {
            self.unfollow_thread(thread_uri).await
        } else {
            self.follow_thread(thread_uri).await
        }
    }

    async fn follow_thread(&self, thread_uri: &str) -> Result<(), String> {
        let now = chrono::Utc::now().timestamp();
        let live_edge = self.thread_live_edge(thread_uri).await?;
        ledger::follow_thread(self.ledger(), CHANNEL, thread_uri, now, live_edge).await?;
        ledger::set_thread_focus(self.ledger(), CHANNEL, thread_uri, now).await
    }

    async fn unfollow_thread(&self, thread_uri: &str) -> Result<(), String> {
        ledger::unfollow_thread(self.ledger(), CHANNEL, thread_uri).await?;
        for update in ledger::list_unresolved(self.ledger(), CHANNEL).await? {
            if update.kind == "review"
                && update
                    .binding_ref
                    .starts_with(&format!("{thread_uri}:event:"))
                && ledger::claim_outbound_cleanup(
                    self.ledger(),
                    &update.id,
                    chrono::Utc::now().timestamp(),
                )
                .await?
            {
                cleanup_claimed_question(self.provider.as_ref(), &update, "✓ thread unfollowed")
                    .await;
            }
        }
        Ok(())
    }

    async fn send_threads_poll(&self, conversation: &str) -> Result<(), String> {
        // Unlike an answered one-shot question, a thread poll is a standing
        // control surface. Its GUID binding remains live for the lifetime of the
        // durable ledger row, including after newer thread polls are issued.
        let (mut threads, total) = self.active_threads().await?;
        if threads.is_empty() {
            return self
                .send_notice(conversation, "Nothing is active right now.")
                .await;
        }
        for (label, thread_uri) in &mut threads {
            if ledger::is_thread_followed(self.ledger(), CHANNEL, thread_uri).await? {
                *label = format!("✓ {label}");
            }
        }
        let caption = if total > threads.len() {
            format!(
                "Follow threads (showing {} most recent of {total})",
                threads.len()
            )
        } else {
            "Follow threads".to_string()
        };
        let binding_ref = format!("{THREAD_POLL_PREFIX}{}", Uuid::new_v4());
        let gate = Gate {
            kind: "question",
            initiated_by: OutboundInitiator::OperatorInbound,
            binding_ref: binding_ref.clone(),
            job_id: None,
            context: String::new(),
            ask: OutboundAsk::Question {
                prompt_id: binding_ref,
                question_index: 0,
                text: caption,
                options: threads
                    .iter()
                    .map(|(label, _)| AskOption {
                        label: label.clone(),
                        description: None,
                    })
                    .collect(),
            },
        };
        let Some(id) = claim_gate(&self.claims, self.ledger(), conversation, "poll", &gate).await?
        else {
            return Ok(());
        };
        require_command_delivery(self.send_claimed(id.clone(), gate).await?)?;
        let bindings: HashMap<String, String> = threads.iter().cloned().collect();
        let options = ThreadPollOptions {
            labels: threads.into_iter().map(|(label, _)| label).collect(),
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

    async fn active_threads(&self) -> Result<(Vec<(String, String)>, usize), String> {
        let mut threads = Vec::new();
        for db in self.orch.db.all_dbs().await {
            let mut found = db.query_all(
                "SELECT p.key, i.number, i.title, i.updated_at FROM issues i JOIN projects p ON p.id = i.project_id WHERE LOWER(i.kind) IN ('issue', 'thread') AND LOWER(i.status) = 'active' ORDER BY i.updated_at DESC",
                (),
                |row| Ok((row.text(0)?, row.i64(1)? as i32, row.text(2)?, row.i64(3)?)),
            ).await.map_err(|error| error.to_string())?;
            threads.append(&mut found);
        }
        threads.sort_by_key(|thread| std::cmp::Reverse(thread.3));
        let total = threads.len();
        Ok((
            threads
                .into_iter()
                .take(THREAD_POLL_LIMIT)
                .map(|(project, number, title, _)| {
                    (
                        format!("{number} · {title}"),
                        format!("cairn://p/{project}/{number}"),
                    )
                })
                .collect(),
            total,
        ))
    }

    async fn thread_live_edge(&self, thread_uri: &str) -> Result<i64, String> {
        let parsed =
            parse_uri(thread_uri).ok_or_else(|| format!("invalid thread URI: {thread_uri}"))?;
        let project = parsed
            .project()
            .ok_or_else(|| "thread URI has no project".to_string())?;
        let number = parsed
            .issue_number()
            .ok_or_else(|| "thread URI has no issue".to_string())?;
        let db = self.orch.db.for_project(project).await;
        db.query_opt_i64(
            "SELECT COALESCE(MAX(e.rowid), 0) FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE p.key = ?1 AND i.number = ?2",
            params![project.to_uppercase(), number],
        ).await.map(|edge| edge.unwrap_or(0)).map_err(|error| error.to_string())
    }

    async fn route_to_thread(&self, thread_uri: &str, text: &str) -> Result<(), String> {
        let parsed =
            parse_uri(thread_uri).ok_or_else(|| format!("invalid thread URI: {thread_uri}"))?;
        let project = parsed
            .project()
            .ok_or_else(|| "thread URI has no project".to_string())?;
        let number = parsed
            .issue_number()
            .ok_or_else(|| "thread URI has no issue".to_string())?;
        let db = self.orch.db.for_project(project).await;
        let job_id = db.query_opt(
            "SELECT j.id FROM jobs j JOIN runs r ON r.job_id = j.id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE p.key = ?1 AND i.number = ?2 AND j.parent_job_id IS NULL ORDER BY r.created_at DESC LIMIT 1",
            params![project.to_uppercase(), number],
            |row| row.text(0),
        ).await.map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{thread_uri} has no addressable node"))?;
        crate::execution::jobs::continue_job_or_enqueue(&self.orch, &job_id, Some(text), None)
            .map(|_| ())
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
        claim_gate(
            &self.claims,
            self.ledger(),
            &self.config.to,
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
        if !self.config.enabled || self.config.to.trim().is_empty() {
            return;
        }
        if let Err(error) = self.sweep_live_gates().await {
            log::warn!("channel gate sweep failed: {error}");
        }
        if let Err(error) = self.sweep_thread_updates().await {
            log::warn!("channel thread update sweep failed: {error}");
        }
    }

    async fn sweep_thread_updates(&self) -> Result<(), String> {
        let follows = ledger::list_thread_follows(self.ledger(), CHANNEL).await?;
        let now = Instant::now();
        for follow in follows {
            let parsed = parse_uri(&follow.thread_uri)
                .ok_or_else(|| format!("invalid followed thread URI: {}", follow.thread_uri))?;
            let project = parsed
                .project()
                .ok_or_else(|| "followed thread has no project".to_string())?;
            let number = parsed
                .issue_number()
                .ok_or_else(|| "followed thread has no issue".to_string())?;
            let db = self.orch.db.for_project(project).await;
            let events = db.query_all(
                "SELECT e.rowid, e.data, i.title, j.id FROM events e JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE p.key = ?1 AND i.number = ?2 AND e.rowid > ?3 AND e.event_type = 'assistant' ORDER BY e.rowid",
                params![project.to_uppercase(), number, follow.cursor_rowid],
                |row| Ok((row.i64(0)?, row.text(1)?, row.text(2)?, row.text(3)?)),
            ).await.map_err(|error| error.to_string())?;
            for (rowid, data, title, job_id) in events {
                let consumed = if let Some(text) = assistant_text(&data) {
                    let gate = Gate {
                        kind: "review",
                        initiated_by: OutboundInitiator::OperatorSubscription,
                        binding_ref: format!("{}:event:{rowid}", follow.thread_uri),
                        job_id: Some(job_id),
                        context: format!("{project}-{number} {title}"),
                        ask: OutboundAsk::Notify { text },
                    };
                    self.deliver_or_defer(gate, OperatorPresence::Away, now)
                        .await?
                } else {
                    true
                };
                if consumed {
                    ledger::advance_thread_cursor(
                        self.ledger(),
                        CHANNEL,
                        &follow.thread_uri,
                        rowid,
                    )
                    .await?;
                } else {
                    break;
                }
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
            self.provider.operator_presence().await
        } else {
            OperatorPresence::Away
        };
        let now = Instant::now();
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
            ledger::mark_expired(self.ledger(), &id, chrono::Utc::now().timestamp()).await?;
        }
        if snapshot_complete {
            for record in ledger::list_unresolved(self.ledger(), CHANNEL).await? {
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
        if self.config.route.question {
            gates.extend(load_questions(db).await?);
        }
        if self.config.route.permission {
            gates.extend(load_permissions(db).await?);
        }
        if self.config.route.review {
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
        if let Some(record) =
            ledger::get_by_binding(self.ledger(), CHANNEL, gate.kind, &gate.binding_ref).await?
        {
            return match record.status.as_str() {
                "failed" => self.send_claimed(record.id, gate).await,
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
            conversation: self.config.to.clone(),
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
                    chrono::Utc::now().timestamp(),
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
        if let Some(record) = ledger::get_by_provider_guid(self.ledger(), CHANNEL, guid).await? {
            return self.resolve_record(record, text).await;
        }
        if is_threads_command(text) {
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
        let Some(record) = ledger::get_by_provider_guid(self.ledger(), CHANNEL, guid).await? else {
            return self.store_unsolicited(Some(guid), sender, text).await;
        };
        if record.binding_ref.starts_with(THREAD_POLL_PREFIX) {
            let options = thread_poll_options(&record)
                .ok_or_else(|| "thread poll has no option bindings".to_string())?;
            if let Some(thread_uri) = options.bindings.get(text) {
                if selected {
                    self.follow_thread(thread_uri).await?;
                } else {
                    self.unfollow_thread(thread_uri).await?;
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
        if is_threads_command(text) {
            if let Err(error) = self.send_threads_poll(sender).await {
                log::warn!("channel could not answer threads command: {error}");
                self.send_notice(sender, &format!("Could not list active threads: {error}"))
                    .await?;
            }
            return Ok(());
        }
        if !text.trim_start().starts_with('/') {
            let focused = ledger::get_thread_focus(self.ledger(), CHANNEL)
                .await?
                .unwrap_or_else(|| {
                    crate::config::settings::load_settings(&self.orch.config_dir)
                        .channels
                        .default_thread
                });
            return self.route_to_thread(&focused, text).await;
        }
        let mut matches = ledger::list_unresolved(self.ledger(), CHANNEL)
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
                if record.binding_ref.starts_with(THREAD_POLL_PREFIX) {
                    let selected_label = thread_poll_answer(&record, text);
                    return self.follow_poll_selection(&record, &selected_label).await;
                }
                let answer = question_answer(&record, text);
                let won_answer_claim = ledger::claim_question_answer(
                    self.ledger(),
                    &record.id,
                    &answer,
                    chrono::Utc::now().timestamp(),
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
                    ledger::answered_for_prompt(self.ledger(), CHANNEL, prompt_id).await?;
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
                resolve_permission_request(
                    &self.orch,
                    &record.binding_ref,
                    decision,
                    PermissionScope::Once,
                )
                .await?;
                ledger::mark_resolved(self.ledger(), &record.id, chrono::Utc::now().timestamp())
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
                )?;
                ledger::mark_resolved(self.ledger(), &record.id, chrono::Utc::now().timestamp())
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
                channel: CHANNEL.into(),
                provider_guid: guid.map(str::to_string),
                sender: sender.into(),
                text: text.into(),
                received_at: chrono::Utc::now().timestamp(),
                acknowledged_at: None,
            },
        )
        .await?;
        self.send_notice(sender, "No active ask — your message is visible in Cairn.")
            .await?;
        ledger::mark_inbound_acknowledged(db, &id, chrono::Utc::now().timestamp()).await?;
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
    config: IMessageChannelConfig,
) -> Vec<tokio::task::JoinHandle<()>> {
    let router = Arc::new(ChannelRouter::new(orch, provider.clone(), config));
    let sweep = router.clone();
    let sweep_task = tokio::spawn(async move {
        // The channel is a live tap: this session carries the asks raised from
        // here on, and the backlog that predates it belongs to the app, not the
        // phone. Sealing it off COMPLETELY is a precondition for sweeping at all,
        // so a failure parks the sweep rather than letting it text the backlog.
        while let Err(error) = sweep.draw_the_session_line().await {
            super::set_router_blocker(Some(error.clone()));
            log::warn!(
                "channel cannot seal the pre-session backlog, so it is not sweeping yet: {error}"
            );
            // A retry must resnapshot the complete backlog to preserve the safety
            // boundary. Pace that expensive work separately from ordinary sweeps.
            tokio::time::sleep(BACKLOG_SEAL_RETRY_INTERVAL).await;
        }
        super::set_router_blocker(None);
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            interval.tick().await;
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
    vec![sweep_task, inbound_task]
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
                    "channel expired dangling review {} because issue {project}-{number} no longer exists",
                    push.id
                );
                continue;
            };
            result.gates.push(Gate {
                kind: "review",
                initiated_by: OutboundInitiator::CairnPush,
                binding_ref: push.id.clone(),
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
async fn claim_gate(
    claims: &ClaimSet,
    ledger: &LocalDb,
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
            channel: CHANNEL,
            kind: gate.kind,
            binding_ref: &gate.binding_ref,
            conversation,
            job_id: gate.job_id.as_deref(),
            rendered_text: &rendered,
            rendering,
            created_at: chrono::Utc::now().timestamp(),
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
mod tests {
    use super::*;
    use crate::channels::ledger::expire_undelivered;
    use crate::storage::migrated_test_db;

    #[tokio::test]
    async fn dangling_review_references_expire_individually_without_aborting_the_batch() {
        let db = migrated_test_db("channel-router-dangling-reviews.db").await;
        db.execute_batch(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at, kind)
               VALUES('reviewer-issue', 'p', 1, 'Reviewer', 'active', 1, 1, 'thread');
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
            vec!["live-review"],
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

    #[test]
    fn threads_command_is_exact_case_insensitive_bare_word() {
        assert!(is_threads_command("threads"));
        assert!(is_threads_command("  Threads\n"));
        assert!(!is_threads_command("/threads"));
        assert!(!is_threads_command("threads please"));
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
            OperatorPresence::Present
        }
    }

    #[tokio::test]
    async fn followed_thread_sweep_sends_immediately_while_operator_is_present() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(migrated_test_db("channel-router-follow-presence.db").await);
        db.execute_batch(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at, kind)
               VALUES ('i', 'p', 1, 'Followed work', 'active', 1, 1, 'thread');
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e', 'build', 'i', 'p', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('j', 'e', 'i', 'p', 'running', 'builder', 'builder', 1, 1);
             INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at)
               VALUES ('r', 'j', 'i', 'running', 1, 1);
             INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('event', 'r', 1, 1, 'assistant', '{\"content\":\"subscribed update\",\"toolUses\":[]}', 1);",
        )
        .await
        .unwrap();
        ledger::follow_thread(&db, CHANNEL, "cairn://p/CAIRN/1", 1, 0)
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

        router.sweep_thread_updates().await.unwrap();

        {
            let sends = provider.sends.lock().unwrap();
            assert_eq!(sends.len(), 1);
            assert_eq!(
                sends[0].initiated_by,
                OutboundInitiator::OperatorSubscription
            );
        }
        assert_eq!(*provider.presence_checks.lock().unwrap(), 0);
        let record = ledger::get_by_binding(&db, CHANNEL, "review", "cairn://p/CAIRN/1:event:1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.status, "sent");
        assert_eq!(record.sent_at, Some(record.created_at));
        assert!(router.deferred_attention.lock().unwrap().is_empty());
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
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at, kind)
               VALUES ('i1', 'p', 1, 'First', 'active', 1, 2, 'thread'),
                      ('i2', 'p', 2, 'Second', 'active', 1, 3, 'thread');",
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
                matches!(sends.as_slice(), [OutboundAsk::Question { .. }, OutboundAsk::Notify { text }] if text.contains("Could not list active threads"))
            );
        }
        let poll = ledger::list_unresolved(&db, CHANNEL)
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.binding_ref.starts_with(THREAD_POLL_PREFIX))
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
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at, kind)
               VALUES ('i1', 'p', 1, 'First', 'active', 1, 3, 'thread'),
                      ('i2', 'p', 2, 'Second', 'active', 1, 2, 'thread');",
        )
        .await
        .unwrap();
        assert!(
            ledger::follow_thread(&db, CHANNEL, "cairn://p/CAIRN/1", 1, 0)
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

        router.send_threads_poll("+15551234567").await.unwrap();
        match &provider.sends.lock().unwrap()[0] {
            OutboundAsk::Question { options, .. } => {
                assert!(options[0].label.starts_with("✓ "));
            }
            _ => panic!("threads command must send a poll"),
        }
        let legacy_bindings = serde_json::json!({
            "✓ 1 · First": "cairn://p/CAIRN/1",
            "2 · Second": "cairn://p/CAIRN/2"
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
            !ledger::is_thread_followed(&db, CHANNEL, "cairn://p/CAIRN/1")
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
            ledger::is_thread_followed(&db, CHANNEL, "cairn://p/CAIRN/1")
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
            !ledger::is_thread_followed(&db, CHANNEL, "cairn://p/CAIRN/1")
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
            ledger::is_thread_followed(&db, CHANNEL, "cairn://p/CAIRN/1")
                .await
                .unwrap()
        );

        router.send_threads_poll("+15551234567").await.unwrap();
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
            .filter(|record| record.binding_ref.starts_with(THREAD_POLL_PREFIX))
            .collect::<Vec<_>>();
        assert_eq!(polls.len(), 2);
        assert!(polls.iter().all(|poll| poll.status == "sent"));
        assert_eq!(
            provider.cleanups.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "standing controls are never routed through answered-question cleanup"
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
    fn followed_thread_subscription_is_exempt_from_presence_deferral() {
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

        assert!(
            !subscription.is_presence_aware(),
            "a followed-thread delivery carries standing operator intent"
        );
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
