use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use cairn_db::turso::params;
use serde::Deserialize;
use uuid::Uuid;

use super::{
    ledger, render_text_floor, AskOption, ChannelProvider, InboundEvent, OutboundAsk,
    OutboundMessage,
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

#[derive(Debug, Deserialize)]
struct StoredQuestion {
    question: String,
    #[serde(default)]
    options: Vec<StoredOption>,
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

#[derive(Debug)]
struct Gate {
    kind: &'static str,
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
        for db in self.orch.db.all_dbs().await {
            fenced += self.fence_existing_gates(&db).await?;
        }
        let expired =
            ledger::expire_undelivered(self.ledger(), CHANNEL, chrono::Utc::now().timestamp())
                .await?;
        if fenced > 0 || expired > 0 {
            log::info!(
                "channel sealed the pre-session backlog: {fenced} gate(s) fenced, {expired} intent(s) expired"
            );
        }
        Ok(())
    }

    /// Claims every gate open in one database right now, across every kind
    /// regardless of the route flags, so turning a route on later cannot dump its
    /// accumulated backlog onto the phone.
    async fn fence_existing_gates(&self, db: &LocalDb) -> Result<usize, String> {
        let mut gates = load_questions(db).await?;
        gates.extend(load_permissions(db).await?);
        gates.extend(load_reviews(db).await?);
        let mut fenced = 0;
        for gate in gates {
            if self.claim(&gate).await?.is_some() {
                fenced += 1;
            }
        }
        Ok(fenced)
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
        for db in self.orch.db.all_dbs().await {
            if let Err(error) = self.sweep_db(&db).await {
                log::warn!("channel gate sweep failed: {error}");
            }
        }
    }

    async fn sweep_db(&self, db: &LocalDb) -> Result<(), String> {
        let mut gates = Vec::new();
        if self.config.route.question {
            gates.extend(load_questions(db).await?);
        }
        if self.config.route.permission {
            gates.extend(load_permissions(db).await?);
        }
        if self.config.route.review {
            gates.extend(load_reviews(db).await?);
        }
        let mut delivered = 0;
        for gate in gates {
            if delivered == SWEEP_LIMIT {
                break;
            }
            if self.deliver(gate).await? {
                delivered += 1;
            }
        }
        Ok(())
    }

    /// Reports whether this call actually put something on the wire, so a sweep's
    /// batch bounds deliveries rather than gates examined.
    async fn deliver(&self, gate: Gate) -> Result<bool, String> {
        // The claim is what makes a gate live: one the channel has already seen --
        // in this session, or in the backlog it sealed off at startup -- is
        // already claimed, and that is how a sweep says "not mine".
        let Some(id) = self.claim(&gate).await? else {
            return Ok(false);
        };
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
            }
        }
        Ok(true)
    }

    pub async fn handle_inbound(&self, event: InboundEvent) -> Result<(), String> {
        match event {
            InboundEvent::Selection {
                bound_guid,
                option_text,
                ..
            } => self.resolve_bound(&bound_guid, &option_text).await,
            InboundEvent::Reply {
                bound_guid, text, ..
            } => self.resolve_bound(&bound_guid, &text).await,
            InboundEvent::Bare { sender, text } => self.resolve_bare(&sender, &text).await,
        }
    }

    async fn resolve_bound(&self, guid: &str, text: &str) -> Result<(), String> {
        if let Some(record) = ledger::get_by_provider_guid(self.ledger(), CHANNEL, guid).await? {
            return self.resolve_record(record, text).await;
        }
        self.store_unsolicited(Some(guid), "unknown", text).await
    }

    async fn resolve_bare(&self, sender: &str, text: &str) -> Result<(), String> {
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
                let answer = question_answer(&record, text);
                ledger::record_answer(self.ledger(), &record.id, &answer).await?;
                ledger::mark_resolved(self.ledger(), &record.id, chrono::Utc::now().timestamp())
                    .await?;
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
            log::warn!(
                "channel cannot seal the pre-session backlog, so it is not sweeping yet: {error}"
            );
            tokio::time::sleep(SWEEP_INTERVAL).await;
        }
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
        let mut rows = conn.query("SELECT p.id, p.questions, COALESCE(p.job_id, r.job_id), COALESCE(j.node_name, j.uri_segment, 'agent') FROM prompts p JOIN runs r ON r.id=p.run_id LEFT JOIN jobs j ON j.id=COALESCE(p.job_id,r.job_id) LEFT JOIN issues i ON i.id=r.issue_id WHERE p.response IS NULL AND COALESCE(i.status,'open') NOT IN ('merged','closed','failed') ORDER BY p.created_at DESC", ()).await?;
        let mut gates = Vec::new();
        while let Some(row) = rows.next().await? {
            let prompt_id = row.text(0)?; let questions_json = row.text(1)?; let job_id = row.opt_text(2)?; let context = format!("[Cairn · {}]", row.text(3)?);
            let questions: Vec<StoredQuestion> = serde_json::from_str(&questions_json).map_err(|e| crate::storage::DbError::Row(e.to_string()))?;
            for (index, question) in questions.into_iter().enumerate() {
                gates.push(Gate { kind: "question", binding_ref: format!("{prompt_id}:{index}"), job_id: job_id.clone(), context: context.clone(), ask: OutboundAsk::Question { prompt_id: prompt_id.clone(), question_index: index, text: question.question, options: question.options.into_iter().map(|o| AskOption { label:o.label, description:o.description }).collect() } });
            }
        }
        Ok(gates)
    })).await.map_err(|e| e.to_string())
}

async fn load_permissions(db: &LocalDb) -> Result<Vec<Gate>, String> {
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query("SELECT pr.id, pr.tool_name, pr.tool_input, COALESCE(pr.job_id,r.job_id), COALESCE(j.node_name,j.uri_segment,'agent') FROM permission_requests pr JOIN runs r ON r.id=pr.run_id LEFT JOIN jobs j ON j.id=COALESCE(pr.job_id,r.job_id) LEFT JOIN issues i ON i.id=r.issue_id WHERE pr.status='pending' AND COALESCE(i.status,'open') NOT IN ('merged','closed','failed') ORDER BY pr.created_at DESC", ()).await?;
        let mut gates=Vec::new(); while let Some(row)=rows.next().await? { let id=row.text(0)?; let tool=row.text(1)?; let input=row.text(2)?; gates.push(Gate { kind:"permission", binding_ref:id.clone(), job_id:row.opt_text(3)?, context:format!("[Cairn · {}]",row.text(4)?), ask:OutboundAsk::Permission { request_id:id, summary:format!("Allow {tool}?\n{input}") } }); } Ok(gates)
    })).await.map_err(|e| e.to_string())
}

async fn load_reviews(db: &LocalDb) -> Result<Vec<Gate>, String> {
    let pushes = db.read(|conn| Box::pin(async move {
        let mut rows=conn.query("SELECT id,recipient,content_ref,wake,boundary,\"key\",created_at,delivered_event_id FROM attention_pushes WHERE delivered_event_id IS NULL AND \"key\" LIKE 'review:%' ORDER BY created_at DESC",()).await?; let mut out=Vec::new(); while let Some(row)=rows.next().await? { out.push(crate::orchestrator::attention_push::Push { id:row.text(0)?,recipient:row.text(1)?,content_ref:row.text(2)?,wake:crate::orchestrator::attention_push::Wake::from_db(&row.text(3)?).unwrap(),boundary:crate::orchestrator::attention_push::Boundary::from_db(&row.text(4)?).unwrap(),key:row.text(5)?,created_at:row.i64(6)?,delivered_event_id:row.opt_text(7)? }); } Ok(out)
    })).await.map_err(|e| e.to_string())?;
    let mut gates = Vec::new();
    for push in pushes {
        if crate::orchestrator::attention_push::lazy_resolve_live(db, &push)
            .await
            .map_err(|e| e.to_string())?
        {
            gates.push(Gate {
                kind: "review",
                binding_ref: push.id.clone(),
                job_id: Some(push.recipient),
                context: "[Cairn · review]".into(),
                ask: OutboundAsk::Notify {
                    text: format!(
                        "Work product ready for review: {}\nReply to send feedback to the agent.",
                        push.content_ref
                    ),
                },
            });
        }
    }
    Ok(gates)
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
}
