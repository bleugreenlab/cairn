use std::{sync::Arc, time::Duration};

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
        for gate in gates {
            self.deliver(db, gate).await?;
        }
        Ok(())
    }

    async fn deliver(&self, db: &LocalDb, gate: Gate) -> Result<(), String> {
        let rendering = if self.provider.capabilities().structured_asks
            && !matches!(gate.ask, OutboundAsk::Notify { .. })
        {
            "poll"
        } else {
            "text"
        };
        let rendered = render_text_floor(&gate.ask);
        let id = Uuid::new_v4().to_string();
        let inserted = ledger::insert_intent(
            db,
            &ledger::NewOutbound {
                id: &id,
                channel: CHANNEL,
                kind: gate.kind,
                binding_ref: &gate.binding_ref,
                conversation: &self.config.to,
                job_id: gate.job_id.as_deref(),
                rendered_text: &rendered,
                rendering,
                created_at: chrono::Utc::now().timestamp(),
            },
        )
        .await?;
        if !inserted {
            return Ok(());
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
            conversation: self.config.to.clone(),
            ask: gate.ask,
            context_header: gate.context,
        };
        match self.provider.send(&message).await {
            Ok(sent) => {
                ledger::mark_sent(
                    db,
                    &id,
                    &sent.primary_guid,
                    sent.caption_guid.as_deref(),
                    options_json.as_deref(),
                    chrono::Utc::now().timestamp(),
                )
                .await?;
            }
            Err(error) => {
                ledger::mark_failed(db, &id, &error).await?;
            }
        }
        Ok(())
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
        for db in self.orch.db.all_dbs().await {
            if let Some(record) = ledger::get_by_provider_guid(&db, CHANNEL, guid).await? {
                return self.resolve_record(&db, record, text).await;
            }
        }
        self.store_unsolicited(Some(guid), "unknown", text).await
    }

    async fn resolve_bare(&self, sender: &str, text: &str) -> Result<(), String> {
        let mut matches = Vec::new();
        for db in self.orch.db.all_dbs().await {
            for record in ledger::list_unresolved(&db, CHANNEL).await? {
                if record.status == "sent"
                    && super::imessage::normalize_handle(&record.conversation)
                        == super::imessage::normalize_handle(sender)
                {
                    matches.push((db.clone(), record));
                }
            }
        }
        if matches.len() == 1 {
            let (db, record) = matches.pop().expect("one match");
            return self.resolve_record(&db, record, text).await;
        }
        if matches.len() > 1 {
            return self.send_notice(sender, "I found more than one active ask. Please reply to the specific message you want to answer.").await;
        }
        self.store_unsolicited(None, sender, text).await
    }

    async fn resolve_record(
        &self,
        db: &LocalDb,
        record: ledger::OutboundRecord,
        text: &str,
    ) -> Result<(), String> {
        if record.status == "resolved" {
            return Ok(());
        }
        match record.kind.as_str() {
            "question" => {
                let answer = question_answer(&record, text);
                ledger::record_answer(db, &record.id, &answer).await?;
                ledger::mark_resolved(db, &record.id, chrono::Utc::now().timestamp()).await?;
                let (prompt_id, _) = record
                    .binding_ref
                    .rsplit_once(':')
                    .ok_or_else(|| format!("invalid question binding: {}", record.binding_ref))?;
                let question_count = prompt_question_count(db, prompt_id).await?;
                let answers = ledger::answered_for_prompt(db, CHANNEL, prompt_id).await?;
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
                ledger::mark_resolved(db, &record.id, chrono::Utc::now().timestamp()).await?;
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
                ledger::mark_resolved(db, &record.id, chrono::Utc::now().timestamp()).await?;
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

async fn load_questions(db: &LocalDb) -> Result<Vec<Gate>, String> {
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query("SELECT p.id, p.questions, COALESCE(p.job_id, r.job_id), COALESCE(j.node_name, j.uri_segment, 'agent') FROM prompts p JOIN runs r ON r.id=p.run_id LEFT JOIN jobs j ON j.id=COALESCE(p.job_id,r.job_id) LEFT JOIN issues i ON i.id=r.issue_id WHERE p.response IS NULL AND COALESCE(i.status,'open') NOT IN ('merged','closed','failed') ORDER BY p.created_at LIMIT 100", ()).await?;
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
        let mut rows = conn.query("SELECT pr.id, pr.tool_name, pr.tool_input, COALESCE(pr.job_id,r.job_id), COALESCE(j.node_name,j.uri_segment,'agent') FROM permission_requests pr JOIN runs r ON r.id=pr.run_id LEFT JOIN jobs j ON j.id=COALESCE(pr.job_id,r.job_id) LEFT JOIN issues i ON i.id=r.issue_id WHERE pr.status='pending' AND COALESCE(i.status,'open') NOT IN ('merged','closed','failed') ORDER BY pr.created_at LIMIT 100", ()).await?;
        let mut gates=Vec::new(); while let Some(row)=rows.next().await? { let id=row.text(0)?; let tool=row.text(1)?; let input=row.text(2)?; gates.push(Gate { kind:"permission", binding_ref:id.clone(), job_id:row.opt_text(3)?, context:format!("[Cairn · {}]",row.text(4)?), ask:OutboundAsk::Permission { request_id:id, summary:format!("Allow {tool}?\n{input}") } }); } Ok(gates)
    })).await.map_err(|e| e.to_string())
}

async fn load_reviews(db: &LocalDb) -> Result<Vec<Gate>, String> {
    let pushes = db.read(|conn| Box::pin(async move {
        let mut rows=conn.query("SELECT id,recipient,content_ref,wake,boundary,\"key\",created_at,delivered_event_id FROM attention_pushes WHERE delivered_event_id IS NULL AND \"key\" LIKE 'review:%' ORDER BY created_at LIMIT 100",()).await?; let mut out=Vec::new(); while let Some(row)=rows.next().await? { out.push(crate::orchestrator::attention_push::Push { id:row.text(0)?,recipient:row.text(1)?,content_ref:row.text(2)?,wake:crate::orchestrator::attention_push::Wake::from_db(&row.text(3)?).unwrap(),boundary:crate::orchestrator::attention_push::Boundary::from_db(&row.text(4)?).unwrap(),key:row.text(5)?,created_at:row.i64(6)?,delivered_event_id:row.opt_text(7)? }); } Ok(out)
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

async fn prompt_question_count(db: &LocalDb, prompt_id: &str) -> Result<usize, String> {
    let json = db
        .query_opt_text(
            "SELECT questions FROM prompts WHERE id=?1",
            params![prompt_id.to_string()],
        )
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "prompt not found".to_string())?;
    serde_json::from_str::<Vec<serde_json::Value>>(&json)
        .map(|v| v.len())
        .map_err(|e| e.to_string())
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
