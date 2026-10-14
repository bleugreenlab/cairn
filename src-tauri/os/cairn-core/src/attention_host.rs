//! Application operations supplied to the durable attention engine.

use crate::models::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use async_trait::async_trait;
use cairn_attention::runtime::{AttentionHost, AttentionRuntime, RecipientHost};
use cairn_attention::wakes::conditions::{ConditionOutcome, WakeCondition};
use cairn_db::storage::LocalDb;
use cairn_db::turso::params;
use cairn_db::{storage::DbResult, turso::Connection};
use std::sync::Arc;

pub(crate) struct CoreRecipients;
#[async_trait]
impl cairn_attention::runtime::RecipientHost for CoreRecipients {
    async fn ensure_thread_session(&self, conn: &Connection, thread_id: &str) -> DbResult<String> {
        crate::threads::ensure_thread_session_conn(conn, thread_id, None).await
    }
    async fn job_is_fenced(&self, conn: &Connection, job_id: &str) -> DbResult<bool> {
        crate::execution::canonical::fence_for_job_conn(conn, job_id)
            .await
            .map(|fence| fence.is_superseded())
    }
    async fn job_issue_is_resolved(&self, conn: &Connection, job_id: &str) -> DbResult<bool> {
        let mut rows = conn
            .query(
                "SELECT 1 FROM jobs j JOIN issues i ON i.id = j.issue_id
                 WHERE j.id = ?1 AND j.thread_id IS NULL AND i.status IN ('merged', 'closed')
                 LIMIT 1",
                params![job_id],
            )
            .await?;
        Ok(rows.next().await?.is_some())
    }
}

pub(crate) fn runtime(orch: &Orchestrator) -> AttentionRuntime {
    AttentionRuntime::new(
        orch.db.local.clone(),
        Arc::new(CoreAttentionHost(orch.clone())),
    )
}
struct CoreAttentionHost(Orchestrator);
#[async_trait]
impl RecipientHost for CoreAttentionHost {
    async fn ensure_thread_session(&self, conn: &Connection, thread_id: &str) -> DbResult<String> {
        CoreRecipients.ensure_thread_session(conn, thread_id).await
    }
    async fn job_is_fenced(&self, conn: &Connection, job_id: &str) -> DbResult<bool> {
        CoreRecipients.job_is_fenced(conn, job_id).await
    }
    async fn job_issue_is_resolved(&self, conn: &Connection, job_id: &str) -> DbResult<bool> {
        CoreRecipients.job_issue_is_resolved(conn, job_id).await
    }
}
#[async_trait]
impl AttentionHost for CoreAttentionHost {
    async fn evaluate_condition(&self, condition: &WakeCondition) -> ConditionOutcome {
        let db = match crate::threads::db_store::open_existing(&condition.home_id).await {
            Ok(Some(db)) => db,
            Ok(None) => return ConditionOutcome::Empty,
            Err(error) => {
                return ConditionOutcome::Error(crate::threads::db_store::sanitize_error(error));
            }
        };
        match crate::resources::run_condition_sql(&db, &condition.sql).await {
            Ok((_rows, 0, _)) => ConditionOutcome::Empty,
            Ok((rows, shown, total)) => ConditionOutcome::Matched { rows, shown, total },
            Err(error) => ConditionOutcome::Error(crate::threads::db_store::sanitize_error(error)),
        }
    }

    async fn database_for_issue(&self, issue_id: &str) -> Result<Arc<LocalDb>, String> {
        crate::issues::crud::owning_db_for_issue(&self.0.db, issue_id)
            .await
            .map_err(|e| e.to_string())
    }
    async fn database_for_project(&self, project_key: &str) -> Arc<LocalDb> {
        self.0.db.for_project(project_key).await
    }
    fn nudge(&self, job_id: &str, urgency: DeliveryUrgency) -> Result<(), String> {
        crate::messages::delivery::nudge_job_for_urgency(&self.0, job_id, urgency)
    }
    async fn message(&self, db: &LocalDb, message_id: &str) -> Option<crate::models::Message> {
        crate::messages::db::get_message_by_id_async(db, message_id)
            .await
            .ok()
            .flatten()
    }
    async fn render_content(
        &self,
        request: cairn_attention::runtime::ContentRequest<'_>,
    ) -> Option<String> {
        use cairn_attention::runtime::ContentRequest;
        match request {
            ContentRequest::BuildChange(reference) => {
                crate::orchestrator::build_change::resolve(&self.0.db.local, reference)
                    .await
                    .ok()
                    .flatten()
                    .map(|change| crate::orchestrator::build_change::render(&change))
            }
            ContentRequest::Checks(job_id) => {
                crate::execution::checks_status::check_wake_body(&self.0, job_id).await
            }
            ContentRequest::ChildStall {
                recipient,
                reference,
            } => {
                crate::orchestrator::lifecycle::render_stall_push_body(
                    &self.0, recipient, reference,
                )
                .await
            }
            ContentRequest::Post(push) => {
                crate::orchestrator::wakes::render_post_push_body(&self.0.db.local, push).await
            }
        }
    }
    /// Record that a terminal child makes a watching thread's turns compactable.
    ///
    /// Best-effort by construction: compaction bookkeeping must never be able to
    /// break attention delivery, so a failure is logged and the push proceeds. The
    /// cost of a missed mark is one chapter's worth of turns staying verbatim until
    /// the child is finalized again or the thread compacts on expiry.
    async fn mark_thread_chapter(
        &self,
        db: &LocalDb,
        recipient: &str,
        child_issue_id: &str,
        child_issue_uri: &str,
        final_status: &str,
    ) {
        if !crate::threads::compaction_capability_for_job(db, recipient)
            .await
            .is_enabled()
        {
            return;
        }
        let title = match db
            .query_opt_text(
                "SELECT title FROM issues WHERE id = ?1",
                params![child_issue_id.to_string()],
            )
            .await
        {
            Ok(Some(title)) => title,
            // A chapter with no overview is exactly the bare address the design
            // rejects, so fall back to the URI's own text rather than an empty line.
            _ => child_issue_uri.to_string(),
        };
        let mark = crate::threads::compaction::ChildMark {
            child_issue_id: child_issue_id.to_string(),
            child_issue_uri: child_issue_uri.to_string(),
            child_title: title,
            final_status: final_status.to_string(),
            marked_at: chrono::Utc::now().timestamp(),
        };
        if let Err(error) =
            crate::threads::compaction::mark_child_terminal(db, recipient, &mark).await
        {
            log::warn!(
                "thread chapter mark for {} on {child_issue_uri} failed: {error}",
                &recipient[..recipient.len().min(8)]
            );
        }
    }

    async fn database_for_job(&self, job_id: &str) -> Result<Arc<LocalDb>, String> {
        crate::execution::routing::owning_db_for_job(&self.0.db, job_id)
            .await
            .map_err(|error| error.to_string())
    }
    fn emit_change(&self, table: &str, action: Option<&str>) {
        match action {
            Some(action) => {
                let _ = self.0.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": table, "action": action}),
                );
            }
            None => self.0.notifier.emit_change(table),
        }
    }
    fn deliver(
        &self,
        job_id: &str,
        sender: Option<&str>,
        message: &str,
        urgency: DeliveryUrgency,
        operator_message_id: Option<&str>,
    ) {
        crate::orchestrator::parent_wake::queue_or_resume_parent(
            &self.0,
            job_id,
            sender,
            message,
            urgency,
            operator_message_id,
        );
    }
}
