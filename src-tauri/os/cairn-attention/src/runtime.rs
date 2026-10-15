//! Application capabilities used while resolving attention recipients.

use async_trait::async_trait;
use cairn_db::storage::LocalDb;
use cairn_db::{storage::DbResult, turso::Connection};
use cairn_domain::DeliveryUrgency;
use std::sync::Arc;

/// Recipient capabilities that need application policy inside an attention read/write.
#[async_trait]
pub trait RecipientHost: Send + Sync {
    /// Use this connection for all writes so session creation shares the caller's
    /// commit or rollback. Do not open another write transaction here.
    async fn ensure_thread_session(&self, conn: &Connection, thread_id: &str) -> DbResult<String>;
    /// Return the application's canonical execution fence using this connection.
    async fn job_is_fenced(&self, conn: &Connection, job_id: &str) -> DbResult<bool>;
    /// Whether the job belongs to a merged or closed issue, using this
    /// connection. Thread-owned jobs answer `false`: dormancy governs them.
    async fn job_issue_is_resolved(&self, conn: &Connection, job_id: &str) -> DbResult<bool>;
}

/// Execution and publication supplied by the application. Durable attention
/// selection, matching and acknowledgement remain in this crate.
#[async_trait]
pub trait AttentionHost: RecipientHost {
    async fn evaluate_condition(
        &self,
        condition: &crate::wakes::conditions::WakeCondition,
    ) -> crate::wakes::conditions::ConditionOutcome;
    async fn database_for_issue(&self, issue_id: &str) -> Result<Arc<LocalDb>, String>;
    async fn database_for_project(&self, project_key: &str) -> Arc<LocalDb>;
    /// Attempt to run a recipient whose durable push has already committed.
    /// Failure leaves the push and its occurrence/edge cursor intact.
    fn nudge(&self, job_id: &str, urgency: DeliveryUrgency) -> Result<(), String>;
    async fn message(&self, db: &LocalDb, message_id: &str) -> Option<cairn_domain::Message>;
    /// Resolve application-specific card content; `None` selects the engine's fallback.
    async fn render_content(&self, request: ContentRequest<'_>) -> Option<String>;
    async fn mark_thread_chapter(
        &self,
        db: &LocalDb,
        recipient: &str,
        child_issue_id: &str,
        child_issue_uri: &str,
        final_status: &str,
    );

    async fn database_for_job(&self, job_id: &str) -> Result<Arc<LocalDb>, String>;
    fn emit_change(&self, table: &str, action: Option<&str>);
    /// Queue or resume a subscription delivery using application admission policy.
    fn deliver(
        &self,
        job_id: &str,
        sender: Option<&str>,
        message: &str,
        urgency: DeliveryUrgency,
        operator_message_id: Option<&str>,
    );
}

/// Private subscription/schedule state plus application routing and execution.
/// Shared push operations receive their owning database explicitly.
#[derive(Clone)]
pub struct AttentionRuntime {
    pub(crate) private_db: Arc<LocalDb>,
    pub(crate) host: Arc<dyn AttentionHost>,
}
impl AttentionRuntime {
    pub fn new(private_db: Arc<LocalDb>, host: Arc<dyn AttentionHost>) -> Self {
        Self { private_db, host }
    }
}

/// Application-owned content included in an attention card.
pub enum ContentRequest<'a> {
    BuildChange(&'a str),
    Checks(&'a str),
    ChildStall {
        recipient: &'a str,
        reference: &'a str,
    },
}
