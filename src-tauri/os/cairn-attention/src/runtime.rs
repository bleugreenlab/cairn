//! Application capabilities used while resolving attention recipients.

use async_trait::async_trait;
use cairn_db::storage::LocalDb;
use cairn_db::storage::{DbResult, TrackedConnection};
use cairn_domain::DeliveryUrgency;
use std::sync::Arc;

/// Recipient capabilities that need application policy inside an attention read/write.
#[async_trait]
pub trait RecipientHost: Send + Sync {
    /// Use this connection for all writes so session creation shares the caller's
    /// commit or rollback. Do not open another write transaction here.
    async fn ensure_thread_session(
        &self,
        conn: &TrackedConnection,
        thread_id: &str,
    ) -> DbResult<String>;
    /// Return the application's canonical execution fence using this connection.
    async fn job_is_fenced(&self, conn: &TrackedConnection, job_id: &str) -> DbResult<bool>;
    /// Whether the job belongs to a merged or closed issue, using this
    /// connection. Thread-owned jobs answer `false`: dormancy governs them.
    async fn job_issue_is_resolved(&self, conn: &TrackedConnection, job_id: &str)
        -> DbResult<bool>;
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
    /// The entry one cited URI resolves to, from the application's one
    /// references resolver; `None` when it names nothing a reader could be shown.
    async fn reference(&self, uri: &str) -> Option<cairn_common::references::ReferenceEntry>;
    /// The `## References` block for a message's text, rendered for an agent by
    /// that same resolver; `None` when the text names nothing that resolves.
    async fn references_block(&self, text: &str) -> Option<String>;
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
    /// Persist one direct delivery and run its recipient under application
    /// admission policy. The message row, its `direct:` push and the receipts
    /// it carries commit in one transaction when the receipts share the
    /// recipient's database; the nudge runs only after that commit.
    async fn deliver(&self, request: DeliveryRequest<'_>) -> DeliveryOutcome;
}

/// One direct delivery to one job.
#[derive(Clone, Debug)]
pub struct DeliveryRequest<'a> {
    pub job_id: &'a str,
    pub sender: Option<&'a str>,
    pub message: &'a str,
    pub urgency: DeliveryUrgency,
    pub operator_message_id: Option<&'a str>,
    /// The message id to write, when the caller derived a deterministic one
    /// ([`crate::wakes::delivery_message_id`]); a fresh id otherwise.
    pub message_id: Option<&'a str>,
    /// Receipts in the runtime's private database this delivery settles.
    pub receipts: &'a [String],
}

impl<'a> DeliveryRequest<'a> {
    pub fn new(job_id: &'a str, message: &'a str, urgency: DeliveryUrgency) -> Self {
        Self {
            job_id,
            sender: None,
            message,
            urgency,
            operator_message_id: None,
            message_id: None,
            receipts: &[],
        }
    }
}

/// What a delivery did. Only `Persisted` and `Launched` settle the receipts
/// the request carried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// The message and its push are durable and the recipient was nudged.
    Persisted { message_id: String, job_id: String },
    /// The recipient had never run, so it was launched with the message as its
    /// prompt (the direct-message launch path).
    Launched { job_id: String },
    /// The recipient cannot take a delivery now, and why. Nothing was written.
    Refused(String),
    /// Persisting failed; nothing the caller can rely on was written.
    Failed(String),
}

impl DeliveryOutcome {
    pub fn job_id(&self) -> Option<&str> {
        match self {
            Self::Persisted { job_id, .. } | Self::Launched { job_id } => Some(job_id),
            Self::Refused(_) | Self::Failed(_) => None,
        }
    }
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
    Checks(&'a str),
    ChildStall {
        recipient: &'a str,
        reference: &'a str,
    },
    ChildUnresumed {
        recipient: &'a str,
        reference: &'a str,
    },
    ChildUnowned {
        recipient: &'a str,
        reference: &'a str,
    },
}
