//! Host resources used by the durable workflow engine.

use crate::{
    advancement::RecomputeOutcome,
    conditions::ExecutedCheckpoint,
    effects::types::{EffectResult, JobEvent, WorkflowEffect},
    models::ConditionOutcome,
    storage::LocalDb,
};
use async_trait::async_trait;
use cairn_platform::services::emitter::EventEmitter;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

/// The entity whose durable state determines the database for an operation.
pub enum WorkflowOwner<'a> {
    Execution(&'a str),
    Job(&'a str),
    ActionRun(&'a str),
    Issue(&'a str),
}

/// External work requested by the engine. Durable workflow mutations never
/// cross this interface.
pub enum Workload {
    AgentJobs(Vec<crate::models::Job>),
    Action {
        action_run_id: String,
        execution_id: String,
        node_id: String,
        ctx: crate::effects::types::EffectContext,
    },
}

/// A decision node asking the host to answer its questions.
///
/// The host owns offer selection and the `decide` call; the engine routes on
/// the typed answers it returns.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionRequest {
    pub execution_id: String,
    pub node_id: String,
    pub node_name: String,
    /// The node's questions, authored model, and thresholds.
    pub decision: crate::models::decision::DecisionNode,
    /// The text every question is asked about: the upstream artifact
    /// context, serialized as pretty JSON.
    pub input: String,
}

/// Application operations supplied to the workflow engine.
///
/// Implementations route entity IDs, run external workloads and publish workflow
/// facts to the rest of the application. Workflow mutation and effect sequencing
/// remain in this crate.
#[async_trait]
pub trait WorkflowHost: Send + Sync {
    fn database(&self, owner: WorkflowOwner<'_>) -> Result<Arc<LocalDb>, String>;
    async fn databases(&self) -> Vec<Arc<LocalDb>>;
    fn claimed_launch_job_ids(&self) -> HashSet<String>;
    fn resolve_coordinate(&self, repository: &Path, branch: &str) -> Result<String, String>;
    fn job_head(&self, job_id: &str) -> Result<String, String>;
    fn pr_publish_input_revision(&self, action_run_id: &str) -> Result<Option<String>, String>;
    /// Answer a decision node's questions. A refused question is an answer,
    /// not an error; an error means nothing answered (no decision-capable
    /// offer, or the provider failed), and blocks the node with that text.
    async fn decide(
        &self,
        request: DecisionRequest,
    ) -> Result<crate::models::decision::Decision, String>;
    fn lifecycle_message(&self, job_id: &str, run_id: Option<&str>, event: JobEvent);
    fn projection_changed(&self, execution_id: &str, outcome: &RecomputeOutcome);
    /// A condition evaluation row was stored, routed or blocked, and the
    /// owning issue (if any) re-projected.
    fn condition_evaluated(
        &self,
        execution_id: &str,
        node_id: &str,
        issue_id: Option<&str>,
        outcome: ConditionOutcome,
    );
    fn continue_job(&self, job_id: &str, message: &str) -> Result<(), String>;
    fn resume_parent(&self, job_id: &str) -> Result<(), String>;
    fn run_error(&self, run_id: &str, error: &str);
    async fn checkpoint(
        &self,
        job_id: &str,
        node_name: &str,
        command: &str,
    ) -> Result<ExecutedCheckpoint, String>;
    async fn execute(&self, workload: Workload) -> Result<Option<EffectResult>, String>;
    /// Exclude every other writer of `execution_id`'s snapshot while the DAG is
    /// reduced. The reduction expands delegated packets by rewriting that
    /// snapshot, so a host that also writes packets into it (spawn persistence
    /// and settlement) returns its per-execution lock here. The value is held
    /// for exactly one reduction and dropped before any effect runs.
    async fn execution_snapshot_guard(&self, _execution_id: &str) -> Box<dyn std::any::Any + Send> {
        Box::new(())
    }
}

/// Resources for workflow operations. The private outbox is distinct from each
/// entity's owning database, including when that entity lives in a team replica.
#[derive(Clone)]
pub struct WorkflowRuntime {
    pub(crate) host: Arc<dyn WorkflowHost>,
    pub(crate) private_db: Arc<LocalDb>,
    pub(crate) emitter: Arc<dyn EventEmitter>,
    pub(crate) config_dir: PathBuf,
    pub(crate) effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,
}

impl WorkflowRuntime {
    pub fn new(
        host: Arc<dyn WorkflowHost>,
        private_db: Arc<LocalDb>,
        emitter: Arc<dyn EventEmitter>,
        config_dir: PathBuf,
        effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,
    ) -> Self {
        Self {
            host,
            private_db,
            emitter,
            config_dir,
            effect_tx,
        }
    }

    /// Persist advancement in the private host outbox before notifying the live
    /// worker. The row remains replayable when there is no receiver.
    pub async fn enqueue_advance(&self, execution_id: &str) -> Result<String, String> {
        let id = crate::effects::outbox::insert_pending_with_payload_async(
            &self.private_db,
            "advance_dag",
            execution_id,
            "{}",
        )
        .await
        .map_err(|error| error.to_string())?;
        if let Some(tx) = &self.effect_tx {
            let _ = tx.send(WorkflowEffect::AdvanceDag {
                execution_id: execution_id.to_owned(),
                outbox_entry_id: Some(id.clone()),
            });
        }
        Ok(id)
    }

    pub(crate) fn database(&self, owner: WorkflowOwner<'_>) -> Result<Arc<LocalDb>, String> {
        self.host.database(owner)
    }
}
