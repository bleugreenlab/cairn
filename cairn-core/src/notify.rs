//! Unified sync-and-emit helpers for write operations.
//!
//! Every DB write that needs frontend invalidation and/or cloud sync currently
//! repeats a 2–3 line pattern: `orch.sync(SyncMessage::Foo(…))` then
//! `emitter.emit("db-change", json!({"table":"foos"}))`.
//!
//! `Notifier` combines both into a single typed call:
//!
//! ```ignore
//! orch.notifier.issue(&issue);          // sync + emit
//! orch.notifier.emit_change("todos");   // emit only (local-only table)
//! ```

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::models;
use crate::services::EventEmitter;
use crate::sync::message::*;

/// Build a fully-scoped `jobs` db-change payload.
///
/// LOAD-BEARING INVARIANT: any `jobs` db-change that carries `jobId` MUST also
/// carry the complete scoping set (`issueId`/`executionId`/`parentJobId`/
/// `parentToolUseId`/`projectId`). The frontend precisely-invalidates from these
/// ids and has no cache-scan fallback to recover a missing one, so a payload
/// with `jobId` but a missing `issueId` would silently skip the issue's
/// job-list pane. Route every single-job change through this builder (it is
/// always complete). Only the documented bare sweeps (`teardown`,
/// `reconcile_stale_runs`) may emit `{table:"jobs"}` with no ids at all, which
/// correctly degrades to a broad `["jobs"]` invalidation.
#[allow(clippy::too_many_arguments)]
pub fn job_db_change_ids(
    action: &str,
    job_id: &str,
    issue_id: Option<&str>,
    execution_id: Option<&str>,
    parent_job_id: Option<&str>,
    parent_tool_use_id: Option<&str>,
    project_id: &str,
) -> Value {
    json!({
        "table": "jobs",
        "action": action,
        "jobId": job_id,
        "issueId": issue_id,
        "executionId": execution_id,
        "parentJobId": parent_job_id,
        "parentToolUseId": parent_tool_use_id,
        "projectId": project_id,
    })
}

/// Build a fully-scoped `jobs` db-change payload from a `Job`.
///
/// See [`job_db_change_ids`] for the scoping invariant.
pub fn job_db_change(job: &models::Job, action: &str) -> Value {
    job_db_change_ids(
        action,
        &job.id,
        job.issue_id.as_deref(),
        job.execution_id.as_deref(),
        job.parent_job_id.as_deref(),
        job.parent_tool_use_id.as_deref(),
        &job.project_id,
    )
}

/// Build a scoped `runs` db-change payload.
///
/// The runs branch only needs `jobId` to scope to the affected job's run list;
/// a payload with no `jobId` degrades to a broad `["runs"]` invalidation.
pub fn run_db_change_ids(action: &str, run_id: &str, job_id: Option<&str>) -> Value {
    json!({
        "table": "runs",
        "action": action,
        "runId": run_id,
        "jobId": job_id,
    })
}

/// Build a scoped `runs` db-change payload from a `Run`.
///
/// See [`run_db_change_ids`].
pub fn run_db_change(run: &models::Run, action: &str) -> Value {
    run_db_change_ids(action, &run.id, run.job_id.as_deref())
}

/// Combines cloud sync and frontend event emission into a single call.
///
/// Created once during Orchestrator construction; shared via `Arc` clone.
/// All methods are fire-and-forget — errors are silently dropped, matching
/// the existing `orch.sync()` behavior.
#[derive(Clone)]
pub struct Notifier {
    sync_tx: Arc<Mutex<Option<mpsc::UnboundedSender<SyncMessage>>>>,
    emitter: Arc<dyn EventEmitter>,
}

impl Notifier {
    pub fn new(
        sync_tx: Arc<Mutex<Option<mpsc::UnboundedSender<SyncMessage>>>>,
        emitter: Arc<dyn EventEmitter>,
    ) -> Self {
        Self { sync_tx, emitter }
    }

    // --- Syncable entities (cloud sync + frontend emit) ---

    pub fn project(&self, p: &models::Project) {
        self.sync_and_emit(SyncMessage::Project(p.into()), "projects");
    }

    pub fn issue(&self, i: &models::Issue) {
        self.sync_and_emit(SyncMessage::Issue(i.into()), "issues");
    }

    pub fn job(&self, j: &models::Job) {
        // Route through the scoped builder so the abstraction stays correct if
        // this (currently caller-less) method ever gains callers.
        self.sync_and_emit_payload(SyncMessage::Job(j.into()), job_db_change(j, "update"));
    }

    pub fn run(&self, r: &models::Run) {
        self.sync_and_emit_payload(SyncMessage::Run(r.into()), run_db_change(r, "update"));
    }

    pub fn event(&self, _e: &models::Event) {
        self.emit_change("events");
    }

    pub fn artifact(&self, a: &models::Artifact) {
        self.sync_and_emit(SyncMessage::Artifact(a.into()), "artifacts");
    }

    pub fn comment(&self, c: &models::Comment) {
        self.sync_and_emit(SyncMessage::Comment(c.into()), "comments");
    }

    // --- Delete (cloud sync + frontend emit) ---

    pub fn deleted(&self, table: &str, id: &str) {
        self.sync(SyncMessage::Delete {
            table: table.to_string(),
            id: id.to_string(),
        });
        self.emit_change(table);
    }

    // --- Local-only entities (emit only, no cloud sync) ---

    /// Emit a `db-change` event for a table that doesn't sync to cloud.
    pub fn emit_change(&self, table: &str) {
        let _ = self.emitter.emit("db-change", json!({"table": table}));
    }

    // --- Raw sync+emit (for manual SyncMessage construction) ---

    /// Send a sync message and emit a db-change event.
    pub fn sync_and_emit(&self, msg: SyncMessage, table: &str) {
        self.sync(msg);
        self.emit_change(table);
    }

    /// Send a sync message and emit a db-change event with a pre-built payload.
    /// Used to carry the fully-scoped builders ([`job_db_change`] /
    /// [`run_db_change`]) instead of a bare `{table}` poke.
    pub fn sync_and_emit_payload(&self, msg: SyncMessage, payload: Value) {
        self.sync(msg);
        let _ = self.emitter.emit("db-change", payload);
    }

    // --- Streaming (fire-and-forget, no emit) ---

    pub fn stream_delta(&self, _run_id: &str, _event_id: &str, _tokens: &str) {}

    // --- Internal ---

    fn sync(&self, msg: SyncMessage) {
        if let Ok(guard) = self.sync_tx.lock() {
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(msg);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::IssueStatus;
    use crate::services::testing::CapturingEmitter;

    fn test_notifier() -> (
        Notifier,
        mpsc::UnboundedReceiver<SyncMessage>,
        Arc<CapturingEmitter>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let sync_tx = Arc::new(Mutex::new(Some(tx)));
        let emitter = Arc::new(CapturingEmitter::new());
        let notifier = Notifier::new(sync_tx, emitter.clone());
        (notifier, rx, emitter)
    }

    fn test_issue() -> models::Issue {
        models::Issue {
            id: "i-1".into(),
            project_id: "p-1".into(),
            number: 1,
            title: "Test".into(),
            description: "".into(),
            status: IssueStatus::Active,
            progress: models::IssueProgress::Active,
            attention: models::IssueAttention::None,
            priority: 0,
            completed_at: None,
            dismissed_at: None,
            created_at: 1000,
            updated_at: 2000,
            backend_override: None,
            merged_at: None,
            closed_at: None,
            parent_issue_id: None,
            unmet_dependency_count: 0,
            depends_on: Vec::new(),
            unmet_depends_on: Vec::new(),
            labels: Vec::new(),
        }
    }

    // ── Notifier tests ──

    #[test]
    fn notifier_issue_syncs_and_emits() {
        let (notifier, mut rx, emitter) = test_notifier();
        let issue = test_issue();

        notifier.issue(&issue);

        // Sync message sent
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, SyncMessage::Issue(_)));

        // db-change emitted
        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "issues");
    }

    #[test]
    fn notifier_emit_change_no_sync() {
        let (notifier, mut rx, emitter) = test_notifier();

        notifier.emit_change("todos");

        // No sync message
        assert!(rx.try_recv().is_err());

        // db-change emitted
        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "todos");
    }

    #[test]
    fn notifier_deleted_syncs_and_emits() {
        let (notifier, mut rx, emitter) = test_notifier();

        notifier.deleted("issues", "i-1");

        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, SyncMessage::Delete { .. }));

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "issues");
    }

    #[test]
    fn notifier_stream_delta_no_emit() {
        let (notifier, mut rx, emitter) = test_notifier();

        notifier.stream_delta("run-1", "evt-1", "hello world");

        assert!(rx.try_recv().is_err());
        assert!(emitter.events_named("db-change").is_empty());
    }

    #[test]
    fn notifier_noop_when_sync_not_active() {
        let sync_tx = Arc::new(Mutex::new(None));
        let emitter = Arc::new(CapturingEmitter::new());
        let notifier = Notifier::new(sync_tx, emitter.clone());

        // Should not panic, emit still works
        notifier.issue(&test_issue());

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
    }

    // ── Scoped payload builder tests ──

    #[test]
    fn job_db_change_ids_carries_complete_scoping_set() {
        let payload = job_db_change_ids(
            "update",
            "job-1",
            Some("issue-1"),
            Some("exec-1"),
            Some("parent-1"),
            Some("tool-1"),
            "project-1",
        );

        assert_eq!(payload["table"], "jobs");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["executionId"], "exec-1");
        assert_eq!(payload["parentJobId"], "parent-1");
        assert_eq!(payload["parentToolUseId"], "tool-1");
        assert_eq!(payload["projectId"], "project-1");
        // No `scoped` flag — the scan is gone, so the marker is unnecessary.
        assert!(payload.get("scoped").is_none());
    }

    #[test]
    fn job_db_change_ids_serializes_absent_child_fields_as_null() {
        let payload = job_db_change_ids("insert", "job-1", None, None, None, None, "project-1");

        // Keys are present and explicitly null (not omitted) so the frontend's
        // payloadString reads them as absent rather than choking.
        assert!(payload.get("issueId").is_some());
        assert!(payload["issueId"].is_null());
        assert!(payload["executionId"].is_null());
        assert!(payload["parentJobId"].is_null());
        assert!(payload["parentToolUseId"].is_null());
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["projectId"], "project-1");
    }

    #[test]
    fn job_db_change_builds_from_job() {
        let job = test_job();
        let payload = job_db_change(&job, "update");

        assert_eq!(payload["table"], "jobs");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["executionId"], "exec-1");
        assert_eq!(payload["parentJobId"], "parent-1");
        assert_eq!(payload["parentToolUseId"], "tool-1");
        assert_eq!(payload["projectId"], "project-1");
    }

    #[test]
    fn run_db_change_ids_carries_run_and_job() {
        let payload = run_db_change_ids("update", "run-1", Some("job-1"));
        assert_eq!(payload["table"], "runs");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["runId"], "run-1");
        assert_eq!(payload["jobId"], "job-1");
        assert!(payload.get("scoped").is_none());
    }

    #[test]
    fn run_db_change_ids_serializes_absent_job_as_null() {
        let payload = run_db_change_ids("insert", "run-1", None);
        assert_eq!(payload["runId"], "run-1");
        assert!(payload.get("jobId").is_some());
        assert!(payload["jobId"].is_null());
    }

    fn test_job() -> models::Job {
        models::Job {
            id: "job-1".into(),
            execution_id: Some("exec-1".into()),
            recipe_node_id: None,
            parent_job_id: Some("parent-1".into()),
            worktree_path: None,
            branch: None,
            base_commit: None,
            pack_anchor: None,
            current_session_id: None,
            status: models::JobStatus::Running,
            agent_config_id: None,
            issue_id: Some("issue-1".into()),
            project_id: "project-1".into(),
            task_description: None,
            model: None,
            created_at: 1000,
            updated_at: 2000,
            completed_at: None,
            started_at: None,
            available_tabs: vec!["chat".into()],
            initial_tab: "chat".into(),
            parent_tool_use_id: Some("tool-1".into()),
            task_index: None,
            node_name: None,
            exec_seq: None,
            base_branch: None,
            uri_segment: None,
        }
    }
}
