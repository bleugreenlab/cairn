//! Frontend-emit helpers for write operations.
//!
//! Every DB write that needs frontend invalidation emits a `db-change` event
//! through the shared [`EventEmitter`]. `Notifier` wraps the emitter so call
//! sites express intent with a typed call:
//!
//! ```ignore
//! orch.notifier.issue(&issue);          // emit an `issues` db-change
//! orch.notifier.emit_change("todos");   // emit an arbitrary table change
//! ```

use std::sync::Arc;

use serde_json::{json, Value};

use crate::models;
use crate::services::EventEmitter;
use crate::storage::{run_db_blocking, LocalDb};

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

/// Build a fully-scoped `events` db-change payload.
///
/// LOAD-BEARING INVARIANT: the live chat transcript's incremental delta cache
/// (`useSessionEvents(sessionId)`) is invalidated ONLY by the `events` resolver,
/// which scopes by `sessionId`. An events insert that emits the wrong
/// `session_id` — or skips the emit entirely — strands the transcript until a
/// manual reload, while the issue-overview job row survives on sibling
/// invalidations and masks the freeze (CAIRN-1916). So every live/finalize
/// events insert must emit through this one builder and always carry both
/// `run_id` and `session_id`. Route durable inserts through
/// [`crate::transcripts::stream_store::insert_event_emit`] so the scoped emit
/// can't be forgotten. Both camel- and snake-cased keys are emitted because the
/// frontend `parseScopeIds` reads either.
pub fn event_db_change(run_id: &str, session_id: Option<&str>, action: &str) -> Value {
    event_db_change_scoped(run_id, session_id, None, action)
}

fn event_issue_id_for_run(db: Arc<LocalDb>, run_id: &str) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        db.query_opt_text(
            "SELECT issue_id FROM runs WHERE id = ?1 LIMIT 1",
            turso::params![run_id.as_str()],
        )
        .await
        .map_err(|e| format!("Failed to load issue id for event db-change: {e}"))
    })
}

pub fn event_db_change_for_run(
    db: Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    action: &str,
) -> Value {
    let issue_id = match event_issue_id_for_run(db, run_id) {
        Ok(issue_id) => issue_id,
        Err(error) => {
            log::warn!("{error}");
            None
        }
    };
    event_db_change_scoped(run_id, session_id, issue_id.as_deref(), action)
}

pub fn event_db_change_scoped(
    run_id: &str,
    session_id: Option<&str>,
    issue_id: Option<&str>,
    action: &str,
) -> Value {
    json!({
        "table": "events",
        "action": action,
        "runId": run_id,
        "run_id": run_id,
        "sessionId": session_id,
        "session_id": session_id,
        "issueId": issue_id,
        "issue_id": issue_id,
    })
}

/// Emits frontend `db-change` events for write operations.
///
/// Created once during Orchestrator construction; shared via `Arc` clone.
/// All methods are fire-and-forget — emit errors are silently dropped.
#[derive(Clone)]
pub struct Notifier {
    emitter: Arc<dyn EventEmitter>,
}

impl Notifier {
    pub fn new(emitter: Arc<dyn EventEmitter>) -> Self {
        Self { emitter }
    }

    // --- Entity change notifications (frontend emit) ---

    pub fn project(&self, _p: &models::Project) {
        self.emit_change("projects");
    }

    pub fn issue(&self, _i: &models::Issue) {
        self.emit_change("issues");
    }

    pub fn job(&self, j: &models::Job) {
        // Route through the scoped builder so the precise job-list invalidation
        // carries the full id set (see [`job_db_change`]).
        let _ = self.emitter.emit("db-change", job_db_change(j, "update"));
    }

    pub fn run(&self, r: &models::Run) {
        let _ = self.emitter.emit("db-change", run_db_change(r, "update"));
    }

    pub fn event(&self, _e: &models::Event) {
        self.emit_change("events");
    }

    pub fn artifact(&self, _a: &models::Artifact) {
        self.emit_change("artifacts");
    }

    pub fn comment(&self, _c: &models::Comment) {
        self.emit_change("comments");
    }

    // --- Delete ---

    pub fn deleted(&self, table: &str, _id: &str) {
        self.emit_change(table);
    }

    // --- Generic ---

    /// Emit a `db-change` event for an arbitrary table.
    pub fn emit_change(&self, table: &str) {
        let _ = self.emitter.emit("db-change", json!({"table": table}));
    }

    // --- Streaming (fire-and-forget, no emit) ---

    pub fn stream_delta(&self, _run_id: &str, _event_id: &str, _tokens: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::IssueStatus;
    use crate::services::testing::CapturingEmitter;

    fn test_notifier() -> (Notifier, Arc<CapturingEmitter>) {
        let emitter = Arc::new(CapturingEmitter::new());
        let notifier = Notifier::new(emitter.clone());
        (notifier, emitter)
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
    fn notifier_issue_emits() {
        let (notifier, emitter) = test_notifier();

        notifier.issue(&test_issue());

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "issues");
    }

    #[test]
    fn notifier_emit_change_emits() {
        let (notifier, emitter) = test_notifier();

        notifier.emit_change("todos");

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "todos");
    }

    #[test]
    fn notifier_deleted_emits() {
        let (notifier, emitter) = test_notifier();

        notifier.deleted("issues", "i-1");

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "issues");
    }

    #[test]
    fn notifier_stream_delta_no_emit() {
        let (notifier, emitter) = test_notifier();

        notifier.stream_delta("run-1", "evt-1", "hello world");

        assert!(emitter.events_named("db-change").is_empty());
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

    #[test]
    fn event_db_change_carries_both_cased_scope_keys() {
        let payload = event_db_change("run-1", Some("session-1"), "insert");
        assert_eq!(payload["table"], "events");
        assert_eq!(payload["action"], "insert");
        // The events resolver scopes the chat transcript by sessionId; both
        // casings must be present so parseScopeIds resolves the precise key.
        assert_eq!(payload["runId"], "run-1");
        assert_eq!(payload["run_id"], "run-1");
        assert_eq!(payload["sessionId"], "session-1");
        assert_eq!(payload["session_id"], "session-1");
        assert!(payload["issueId"].is_null());
    }

    #[test]
    fn event_db_change_scoped_carries_issue_id() {
        let payload = event_db_change_scoped("run-1", Some("session-1"), Some("issue-1"), "insert");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["issue_id"], "issue-1");
    }

    #[test]
    fn event_db_change_serializes_absent_session_as_null() {
        let payload = event_db_change("run-1", None, "insert");
        assert_eq!(payload["runId"], "run-1");
        assert!(payload.get("sessionId").is_some());
        assert!(payload["sessionId"].is_null());
        assert!(payload["session_id"].is_null());
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
