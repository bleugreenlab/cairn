//! Frontend notifications for application writes.
//!
//! `Notifier` publishes typed `db-change` payloads from `cairn-db` through the
//! application's shared [`EventEmitter`].

use crate::models;
use crate::services::EventEmitter;
pub use cairn_db::changes::*;
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct Notifier {
    emitter: Arc<dyn EventEmitter>,
}

impl Notifier {
    pub(crate) fn new(emitter: Arc<dyn EventEmitter>) -> Self {
        Self { emitter }
    }

    // --- Entity change notifications (frontend emit) ---

    pub fn project(&self, _p: &models::Project) {
        self.emit_change("projects");
    }

    pub fn issue(&self, issue: &models::Issue) {
        let _ = self
            .emitter
            .emit("db-change", issue_db_change(issue, "update"));
    }

    pub fn thread(&self, thread: &models::Thread) {
        let _ = self
            .emitter
            .emit("db-change", thread_db_change(thread, "update"));
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

    pub(crate) fn artifact(&self, _a: &models::Artifact) {
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
    pub(crate) fn emit_change(&self, table: &str) {
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

    fn test_thread(name: &str) -> models::Thread {
        models::Thread {
            id: "thread-1".into(),
            project_id: "project-1".into(),
            name: name.into(),
            jurisdiction: None,
            status: models::ThreadStatus::Active,
            attention: "none".into(),
            definition: None,
            migrated_from_number: None,
            created_at: 1000,
            updated_at: 2000,
        }
    }

    #[test]
    fn thread_db_change_carries_complete_scope_and_null_previous_name() {
        let thread = test_thread("roadmap");
        let payload = thread_db_change(&thread, "insert");
        assert_eq!(payload["table"], "threads");
        assert_eq!(payload["action"], "insert");
        assert_eq!(payload["threadId"], "thread-1");
        assert_eq!(payload["projectId"], "project-1");
        assert_eq!(payload["name"], "roadmap");
        assert!(payload.get("previousName").is_some());
        assert!(payload["previousName"].is_null());
    }

    #[test]
    fn thread_update_db_change_only_names_a_previous_name_for_a_rename() {
        let previous = test_thread("roadmap");
        let unchanged = thread_update_db_change(&previous, &previous);
        assert!(unchanged["previousName"].is_null());

        let updated = test_thread("north-star");
        let renamed = thread_update_db_change(&previous, &updated);
        assert_eq!(renamed["name"], "north-star");
        assert_eq!(renamed["previousName"], "roadmap");
    }

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
            author: None,
            author_display: None,
            backend_override: None,
            merged_at: None,
            closed_at: None,
            parent_issue_id: None,
            parent_thread_id: None,
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
    fn issue_db_change_carries_project_scope() {
        let payload = issue_db_change(&test_issue(), "update");
        assert_eq!(payload["table"], "issues");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["issueId"], "i-1");
        assert_eq!(payload["projectId"], "p-1");
    }

    #[test]
    fn job_db_change_ids_carries_complete_scoping_set() {
        let payload = job_db_change_ids(
            "update",
            "job-1",
            Some("issue-1"),
            Some("thread-1"),
            Some("exec-1"),
            Some("parent-1"),
            Some("tool-1"),
            "project-1",
        );

        assert_eq!(payload["table"], "jobs");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["threadId"], "thread-1");
        assert_eq!(payload["executionId"], "exec-1");
        assert_eq!(payload["parentJobId"], "parent-1");
        assert_eq!(payload["parentToolUseId"], "tool-1");
        assert_eq!(payload["projectId"], "project-1");
        // No `scoped` flag — the scan is gone, so the marker is unnecessary.
        assert!(payload.get("scoped").is_none());
    }

    #[test]
    fn job_db_change_ids_serializes_absent_child_fields_as_null() {
        let payload =
            job_db_change_ids("insert", "job-1", None, None, None, None, None, "project-1");

        // Keys are present and explicitly null (not omitted) so the frontend's
        // payloadString reads them as absent rather than choking.
        assert!(payload.get("issueId").is_some());
        assert!(payload["issueId"].is_null());
        assert!(payload["threadId"].is_null());
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
        assert_eq!(payload["threadId"], "thread-1");
        assert_eq!(payload["executionId"], "exec-1");
        assert_eq!(payload["parentJobId"], "parent-1");
        assert_eq!(payload["parentToolUseId"], "tool-1");
        assert_eq!(payload["projectId"], "project-1");
    }

    #[test]
    fn run_db_change_ids_carries_run_and_job() {
        let payload = run_db_change_ids(
            "update",
            "run-1",
            Some("job-1"),
            Some("issue-1"),
            Some("project-1"),
        );
        assert_eq!(payload["table"], "runs");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["runId"], "run-1");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["projectId"], "project-1");
        assert!(payload.get("scoped").is_none());
    }

    #[test]
    fn run_db_change_ids_serializes_absent_job_as_null() {
        let payload = run_db_change_ids("insert", "run-1", None, None, None);
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
    fn event_db_change_scoped_carries_the_whole_resolved_scope() {
        let scope = EventRunScope {
            issue_id: Some("issue-1".into()),
            thread_id: None,
            job_id: Some("job-1".into()),
            project_id: Some("project-1".into()),
        };
        let payload = event_db_change_scoped("run-1", Some("session-1"), &scope, "insert");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["issue_id"], "issue-1");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["job_id"], "job-1");
        // An issue-owned event names no thread, so a thread-scoped surface is
        // never invalidated by an issue execution's traffic.
        assert!(payload["threadId"].is_null());
        assert!(payload["thread_id"].is_null());
        // The point of the whole change: a live surface can filter this frame
        // by project without a follow-up query.
        assert_eq!(payload["projectId"], "project-1");
        assert_eq!(payload["project_id"], "project-1");
    }

    /// The thread-owned mirror of the issue-owned case above: a thread session's
    /// event names its thread and no issue, so `parseScopeIds` can route the
    /// frame to the thread pane's skyline key on either casing.
    #[test]
    fn event_db_change_scoped_carries_the_owning_thread() {
        let scope = EventRunScope {
            issue_id: None,
            thread_id: Some("thread-1".into()),
            job_id: Some("job-1".into()),
            project_id: Some("project-1".into()),
        };
        let payload = event_db_change_scoped("run-1", Some("session-1"), &scope, "insert");
        assert_eq!(payload["threadId"], "thread-1");
        assert_eq!(payload["thread_id"], "thread-1");
        assert!(payload["issueId"].is_null());
        assert!(payload["issue_id"].is_null());
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["projectId"], "project-1");
    }

    #[test]
    fn event_db_change_scoped_serializes_an_unresolved_scope_as_explicit_nulls() {
        let payload = event_db_change_scoped(
            "run-1",
            Some("session-1"),
            &EventRunScope::default(),
            "insert",
        );
        // Present-and-null, not absent: a subscriber filtering on projectId must
        // be able to tell "this run has no project" from "this build predates
        // the key".
        for key in ["issueId", "threadId", "jobId", "projectId"] {
            assert!(payload.get(key).is_some(), "{key} must be present");
            assert!(payload[key].is_null(), "{key} must be null");
        }
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
            branch: None,
            pack_anchor: None,
            current_session_id: None,
            status: models::JobStatus::Running,
            agent_config_id: None,
            issue_id: Some("issue-1".into()),
            thread_id: Some("thread-1".into()),
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
