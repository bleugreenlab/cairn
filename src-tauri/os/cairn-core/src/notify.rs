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

    /// Announce an artifact write, scoped to the job that owns it. `db` is the
    /// database the artifact was written to.
    pub(crate) async fn artifact(&self, db: &crate::storage::LocalDb, a: &models::Artifact) {
        let change = artifact_db_change_for_id(db, &a.id, "update").await;
        let _ = self.emitter.emit("db-change", change);
        // Every artifact write announces itself here, so this is the one place
        // a confirmation can tell held executions their evidence moved.
        if a.confirmed {
            crate::execution::gates::sweep::notify_gate_evidence_changed();
        }
    }

    pub fn comment(&self, _c: &models::Comment) {
        self.emit_change("comments");
    }

    // --- Generic ---

    /// Emit an unscoped `db-change` event for a table no sidebar projection
    /// reads. A projection input must go through its scoped builder instead
    /// (`projection_input_emits_go_through_scoped_builders` enforces it).
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
            held_executions: Vec::new(),
            wont_start_executions: Vec::new(),
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

    // ── Projection-input scoping, enforced at the source ──

    /// Unscoped emits on a projection input that are genuine multi-row sweeps:
    /// each can have moved any project, so invalidating every project's
    /// snapshot is the honest answer. `(path under src-tauri, table, why)`.
    const UNSCOPED_SWEEPS: &[(&str, &str, &str)] = &[
        (
            "os/cairn-session/src/runs/queries.rs",
            "runs",
            "reconcile_stale_runs settles stale runs of every project at startup",
        ),
        (
            "os/cairn-session/src/runs/queries.rs",
            "turns",
            "reconcile_stale_runs settles orphaned turns of every project at startup",
        ),
        (
            "os/cairn-core/src/resources/mutations/labels.rs",
            "issue_labels",
            "a workspace label's links can sit in any project",
        ),
        (
            "os/cairn-core/src/resources/mutations/labels.rs",
            "issues",
            "deleting a workspace label changes issues in any project",
        ),
        (
            "cairn-transport/src/routes/invoke/sync_commands/records.rs",
            "issue_labels",
            "deleting a workspace label unlinks it from issues in any project",
        ),
    ];

    /// The files that ARE the scoped builders.
    const BUILDER_FILES: &[&str] = &["os/cairn-db/src/changes.rs", "os/cairn-core/src/notify.rs"];

    fn rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if path.is_dir() {
                if name != "tests" {
                    rust_sources(&path, out);
                }
            } else if name.ends_with(".rs") && name != "tests.rs" && !name.ends_with("_tests.rs") {
                out.push(path);
            }
        }
    }

    /// What the lexer is inside of where one line ends and the next begins.
    /// Strings span lines (SQL, raw strings), so this has to carry over.
    #[derive(Default)]
    struct LexState {
        /// `Some(hashes)` inside a raw string, `Some(usize::MAX)` inside a
        /// plain one.
        string: Option<usize>,
        block_comments: usize,
    }

    const PLAIN_STRING: usize = usize::MAX;

    impl LexState {
        fn in_code(&self) -> bool {
            self.string.is_none() && self.block_comments == 0
        }
    }

    fn is_ident(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    /// `{` minus `}` on one line, outside strings, char literals and comments,
    /// advancing `state` past the line.
    fn brace_delta(state: &mut LexState, line: &str) -> i64 {
        let chars: Vec<char> = line.chars().collect();
        let at = |i: usize| chars.get(i).copied();
        let mut delta = 0;
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if state.block_comments > 0 {
                match (c, at(i + 1)) {
                    ('*', Some('/')) => {
                        state.block_comments -= 1;
                        i += 2;
                    }
                    ('/', Some('*')) => {
                        state.block_comments += 1;
                        i += 2;
                    }
                    _ => i += 1,
                }
                continue;
            }
            match state.string {
                Some(PLAIN_STRING) => {
                    match c {
                        '\\' => i += 1,
                        '"' => state.string = None,
                        _ => {}
                    }
                    i += 1;
                    continue;
                }
                Some(hashes) => {
                    if c == '"' && (1..=hashes).all(|offset| at(i + offset) == Some('#')) {
                        state.string = None;
                        i += hashes;
                    }
                    i += 1;
                    continue;
                }
                None => {}
            }
            match c {
                '/' if at(i + 1) == Some('/') => break,
                '/' if at(i + 1) == Some('*') => {
                    state.block_comments += 1;
                    i += 1;
                }
                '"' => state.string = Some(PLAIN_STRING),
                'r' if matches!(at(i + 1), Some('"' | '#'))
                    && (i == 0
                        || !is_ident(chars[i - 1])
                        || (chars[i - 1] == 'b' && (i < 2 || !is_ident(chars[i - 2])))) =>
                {
                    let hashes = chars[i + 1..].iter().take_while(|h| **h == '#').count();
                    if at(i + 1 + hashes) == Some('"') {
                        state.string = Some(hashes);
                        i += 1 + hashes;
                    }
                }
                // A char literal (`'x'`, `'\n'`, `'"'`); anything else after a
                // quote is a lifetime.
                '\'' if at(i + 1) == Some('\\') => {
                    i += 2;
                    while i < chars.len() && chars[i] != '\'' {
                        i += 1;
                    }
                }
                '\'' if at(i + 2) == Some('\'') => i += 2,
                '{' => delta += 1,
                '}' => delta -= 1,
                _ => {}
            }
            i += 1;
        }
        delta
    }

    /// Line-numbered production lines: every `#[cfg(test)]` item (a test module
    /// above all) is dropped, so a test may build any frame it likes.
    fn production_lines(source: &str) -> Vec<(usize, &str)> {
        let mut kept = Vec::new();
        let mut state = LexState::default();
        let mut pending_test_item = false;
        let mut skip_depth: Option<i64> = None;
        for (index, line) in source.lines().enumerate() {
            let starts_in_code = state.in_code();
            let delta = brace_delta(&mut state, line);
            if let Some(depth) = skip_depth.as_mut() {
                *depth += delta;
                if *depth <= 0 {
                    skip_depth = None;
                }
                continue;
            }
            let trimmed = line.trim();
            if starts_in_code && trimmed.starts_with("#[cfg(test)]") {
                pending_test_item = true;
                continue;
            }
            if pending_test_item {
                if trimmed.starts_with("#[") || trimmed.is_empty() {
                    continue;
                }
                pending_test_item = false;
                if delta > 0 {
                    skip_depth = Some(delta);
                }
                continue;
            }
            if !trimmed.starts_with("//") {
                kept.push((index + 1, line));
            }
        }
        kept
    }

    /// Every string literal on a line, in order.
    fn string_literals(line: &str) -> Vec<&str> {
        line.split('"').skip(1).step_by(2).collect()
    }

    /// The tables a line emits a `db-change` for by name rather than through a
    /// builder.
    fn named_tables(line: &str, lookahead: &[(usize, &str)]) -> Vec<String> {
        let mut tables = Vec::new();
        let mut rest = line;
        while let Some(at) = rest.find("\"table\"") {
            rest = &rest[at + "\"table\"".len()..];
            if let Some(value) = rest.trim_start().strip_prefix(':') {
                if let Some(literal) = value.trim_start().strip_prefix('"') {
                    if let Some(end) = literal.find('"') {
                        tables.push(literal[..end].to_string());
                    }
                }
            }
        }
        for helper in ["emit_change(", "emit_db_change("] {
            let mut rest = line;
            while let Some(at) = rest.find(helper) {
                rest = &rest[at + helper.len()..];
                if let Some(first) = string_literals(rest).first() {
                    tables.push((*first).to_string());
                }
            }
        }
        // `for table in ["jobs", "executions"] { emit("db-change", json!({"table": table})) }`
        if line.contains("for table in [")
            && lookahead.iter().any(|(_, next)| next.contains("db-change"))
        {
            tables.extend(string_literals(line).into_iter().map(str::to_string));
        }
        tables
    }

    #[test]
    fn projection_input_emits_go_through_scoped_builders() {
        use crate::orchestrator::generation_cache::{
            every_projection_input_table, WORKSPACE_PROJECTION_INPUT_TABLES,
        };
        let scoped: Vec<&str> = every_projection_input_table()
            .into_iter()
            .filter(|table| !WORKSPACE_PROJECTION_INPUT_TABLES.contains(table))
            .collect();

        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let os = manifest
            .parent()
            .expect("cairn-core lives under src-tauri/os");
        let src_tauri = os.parent().expect("src-tauri/os lives under src-tauri");
        let mut files = Vec::new();
        for entry in std::fs::read_dir(os).expect("read src-tauri/os").flatten() {
            rust_sources(&entry.path().join("src"), &mut files);
        }
        rust_sources(&src_tauri.join("cairn-transport/src"), &mut files);
        assert!(
            files.len() > 100,
            "the scan found only {} files",
            files.len()
        );

        let mut violations = Vec::new();
        let mut sweeps_seen = std::collections::HashSet::new();
        for file in &files {
            let relative = file
                .strip_prefix(src_tauri)
                .unwrap_or(file)
                .to_string_lossy()
                .replace('\\', "/");
            if BUILDER_FILES.contains(&relative.as_str()) {
                continue;
            }
            let source = std::fs::read_to_string(file).expect("read source");
            let lines = production_lines(&source);
            for (position, (number, line)) in lines.iter().enumerate() {
                let lookahead = &lines[position + 1..(position + 7).min(lines.len())];
                for table in named_tables(line, lookahead) {
                    if !scoped.contains(&table.as_str()) {
                        continue;
                    }
                    if let Some(sweep) = UNSCOPED_SWEEPS
                        .iter()
                        .find(|(path, swept, _)| *path == relative && *swept == table)
                    {
                        sweeps_seen.insert((sweep.0, sweep.1));
                        continue;
                    }
                    violations.push(format!("{relative}:{number}: `{table}`: {}", line.trim()));
                }
            }
        }
        assert!(
            violations.is_empty(),
            "db-change frames on a project-scoped projection input must carry projectId \
             (and jobId where the row has one). Use a scoped builder from \
             cairn_db::changes (re-exported by crate::notify) instead of naming the table; \
             a genuine multi-row sweep belongs in UNSCOPED_SWEEPS with its reason.\n{}",
            violations.join("\n")
        );
        let stale: Vec<_> = UNSCOPED_SWEEPS
            .iter()
            .filter(|(path, table, _)| !sweeps_seen.contains(&(*path, *table)))
            .collect();
        assert!(
            stale.is_empty(),
            "UNSCOPED_SWEEPS entries no longer match a site: {stale:?}"
        );
    }

    #[test]
    fn the_source_scan_sees_every_raw_form_and_skips_test_modules() {
        let source = r##"
fn live() {
    emit("db-change", json!({"table": "turns", "action": "update"}));
    orch.notifier.emit_change("jobs");
    emit_db_change(orch, "issues", "update");
    for table in ["executions"] {
        let _ = emitter.emit("db-change", json!({"table": table}));
    }
    // emit_change("jobs") in a comment is not an emit
}
#[cfg(test)]
mod tests {
    fn frame() { json!({"table": "prompts", "x": "{"}); }
    const QUOTE: char = '"';
    const BRACE: char = '}';
    fn sql<'a>(x: &'a str) -> &'a str { x }
    const MULTI: &str = "spans
        } lines";
    const RAW: &str = r#"a "quoted" } raw"#;
    fn later() { json!({"table": "jobs"}); }
}
fn after() { emit_change("runs"); }
"##;
        let lines = production_lines(source);
        let found: Vec<String> = lines
            .iter()
            .enumerate()
            .flat_map(|(position, (_, line))| {
                named_tables(line, &lines[position + 1..(position + 7).min(lines.len())])
            })
            .collect();
        assert_eq!(found, ["turns", "jobs", "issues", "executions", "runs"]);
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
