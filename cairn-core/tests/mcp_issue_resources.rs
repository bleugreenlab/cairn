mod common;

use std::sync::Arc;

use cairn_common::uri::{build_issue_uri, build_project_issues_uri, build_project_uri};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::{LocalDb, RowExt};
use common::{change_resource as change, read_resource};
use serde_json::json;
use turso::params;

struct IssueProjectFixture {
    _temp: tempfile::TempDir,
    db: Arc<LocalDb>,
    orch: Orchestrator,
    project_key: String,
    project_id: String,
}

async fn issue_project_fixture(project_key: &str) -> IssueProjectFixture {
    let (temp, db, orch) = common::resource_orchestrator_fixture().await;
    let project_id = common::create_project(&db, project_key).await;
    IssueProjectFixture {
        _temp: temp,
        db,
        orch,
        project_key: project_key.to_string(),
        project_id,
    }
}

impl IssueProjectFixture {
    fn project_issues_uri(&self) -> String {
        build_project_issues_uri(&self.project_key)
    }

    async fn read_project_issues(&self, query: Option<&str>) -> String {
        let base = self.project_issues_uri();
        let uri = match query {
            Some(query) => format!("{base}?{query}"),
            None => base,
        };
        read_resource(&self.orch, uri).await
    }

    async fn insert_issue(&self, number: i64, title: &str) -> String {
        self.insert_issue_with_status_and_time(number, title, "active", 1)
            .await
    }

    async fn insert_issue_with_status_and_time(
        &self,
        number: i64,
        title: &str,
        status: &str,
        updated_at: i64,
    ) -> String {
        insert_issue_with_status_and_time(
            &self.db,
            &self.project_id,
            number,
            title,
            status,
            updated_at,
        )
        .await
    }
}

async fn insert_issue_with_status_and_time(
    db: &LocalDb,
    project_id: &str,
    number: i64,
    title: &str,
    status: &str,
    updated_at: i64,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let title = title.to_string();
    let status = status.to_string();
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            id.as_str(),
            project_id.as_str(),
            number,
            title.as_str(),
            status.as_str(),
            updated_at,
            updated_at
        ],
    )
    .await
    .unwrap();
    id
}

async fn insert_dependency(db: &LocalDb, issue_id: &str, depends_on_uri: &str) {
    let issue_id = issue_id.to_string();
    let depends_on_uri = depends_on_uri.to_string();
    db.execute(
        "INSERT INTO issue_dependencies(issue_id, depends_on_uri, created_at)
         VALUES (?1, ?2, 1)",
        params![issue_id.as_str(), depends_on_uri.as_str()],
    )
    .await
    .unwrap();
}

async fn insert_file_change(
    db: &LocalDb,
    project_id: &str,
    issue_id: &str,
    job_id: &str,
    file_path: &str,
) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    let job_id = job_id.to_string();
    let file_path = file_path.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let job_id = job_id.clone();
        let file_path = file_path.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'complete', 1, 1)",
                params![job_id.as_str(), project_id.as_str(), issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO file_changes(id, job_id, file_path, status, created_at)
                 VALUES (?1, ?2, ?3, 'modified', 1)",
                params![
                    uuid::Uuid::new_v4().to_string().as_str(),
                    job_id.as_str(),
                    file_path.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn set_project_context(db: &LocalDb, project_id: &str, context: &str) {
    let project_id = project_id.to_string();
    let context = context.to_string();
    db.execute(
        "UPDATE projects SET context = ?1 WHERE id = ?2",
        params![context.as_str(), project_id.as_str()],
    )
    .await
    .unwrap();
}

fn assert_contains_before(haystack: &str, earlier: &str, later: &str) {
    let earlier_index = haystack
        .find(earlier)
        .unwrap_or_else(|| panic!("missing expected text: {earlier}"));
    let later_index = haystack
        .find(later)
        .unwrap_or_else(|| panic!("missing expected text: {later}"));
    assert!(
        earlier_index < later_index,
        "expected '{earlier}' to appear before '{later}'"
    );
}

#[track_caller]
fn assert_contains_all(haystack: &str, expected: &[&str]) {
    for &text in expected {
        assert!(haystack.contains(text), "missing expected text: {text}");
    }
}

#[track_caller]
fn assert_not_contains_any(haystack: &str, unexpected: &[&str]) {
    for &text in unexpected {
        assert!(!haystack.contains(text), "unexpected text present: {text}");
    }
}

#[tokio::test]
async fn read_project_issues_uses_current_database_path() {
    let fixture = issue_project_fixture("MCP").await;
    fixture.insert_issue(1, "First resource issue").await;

    let output = fixture.read_project_issues(None).await;
    assert!(output.contains("# Issues"));
    assert!(output.contains("MCP-1"));
    assert!(output.contains("First resource issue"));

    fixture.insert_issue(2, "Write after read").await;
    assert_eq!(
        common::query_i64(&fixture.db, "SELECT COUNT(*) FROM issues")
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn node_and_task_resources_resolve_by_stored_uri_segment() {
    let fixture = issue_project_fixture("MCP").await;
    let issue_id = fixture
        .insert_issue_with_status_and_time(1, "Stored segment issue", "active", 1)
        .await;

    let project_id = fixture.project_id.clone();
    fixture.db.write(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-stored', 'recipe', ?1, ?2, 'running', 1, 1)",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
                 VALUES ('node-stored', 'exec-stored', 'builder', ?1, ?2, 'Renamed Builder', 'complete', 1, 1, 'builder')",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, task_index, created_at, updated_at, uri_segment)
                 VALUES ('task-stored', 'exec-stored', 'node-stored', ?1, ?2, 'Renamed Task', 'complete', 0, 2, 2, 'explore')",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, created_at, updated_at)
                 VALUES ('artifact-node', 'node-stored', 'text', 'node artifact', 3, 3)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, created_at, updated_at)
                 VALUES ('artifact-task', 'task-stored', 'text', 'task artifact', 4, 4)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    assert!(read_resource(
        &fixture.orch,
        "cairn://p/MCP/1/1/builder/artifact".to_string()
    )
    .await
    .starts_with("node artifact"));
    assert!(read_resource(
        &fixture.orch,
        "cairn://p/MCP/1/1/builder/task/explore/artifact".to_string()
    )
    .await
    .starts_with("task artifact"));
    assert!(read_resource(
        &fixture.orch,
        "cairn://p/MCP/1/1/renamed-builder/artifact".to_string()
    )
    .await
    .contains("Node 'renamed-builder' not found"));
    assert!(read_resource(
        &fixture.orch,
        "cairn://p/MCP/1/1/builder/task/renamed-task/artifact".to_string()
    )
    .await
    .contains("Task 'renamed-task' not found"));
}

#[tokio::test]
async fn node_summary_surfaces_activity_and_latest_assistant_line() {
    let fixture = issue_project_fixture("MCP").await;
    let issue_id = fixture
        .insert_issue_with_status_and_time(1, "Activity issue", "active", 1)
        .await;

    let project_id = fixture.project_id.clone();
    fixture
        .db
        .write(|conn| {
            let project_id = project_id.clone();
            let issue_id = issue_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-act', 'recipe', ?1, ?2, 'running', 1, 1)",
                    params![issue_id.as_str(), project_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at, started_at, uri_segment)
                     VALUES ('node-act', 'exec-act', 'planner', ?1, ?2, 'planner', 'running', 1, 1, 1, 'planner')",
                    params![issue_id.as_str(), project_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, job_id, status, created_at, updated_at)
                     VALUES ('run-act', 'node-act', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, sequence, created_at, updated_at)
                     VALUES ('turn-a', 'sess-act', 'run-act', 1, 1, 1), ('turn-b', 'sess-act', 'run-act', 2, 2, 2)",
                    (),
                )
                .await?;
                // Two assistant turns (earlier + later) and a tool result in
                // between; the later assistant line is the expected "Latest".
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
                     VALUES ('ev-1', 'run-act', 1, 10, 'assistant', '{\"content\":\"Mapping the parser flow.\"}', 10, 'turn-a')",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
                     VALUES ('ev-2', 'run-act', 2, 20, 'tool_result', '{\"toolResult\":\"ok\"}', 20, 'turn-a')",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
                     VALUES ('ev-3', 'run-act', 3, 30, 'assistant', '{\"content\":\"Drafting the plan now.\"}', 30, 'turn-b')",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

    let summary = read_resource(&fixture.orch, "cairn://p/MCP/1/1/planner".to_string()).await;
    assert_contains_all(
        &summary,
        &[
            "## Activity",
            "2 turns",
            "1 run",
            "3 events",
            "last active",
            "Latest: Drafting the plan now.",
        ],
    );
    // The earlier assistant line is superseded, not surfaced.
    assert_not_contains_any(&summary, &["Mapping the parser flow."]);
}

#[tokio::test]
async fn project_root_is_overview_and_project_issues_is_collection() {
    let fixture = issue_project_fixture("MCP").await;
    set_project_context(
        &fixture.db,
        &fixture.project_id,
        "Project context for resource overview",
    )
    .await;

    fixture
        .insert_issue_with_status_and_time(1, "Oldest full listing only", "active", 1)
        .await;
    fixture
        .insert_issue_with_status_and_time(2, "Waiting activity", "Waiting", 2)
        .await;
    fixture
        .insert_issue_with_status_and_time(3, "Merged activity", "Merged", 3)
        .await;
    fixture
        .insert_issue_with_status_and_time(4, "Closed activity", "Closed", 4)
        .await;
    fixture
        .insert_issue_with_status_and_time(5, "Open activity", "open", 5)
        .await;
    fixture
        .insert_issue_with_status_and_time(6, "Newest activity", "active", 6)
        .await;

    let project_root = read_resource(&fixture.orch, build_project_uri("MCP")).await;
    let issue_collection = fixture.read_project_issues(None).await;

    assert_contains_all(
        &project_root,
        &[
            "# Test Project",
            "Project context for resource overview",
            "## Stats",
            "Total issues: 6",
            "Open: 3",
            "Waiting: 1",
            "Merged: 1",
            "Closed: 1",
            "## Recent Activity",
            "Newest activity",
            "## links",
            "[issues]",
            "[messages]",
            "## actions",
            "create issue",
        ],
    );
    assert_not_contains_any(
        &project_root,
        &["# Issues — MCP", "Oldest full listing only"],
    );

    assert_contains_all(
        &issue_collection,
        &[
            "# Issues — MCP",
            "6 issue(s)",
            "Oldest full listing only",
            "Newest activity",
            "## links",
            "[up]",
            "## filters",
            "status=backlog,active",
            "sort=updated_desc|created_asc|created_desc|updated_asc",
            "## actions",
            "create issue",
        ],
    );
    // The optional create+start key is advertised on the create action.
    assert!(issue_collection.contains("execution(object"));
    assert_not_contains_any(
        &issue_collection,
        &[
            "## Stats",
            "## Recent Activity",
            "Project context for resource overview",
        ],
    );
}

#[tokio::test]
async fn issue_read_renders_dependencies_and_possibly_related_issues() {
    let fixture = issue_project_fixture("REL").await;
    let current_id = fixture
        .insert_issue_with_status_and_time(1, "Current work", "active", 3)
        .await;
    let blocker_id = fixture
        .insert_issue_with_status_and_time(2, "Active blocker", "active", 2)
        .await;
    let related_id = fixture
        .insert_issue_with_status_and_time(3, "Related work", "active", 1)
        .await;
    insert_dependency(&fixture.db, &current_id, &build_issue_uri("REL", 2)).await;
    insert_file_change(
        &fixture.db,
        &fixture.project_id,
        &current_id,
        "current-job",
        "src/lib.rs",
    )
    .await;
    insert_file_change(
        &fixture.db,
        &fixture.project_id,
        &related_id,
        "related-job",
        "src/lib.rs",
    )
    .await;
    insert_file_change(
        &fixture.db,
        &fixture.project_id,
        &blocker_id,
        "blocker-job",
        "src/other.rs",
    )
    .await;

    let output = read_resource(&fixture.orch, build_issue_uri("REL", 1)).await;

    assert!(output.contains("## dependencies"));
    assert!(output.contains("[REL-2](cairn://p/REL/2) [○] Active blocker"));
    assert!(output.contains("## possibly related"));
    assert!(output.contains("[Related work](cairn://p/REL/3) — 1 file overlap"));
    assert!(!output.contains("[Current work](cairn://p/REL/1) —"));
    assert!(!output.contains("[Active blocker](cairn://p/REL/2) —"));
}

#[tokio::test]
async fn project_issues_filters_by_status_and_dependency_readiness() {
    let fixture = issue_project_fixture("FIL").await;
    let active_blocker_id = fixture
        .insert_issue_with_status_and_time(1, "Active blocker", "active", 1)
        .await;
    let closed_blocker_id = fixture
        .insert_issue_with_status_and_time(2, "Closed blocker", "closed", 2)
        .await;
    let ready_id = fixture
        .insert_issue_with_status_and_time(3, "Ready dependent", "active", 3)
        .await;
    let blocked_id = fixture
        .insert_issue_with_status_and_time(4, "Blocked dependent", "active", 4)
        .await;
    fixture
        .insert_issue_with_status_and_time(5, "Waiting issue", "waiting", 5)
        .await;
    insert_dependency(&fixture.db, &ready_id, &build_issue_uri("FIL", 2)).await;
    insert_dependency(&fixture.db, &blocked_id, &build_issue_uri("FIL", 1)).await;

    let ready_output = fixture.read_project_issues(Some("ready=true")).await;
    let blocked_output = fixture.read_project_issues(Some("ready=false")).await;
    let waiting_output = fixture.read_project_issues(Some("status=waiting")).await;
    let invalid_output = fixture.read_project_issues(Some("ready=maybe")).await;
    let unknown_output = fixture.read_project_issues(Some("search=dep")).await;

    assert_contains_all(
        &ready_output,
        &[
            "filtered by ready=true",
            "Ready dependent",
            "Active blocker",
            "Closed blocker",
        ],
    );
    assert_not_contains_any(&ready_output, &["Blocked dependent"]);
    assert_contains_all(
        &blocked_output,
        &["filtered by ready=false", "Blocked dependent"],
    );
    assert_not_contains_any(&blocked_output, &["Ready dependent"]);
    assert_contains_all(
        &waiting_output,
        &["filtered by status=waiting", "Waiting issue"],
    );
    assert_not_contains_any(&waiting_output, &["Ready dependent"]);
    assert!(invalid_output.contains("Invalid ready query parameter: maybe"));
    assert_contains_all(
        &unknown_output,
        &[
            "Unsupported query parameter 'search' for project issues",
            "Supported parameters: status, limit, offset, sort, ready, label, labels",
        ],
    );
    let _ = (active_blocker_id, closed_blocker_id);
}

#[tokio::test]
async fn project_issues_applies_limit_status_and_sort_filters() {
    let fixture = issue_project_fixture("QRY").await;
    fixture
        .insert_issue_with_status_and_time(1, "Old backlog", "backlog", 1)
        .await;
    fixture
        .insert_issue_with_status_and_time(2, "New active", "active", 3)
        .await;
    fixture
        .insert_issue_with_status_and_time(3, "Middle backlog", "backlog", 2)
        .await;
    fixture
        .insert_issue_with_status_and_time(4, "Waiting out", "waiting", 4)
        .await;

    let status_output = fixture
        .read_project_issues(Some("status=backlog,active&limit=2&sort=created_asc"))
        .await;
    let updated_output = fixture
        .read_project_issues(Some("sort=updated_desc&limit=3"))
        .await;
    let invalid_sort = fixture.read_project_issues(Some("sort=number_desc")).await;
    let invalid_status = fixture.read_project_issues(Some("status=open")).await;
    let invalid_limit = fixture.read_project_issues(Some("limit=0")).await;
    let label_output = fixture.read_project_issues(Some("label=frontend")).await;

    assert_contains_all(
        &status_output,
        &[
            "2 issue(s), filtered by status=backlog,active, sort=created_asc",
            "Old backlog",
            "Middle backlog",
        ],
    );
    assert_not_contains_any(&status_output, &["New active", "Waiting out"]);
    assert_contains_before(&updated_output, "Waiting out", "New active");
    assert_contains_before(&updated_output, "New active", "Middle backlog");
    assert_not_contains_any(&updated_output, &["Old backlog"]);
    assert!(invalid_sort.contains("Invalid sort query parameter: number_desc"));
    assert!(invalid_status.contains("Invalid status query parameter: open"));
    assert!(invalid_limit.contains("Invalid limit query parameter: 0"));
    // Label filtering is supported; none of the seeded issues carry a "frontend"
    // label, so the filtered listing excludes them all.
    assert_not_contains_any(
        &label_output,
        &["Old backlog", "Middle backlog", "New active", "Waiting out"],
    );
}

#[tokio::test]
async fn project_issues_default_limit_caps_results() {
    let fixture = issue_project_fixture("CAP").await;
    for number in 1..=25 {
        fixture
            .insert_issue_with_status_and_time(
                number,
                &format!("Issue {number}"),
                "backlog",
                number,
            )
            .await;
    }

    let output = fixture.read_project_issues(None).await;

    assert!(output.contains("20 issue(s)"));
    assert!(output.contains("Issue 25"));
    assert!(output.contains("Issue 6"));
    assert!(!output.contains("Issue 5"));
}

// --- Gated artifact resolution via `write` (CAIRN-1174) ---

/// Seed an execution with a blocked `planner` job (unconfirmed `plan` artifact)
/// and a completed `builder` job (confirmed `pr` artifact).
async fn seed_gated_execution(db: &LocalDb, project_id: &str, issue_id: &str) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-gate', 'recipe', ?1, ?2, 'running', 1, 1)",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
                 VALUES ('job-planner', 'exec-gate', 'planner', ?1, ?2, 'Planner', 'blocked', 1, 1, 'planner')",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
                 VALUES ('job-builder', 'exec-gate', 'builder', ?1, ?2, 'Builder', 'complete', 1, 1, 'builder')",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
                 VALUES ('artifact-plan', 'job-planner', 'plan', 'the plan body', 1, 'plan', 3, 3, 0)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
                 VALUES ('artifact-pr', 'job-builder', 'pr', 'the pr body', 1, 'pr', 4, 4, 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn apply_change(
    orch: &Orchestrator,
    target: &str,
    mode: &str,
    payload: serde_json::Value,
) -> String {
    change(
        orch,
        json!([{ "target": target, "mode": mode, "payload": payload }]),
    )
    .await
}

async fn job_status(db: &LocalDb, job_id: &str) -> String {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM jobs WHERE id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            rows.next().await?.unwrap().text(0)
        })
    })
    .await
    .unwrap()
}

async fn latest_artifact_data_and_version(db: &LocalDb, job_id: &str) -> (serde_json::Value, i64) {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data, version FROM artifacts WHERE job_id = ?1 ORDER BY version DESC LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.unwrap();
            let data = row.text(0)?;
            let version = row.i64(1)?;
            Ok::<_, cairn_core::internal::storage::DbError>((
                serde_json::from_str(&data).unwrap(),
                version,
            ))
        })
    })
    .await
    .unwrap()
}

struct GatedArtifactFixture {
    _temp: tempfile::TempDir,
    db: Arc<LocalDb>,
    orch: Orchestrator,
}

async fn gated_artifact_fixture(issue_title: &str) -> GatedArtifactFixture {
    let IssueProjectFixture {
        _temp,
        db,
        orch,
        project_id,
        ..
    } = issue_project_fixture("MCP").await;
    let issue_id =
        insert_issue_with_status_and_time(&db, &project_id, 1, issue_title, "active", 1).await;
    seed_gated_execution(&db, &project_id, &issue_id).await;
    GatedArtifactFixture { _temp, db, orch }
}

async fn replace_plan_artifact_data(db: &LocalDb, data: serde_json::Value) {
    let data = data.to_string();
    db.write(|conn| {
        let data = data.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE artifacts SET data = ?1 WHERE id = 'artifact-plan'",
                params![data.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn gated_plan_artifact_fixture(
    issue_title: &str,
    data: serde_json::Value,
) -> GatedArtifactFixture {
    let fixture = gated_artifact_fixture(issue_title).await;
    replace_plan_artifact_data(&fixture.db, data).await;
    fixture
}

#[tokio::test]
async fn artifact_text_replacement_patch_updates_latest_object_version() {
    let fixture = gated_plan_artifact_fixture(
        "Structured plan patch",
        json!({"title":"Plan","summary":"summary","content":"stale text"}),
    )
    .await;

    let result = apply_change(
        &fixture.orch,
        "cairn://p/MCP/1/1/planner/plan",
        "patch",
        json!({ "old_string": "stale", "new_string": "corrected" }),
    )
    .await;
    assert!(result.contains("Artifact patched"), "unexpected: {result}");

    let (data, version) = latest_artifact_data_and_version(&fixture.db, "job-planner").await;
    assert_eq!(version, 2);
    assert_eq!(data["content"], "corrected text");
    assert_eq!(data["title"], "Plan");
    assert!(data.get("old_string").is_none());
    assert!(data.get("new_string").is_none());
}

#[tokio::test]
async fn artifact_text_replacement_patch_failure_does_not_store_version() {
    let fixture = gated_plan_artifact_fixture(
        "Structured plan patch failure",
        json!({"title":"Plan","summary":"summary","content":"stale stale"}),
    )
    .await;

    let missing = apply_change(
        &fixture.orch,
        "cairn://p/MCP/1/1/planner/plan",
        "patch",
        json!({ "old_string": "absent", "new_string": "corrected" }),
    )
    .await;
    assert!(
        missing.contains("old_string not found"),
        "unexpected: {missing}"
    );
    assert_eq!(
        latest_artifact_data_and_version(&fixture.db, "job-planner")
            .await
            .1,
        1
    );

    let ambiguous = apply_change(
        &fixture.orch,
        "cairn://p/MCP/1/1/planner/plan",
        "patch",
        json!({ "old_string": "stale", "new_string": "corrected" }),
    )
    .await;
    assert!(
        ambiguous.contains("matched 2 times"),
        "unexpected: {ambiguous}"
    );
    assert_eq!(
        latest_artifact_data_and_version(&fixture.db, "job-planner")
            .await
            .1,
        1
    );
}

#[tokio::test]
async fn artifact_mixed_text_and_field_patch_rejects_without_new_version() {
    let fixture = gated_plan_artifact_fixture(
        "Structured plan mixed patch",
        json!({"title":"Plan","summary":"old summary","content":"stale text"}),
    )
    .await;

    let result = apply_change(
        &fixture.orch,
        "cairn://p/MCP/1/1/planner/plan",
        "patch",
        json!({ "old_string": "stale", "new_string": "corrected", "summary": "new summary" }),
    )
    .await;
    assert!(result.contains("cannot be mixed"), "unexpected: {result}");

    let (data, version) = latest_artifact_data_and_version(&fixture.db, "job-planner").await;
    assert_eq!(version, 1);
    assert_eq!(data["content"], "stale text");
    assert_eq!(data["summary"], "old summary");
}

#[tokio::test]
async fn artifact_field_merge_patch_preserves_unedited_fields() {
    let fixture = gated_plan_artifact_fixture(
        "Structured plan field merge",
        json!({"title":"Plan","summary":"keep me","content":"stale text"}),
    )
    .await;

    let result = apply_change(
        &fixture.orch,
        "cairn://p/MCP/1/1/planner/plan",
        "patch",
        json!({ "content": "fully revised content" }),
    )
    .await;
    assert!(result.contains("Artifact patched"), "unexpected: {result}");

    let (data, version) = latest_artifact_data_and_version(&fixture.db, "job-planner").await;
    assert_eq!(version, 2);
    assert_eq!(data["content"], "fully revised content");
    assert_eq!(data["title"], "Plan");
    assert_eq!(data["summary"], "keep me");
}

#[tokio::test]
async fn blocked_artifact_read_surfaces_resolution_actions() {
    let fixture = gated_artifact_fixture("Gated plan").await;

    // Blocked producing job: the read carries the gate-resolution affordance.
    let blocked = read_resource(&fixture.orch, "cairn://p/MCP/1/1/planner/plan".to_string()).await;
    assert!(blocked.contains("the plan body"));
    assert!(blocked.contains("## actions"));
    assert!(blocked.contains("confirmed:true"));
    assert!(blocked.contains("cairn://p/MCP/1/1/planner/plan"));
    assert!(blocked.contains("continue"));
    // The continue action targets the producing node, not the artifact.
    assert!(blocked.contains("target:\"cairn://p/MCP/1/1/planner\""));

    // Non-blocked producing job: no resolution affordance, raw artifact only.
    let done = read_resource(&fixture.orch, "cairn://p/MCP/1/1/builder/pr".to_string()).await;
    assert!(done.starts_with("the pr body"), "done: {done}");
}

#[tokio::test]
async fn confirm_change_with_confirmed_false_is_rejected() {
    let fixture = gated_artifact_fixture("Gated plan").await;

    let result = apply_change(
        &fixture.orch,
        "cairn://p/MCP/1/1/planner/plan",
        "patch",
        json!({ "confirmed": false }),
    )
    .await;
    assert!(result.contains("must be true"), "unexpected: {result}");
    // The gate stays closed.
    assert_eq!(job_status(&fixture.db, "job-planner").await, "blocked");
}

#[tokio::test]
async fn confirm_change_on_job_without_unconfirmed_artifact_is_rejected() {
    let fixture = gated_artifact_fixture("Gated plan").await;

    // The builder job is already complete with a confirmed artifact — there is
    // nothing left to confirm, so the change is rejected. Confirmability keys off
    // the unconfirmed artifact, not the job's blocked state (CAIRN-1576).
    let result = apply_change(
        &fixture.orch,
        "cairn://p/MCP/1/1/builder/pr",
        "patch",
        json!({ "confirmed": true }),
    )
    .await;
    assert!(
        result.contains("no unconfirmed artifact to confirm"),
        "unexpected: {result}"
    );
}

// --- Schema-named artifact URIs (CAIRN-1219) ---

#[tokio::test]
async fn generic_artifact_alias_resolves_to_schema_named_uri() {
    let fixture = gated_artifact_fixture("Gated plan").await;

    // Reading through the generic `/artifact` alias still surfaces the canonical,
    // schema-named URI in the gate-resolution affordance — `/artifact` is never
    // presented as a destination.
    let aliased = read_resource(
        &fixture.orch,
        "cairn://p/MCP/1/1/planner/artifact".to_string(),
    )
    .await;
    assert!(aliased.contains("the plan body"));
    assert!(aliased.contains("## actions"));
    assert!(aliased.contains("cairn://p/MCP/1/1/planner/plan"));
    assert!(!aliased.contains("planner/artifact"));

    // The node summary lists the artifact at its schema-named URI too.
    let summary = read_resource(&fixture.orch, "cairn://p/MCP/1/1/planner".to_string()).await;
    assert!(summary.contains("Artifact: `cairn://p/MCP/1/1/planner/plan`"));
    assert!(!summary.contains("planner/artifact"));
}

#[tokio::test]
async fn artifact_uri_prefers_output_name_over_artifact_type() {
    let fixture = issue_project_fixture("MCP").await;
    let issue_id = fixture
        .insert_issue_with_status_and_time(1, "Inherited schema", "active", 1)
        .await;

    // A blocked builder whose stored artifact carries a distinct output_name
    // (`create-pr`) and artifact_type (`pr`). The output_name is the resolved
    // schema name and must win over the type when addressing the artifact.
    let project_id_seed = fixture.project_id.clone();
    let issue_id_seed = issue_id.clone();
    fixture.db.write(|conn| {
        let project_id = project_id_seed.clone();
        let issue_id = issue_id_seed.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-inherit', 'recipe', ?1, ?2, 'running', 1, 1)",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
                 VALUES ('job-inherit', 'exec-inherit', 'builder', ?1, ?2, 'Builder', 'blocked', 1, 1, 'builder')",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
                 VALUES ('artifact-inherit', 'job-inherit', 'pr', 'pr draft', 1, 'create-pr', 3, 3, 0)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let aliased = read_resource(
        &fixture.orch,
        "cairn://p/MCP/1/1/builder/artifact".to_string(),
    )
    .await;
    assert!(aliased.contains("cairn://p/MCP/1/1/builder/create-pr"));
    assert!(!aliased.contains("builder/artifact"));
    // The generic alias and the bare artifact_type are both absent as a target.
    assert!(!aliased.contains("builder/pr\""));
}

// --- Per-name artifact chains: named reads + full listing (CAIRN-1942) ---

/// Seed one complete `builder` job carrying two independent named artifact
/// chains: `plan` (v1 -> v2) and `notes` (v1). Each addressed name is its own
/// `output_name` identity, exactly as the write path now stores them.
async fn seed_two_named_artifacts(fixture: &IssueProjectFixture, issue_id: &str) {
    let project_id = fixture.project_id.clone();
    let issue_id = issue_id.to_string();
    fixture
        .db
        .write(|conn| {
            let project_id = project_id.clone();
            let issue_id = issue_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-multi', 'recipe', ?1, ?2, 'running', 1, 1)",
                    params![issue_id.as_str(), project_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
                     VALUES ('job-multi', 'exec-multi', 'builder', ?1, ?2, 'Builder', 'complete', 1, 1, 'builder')",
                    params![issue_id.as_str(), project_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
                     VALUES ('plan-v1', 'job-multi', 'plan', '{\"content\":\"plan one\"}', 1, 'plan', 3, 3, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO artifacts(id, job_id, artifact_type, data, version, parent_version_id, output_name, created_at, updated_at, confirmed)
                     VALUES ('plan-v2', 'job-multi', 'plan', '{\"content\":\"plan two\"}', 2, 'plan-v1', 'plan', 4, 4, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
                     VALUES ('notes-v1', 'job-multi', 'notes', '{\"content\":\"notes one\"}', 1, 'notes', 5, 5, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn node_summary_lists_every_named_artifact() {
    let fixture = issue_project_fixture("MCP").await;
    let issue_id = fixture
        .insert_issue_with_status_and_time(1, "Multi-artifact node", "active", 1)
        .await;
    seed_two_named_artifacts(&fixture, &issue_id).await;

    let summary = read_resource(&fixture.orch, "cairn://p/MCP/1/1/builder".to_string()).await;
    // Both named chains surface at their canonical, schema-named URIs.
    assert!(summary.contains("- Artifacts:"), "unexpected: {summary}");
    assert!(summary.contains("cairn://p/MCP/1/1/builder/plan"));
    assert!(summary.contains("cairn://p/MCP/1/1/builder/notes"));
}

#[tokio::test]
async fn named_read_returns_its_own_chain_latest() {
    let fixture = issue_project_fixture("MCP").await;
    let issue_id = fixture
        .insert_issue_with_status_and_time(1, "Multi-artifact read", "active", 1)
        .await;
    seed_two_named_artifacts(&fixture, &issue_id).await;

    // The `plan` read returns the plan chain's latest version (v2), never notes.
    let plan = read_resource(&fixture.orch, "cairn://p/MCP/1/1/builder/plan".to_string()).await;
    assert!(plan.contains("plan two"), "unexpected: {plan}");
    assert!(!plan.contains("plan one"));
    assert!(!plan.contains("notes one"));

    // The `notes` read returns its own chain, independent of plan.
    let notes = read_resource(&fixture.orch, "cairn://p/MCP/1/1/builder/notes".to_string()).await;
    assert!(notes.contains("notes one"), "unexpected: {notes}");
    assert!(!notes.contains("plan two"));
}

// --- Issue create + optional execution start (CAIRN-1192) ---

#[tokio::test]
async fn issue_create_without_execution_creates_issue_only() {
    let fixture = issue_project_fixture("MCP").await;

    let result = apply_change(
        &fixture.orch,
        &fixture.project_issues_uri(),
        "append",
        json!({ "title": "Plain create", "description": "no exec" }),
    )
    .await;

    assert!(
        result.contains("Created issue MCP-1"),
        "unexpected: {result}"
    );
    assert_eq!(
        common::query_i64(&fixture.db, "SELECT COUNT(*) FROM issues")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        common::query_i64(&fixture.db, "SELECT COUNT(*) FROM executions")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn issue_create_result_carries_structured_issue_ref() {
    let fixture = issue_project_fixture("MCP").await;

    let result = apply_change(
        &fixture.orch,
        &fixture.project_issues_uri(),
        "append",
        json!({ "title": "Structured ref", "description": "carries data" }),
    )
    .await;

    // The write result's applied[].data carries the created issue's identifiers
    // so transcript renderers can build a drag target without parsing the
    // human-readable summary.
    let report: serde_json::Value =
        serde_json::from_str(&result).expect("change result should be JSON");
    let applied = report
        .get("applied")
        .and_then(|a| a.as_array())
        .expect("applied array present");
    let data = applied[0]
        .get("data")
        .expect("applied[0].data present for issue create");
    assert_eq!(data.get("projectKey").and_then(|v| v.as_str()), Some("MCP"));
    assert_eq!(data.get("number").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        data.get("uri").and_then(|v| v.as_str()),
        Some(build_issue_uri("MCP", 1).as_str())
    );
}

#[tokio::test]
async fn issue_create_with_execution_creates_issue_then_surfaces_start_failure() {
    let fixture = issue_project_fixture("MCP").await;

    // No recipe config exists in the temp config dir, so the start fails — but
    // the issue must already be durable, and the failure must name the
    // executions collection so the caller can retry the start.
    let result = apply_change(
        &fixture.orch,
        &fixture.project_issues_uri(),
        "append",
        json!({ "title": "Create and start", "execution": { "recipe": "no-such-recipe" } }),
    )
    .await;

    assert!(
        result.contains("Issue created, but starting the execution failed"),
        "unexpected: {result}"
    );
    assert!(result.contains("/executions"), "unexpected: {result}");
    assert_eq!(
        common::query_i64(&fixture.db, "SELECT COUNT(*) FROM issues")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        common::query_i64(&fixture.db, "SELECT COUNT(*) FROM executions")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn issue_create_rejects_malformed_execution_before_creating() {
    let fixture = issue_project_fixture("MCP").await;

    // A non-object execution is rejected at parse time, before the issue row is
    // written — a malformed spec must not leave a half-created issue behind.
    let bad_shape = apply_change(
        &fixture.orch,
        &fixture.project_issues_uri(),
        "append",
        json!({ "title": "Bad exec", "execution": "claude" }),
    )
    .await;
    assert!(
        bad_shape.contains("must be an object"),
        "unexpected: {bad_shape}"
    );

    // A non-string field inside execution is likewise rejected.
    let bad_field = apply_change(
        &fixture.orch,
        &fixture.project_issues_uri(),
        "append",
        json!({ "title": "Bad field", "execution": { "recipe": 5 } }),
    )
    .await;
    assert!(
        bad_field.contains("execution.recipe must be a string"),
        "unexpected: {bad_field}"
    );

    assert_eq!(
        common::query_i64(&fixture.db, "SELECT COUNT(*) FROM issues")
            .await
            .unwrap(),
        0
    );
}
