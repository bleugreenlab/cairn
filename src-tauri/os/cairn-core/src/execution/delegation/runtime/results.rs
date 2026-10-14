use crate::models::{AgentConfig, JobStatus};
use crate::storage::{LocalDb, RowExt};
use cairn_common::protocol::CallbackResponse;

use super::common::select_optional_text;

/// How a delegated child settled, as its own job reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskOutcome {
    Succeeded,
    Failed,
    Cancelled,
    /// Neither the job nor its run reached a terminal state. The child is still
    /// working (or something dropped it); there is nothing to report as a result.
    Unsettled,
}

/// Decide a delegated child's outcome from its job status, with its run status
/// as a fallback only.
///
/// The job is the authority, matching what the resume path and
/// `refresh_packet_state` already key on. `runs.status` cannot be that
/// authority: a child that finishes through the `return` tool is transitioned
/// *warm* and its process is deliberately retained (CAIRN-1576), so it never
/// reaches the process exit that would advance its run past `live`. Gating on
/// the run therefore reported every fast, warm-completing child as "unknown
/// status" and discarded the artifact it had already written.
///
/// The terminal set is [`JobStatus::is_terminal`]'s, reached by parsing rather
/// than by re-spelling it here, so the two cannot drift.
///
/// The run still answers for a child whose job row says nothing terminal — a run
/// that crashed before its job was ever recomputed.
fn classify_task_outcome(job_status: Option<&str>, run_status: Option<&str>) -> TaskOutcome {
    let job_status = job_status.and_then(|status| status.parse::<JobStatus>().ok());
    if let Some(status) = job_status.filter(JobStatus::is_terminal) {
        return match status {
            JobStatus::Complete => TaskOutcome::Succeeded,
            JobStatus::Cancelled => TaskOutcome::Cancelled,
            // `is_terminal` admits exactly Complete, Failed, and Cancelled.
            _ => TaskOutcome::Failed,
        };
    }
    match run_status {
        Some("complete") | Some("completed") | Some("exited") => TaskOutcome::Succeeded,
        Some("failed") | Some("crashed") => TaskOutcome::Failed,
        _ => TaskOutcome::Unsettled,
    }
}

pub(super) async fn build_task_callback_response(
    db: &LocalDb,
    run_id: &str,
    node_id: &str,
    agent_config: &AgentConfig,
    task_description: &str,
    expected_artifact_type: &str,
) -> CallbackResponse {
    let job_status = select_optional_text(db, "SELECT status FROM jobs WHERE id = ?1", node_id)
        .await
        .ok()
        .flatten();
    let run_status = select_optional_text(db, "SELECT status FROM runs WHERE id = ?1", run_id)
        .await
        .ok()
        .flatten();
    let outcome = classify_task_outcome(job_status.as_deref(), run_status.as_deref());

    // A written artifact is a real result whatever the child's status says, so
    // it is looked up before the outcome is branched on rather than inside the
    // success arm. A child that produced its output and then failed still has
    // something the caller can read, and its address is always worth naming.
    let artifact =
        match latest_nonempty_artifact_content(db, node_id, Some(expected_artifact_type)).await {
            Some(content) => Some(content),
            None => latest_nonempty_artifact_content(db, node_id, None).await,
        };
    let artifact_uri = task_artifact_uri(db, node_id).await;

    let result = match (outcome, artifact) {
        (TaskOutcome::Succeeded, Some(content)) => content,
        (TaskOutcome::Succeeded, None) => latest_nonempty_assistant_content(db, run_id)
            .await
            .unwrap_or_else(|| "Task completed.".to_string()),
        (TaskOutcome::Failed, artifact) => failure_text(
            agent_config,
            task_description,
            "failed",
            "The agent encountered an error.",
            artifact.as_deref(),
        ),
        (TaskOutcome::Cancelled, artifact) => failure_text(
            agent_config,
            task_description,
            "was cancelled",
            "The task was stopped before it finished.",
            artifact.as_deref(),
        ),
        // A child that has written an artifact but not settled is most often a
        // `blocked` checkpoint awaiting approval. Report what it wrote — it is
        // real — but never let it read as the task's final answer.
        (TaskOutcome::Unsettled, Some(content)) => format!(
            "Agent '{}' has not settled yet (job: {}, run: {}). What it has written so far:\n\n{}",
            agent_config.name,
            job_status.as_deref().unwrap_or("unknown"),
            run_status.as_deref().unwrap_or("unknown"),
            content
        ),
        (TaskOutcome::Unsettled, None) => format!(
            "Agent '{}' has not settled (job: {}, run: {}).\n\nTask: {}",
            agent_config.name,
            job_status.as_deref().unwrap_or("unknown"),
            run_status.as_deref().unwrap_or("unknown"),
            task_description
        ),
    };

    CallbackResponse {
        result,
        artifact_uri,
        ..Default::default()
    }
}

/// A failure report that still carries whatever the child managed to produce.
fn failure_text(
    agent_config: &AgentConfig,
    task_description: &str,
    verb: &str,
    detail: &str,
    artifact: Option<&str>,
) -> String {
    let mut text = format!(
        "Agent '{}' {verb}.\n\nTask: {}\n\n{detail}",
        agent_config.name, task_description
    );
    if let Some(artifact) = artifact {
        text.push_str(&format!("\n\nPartial result it had written:\n\n{artifact}"));
    }
    text
}

/// The artifact URI for a delegated child job, derived from the child's own
/// canonical home.
///
/// An artifact lives at its producing job's home plus its name, so this is that
/// one resolution plus `/artifact` rather than a second coordinate rebuild. The
/// rebuild it replaces started from the parent's issue number, which a thread
/// does not have: every task a thread spawned reported no URI at all, leaving
/// the caller unable to address the task it had just minted.
pub(super) async fn task_artifact_uri(db: &LocalDb, task_job_id: &str) -> Option<String> {
    let home = crate::jobs::queries::home_uri_for_job(db, task_job_id)
        .await
        .ok()
        .flatten()?;
    Some(format!("{home}/artifact"))
}

fn normalize_result_text(text: String) -> Option<String> {
    if text.trim().is_empty() {
        None
    } else {
        Some(text)
    }
}

fn parse_nonempty_artifact_content(data_json: &str) -> Option<String> {
    let data: serde_json::Value = serde_json::from_str(data_json).ok()?;
    // A freeform `return` artifact carries its result in a `content` string.
    if let Some(content) = data.get("content").and_then(|value| value.as_str()) {
        return normalize_result_text(content.to_string());
    }
    // A structured artifact (a preset like `review` or an inline custom schema)
    // has no `content` field; render the whole validated JSON so the parent
    // receives the structured result instead of falling back to chat text.
    match &data {
        serde_json::Value::Object(map) if !map.is_empty() => serde_json::to_string_pretty(&data)
            .ok()
            .and_then(normalize_result_text),
        _ => None,
    }
}

fn parse_nonempty_assistant_content(data_json: &str) -> Option<String> {
    serde_json::from_str::<crate::agent_process::stream::TranscriptEvent>(data_json)
        .ok()
        .and_then(|event| event.content)
        .and_then(normalize_result_text)
}

async fn latest_nonempty_artifact_content(
    db: &LocalDb,
    job_id: &str,
    artifact_type: Option<&str>,
) -> Option<String> {
    let job_id = job_id.to_string();
    let artifact_type = artifact_type.map(str::to_string);
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = if let Some(artifact_type) = artifact_type.as_deref() {
                conn.query(
                    "
                    SELECT data FROM artifacts
                    WHERE job_id = ?1 AND artifact_type = ?2
                    ORDER BY version DESC
                    LIMIT 10
                    ",
                    (job_id.as_str(), artifact_type),
                )
                .await?
            } else {
                conn.query(
                    "
                    SELECT data FROM artifacts
                    WHERE job_id = ?1
                    ORDER BY version DESC
                    LIMIT 10
                    ",
                    (job_id.as_str(),),
                )
                .await?
            };
            let mut data = Vec::new();
            while let Some(row) = rows.next().await? {
                data.push(row.text(0)?);
            }
            Ok(data)
        })
    })
    .await
    .ok()
    .and_then(|rows| {
        rows.into_iter()
            .filter_map(|data_json| parse_nonempty_artifact_content(&data_json))
            .next()
    })
}

async fn latest_nonempty_assistant_content(db: &LocalDb, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT data FROM events
                    WHERE run_id = ?1 AND event_type = 'assistant'
                    ORDER BY sequence DESC
                    LIMIT 20
                    ",
                    (run_id.as_str(),),
                )
                .await?;
            let mut data = Vec::new();
            while let Some(row) = rows.next().await? {
                data.push(row.text(0)?);
            }
            Ok(data)
        })
    })
    .await
    .ok()
    .and_then(|rows| {
        rows.into_iter()
            .filter_map(|data_json| parse_nonempty_assistant_content(&data_json))
            .next()
    })
}

pub(super) async fn latest_nonempty_artifact_content_arc(
    db: std::sync::Arc<LocalDb>,
    job_id: String,
    artifact_type: Option<String>,
) -> Option<String> {
    latest_nonempty_artifact_content(&db, &job_id, artifact_type.as_deref()).await
}

pub(super) async fn latest_nonempty_assistant_content_arc(
    db: std::sync::Arc<LocalDb>,
    run_id: String,
) -> Option<String> {
    latest_nonempty_assistant_content(&db, &run_id).await
}

#[cfg(test)]
mod outcome_tests {
    use super::{classify_task_outcome, TaskOutcome};

    /// The bug, at its smallest: a warm-completing child's job says `complete`
    /// while its run is still `live`, because the process is retained and never
    /// exits. The job decides.
    #[test]
    fn a_complete_job_with_a_live_run_succeeded() {
        assert_eq!(
            classify_task_outcome(Some("complete"), Some("live")),
            TaskOutcome::Succeeded
        );
    }

    #[test]
    fn job_status_outranks_run_status() {
        assert_eq!(
            classify_task_outcome(Some("failed"), Some("exited")),
            TaskOutcome::Failed
        );
        assert_eq!(
            classify_task_outcome(Some("cancelled"), Some("live")),
            TaskOutcome::Cancelled
        );
    }

    /// A run that crashed before its job was ever recomputed still answers.
    #[test]
    fn run_status_answers_when_the_job_says_nothing_terminal() {
        assert_eq!(
            classify_task_outcome(Some("running"), Some("crashed")),
            TaskOutcome::Failed
        );
        assert_eq!(
            classify_task_outcome(None, Some("exited")),
            TaskOutcome::Succeeded
        );
    }

    #[test]
    fn neither_terminal_is_unsettled() {
        assert_eq!(
            classify_task_outcome(Some("running"), Some("live")),
            TaskOutcome::Unsettled
        );
    }
}

#[cfg(test)]
mod callback_response_tests {
    use super::build_task_callback_response;
    use crate::models::AgentConfig;
    use crate::storage::LocalDb;

    fn agent() -> AgentConfig {
        AgentConfig {
            id: "explore".to_string(),
            name: "Explore".to_string(),
            description: String::new(),
            prompt: String::new(),
            tools: Vec::new(),
            tier: None,
            workspace_id: None,
            project_id: None,
            created_at: 0,
            updated_at: 0,
            disallowed_tools: None,
            skills: None,
            fence: None,
            backend_preference: None,
            icon: None,
            selection: None,
            extras: None,
        }
    }

    /// Seed the exact live state the "unknown status" reports came from: the
    /// child job is `complete` and its artifact is written, while its run row
    /// is still `live` because the warm process never exited.
    async fn warm_completed_child() -> LocalDb {
        let db = crate::storage::migrated_test_db("delegated-result.db").await;
        db.execute_script(
            r#"INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p1', 'default', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
               INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
               VALUES ('i1', 'p1', 7, 'Parent issue', '', 'active', 'active', 'none', 0, 1, 1);
               INSERT INTO jobs (id, project_id, issue_id, status, node_name, uri_segment, created_at, updated_at)
               VALUES ('j-parent', 'p1', 'i1', 'running', 'builder', 'builder', 1, 1);
               INSERT INTO jobs (id, parent_job_id, project_id, issue_id, status, node_name, uri_segment, created_at, updated_at)
               VALUES ('j-task', 'j-parent', 'p1', 'i1', 'complete', 'probe', 'probe', 2, 2);
               INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at)
               VALUES ('r-task', 'j-task', 'i1', 'live', 2, 2);
               INSERT INTO artifacts (id, job_id, artifact_type, output_name, data, created_at, updated_at)
               VALUES ('a-task', 'j-task', 'return', 'return', '{"content":"the explored answer"}', 3, 3);"#,
        )
        .await
        .unwrap();
        db
    }

    /// The child completed and wrote its artifact; the caller must receive that
    /// artifact and an address for it, not "finished with unknown status" and
    /// no URI. This test fails against the run-status gate it replaced.
    #[tokio::test]
    async fn a_warm_completed_child_returns_its_artifact_and_uri() {
        let db = warm_completed_child().await;
        let response =
            build_task_callback_response(&db, "r-task", "j-task", &agent(), "explore it", "return")
                .await;
        assert_eq!(response.result, "the explored answer");
        assert_eq!(
            response.artifact_uri.as_deref(),
            Some("cairn://p/cairn/7/1/builder/task/probe/artifact")
        );
        assert!(!response.result.contains("unknown status"));
    }

    /// A child that produced output and then failed still has a readable result,
    /// and its address is named either way.
    #[tokio::test]
    async fn a_failed_child_still_names_what_it_wrote() {
        let db = warm_completed_child().await;
        db.execute("UPDATE jobs SET status = 'failed' WHERE id = 'j-task'", ())
            .await
            .unwrap();
        let response =
            build_task_callback_response(&db, "r-task", "j-task", &agent(), "explore it", "return")
                .await;
        assert!(response.result.contains("failed"), "{}", response.result);
        assert!(
            response.result.contains("the explored answer"),
            "{}",
            response.result
        );
        assert!(response.artifact_uri.is_some());
    }

    /// With nothing terminal and nothing written, the report names the two
    /// statuses it actually saw instead of the opaque "unknown status".
    #[tokio::test]
    async fn an_unsettled_child_with_no_artifact_reports_both_statuses() {
        let db = warm_completed_child().await;
        db.execute("UPDATE jobs SET status = 'running' WHERE id = 'j-task'", ())
            .await
            .unwrap();
        db.execute("DELETE FROM artifacts WHERE job_id = 'j-task'", ())
            .await
            .unwrap();
        let response =
            build_task_callback_response(&db, "r-task", "j-task", &agent(), "explore it", "return")
                .await;
        assert!(
            response.result.contains("job: running") && response.result.contains("run: live"),
            "{}",
            response.result
        );
    }
}

#[cfg(test)]
mod result_render_tests {
    use super::parse_nonempty_artifact_content;

    #[test]
    fn freeform_return_artifact_uses_content_field() {
        let out = parse_nonempty_artifact_content(r#"{"content":"the result"}"#);
        assert_eq!(out.as_deref(), Some("the result"));
    }

    #[test]
    fn structured_artifact_renders_full_json() {
        let out = parse_nonempty_artifact_content(r#"{"approval":"approved","summary":"ok"}"#)
            .expect("structured artifact renders");
        assert!(out.contains("approval"));
        assert!(out.contains("approved"));
    }

    #[test]
    fn empty_artifact_object_is_none() {
        assert!(parse_nonempty_artifact_content("{}").is_none());
    }
}
