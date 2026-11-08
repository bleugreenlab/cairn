use crate::models::JobStatus;
use crate::storage::events::history::{EventFilter, HistoryError, HistoryReader};
use crate::storage::{LocalDb, RowExt};
use cairn_common::protocol::CallbackResponse;

use super::common::{latest_run_for_job, select_optional_text};

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
/// The run answers only for failure, for a child whose job row says nothing
/// terminal: a run that crashed before its job was ever recomputed still
/// settles it as failed. A clean run exit says nothing about whether the child
/// met its contract. A task that ends its turn without `return` exits just as
/// cleanly, and its job rests `blocked` while it is re-prompted for its result,
/// so only the job can report success. Every clean turn end recomputes the job
/// before it signals the inline wait, so a child that did return is read as
/// `complete` by the time the wait re-checks it.
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
        Some("failed") | Some("crashed") => TaskOutcome::Failed,
        _ => TaskOutcome::Unsettled,
    }
}

/// Whether a delegated child has settled, as [`classify_task_outcome`] reads it
/// from the child's job and its latest run.
///
/// This is what an inline wait keys on. It needs only the job, so a child that
/// was created without a run, or failed or was cancelled before it ever had
/// one, settles here exactly like one that ran.
pub(super) async fn task_has_settled(db: &LocalDb, job_id: &str) -> bool {
    let job_status = select_optional_text(db, "SELECT status FROM jobs WHERE id = ?1", job_id)
        .await
        .ok()
        .flatten();
    let run_status = match latest_run_for_job(db, job_id).await.ok().flatten() {
        Some(run_id) => select_optional_text(db, "SELECT status FROM runs WHERE id = ?1", &run_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    classify_task_outcome(job_status.as_deref(), run_status.as_deref()) != TaskOutcome::Unsettled
}

/// What a delegated child's parent receives, read once from the child's job.
///
/// Both the inline wait and the durable resume render from this, so a child
/// that never wrote its result cannot read as having answered on either path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TaskResult {
    /// The child completed and wrote its result.
    Returned {
        content: String,
    },
    /// The child's work ended without its result. `asked` says whether it was
    /// re-prompted for it once first. `last_message` is what it last said,
    /// reported as exactly that and never as the result.
    EndedWithoutReturn {
        last_message: Option<String>,
        asked: bool,
    },
    Failed {
        partial: Option<String>,
    },
    Cancelled {
        partial: Option<String>,
    },
    /// Neither the job nor its run is terminal. `partial` is whatever the child
    /// has written so far, most often a `blocked` checkpoint awaiting approval.
    Unsettled {
        job_status: Option<String>,
        run_status: Option<String>,
        partial: Option<String>,
    },
}

/// Read a delegated child's result. `artifact_name` is the artifact its
/// contract owes; a child that never had a run reads from its job alone.
pub(super) async fn task_result(
    db: &LocalDb,
    job_id: &str,
    artifact_name: &str,
) -> Result<TaskResult, HistoryError> {
    let (job_status, abandoned) = db
        .query_opt(
            "SELECT status, return_abandoned_at IS NOT NULL FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
            |row| Ok((row.text(0)?, row.i64(1)? != 0)),
        )
        .await
        .ok()
        .flatten()
        .map_or((None, false), |(status, abandoned)| {
            (Some(status), abandoned)
        });
    let run_id = latest_run_for_job(db, job_id).await.ok().flatten();
    let run_status = match run_id.as_deref() {
        Some(run_id) => select_optional_text(db, "SELECT status FROM runs WHERE id = ?1", run_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let outcome = classify_task_outcome(job_status.as_deref(), run_status.as_deref());

    // A written artifact is a real result whatever the child's status says, so
    // it is looked up before the outcome is branched on. A child that produced
    // its output and then failed still has something the caller can read.
    let artifact = match latest_nonempty_artifact_content(db, job_id, Some(artifact_name)).await {
        Some(content) => Some(content),
        None => latest_nonempty_artifact_content(db, job_id, None).await,
    };
    let last_message = || async {
        match run_id.as_deref() {
            Some(run_id) => latest_nonempty_assistant_content(db, run_id).await,
            None => Ok(None),
        }
    };

    Ok(match (outcome, artifact) {
        (TaskOutcome::Succeeded, Some(content)) => TaskResult::Returned { content },
        (TaskOutcome::Succeeded, None) => TaskResult::EndedWithoutReturn {
            last_message: last_message().await?,
            asked: false,
        },
        (TaskOutcome::Failed, None) if abandoned => TaskResult::EndedWithoutReturn {
            last_message: last_message().await?,
            asked: true,
        },
        (TaskOutcome::Failed, partial) => TaskResult::Failed { partial },
        (TaskOutcome::Cancelled, partial) => TaskResult::Cancelled { partial },
        (TaskOutcome::Unsettled, partial) => TaskResult::Unsettled {
            job_status,
            run_status,
            partial,
        },
    })
}

/// The text a parent receives for one delegated child. `title` is the task's
/// own description; `task_uri` is the child's node, named so a parent handed
/// no result can still read what the child did.
pub(super) fn render_task_result(
    title: &str,
    task_uri: Option<&str>,
    result: &TaskResult,
) -> String {
    match result {
        TaskResult::Returned { content } => content.clone(),
        TaskResult::EndedWithoutReturn {
            last_message,
            asked,
        } => {
            let mut text = format!(
                "Task '{title}' ended without returning a result ({}). Its last message was: {}.",
                if *asked { "asked once" } else { "not asked" },
                last_message.as_deref().unwrap_or("none"),
            );
            if let Some(uri) = task_uri {
                text.push_str(&format!(" Read it at {uri}."));
            }
            text
        }
        TaskResult::Failed { partial } => failure_text(
            title,
            "failed",
            "The agent encountered an error.",
            partial.as_deref(),
        ),
        TaskResult::Cancelled { partial } => failure_text(
            title,
            "was cancelled",
            "The task was stopped before it finished.",
            partial.as_deref(),
        ),
        // Report what an unsettled child wrote — it is real — but never let it
        // read as the task's final answer.
        TaskResult::Unsettled {
            job_status,
            run_status,
            partial,
        } => {
            let statuses = format!(
                "job: {}, run: {}",
                job_status.as_deref().unwrap_or("unknown"),
                run_status.as_deref().unwrap_or("unknown"),
            );
            match partial {
                Some(content) => format!(
                    "Task '{title}' has not settled yet ({statuses}). What it has written so far:\n\n{content}"
                ),
                None => format!("Task '{title}' has not settled ({statuses})."),
            }
        }
    }
}

/// A delegated child's inline result, rendered by [`render_task_result`], with
/// the address of its artifact.
pub(super) async fn build_task_callback_response(
    db: &LocalDb,
    job_id: &str,
    title: &str,
    artifact_name: &str,
) -> CallbackResponse {
    let result = match task_result(db, job_id, artifact_name).await {
        Ok(result) => result,
        Err(error) => {
            return CallbackResponse {
                result: format!("Cannot read result for task '{title}': {error}"),
                ..Default::default()
            };
        }
    };
    let home = crate::jobs::queries::home_uri_for_job(db, job_id)
        .await
        .ok()
        .flatten();
    CallbackResponse {
        result: render_task_result(title, home.as_deref(), &result),
        artifact_uri: home.map(|home| format!("{home}/artifact")),
        ..Default::default()
    }
}

/// A failure report that still carries whatever the child managed to produce.
fn failure_text(title: &str, verb: &str, detail: &str, artifact: Option<&str>) -> String {
    let mut text = format!("Task '{title}' {verb}.\n\n{detail}");
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

async fn latest_nonempty_assistant_content(
    db: &LocalDb,
    run_id: &str,
) -> Result<Option<String>, HistoryError> {
    let page = HistoryReader::new(db)
        .run_events_window(
            run_id,
            i64::MAX,
            i64::MIN,
            20,
            EventFilter::types(&["assistant"]),
        )
        .await?;
    Ok(page
        .rows
        .into_iter()
        .find_map(|row| parse_nonempty_assistant_content(&row.event.data)))
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
    fn a_crashed_run_answers_when_the_job_says_nothing_terminal() {
        assert_eq!(
            classify_task_outcome(Some("running"), Some("crashed")),
            TaskOutcome::Failed
        );
    }

    /// A clean exit is not success. A task that ended its turn without
    /// `return` exits cleanly with its job `blocked` on the missing result;
    /// reading that exit as success handed its parent the task's last message
    /// as though it were the answer (cairn/5783).
    #[test]
    fn a_clean_run_exit_says_nothing_about_the_contract() {
        assert_eq!(
            classify_task_outcome(Some("blocked"), Some("exited")),
            TaskOutcome::Unsettled
        );
        assert_eq!(
            classify_task_outcome(None, Some("exited")),
            TaskOutcome::Unsettled
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
    use super::{
        build_task_callback_response, latest_nonempty_assistant_content, render_task_result,
        task_result, TaskResult,
    };
    use crate::storage::LocalDb;

    const TASK_URI: &str = "cairn://p/cairn/7/1/builder/task/probe";

    async fn respond(db: &LocalDb) -> cairn_common::protocol::CallbackResponse {
        build_task_callback_response(db, "j-task", "explore it", "return").await
    }

    /// What the child last said, on its run.
    async fn say(db: &LocalDb, words: &str) {
        db.execute(
            "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES ('said', 'r-task', 1, 1, 'assistant', ?1, 1)",
            (
                serde_json::json!({"eventType": "assistant", "content": words, "isError": false})
                    .to_string(),
            ),
        )
        .await
        .unwrap();
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
        let response = respond(&db).await;
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
        let response = respond(&db).await;
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
        let response = respond(&db).await;
        assert!(
            response.result.contains("job: running") && response.result.contains("run: live"),
            "{}",
            response.result
        );
    }

    /// A child cancelled before it ever had a run still renders, from its job.
    #[tokio::test]
    async fn a_child_with_no_run_renders_from_its_job() {
        let db = warm_completed_child().await;
        db.execute_script(
            "DELETE FROM runs WHERE job_id = 'j-task';
             DELETE FROM artifacts WHERE job_id = 'j-task';
             UPDATE jobs SET status = 'cancelled' WHERE id = 'j-task';",
        )
        .await
        .unwrap();
        let response = respond(&db).await;
        assert!(
            response.result.contains("was cancelled"),
            "{}",
            response.result
        );
        assert!(super::task_has_settled(&db, "j-task").await);

        db.execute("UPDATE jobs SET status = 'pending' WHERE id = 'j-task'", ())
            .await
            .unwrap();
        assert!(
            !super::task_has_settled(&db, "j-task").await,
            "a created, unstarted child has not settled"
        );
    }

    /// A child that completed without writing its result is reported as having
    /// ended without one, never as the bare text it last said. This is the
    /// fallback the inline path used to render as the answer.
    #[tokio::test]
    async fn a_complete_child_with_no_result_ended_without_return() {
        let db = warm_completed_child().await;
        db.execute("DELETE FROM artifacts WHERE job_id = 'j-task'", ())
            .await
            .unwrap();
        say(&db, "Let me check X").await;
        assert_eq!(
            task_result(&db, "j-task", "return").await.unwrap(),
            TaskResult::EndedWithoutReturn {
                last_message: Some("Let me check X".to_string()),
                asked: false,
            }
        );
        let response = respond(&db).await;
        assert_eq!(
            response.result,
            format!(
                "Task 'explore it' ended without returning a result (not asked). Its last message \
                 was: Let me check X. Read it at {TASK_URI}."
            )
        );
    }

    /// A child given up on after its one re-prompt fails with text that says
    /// it was asked, what it last said, and where to read it.
    #[tokio::test]
    async fn an_abandoned_child_says_it_was_asked_once() {
        let db = warm_completed_child().await;
        db.execute_script(
            "DELETE FROM artifacts WHERE job_id = 'j-task';
             UPDATE jobs SET status = 'failed', return_abandoned_at = 5 WHERE id = 'j-task';
             UPDATE runs SET status = 'exited' WHERE id = 'r-task';",
        )
        .await
        .unwrap();
        say(&db, "Still looking").await;
        let response = respond(&db).await;
        assert_eq!(
            response.result,
            format!(
                "Task 'explore it' ended without returning a result (asked once). Its last message \
                 was: Still looking. Read it at {TASK_URI}."
            )
        );
        assert_eq!(
            response.artifact_uri.as_deref(),
            Some("cairn://p/cairn/7/1/builder/task/probe/artifact")
        );
    }

    async fn fallback_child() -> LocalDb {
        let db = warm_completed_child().await;
        db.execute_script(
            "DELETE FROM artifacts WHERE job_id = 'j-task';
             INSERT INTO executions(id, recipe_id, project_id, issue_id, status, started_at)
               VALUES ('exec-task', 'recipe', 'p1', 'i1', 'complete', 1);
             UPDATE jobs SET execution_id = 'exec-task' WHERE id = 'j-task';",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn delegated_last_assistant_keeps_main_one_statement_one_checkout() {
        use crate::storage::history_pack::testing::publish_for_test;
        let db = fallback_child().await;
        say(&db, "last assistant").await;
        for cataloged in [false, true] {
            if cataloged {
                publish_for_test(&db, "exec-task").await;
            }
            let before = (db.read_statement_count(), db.read_checkout_count());
            assert_eq!(
                latest_nonempty_assistant_content(&db, "r-task")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("last assistant")
            );
            assert_eq!(
                (
                    db.read_statement_count() - before.0,
                    db.read_checkout_count() - before.1
                ),
                (1, 1)
            );
        }
    }

    #[tokio::test]
    async fn fallback_keeps_only_the_last_twenty_assistant_rows_across_sources() {
        use crate::storage::events::{compress, CODEC_ZSTD_V1};
        use crate::storage::history_pack::testing::{evict_all, publish_for_test, read_both};

        for boundary in [Some("the twentieth assistant"), None] {
            let db = fallback_child().await;
            for sequence in 1..=52 {
                let event_type = if sequence <= 21 {
                    "assistant"
                } else {
                    "system:status"
                };
                let content = match sequence {
                    1 => "outside the window",
                    2 => boundary.unwrap_or(" "),
                    3..=21 => " ",
                    _ => "not an assistant",
                };
                let body = serde_json::json!({
                    "eventType": event_type, "content": content, "isError": false
                })
                .to_string();
                let (data, blob, storage_mode, codec) = if sequence == 2 {
                    (
                        "{\"_archived\":true}".to_string(),
                        Some(compress(body.as_bytes()).unwrap()),
                        Some("zstd"),
                        Some(CODEC_ZSTD_V1),
                    )
                } else {
                    (
                        if sequence == 21 {
                            // The event index requires JSON, but the transcript parser
                            // rejects a non-string content field.
                            serde_json::json!({"eventType": "assistant", "content": 42, "isError": false})
                                .to_string()
                        } else {
                            body
                        },
                        None,
                        None,
                        None,
                    )
                };
                db.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data,
                                        created_at, storage_mode, data_blob, codec)
                     VALUES (?1, 'r-task', ?2, ?3, ?4, ?5, ?3, ?6, ?7, ?8)",
                    cairn_db::turso::params![
                        format!("window-{sequence}"),
                        sequence,
                        100 - sequence,
                        event_type,
                        data,
                        storage_mode,
                        blob,
                        codec
                    ],
                )
                .await
                .unwrap();
            }
            publish_for_test(&db, "exec-task").await;
            let (hot, pack) = read_both(&db, || async {
                task_result(&db, "j-task", "return").await.unwrap()
            })
            .await;
            let expected = TaskResult::EndedWithoutReturn {
                last_message: boundary.map(str::to_string),
                asked: false,
            };
            assert_eq!(hot, expected);
            assert_eq!(pack, expected);
            evict_all(&db).await;
            assert_eq!(
                task_result(&db, "j-task", "return").await.unwrap(),
                expected
            );
        }
    }

    struct Quarantined(std::path::PathBuf);

    impl Drop for Quarantined {
        fn drop(&mut self) {
            crate::storage::quarantine::quarantine().install_for(
                &self.0,
                crate::storage::quarantine::QuarantineSet::default(),
            );
        }
    }

    #[tokio::test]
    async fn fallback_regates_quarantine_after_packing() {
        use crate::storage::history_pack::testing::{publish_for_test, read_both};
        use crate::storage::quarantine::{
            quarantine, QuarantineSet, TRANSCRIPT_EVENT_SINK, WITHHELD_PREFIX,
        };

        let db = fallback_child().await;
        say(&db, "must not escape").await;
        let event_id = format!("{}-said", db.path().display());
        db.execute(
            "UPDATE events SET id = ?1 WHERE id = 'said'",
            (event_id.clone(),),
        )
        .await
        .unwrap();
        publish_for_test(&db, "exec-task").await;
        let source = db.path().with_extension("quarantine-test");
        quarantine().install_for(
            &source,
            QuarantineSet::from_entries([(
                TRANSCRIPT_EVENT_SINK.to_string(),
                event_id,
                "delegated-fallback".to_string(),
            )]),
        );
        let _guard = Quarantined(source);
        let (hot, pack) = read_both(&db, || async {
            task_result(&db, "j-task", "return").await.unwrap()
        })
        .await;
        assert_eq!(hot, pack);
        let TaskResult::EndedWithoutReturn {
            last_message: Some(message),
            ..
        } = pack
        else {
            panic!("expected the withheld last-message notice");
        };
        assert!(message.contains(WITHHELD_PREFIX), "{message}");
        assert!(!message.contains("must not escape"), "{message}");
    }

    #[tokio::test]
    async fn fallback_archive_errors_are_not_empty_task_results() {
        use crate::storage::events::history::{HistoryError, HistorySource};
        use crate::storage::history_pack::testing::publish_for_test;

        let db = fallback_child().await;
        say(&db, "hot content must not mask an unavailable pack").await;
        publish_for_test(&db, "exec-task").await;
        db.set_history_source_override(Some(HistorySource::Pack));
        // A lost file is an offline local archive, not a reason to read hot.
        db.execute(
            "UPDATE history_tier SET content_hash = ?1 WHERE scope_id = 'exec-task'",
            ("0".repeat(64),),
        )
        .await
        .unwrap();
        let error = task_result(&db, "j-task", "return").await.unwrap_err();
        assert!(matches!(error, HistoryError::Offline { .. }), "{error}");
        let response = respond(&db).await;
        assert!(
            response.result.starts_with("Cannot read result for task"),
            "{}",
            response.result
        );
        assert!(
            response.result.contains("archive offline"),
            "{}",
            response.result
        );
        assert!(
            !response.result.contains("last message"),
            "{}",
            response.result
        );
        assert!(
            !response.result.contains("hot content"),
            "{}",
            response.result
        );
        assert!(response.artifact_uri.is_none());

        let root = db.path().with_extension("archive");
        std::fs::rename(&root, root.with_extension("unplugged")).unwrap();
        assert!(matches!(
            task_result(&db, "j-task", "return").await.unwrap_err(),
            HistoryError::Offline { .. }
        ));
    }

    /// A child that said nothing is named as having said nothing.
    #[test]
    fn an_unspoken_ending_names_no_last_message() {
        let text = render_task_result(
            "map it",
            None,
            &TaskResult::EndedWithoutReturn {
                last_message: None,
                asked: true,
            },
        );
        assert_eq!(
            text,
            "Task 'map it' ended without returning a result (asked once). Its last message was: none."
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
