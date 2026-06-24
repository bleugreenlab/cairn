//! Memory command helpers.

use crate::models::{CreateMemory, Memory, UpdateMemory};
use crate::storage::{DbError, LocalDb, RowExt};
use turso::params;

pub async fn create(db: &LocalDb, input: CreateMemory) -> Result<Memory, String> {
    let id = uuid::Uuid::new_v4().to_string();

    crate::memories::db::create_memory(
        db,
        &id,
        input.name.as_deref(),
        &input.content,
        input.project_id.as_deref(),
        &input.scope.to_string(),
        &input.scope_value,
        input.job_id.as_deref(),
        input.node_seq,
        input.provenance_uri.as_deref(),
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn update(db: &LocalDb, input: UpdateMemory) -> Result<Memory, String> {
    let has_change = input.content.is_some() || input.status.is_some();

    if has_change {
        let status = input.status.as_ref().map(|status| status.to_string());

        crate::memories::db::update_memory(
            db,
            &input.id,
            input.content.as_deref(),
            status.as_deref(),
        )
        .await
        .map_err(|error| error.to_string())?;
    }

    crate::memories::db::load_memory(db, &input.id)
        .await
        .map_err(|error| error.to_string())
}

pub async fn delete(db: &LocalDb, id: &str) -> Result<(), String> {
    crate::memories::db::delete_memory(db, id)
        .await
        .map_err(|error| error.to_string())
}

/// Confirm the surviving draft memories captured by a completed job and enqueue
/// their content embedding. Confirmation is intentionally tied to job completion:
/// drafts that never reach this hook remain inert drafts.
pub fn confirm_drafts_for_completed_job(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<Vec<Memory>, String> {
    let db = orch.db.local.clone();
    let job_id_owned = job_id.to_string();
    let memories = crate::execution::advancement::run_advancement_db(async move {
        crate::memories::db::confirm_draft_memories_for_job(&db, &job_id_owned)
            .await
            .map_err(|e| e.to_string())
    })?;
    for memory in &memories {
        let db = orch.db.local.clone();
        let memory_for_uri = memory.clone();
        let content = memory.content.clone();
        let uri = crate::execution::advancement::run_advancement_db(async move {
            crate::memories::db::build_node_memory_uri_for_memory(&db, &memory_for_uri)
                .await
                .map_err(|e| e.to_string())
        })?;
        orch.enqueue_resource_embed(&uri, content);
    }
    Ok(memories)
}

#[derive(Debug, Clone)]
pub struct MemoryReviewCompletion {
    pub confirmed_count: usize,
    pub confirmed_scopes: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct MemoryReviewIdleState {
    pub state: Option<String>,
    pub draft_count: usize,
    pub has_artifact: bool,
    /// True when this job is a sub-agent task (it has a `parent_job_id`), as
    /// opposed to a top-level recipe node. Reflection prompts are skipped for
    /// tasks; review prompts still fire for them when they captured drafts.
    pub is_task: bool,
}

/// Queue the one-shot draft-memory review prompt when a job with draft memories
/// first goes idle. The pending direct is delivered by the existing
/// flush-on-idle path immediately after the idle hook returns.
pub fn send_memory_review_on_idle(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
    run_id: &str,
) -> Result<bool, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    let run_id = run_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id.clone();
            let run_id = run_id.clone();
            let prompt_db = db.clone();
            Box::pin(async move {
                let mut job_rows = conn
                    .query(
                        "SELECT memory_review_state,
                                EXISTS (SELECT 1 FROM artifacts WHERE job_id = jobs.id LIMIT 1),
                                parent_job_id IS NOT NULL
                         FROM jobs WHERE id = ?1 LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                let Some(job_row) = job_rows.next().await? else {
                    return Err(DbError::Row(format!("Job not found: {job_id}")));
                };
                let already_reviewed = job_row.opt_text(0)?.is_some();
                let has_artifact = job_row.i64(1)? != 0;
                let is_task = job_row.i64(2)? != 0;
                if already_reviewed || !has_artifact {
                    return Ok(false);
                }

                let sql = format!(
                    "SELECT {} FROM memories WHERE job_id = ?1 AND status = 'draft' ORDER BY node_seq ASC, created_at ASC",
                    crate::memories::db::MEMORY_COLUMNS_FOR_COMMANDS
                );
                let mut rows = conn.query(&sql, params![job_id.as_str()]).await?;
                let mut drafts = Vec::new();
                while let Some(row) = rows.next().await? {
                    drafts.push(crate::memories::db::memory_from_row_for_commands(&row)?);
                }

                // Drafts present -> review them; this fires for every run,
                // sub-agent tasks included. No drafts -> invite a reflection,
                // but only for top-level node jobs that did substantial work.
                // Tasks (parent_job_id set) skip the nudge so every Explore/etc.
                // isn't pushed to memorize, and trivial jobs skip it (too little
                // activity to be worth a reflection) to avoid noisy end-of-job
                // interruptions.
                let content = if drafts.is_empty() {
                    if is_task
                        || !job_activity_exceeds_threshold(
                            conn,
                            &job_id,
                            REFLECTION_ACTIVITY_THRESHOLD,
                        )
                        .await?
                    {
                        return Ok(false);
                    }
                    build_reflection_prompt()
                } else {
                    build_review_prompt(&prompt_db, &drafts).await?
                };

                conn.execute(
                    "UPDATE jobs
                     SET memory_review_state = 'sent', updated_at = ?1
                     WHERE id = ?2 AND memory_review_state IS NULL",
                    params![chrono::Utc::now().timestamp(), job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO messages (
                        id, channel_type, channel_id, sender_run_id, sender_name,
                        recipient_run_id, content, created_at
                     ) VALUES (?1, 'direct', NULL, NULL, 'system', ?2, ?3, ?4)",
                    params![
                        uuid::Uuid::new_v4().to_string().as_str(),
                        run_id.as_str(),
                        content.as_str(),
                        chrono::Utc::now().timestamp(),
                    ],
                )
                .await?;
                Ok(true)
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

async fn build_review_prompt(db: &LocalDb, drafts: &[Memory]) -> Result<String, DbError> {
    let mut prompt = String::from(
        "Before you finish, review the memories you captured during this job. \
Edit any that need fixing, and delete any that are wrong, redundant, too ephemeral, or not durable. \
If you learned something durable you didn't capture \u{2014} a project or workspace fact you had to figure out, a user correction, a constraint or gotcha that would help an agent starting this work cold \u{2014} add it now. \
Use the `write` verb: `mode:\"patch\"` or `mode:\"delete\"` on the URIs below to revise drafts, or `mode:\"append\"` to `cairn:~/memories` to add one (set `scope` to `project`, `role`, or `workspace`). \
Whatever remains after this review turn is saved for triage.\n\nDraft memories:\n",
    );
    for memory in drafts {
        let uri = crate::memories::db::build_node_memory_uri_for_memory(db, memory).await?;
        prompt.push_str(&format!("- `{uri}`\n  Content: {}\n", memory.content));
    }
    Ok(prompt)
}

/// Minimum number of recorded events across a job's runs before the no-drafts
/// reflection nudge is sent. Chosen against the observed data shape: a builder
/// that did real work logs hundreds of events even when it finishes in one or
/// two turns (each tool call records an assistant event plus a result event),
/// while a trivial job logs only a handful. 50 sits well clear of the trivial
/// floor yet far below the heavy-worker band, so substantial jobs are invited
/// to reflect without interrupting near-no-op ones.
const REFLECTION_ACTIVITY_THRESHOLD: i64 = 50;

/// Whether a job did enough work to be worth a reflection nudge, measured by the
/// total event count across every run of the job. Events, not turns, are the
/// unit: the heaviest builders do hundreds of tool calls in only one or two
/// turns, so a turn count systematically under-invites exactly the busiest jobs.
/// Counting across all of the job's runs (rather than one run's session) keeps
/// the measure from being deflated by session rotation over a long job.
async fn job_activity_exceeds_threshold(
    conn: &turso::Connection,
    job_id: &str,
    threshold: i64,
) -> Result<bool, DbError> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*)
             FROM events
             WHERE run_id IN (SELECT id FROM runs WHERE job_id = ?1)",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(false);
    };
    Ok(row.i64(0)? > threshold)
}

fn build_reflection_prompt() -> String {
    String::from(
        "Before you finish, take a moment to reflect: did you learn anything durable during this job that would help an agent doing similar work next time? \
Things worth capturing include a project or workspace fact you had to figure out, a command or path that differs from the docs, a user correction, or a constraint or gotcha that cost you time. \
If so, capture it with the `write` verb, `mode:\"append\"` to `cairn:~/memories`, and set `scope` to `project`, `role`, or `workspace`. \
Keep it to genuinely durable lessons and skip anything specific to just this issue. If nothing comes to mind, that's fine \u{2014} just finish.",
    )
}

pub fn memory_review_idle_state_for_job(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<MemoryReviewIdleState, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT memory_review_state,
                                (SELECT COUNT(*) FROM memories WHERE job_id = jobs.id AND status = 'draft'),
                                EXISTS (SELECT 1 FROM artifacts WHERE job_id = jobs.id LIMIT 1),
                                parent_job_id IS NOT NULL
                         FROM jobs WHERE id = ?1 LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(MemoryReviewIdleState {
                        state: None,
                        draft_count: 0,
                        has_artifact: false,
                        is_task: false,
                    });
                };
                let draft_count = usize::try_from(row.i64(1)?).unwrap_or(0);
                Ok(MemoryReviewIdleState {
                    state: row.opt_text(0)?,
                    draft_count,
                    has_artifact: row.i64(2)? != 0,
                    is_task: row.i64(3)? != 0,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn complete_sent_memory_review(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<MemoryReviewCompletion, String> {
    let confirmed_memories = confirm_drafts_for_completed_job(orch, job_id)?;
    let confirmed_scopes =
        crate::memories::triage::distinct_scopes_from_memories(&confirmed_memories);
    set_memory_review_done(orch, job_id)?;
    Ok(MemoryReviewCompletion {
        confirmed_count: confirmed_memories.len(),
        confirmed_scopes,
    })
}

fn set_memory_review_done(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<(), String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.execute(
            "UPDATE jobs SET memory_review_state = 'done', updated_at = ?1 WHERE id = ?2 AND memory_review_state = 'sent'",
            params![chrono::Utc::now().timestamp(), job_id.as_str()],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
    })
}

pub fn confirm_drafts_for_completed_job_without_artifact_review(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<MemoryReviewCompletion, String> {
    if job_has_artifacts_or_review_state(orch, job_id)? {
        return Ok(MemoryReviewCompletion {
            confirmed_count: 0,
            confirmed_scopes: Vec::new(),
        });
    }
    let confirmed_memories = confirm_drafts_for_completed_job(orch, job_id)?;
    Ok(MemoryReviewCompletion {
        confirmed_count: confirmed_memories.len(),
        confirmed_scopes: crate::memories::triage::distinct_scopes_from_memories(
            &confirmed_memories,
        ),
    })
}

/// Reconciliation step 1: confirm drafts stranded on terminal jobs.
///
/// A draft only reaches `pending` on a clean completion hook or a completed
/// review. A failed job, or a review interrupted by process eviction / app
/// restart, leaves its drafts inert. This sweep advances to `pending` every
/// draft whose owning job is terminal and is not in an in-flight review (a job
/// whose `memory_review_state = 'sent'` and whose review turn has not yet
/// ended), enqueueing content embeds for the confirmed drafts. Returns the
/// total confirmed count.
pub fn confirm_orphaned_drafts(orch: &crate::orchestrator::Orchestrator) -> Result<usize, String> {
    let db = orch.db.local.clone();
    let jobs = crate::execution::advancement::run_advancement_db(async move {
        crate::memories::db::terminal_jobs_with_draft_memories(&db)
            .await
            .map_err(|e| e.to_string())
    })?;
    let mut confirmed = 0usize;
    for (job_id, review_state) in jobs {
        // A 'sent' review whose turn has not ended is still in flight: the agent
        // may still be editing drafts. Skip it; the review-completion hook (or a
        // later sweep once the turn ends) confirms its survivors.
        if review_state.as_deref() == Some("sent")
            && !crate::orchestrator::lifecycle::memory_review_turn_ended(orch, &job_id)
        {
            continue;
        }
        let memories = confirm_drafts_for_completed_job(orch, &job_id)?;
        confirmed += memories.len();
    }
    Ok(confirmed)
}

fn job_has_artifacts_or_review_state(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<bool, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.query_one(
            "SELECT CASE WHEN memory_review_state IS NOT NULL OR EXISTS (SELECT 1 FROM artifacts WHERE job_id = jobs.id LIMIT 1) THEN 1 ELSE 0 END FROM jobs WHERE id = ?1",
            params![job_id.as_str()],
            |row| row.i64(0).map(|value| value != 0),
        )
        .await
        .map_err(|e| e.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::send_memory_review_on_idle;
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;
    use tempfile::TempDir;
    use turso::params;

    struct TestOrch {
        _temp: TempDir,
        orch: Orchestrator,
    }

    async fn test_orch() -> TestOrch {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();

        let local = Arc::new(
            LocalDb::open(temp.path().join("review-test.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/project', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-main', 'project-1', 42, 'Main', 'active', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('exec-main', 'recipe', 'issue-main', 'project-1', 'running', 1, 1)",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db = Arc::new(DbState::new(local, search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(db, services, config_dir).build();
        TestOrch { _temp: temp, orch }
    }

    async fn insert_job(test: &TestOrch, job_id: &str, parent_job_id: Option<&str>) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, node_name, uri_segment, parent_job_id, created_at, updated_at)
                 VALUES (?1, 'exec-main', 'issue-main', 'project-1', 'running', 'builder', 'builder', ?2, 1, 1)",
                params![job_id, parent_job_id],
            )
            .await
            .unwrap();
    }

    async fn insert_run(test: &TestOrch, run_id: &str, job_id: &str, session_id: &str) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES (?1, 'issue-main', 'project-1', ?2, 'running', ?3, 1, 1)",
                params![run_id, job_id, session_id],
            )
            .await
            .unwrap();
    }

    async fn insert_events(test: &TestOrch, run_id: &str, count: i64) {
        for seq in 0..count {
            let id = format!("{run_id}-event-{seq}");
            test.orch
                .db
                .local
                .execute(
                    "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES (?1, ?2, ?3, 1, 'assistant', '{}', 1)",
                    params![id.as_str(), run_id, seq],
                )
                .await
                .unwrap();
        }
    }

    async fn insert_artifact(test: &TestOrch, job_id: &str) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, data, created_at, updated_at)
                 VALUES (?1, ?2, 'create-pr', '{}', 1, 1)",
                params![format!("{job_id}-artifact").as_str(), job_id],
            )
            .await
            .unwrap();
    }

    async fn insert_draft_memory(test: &TestOrch, job_id: &str, id: &str) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, provenance_uri, created_at, updated_at)
                 VALUES (?1, ?1, 'project-1', 'a durable fact', 'draft', 'project', 'project-1', ?2, 1, 'cairn://p/PRJ/42/1/builder/chat/turn/2', 1, 1)",
                params![id, job_id],
            )
            .await
            .unwrap();
    }

    async fn review_state(test: &TestOrch, job_id: &str) -> Option<String> {
        test.orch
            .db
            .local
            .query_one(
                "SELECT memory_review_state FROM jobs WHERE id = ?1",
                params![job_id],
                |row| row.opt_text(0),
            )
            .await
            .unwrap()
    }

    async fn message_to_run(test: &TestOrch, run_id: &str) -> Option<String> {
        test.orch
            .db
            .local
            .query_opt(
                "SELECT content FROM messages WHERE recipient_run_id = ?1 LIMIT 1",
                params![run_id],
                |row| row.text(0),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn heavy_but_few_turns_job_is_nudged_to_reflect() {
        let test = test_orch().await;
        insert_job(&test, "job-heavy", None).await;
        // Two runs (e.g. resumed across a session rotation), each below the
        // threshold alone but well over it combined, proving the count spans
        // the whole job rather than one run's session.
        insert_run(&test, "run-a", "job-heavy", "session-1").await;
        insert_run(&test, "run-b", "job-heavy", "session-2").await;
        insert_events(&test, "run-a", 30).await;
        insert_events(&test, "run-b", 40).await;
        insert_artifact(&test, "job-heavy").await;

        let sent = send_memory_review_on_idle(&test.orch, "job-heavy", "run-b").unwrap();
        assert!(
            sent,
            "a heavy job with no drafts should be nudged to reflect"
        );
        assert_eq!(
            review_state(&test, "job-heavy").await.as_deref(),
            Some("sent")
        );
        let content = message_to_run(&test, "run-b").await.expect("a message");
        assert!(
            content.contains("take a moment to reflect"),
            "expected the reflection prompt, got: {content}"
        );
    }

    #[tokio::test]
    async fn trivial_job_is_not_nudged() {
        let test = test_orch().await;
        insert_job(&test, "job-trivial", None).await;
        insert_run(&test, "run-t", "job-trivial", "session-1").await;
        insert_events(&test, "run-t", 5).await;
        insert_artifact(&test, "job-trivial").await;

        let sent = send_memory_review_on_idle(&test.orch, "job-trivial", "run-t").unwrap();
        assert!(!sent, "a trivial job should not be nudged");
        assert_eq!(review_state(&test, "job-trivial").await, None);
        assert_eq!(message_to_run(&test, "run-t").await, None);
    }

    #[tokio::test]
    async fn task_skips_reflection_nudge_even_when_heavy() {
        let test = test_orch().await;
        insert_job(&test, "job-parent", None).await;
        insert_job(&test, "job-task", Some("job-parent")).await;
        insert_run(&test, "run-task", "job-task", "session-1").await;
        insert_events(&test, "run-task", 80).await;
        insert_artifact(&test, "job-task").await;

        let sent = send_memory_review_on_idle(&test.orch, "job-task", "run-task").unwrap();
        assert!(
            !sent,
            "a sub-agent task should never get the reflection nudge"
        );
        assert_eq!(review_state(&test, "job-task").await, None);
        assert_eq!(message_to_run(&test, "run-task").await, None);
    }

    #[tokio::test]
    async fn drafts_present_are_always_reviewed_regardless_of_activity() {
        let test = test_orch().await;
        insert_job(&test, "job-drafts", None).await;
        insert_run(&test, "run-d", "job-drafts", "session-1").await;
        // Far below the activity threshold: the drafts-present path must fire
        // unconditionally, ignoring the activity gate entirely.
        insert_events(&test, "run-d", 3).await;
        insert_artifact(&test, "job-drafts").await;
        insert_draft_memory(&test, "job-drafts", "mem-1").await;

        let sent = send_memory_review_on_idle(&test.orch, "job-drafts", "run-d").unwrap();
        assert!(sent, "a job with draft memories must always be reviewed");
        assert_eq!(
            review_state(&test, "job-drafts").await.as_deref(),
            Some("sent")
        );
        let content = message_to_run(&test, "run-d").await.expect("a message");
        assert!(
            content.contains("review the memories you captured"),
            "expected the draft-review prompt, got: {content}"
        );
    }
}
