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
                // but only for top-level node jobs after a meaningfully long
                // session. Tasks (parent_job_id set) skip the nudge so every
                // Explore/etc. isn't pushed to memorize, and short sessions skip
                // it to avoid noisy end-of-job interruptions.
                let content = if drafts.is_empty() {
                    if is_task || !run_session_exceeds_turn_threshold(conn, &run_id, 10).await? {
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

async fn run_session_exceeds_turn_threshold(
    conn: &turso::Connection,
    run_id: &str,
    threshold: i64,
) -> Result<bool, DbError> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*)
             FROM turns
             WHERE session_id = (SELECT session_id FROM runs WHERE id = ?1)",
            params![run_id],
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
