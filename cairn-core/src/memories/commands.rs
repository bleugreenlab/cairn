//! Memory command helpers.

use std::sync::Arc;

use crate::execution::routing::owning_db_for_job;
use crate::models::{CreateMemory, Memory, UpdateMemory};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;

pub async fn create(db: &LocalDb, input: CreateMemory) -> Result<Memory, String> {
    let id = ids::mint_child(
        input
            .project_id
            .as_deref()
            .or(input.job_id.as_deref())
            .unwrap_or(""),
    );

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

pub(crate) async fn update(db: &LocalDb, input: UpdateMemory) -> Result<Memory, String> {
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
fn confirm_drafts_for_completed_job(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<Vec<Memory>, String> {
    let db = resolve_owning_db_for_job_sync(orch, job_id)?;
    confirm_drafts_for_completed_job_in_db(orch, db, job_id)
}

/// Confirm a merged issue's surviving draft memories, then spawn triage issues
/// for any pool that reaches threshold.
///
/// Both halves are team-safe. The confirm half routes to the issue's owning
/// database (the synced replica for a team issue, below). The spawn half
/// delegates to [`crate::memories::triage::maybe_spawn_triage`], whose
/// PROJECT-scope pools self-route to their owning database by `scope_value` (a
/// routable `project_id`), so a team merge's project-scoped drafts both confirm
/// to `pending` AND spawn a triage issue in the same replica (CAIRN-2587). ROLE
/// and WORKSPACE pools are cross-project, cross-database aggregates and remain
/// PRIVATE-DB by explicit contract pending a team-orchestration decision.
pub(crate) async fn confirm_and_spawn_drafts_for_merged_issue(
    orch: crate::orchestrator::Orchestrator,
    issue_id: &str,
) -> Result<Vec<String>, String> {
    let issue_id = issue_id.to_string();
    // Route to the database that OWNS this issue: a merged TEAM issue's draft
    // memories (and the jobs that captured them) live wholly in that team's
    // synced replica, so reading `orch.db.local` would confirm zero drafts.
    // Fail-closed — a team issue whose replica is not open errors rather than
    // silently reading the empty private DB (the CAIRN-2170 split-brain class);
    // a bare local id short-circuits to the private DB, a strict no-op. The
    // per-job confirmation below re-routes by job id (the completed-job path).
    let db = crate::issues::crud::owning_db_for_issue(&orch.db, &issue_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_ids = crate::memories::db::draft_memory_job_ids_for_issue(&db, &issue_id)
        .await
        .map_err(|error| error.to_string())?;
    let mut confirmed_memories = Vec::new();
    for job_id in job_ids {
        confirmed_memories.extend(confirm_drafts_for_completed_job(&orch, &job_id)?);
    }
    let confirmed_scopes =
        crate::memories::triage::distinct_scopes_from_memories(&confirmed_memories);
    crate::memories::triage::maybe_spawn_triage(orch, confirmed_scopes).await
}

fn confirm_drafts_for_completed_job_in_db(
    orch: &crate::orchestrator::Orchestrator,
    db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Vec<Memory>, String> {
    let job_id_owned = job_id.to_string();
    let memories = crate::execution::advancement::run_advancement_db({
        let db = db.clone();
        async move {
            crate::memories::db::confirm_draft_memories_for_job(&db, &job_id_owned)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    for memory in &memories {
        let memory_for_uri = memory.clone();
        let content = memory.content.clone();
        let uri = crate::execution::advancement::run_advancement_db({
            let db = db.clone();
            async move {
                crate::memories::db::build_node_memory_uri_for_memory(&db, &memory_for_uri)
                    .await
                    .map_err(|e| e.to_string())
            }
        })?;
        orch.enqueue_resource_embed(&uri, content);
    }
    Ok(memories)
}

fn resolve_owning_db_for_job_sync(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<Arc<LocalDb>, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())
    })
}

#[derive(Debug, Clone)]
pub struct MemoryReviewCompletion {
    pub(crate) confirmed_count: usize,
    pub(crate) confirmed_scopes: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct MemoryReviewIdleState {
    pub(crate) state: Option<String>,
    pub(crate) draft_count: usize,
    pub(crate) has_output_artifact: bool,
    /// True when this job is a sub-agent task (it has a `parent_job_id`), as
    /// opposed to a top-level recipe node. Reflection prompts are skipped for
    /// tasks; review prompts still fire for them when they captured drafts.
    pub(crate) is_task: bool,
}

/// Queue the one-shot draft-memory review prompt when a job with draft memories
/// first goes idle. The pending direct is delivered by the existing
/// flush-on-idle path immediately after the idle hook returns.
pub(crate) fn send_memory_review_on_idle(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
    run_id: &str,
) -> Result<bool, String> {
    if !orch.get_settings().memory_review_enabled {
        return Ok(false);
    }
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    let run_id = run_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id.clone();
            let run_id = run_id.clone();
            let prompt_db = db.clone();
            Box::pin(async move {
                let gate = memory_review_idle_state_conn(conn, &job_id)
                    .await?
                    .ok_or_else(|| DbError::Row(format!("Job not found: {job_id}")))?;
                let already_reviewed = gate.state.is_some();
                let is_task = gate.is_task;
                if already_reviewed || !gate.has_output_artifact {
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

                let updated = conn.execute(
                    "UPDATE jobs
                     SET memory_review_state = 'sent', updated_at = ?1
                     WHERE id = ?2 AND memory_review_state IS NULL",
                    params![chrono::Utc::now().timestamp(), job_id.as_str()],
                )
                .await?;
                if updated == 0 {
                    return Ok(false);
                }
                crate::messages::delivery::insert_system_direct_push_conn(
                    conn,
                    &job_id,
                    &run_id,
                    &content,
                    crate::messages::queued::DeliveryUrgency::Queue,
                )
                .await?;
                Ok(true)
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

#[derive(Debug, Clone)]
struct StrandedMemoryReview {
    job_id: String,
    message_id: String,
}

fn memory_review_turn_count_for_job(
    db: std::sync::Arc<LocalDb>,
    job_id: &str,
) -> Result<i64, String> {
    let job_id = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.query_one(
            "SELECT COUNT(*) FROM turns WHERE job_id = ?1 AND start_reason = 'memory_review'",
            (job_id,),
            |row| row.i64(0),
        )
        .await
        .map_err(|e| e.to_string())
    })
}

fn latest_memory_review_direct_message_id(
    db: std::sync::Arc<LocalDb>,
    job_id: &str,
) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.query_opt_text(
            "SELECT m.id
             FROM messages m
             JOIN runs r ON r.id = m.recipient_run_id
             WHERE r.job_id = ?1
               AND m.channel_type = 'direct'
               AND m.sender_name = 'system'
               AND (
                   m.content LIKE 'Before you finish, review the memories%'
                   OR m.content LIKE 'Before you finish, take a moment to reflect:%'
               )
             ORDER BY m.created_at DESC
             LIMIT 1",
            (job_id,),
        )
        .await
        .map_err(|e| e.to_string())
    })
}

/// Reconcile Memory Reviews that were marked `sent` before direct delivery moved
/// to attention pushes. Conservative gates keep user-authored follow-ups and live
/// turns in front: this only nudges idle jobs with an open current session, no
/// queued user message, no active turn, and no existing memory-review turn.
pub(crate) fn reconcile_stranded_memory_reviews(
    orch: crate::orchestrator::Orchestrator,
) -> Result<usize, String> {
    if !orch.get_settings().memory_review_enabled {
        return Ok(0);
    }
    let db = orch.db.local.clone();
    let eligible_jobs = crate::execution::advancement::run_advancement_db(async move {
        db.query_all(
            "SELECT j.id
             FROM jobs j
             JOIN sessions s ON s.id = j.current_session_id AND s.status = 'open'
             WHERE j.memory_review_state = 'sent'
               AND NOT EXISTS (
                   SELECT 1 FROM turns t
                   WHERE t.job_id = j.id AND t.start_reason = 'memory_review'
                   LIMIT 1
               )
               AND NOT EXISTS (
                   SELECT 1 FROM queued_messages q
                   WHERE q.job_id = j.id AND q.delivered_at IS NULL
                   LIMIT 1
               )
               AND NOT EXISTS (
                   SELECT 1 FROM turns live
                   WHERE live.job_id = j.id AND live.state IN ('pending', 'running', 'yielded')
                   LIMIT 1
               )
             ORDER BY j.updated_at ASC",
            (),
            |row| row.text(0),
        )
        .await
        .map_err(|e| e.to_string())
    })?;

    let mut stranded = Vec::new();
    for job_id in eligible_jobs {
        if let Some(message_id) =
            latest_memory_review_direct_message_id(orch.db.local.clone(), &job_id)?
        {
            stranded.push(StrandedMemoryReview { job_id, message_id });
        }
    }

    let mut resumed = 0;
    for review in stranded {
        if let Err(error) = crate::messages::delivery::enqueue_direct_push(
            &orch,
            &review.job_id,
            &review.message_id,
            crate::messages::queued::DeliveryUrgency::Queue,
        ) {
            log::warn!(
                "memory review reconcile: failed to enqueue direct push for job {}: {}",
                review.job_id,
                error
            );
            continue;
        }

        let before = memory_review_turn_count_for_job(orch.db.local.clone(), &review.job_id)?;
        match crate::execution::jobs::continue_job_impl(&orch, &review.job_id, None, None, None) {
            Ok(_) => resumed += 1,
            Err(error) => {
                let after =
                    memory_review_turn_count_for_job(orch.db.local.clone(), &review.job_id)?;
                if after > before {
                    resumed += 1;
                } else {
                    log::warn!(
                        "memory review reconcile: failed to resume job {}: {}",
                        review.job_id,
                        error
                    );
                }
            }
        }
    }
    Ok(resumed)
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
    conn: &cairn_db::turso::Connection,
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

pub(crate) fn memory_review_idle_state_for_job(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<MemoryReviewIdleState, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                memory_review_idle_state_conn(conn, &job_id)
                    .await
                    .map(|state| {
                        state.unwrap_or(MemoryReviewIdleState {
                            state: None,
                            draft_count: 0,
                            has_output_artifact: false,
                            is_task: false,
                        })
                    })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

async fn memory_review_idle_state_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<Option<MemoryReviewIdleState>, DbError> {
    let mut rows = conn
        .query(
            "SELECT memory_review_state,
                    (SELECT COUNT(*) FROM memories WHERE job_id = jobs.id AND status = 'draft'),
                    parent_job_id,
                    recipe_node_id,
                    execution_id
             FROM jobs WHERE id = ?1 LIMIT 1",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };

    let draft_count = usize::try_from(row.i64(1)?).unwrap_or(0);
    let parent_job_id = row.opt_text(2)?;
    let recipe_node_id = row.opt_text(3)?;
    let execution_id = row.opt_text(4)?;
    let terminal_name = terminal_output_artifact_name_conn(
        conn,
        parent_job_id.as_deref(),
        recipe_node_id.as_deref(),
        execution_id.as_deref(),
    )
    .await?;

    Ok(Some(MemoryReviewIdleState {
        state: row.opt_text(0)?,
        draft_count,
        has_output_artifact: output_artifact_present_conn(conn, job_id, terminal_name.as_deref())
            .await?,
        is_task: parent_job_id.is_some(),
    }))
}

async fn terminal_output_artifact_name_conn(
    conn: &cairn_db::turso::Connection,
    parent_job_id: Option<&str>,
    recipe_node_id: Option<&str>,
    execution_id: Option<&str>,
) -> Result<Option<String>, DbError> {
    if let (Some(node_id), Some(execution_id)) = (recipe_node_id, execution_id) {
        return match crate::execution::jobs::find_downstream_artifact_schema_conn(
            conn,
            node_id,
            execution_id,
        )
        .await
        {
            Ok(info) => Ok(info.and_then(|info| info.artifact_name)),
            Err(DbError::Row(message)) if message.starts_with("Execution has no snapshot:") => {
                Ok(None)
            }
            Err(error) => Err(error),
        };
    }

    // Delegated sub-agent tasks do not have a recipe node, but their terminal
    // artifact is still addressed and stored under the task return contract.
    if parent_job_id.is_some() {
        return Ok(Some("result".to_string()));
    }

    Ok(None)
}

async fn output_artifact_present_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    terminal_name: Option<&str>,
) -> Result<bool, DbError> {
    let mut rows = conn
        .query(
            "SELECT 1
             FROM artifacts
             WHERE job_id = ?1
               AND (
                   (?2 IS NOT NULL AND output_name = ?2)
                   OR artifact_type = 'create-pr'
                   OR (artifact_type = 'plan' AND confirmed = 0)
               )
             LIMIT 1",
            params![job_id, terminal_name],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

pub(crate) fn complete_sent_memory_review(
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
    let db = resolve_owning_db_for_job_sync(orch, job_id)?;
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

pub(crate) fn confirm_drafts_for_completed_job_without_artifact_review(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
) -> Result<MemoryReviewCompletion, String> {
    let db = match resolve_owning_db_for_job_sync(orch, job_id) {
        Ok(db) => db,
        Err(error) => {
            log::warn!(
                "draft-memory safety net: failed to route completed job {job_id}: {error}; skipping"
            );
            return Ok(MemoryReviewCompletion {
                confirmed_count: 0,
                confirmed_scopes: Vec::new(),
            });
        }
    };
    if job_has_artifacts_or_review_state(db.clone(), job_id)? {
        return Ok(MemoryReviewCompletion {
            confirmed_count: 0,
            confirmed_scopes: Vec::new(),
        });
    }
    let confirmed_memories = confirm_drafts_for_completed_job_in_db(orch, db, job_id)?;
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
pub(crate) fn confirm_orphaned_drafts(
    orch: &crate::orchestrator::Orchestrator,
) -> Result<usize, String> {
    // Discovery spans EVERY open database: a team run's stranded terminal-job
    // drafts live in that team's synced replica, so scanning only the private DB
    // would never confirm them (the CAIRN-2587 team-triage gap). The per-job
    // `confirm_drafts_for_completed_job` below re-routes by job id, so each
    // confirm lands in the job's own owning database.
    let dbs = orch.db.clone();
    let jobs = crate::execution::advancement::run_advancement_db(async move {
        let mut all = Vec::new();
        for db in dbs.all_dbs().await {
            all.extend(
                crate::memories::db::terminal_jobs_with_draft_memories(&db)
                    .await
                    .map_err(|e| e.to_string())?,
            );
        }
        Ok::<_, String>(all)
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

fn job_has_artifacts_or_review_state(db: Arc<LocalDb>, job_id: &str) -> Result<bool, String> {
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
    use cairn_db::turso::params;
    use std::sync::Arc;
    use tempfile::TempDir;

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
                    let snapshot = serde_json::json!({
                        "recipe": {
                            "id": "recipe",
                            "name": "Recipe",
                            "description": null,
                            "trigger": "manual",
                            "nodes": [
                                {
                                    "id": "agent",
                                    "nodeType": "agent",
                                    "name": "builder",
                                    "position": { "x": 0.0, "y": 0.0 },
                                    "agentConfig": { "agentConfigId": null }
                                },
                                {
                                    "id": "output",
                                    "nodeType": "artifact",
                                    "name": "create-pr",
                                    "position": { "x": 1.0, "y": 0.0 },
                                    "artifactConfig": {
                                        "name": "create-pr",
                                        "schema": { "type": "object" },
                                        "confirmPolicy": "auto"
                                    }
                                },
                                {
                                    "id": "board",
                                    "nodeType": "artifact",
                                    "name": "board",
                                    "position": { "x": 0.0, "y": 1.0 },
                                    "artifactConfig": {
                                        "name": "board",
                                        "schema": { "type": "object" },
                                        "confirmPolicy": "auto"
                                    }
                                }
                            ],
                            "edges": [
                                {
                                    "id": "agent-output",
                                    "edgeType": "context",
                                    "sourceNodeId": "agent",
                                    "sourceHandle": "context-out",
                                    "targetNodeId": "output",
                                    "targetHandle": "context-in"
                                },
                                {
                                    "id": "agent-board",
                                    "edgeType": "context",
                                    "sourceNodeId": "agent",
                                    "sourceHandle": "context-self",
                                    "targetNodeId": "board",
                                    "targetHandle": "context-in"
                                }
                            ]
                        },
                        "agents": {},
                        "skills": {},
                        "triggerContext": {
                            "issueId": "issue-main",
                            "projectId": "project-1",
                            "triggerType": "manual"
                        },
                        "delegatedPackets": [],
                        "createdAt": 1
                    })
                    .to_string();
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('exec-main', 'recipe', 'issue-main', 'project-1', 'running', 1, 1, ?1)",
                        params![snapshot.as_str()],
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
        let recipe_node_id = parent_job_id.is_none().then_some("agent");
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, node_name, uri_segment, parent_job_id, created_at, updated_at)
                 VALUES (?1, 'exec-main', ?3, 'issue-main', 'project-1', 'running', 'builder', 'builder', ?2, 1, 1)",
                params![job_id, parent_job_id, recipe_node_id],
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
        insert_named_artifact(test, job_id, "create-pr").await;
    }

    async fn insert_named_artifact(test: &TestOrch, job_id: &str, output_name: &str) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, output_name, data, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?3, '{}', 1, 1)",
                params![
                    format!("{job_id}-{output_name}-artifact").as_str(),
                    job_id,
                    output_name
                ],
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

    async fn migrated_team_db(temp: &TempDir) -> Arc<LocalDb> {
        let db = Arc::new(
            LocalDb::open(temp.path().join("team-memory-test.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_team_memory_job(
        db: &LocalDb,
        team_id: &str,
        job_id: &str,
        memory_id: &str,
        review_state: Option<&str>,
    ) {
        let project_id = format!("{team_id}~00000000-0000-4000-8000-100000000001");
        let issue_id = format!("{team_id}~00000000-0000-4000-8000-100000000002");
        let execution_id = format!("{team_id}~00000000-0000-4000-8000-100000000003");
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES (?1, 'default', 'Team Project', 'TP', '/tmp/team-project', 1, 1)",
            params![project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, merged_at, created_at, updated_at) VALUES (?1, ?2, 7, 'Team Issue', 'merged', 2, 1, 2)",
            params![issue_id.as_str(), project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES (?1, 'recipe', ?2, ?3, 'running', 1, 1)",
            params![execution_id.as_str(), issue_id.as_str(), project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, node_name, uri_segment, memory_review_state, created_at, updated_at)
             VALUES (?1, ?2, 'agent', ?3, ?4, 'complete', 'builder', 'builder', ?5, 1, 1)",
            params![job_id, execution_id.as_str(), issue_id.as_str(), project_id.as_str(), review_state],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, provenance_uri, created_at, updated_at)
             VALUES (?1, ?1, ?2, 'a durable team fact', 'draft', 'project', ?2, ?3, 1, 'cairn://p/TP/7/1/builder/chat/turn/2', 1, 1)",
            params![memory_id, project_id.as_str(), job_id],
        )
        .await
        .unwrap();
    }

    async fn memory_status(db: &LocalDb, memory_id: &str) -> Option<String> {
        db.query_opt(
            "SELECT status FROM memories WHERE id = ?1",
            params![memory_id],
            |row| row.text(0),
        )
        .await
        .unwrap()
    }

    async fn job_review_state(db: &LocalDb, job_id: &str) -> Option<String> {
        db.query_one(
            "SELECT memory_review_state FROM jobs WHERE id = ?1",
            params![job_id],
            |row| row.opt_text(0),
        )
        .await
        .unwrap()
    }

    async fn table_count_by_id(db: &LocalDb, table: &str, id: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE id = ?1");
        db.query_one(&sql, params![id], |row| row.i64(0))
            .await
            .unwrap()
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
    async fn team_no_artifact_safety_net_confirms_drafts_in_team_replica() {
        let test = test_orch().await;
        let team_temp = tempfile::tempdir().unwrap();
        let team_id = "teammem";
        let job_id = "teammem~00000000-0000-4000-8000-100000000010";
        let memory_id = "teammem~00000000-0000-4000-8000-100000000011";
        let team_db = migrated_team_db(&team_temp).await;
        seed_team_memory_job(&team_db, team_id, job_id, memory_id, None).await;
        test.orch
            .db
            .insert_team_db_for_test(team_id, team_db.clone())
            .await;

        let completion =
            super::confirm_drafts_for_completed_job_without_artifact_review(&test.orch, job_id)
                .unwrap();

        assert_eq!(completion.confirmed_count, 1);
        assert_eq!(
            memory_status(&team_db, memory_id).await.as_deref(),
            Some("pending")
        );
        assert_eq!(
            table_count_by_id(&test.orch.db.local, "memories", memory_id).await,
            0,
            "the team draft must not be read from or written into the private database"
        );
    }

    #[tokio::test]
    async fn team_sent_memory_review_completion_marks_team_job_done() {
        let test = test_orch().await;
        let team_temp = tempfile::tempdir().unwrap();
        let team_id = "teamdone";
        let job_id = "teamdone~00000000-0000-4000-8000-100000000010";
        let memory_id = "teamdone~00000000-0000-4000-8000-100000000011";
        let team_db = migrated_team_db(&team_temp).await;
        seed_team_memory_job(&team_db, team_id, job_id, memory_id, Some("sent")).await;
        test.orch
            .db
            .insert_team_db_for_test(team_id, team_db.clone())
            .await;

        let completion = super::complete_sent_memory_review(&test.orch, job_id).unwrap();

        assert_eq!(completion.confirmed_count, 1);
        assert_eq!(
            memory_status(&team_db, memory_id).await.as_deref(),
            Some("pending")
        );
        assert_eq!(
            job_review_state(&team_db, job_id).await.as_deref(),
            Some("done")
        );
        assert_eq!(
            table_count_by_id(&test.orch.db.local, "jobs", job_id).await,
            0,
            "the review-state update must route to the team replica, not private"
        );
    }

    #[tokio::test]
    async fn team_merged_issue_confirms_drafts_in_team_replica() {
        let test = test_orch().await;
        let team_temp = tempfile::tempdir().unwrap();
        let team_id = "teammerge";
        let job_id = "teammerge~00000000-0000-4000-8000-100000000010";
        let memory_id = "teammerge~00000000-0000-4000-8000-100000000011";
        let issue_id = format!("{team_id}~00000000-0000-4000-8000-100000000002");
        let team_db = migrated_team_db(&team_temp).await;
        seed_team_memory_job(&team_db, team_id, job_id, memory_id, None).await;
        test.orch
            .db
            .insert_team_db_for_test(team_id, team_db.clone())
            .await;

        super::confirm_and_spawn_drafts_for_merged_issue(test.orch.clone(), &issue_id)
            .await
            .unwrap();

        assert_eq!(
            memory_status(&team_db, memory_id).await.as_deref(),
            Some("pending"),
            "a merged TEAM issue's draft must be confirmed in its own replica"
        );
        assert_eq!(
            table_count_by_id(&test.orch.db.local, "memories", memory_id).await,
            0,
            "the team draft must not be read from or written into the private database"
        );
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
    async fn working_doc_artifact_does_not_trigger_reflection_nudge() {
        let test = test_orch().await;
        insert_job(&test, "job-board-only", None).await;
        insert_run(&test, "run-board", "job-board-only", "session-1").await;
        insert_events(&test, "run-board", 80).await;
        insert_named_artifact(&test, "job-board-only", "board").await;

        let sent = send_memory_review_on_idle(&test.orch, "job-board-only", "run-board").unwrap();
        assert!(
            !sent,
            "a context-self working-doc artifact must not look like job completion"
        );
        assert_eq!(review_state(&test, "job-board-only").await, None);
        assert_eq!(message_to_run(&test, "run-board").await, None);
    }

    #[tokio::test]
    async fn working_doc_artifact_does_not_trigger_draft_review() {
        let test = test_orch().await;
        insert_job(&test, "job-board-drafts", None).await;
        insert_run(&test, "run-board-drafts", "job-board-drafts", "session-1").await;
        insert_events(&test, "run-board-drafts", 80).await;
        insert_named_artifact(&test, "job-board-drafts", "board").await;
        insert_draft_memory(&test, "job-board-drafts", "mem-board").await;

        let sent =
            send_memory_review_on_idle(&test.orch, "job-board-drafts", "run-board-drafts").unwrap();
        assert!(
            !sent,
            "draft review must wait for the declared output artifact, not a working doc"
        );
        assert_eq!(review_state(&test, "job-board-drafts").await, None);
        assert_eq!(message_to_run(&test, "run-board-drafts").await, None);
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
        insert_named_artifact(&test, "job-task", "result").await;

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
