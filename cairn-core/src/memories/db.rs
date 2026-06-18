//! Queries for the memories system.

use std::collections::HashSet;

use turso::params;

use crate::embeddings::vector;
use crate::models::{Memory, MemoryScope, MemoryStatus, MemoryTriageDecision};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

const MEMORY_COLUMNS: &str = "id, name, project_id, content, status, \
     scope, scope_value, job_id, node_seq, promoted_commit_sha, reason, \
     triage_decision, deferred_scope, deferred_scope_value, provenance_uri, \
     created_at, updated_at";

pub(crate) const MEMORY_COLUMNS_FOR_COMMANDS: &str = MEMORY_COLUMNS;

fn memory_from_row(row: &turso::Row) -> DbResult<Memory> {
    memory_from_row_inner(row)
}

pub(crate) fn memory_from_row_for_commands(row: &turso::Row) -> DbResult<Memory> {
    memory_from_row_inner(row)
}

fn memory_from_row_inner(row: &turso::Row) -> DbResult<Memory> {
    Ok(Memory {
        id: row.text(0)?,
        name: row.opt_text(1)?,
        project_id: row.opt_text(2)?,
        content: row.text(3)?,
        status: row.text(4)?.parse().unwrap_or(MemoryStatus::Draft),
        scope: row.text(5)?.parse().unwrap_or(MemoryScope::Workspace),
        scope_value: row.text(6)?,
        job_id: row.opt_text(7)?,
        node_seq: row.opt_i64(8)?,
        promoted_commit_sha: row.opt_text(9)?,
        reason: row.opt_text(10)?,
        triage_decision: row
            .opt_text(11)?
            .and_then(|raw| raw.parse::<MemoryTriageDecision>().ok()),
        deferred_scope: row
            .opt_text(12)?
            .and_then(|raw| raw.parse::<MemoryScope>().ok()),
        deferred_scope_value: row.opt_text(13)?,
        provenance_uri: row.opt_text(14)?,
        created_at: row.i64(15)?,
        updated_at: row.i64(16)?,
    })
}

async fn load_memory_conn(conn: &turso::Connection, memory_id: &str) -> DbResult<Memory> {
    let sql = format!("SELECT {MEMORY_COLUMNS} FROM memories WHERE id = ?1 LIMIT 1");
    let mut rows = conn.query(&sql, params![memory_id]).await?;

    let Some(row) = rows.next().await? else {
        return Err(DbError::Row(format!("Memory not found: {memory_id}")));
    };

    memory_from_row(&row)
}

pub async fn load_memory(db: &LocalDb, memory_id: &str) -> DbResult<Memory> {
    let memory_id = memory_id.to_string();
    db.read(|conn| {
        let memory_id = memory_id.clone();
        Box::pin(async move { load_memory_conn(conn, &memory_id).await })
    })
    .await
}

pub async fn load_all_memories(db: &LocalDb, project_id: Option<&str>) -> DbResult<Vec<Memory>> {
    let project_id = project_id.map(str::to_string);
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let where_clause = match project_id.as_deref() {
                Some("workspace") => "WHERE project_id = 'workspace'",
                Some(_) => "WHERE project_id = ?1 OR project_id = 'workspace'",
                None => "",
            };
            let sql = format!(
                "SELECT {MEMORY_COLUMNS} FROM memories {where_clause} ORDER BY created_at DESC"
            );

            let mut rows = match project_id.as_deref() {
                Some("workspace") | None => conn.query(&sql, ()).await?,
                Some(project_id) => conn.query(&sql, params![project_id]).await?,
            };

            let mut memories = Vec::new();
            while let Some(row) = rows.next().await? {
                memories.push(memory_from_row(&row)?);
            }
            Ok(memories)
        })
    })
    .await
}

pub async fn count_pending_memories(db: &LocalDb, project_id: &str) -> DbResult<i64> {
    let project_id = project_id.to_string();
    db.query_one(
        "SELECT COUNT(*) FROM memories WHERE status = 'pending' AND project_id = ?1",
        params![project_id.as_str()],
        |row| row.i64(0),
    )
    .await
}

pub async fn pending_memories_for_scope(
    db: &LocalDb,
    scope: &str,
    scope_value: &str,
    limit: i64,
) -> DbResult<Vec<Memory>> {
    let scope = scope.to_string();
    let scope_value = scope_value.to_string();
    db.query_all(
        format!(
            "SELECT {MEMORY_COLUMNS} FROM memories \
             WHERE status = 'pending' AND scope = ?1 AND scope_value = ?2 \
             ORDER BY created_at ASC, id ASC LIMIT ?3"
        ),
        params![scope.as_str(), scope_value.as_str(), limit.max(0)],
        memory_from_row,
    )
    .await
}

pub async fn count_pending_memories_for_scope(
    db: &LocalDb,
    scope: &str,
    scope_value: &str,
) -> DbResult<i64> {
    let scope = scope.to_string();
    let scope_value = scope_value.to_string();
    db.query_one(
        "SELECT COUNT(*) FROM memories WHERE status = 'pending' AND scope = ?1 AND scope_value = ?2",
        params![scope.as_str(), scope_value.as_str()],
        |row| row.i64(0),
    )
    .await
}

pub async fn claim_pending_memories_for_scope(
    db: &LocalDb,
    scope: &str,
    scope_value: &str,
    limit: i64,
) -> DbResult<Vec<Memory>> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let scope = scope.to_string();
    let scope_value = scope_value.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let scope = scope.clone();
        let scope_value = scope_value.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {MEMORY_COLUMNS} FROM memories \
                 WHERE status = 'pending' AND scope = ?1 AND scope_value = ?2 \
                 ORDER BY created_at ASC, id ASC LIMIT ?3"
            );
            let mut rows = conn
                .query(&sql, params![scope.as_str(), scope_value.as_str(), limit])
                .await?;
            let mut memories = Vec::new();
            while let Some(row) = rows.next().await? {
                memories.push(memory_from_row(&row)?);
            }
            if memories.len() < limit as usize {
                return Ok(Vec::new());
            }
            for memory in &memories {
                conn.execute(
                    "UPDATE memories SET status = 'claimed', updated_at = ?1 \
                     WHERE id = ?2 AND status = 'pending' AND scope = ?3 AND scope_value = ?4",
                    params![
                        now,
                        memory.id.as_str(),
                        scope.as_str(),
                        scope_value.as_str()
                    ],
                )
                .await?;
            }
            Ok(memories)
        })
    })
    .await
}

pub async fn set_memories_status(db: &LocalDb, ids: &[String], status: &str) -> DbResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let ids = ids.to_vec();
    let status = status.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let ids = ids.clone();
        let status = status.clone();
        Box::pin(async move {
            for id in &ids {
                conn.execute(
                    "UPDATE memories SET status = ?1, updated_at = ?2 WHERE id = ?3",
                    params![status.as_str(), now, id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

pub async fn load_draft_memories_for_job(db: &LocalDb, job_id: &str) -> DbResult<Vec<Memory>> {
    let job_id = job_id.to_string();
    db.query_all(
        format!(
            "SELECT {MEMORY_COLUMNS} FROM memories \
             WHERE job_id = ?1 AND status = 'draft' \
             ORDER BY node_seq ASC, created_at ASC"
        ),
        params![job_id.as_str()],
        memory_from_row,
    )
    .await
}

pub async fn load_memories_for_job(db: &LocalDb, job_id: &str) -> DbResult<Vec<Memory>> {
    let job_id = job_id.to_string();
    db.query_all(
        format!(
            "SELECT {MEMORY_COLUMNS} FROM memories WHERE job_id = ?1 ORDER BY node_seq ASC, created_at ASC"
        ),
        params![job_id.as_str()],
        memory_from_row,
    )
    .await
}
pub async fn next_node_memory_seq(db: &LocalDb, job_id: &str) -> DbResult<i64> {
    let job_id = job_id.to_string();
    db.query_one(
        "SELECT COALESCE(MAX(node_seq), 0) + 1 FROM memories WHERE job_id = ?1",
        params![job_id.as_str()],
        |row| row.i64(0),
    )
    .await
}

/// The role identity for a job: its `agent_config_id` — the agent prompt name
/// that resolves to the canon file `{role}.md` and to `get_agent_config(role)`.
/// Falls back to `node_name` when the job was not started from a named agent
/// config. Returns `None` when the job row is missing.
///
/// This is the correct value for a `role`-scoped memory's `scope_value`: the
/// recipe node name (e.g. `agent-1`) is a layout label, while the role
/// (e.g. `builder`) is what canon promotion and the role memory pool key on.
pub async fn role_for_job(db: &LocalDb, job_id: &str) -> DbResult<Option<String>> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT agent_config_id, node_name FROM jobs WHERE id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.opt_text(0)?.or(row.opt_text(1)?)),
                None => Ok(None),
            }
        })
    })
    .await
}

pub async fn confirm_draft_memories_for_job(db: &LocalDb, job_id: &str) -> DbResult<Vec<Memory>> {
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {MEMORY_COLUMNS} FROM memories \
                 WHERE job_id = ?1 AND status = 'draft' \
                 ORDER BY node_seq ASC, created_at ASC"
            );
            let mut rows = conn.query(&sql, params![job_id.as_str()]).await?;
            let mut memories = Vec::new();
            while let Some(row) = rows.next().await? {
                memories.push(memory_from_row(&row)?);
            }
            for memory in &memories {
                conn.execute(
                    "UPDATE memories SET status = 'pending', updated_at = ?1 WHERE id = ?2 AND status = 'draft'",
                    params![now, memory.id.as_str()],
                )
                .await?;
            }
            Ok(memories)
        })
    })
    .await
}

pub async fn record_triage_issue_batch(
    db: &LocalDb,
    issue_id: &str,
    memory_ids: &[String],
) -> DbResult<()> {
    if memory_ids.is_empty() {
        return Ok(());
    }
    let issue_id = issue_id.to_string();
    let memory_ids = memory_ids.to_vec();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        let memory_ids = memory_ids.clone();
        Box::pin(async move {
            for memory_id in &memory_ids {
                conn.execute(
                    "INSERT OR IGNORE INTO memory_triage_issue_memories (issue_id, memory_id)
                     VALUES (?1, ?2)",
                    params![issue_id.as_str(), memory_id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

pub async fn triage_batch_memories_for_issue(
    db: &LocalDb,
    issue_id: &str,
) -> DbResult<Vec<Memory>> {
    let issue_id = issue_id.to_string();
    db.query_all(
        format!(
            "SELECT {MEMORY_COLUMNS} FROM memories m
             JOIN memory_triage_issue_memories tm ON tm.memory_id = m.id
             WHERE tm.issue_id = ?1
             ORDER BY tm.rowid ASC, m.created_at ASC, m.id ASC"
        ),
        params![issue_id.as_str()],
        memory_from_row,
    )
    .await
}

pub async fn record_triage_decision(
    db: &LocalDb,
    id: &str,
    decision: MemoryTriageDecision,
    reason: &str,
    deferred_scope: Option<MemoryScope>,
    deferred_scope_value: Option<&str>,
) -> DbResult<()> {
    let id = id.to_string();
    let decision = decision.to_string();
    let reason = reason.to_string();
    let deferred_scope = deferred_scope.map(|scope| scope.to_string());
    let deferred_scope_value = deferred_scope_value.map(str::to_string);
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let id = id.clone();
        let decision = decision.clone();
        let reason = reason.clone();
        let deferred_scope = deferred_scope.clone();
        let deferred_scope_value = deferred_scope_value.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE memories
                 SET triage_decision = ?1, reason = ?2, deferred_scope = ?3,
                     deferred_scope_value = ?4, updated_at = ?5
                 WHERE id = ?6",
                params![
                    decision.as_str(),
                    reason.as_str(),
                    deferred_scope.as_deref(),
                    deferred_scope_value.as_deref(),
                    now,
                    id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

pub async fn clear_triage_decisions(db: &LocalDb, ids: &[String]) -> DbResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let ids = ids.to_vec();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let ids = ids.clone();
        Box::pin(async move {
            for id in &ids {
                conn.execute(
                    "UPDATE memories
                     SET triage_decision = NULL, reason = NULL, promoted_commit_sha = NULL,
                         deferred_scope = NULL, deferred_scope_value = NULL, updated_at = ?1
                     WHERE id = ?2",
                    params![now, id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

pub async fn set_memories_promoted_commit_sha(
    db: &LocalDb,
    ids: &[String],
    sha: &str,
) -> DbResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let ids = ids.to_vec();
    let sha = sha.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let ids = ids.clone();
        let sha = sha.clone();
        Box::pin(async move {
            for id in &ids {
                conn.execute(
                    "UPDATE memories SET promoted_commit_sha = ?1, updated_at = ?2 WHERE id = ?3",
                    params![sha.as_str(), now, id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

pub async fn resolve_triage_batch_on_merge(db: &LocalDb, issue_id: &str) -> DbResult<Vec<String>> {
    let memories = triage_batch_memories_for_issue(db, issue_id).await?;
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let memories = memories.clone();
        let mut ids = Vec::new();
        Box::pin(async move {
            for memory in &memories {
                ids.push(memory.id.clone());
                match memory.triage_decision {
                    Some(MemoryTriageDecision::Promote) => {
                        conn.execute(
                            "UPDATE memories SET status = 'promoted', updated_at = ?1 WHERE id = ?2",
                            params![now, memory.id.as_str()],
                        )
                        .await?;
                    }
                    Some(MemoryTriageDecision::Discard) => {
                        conn.execute(
                            "UPDATE memories SET status = 'discarded', updated_at = ?1 WHERE id = ?2",
                            params![now, memory.id.as_str()],
                        )
                        .await?;
                    }
                    Some(MemoryTriageDecision::Defer) => {
                        if let Some(scope) = memory.deferred_scope.as_ref() {
                            let scope_raw = scope.to_string();
                            let scope_value = memory.deferred_scope_value.as_deref().unwrap_or(match scope {
                                MemoryScope::Workspace => "workspace",
                                _ => memory.scope_value.as_str(),
                            });
                            let project_id = match scope {
                                MemoryScope::Project => scope_value,
                                MemoryScope::Workspace => "workspace",
                                // CAIRN-1493 supplies per-(scope,value) pending batching and
                                // role-pool routing; until integrated, retain the owning project id.
                                MemoryScope::Role => memory.project_id.as_deref().unwrap_or("workspace"),
                            };
                            conn.execute(
                                "UPDATE memories
                                 SET status = 'pending', scope = ?1, scope_value = ?2,
                                     project_id = ?3, updated_at = ?4
                                 WHERE id = ?5",
                                params![scope_raw.as_str(), scope_value, project_id, now, memory.id.as_str()],
                            )
                            .await?;
                        } else {
                            conn.execute(
                                "UPDATE memories SET status = 'deferred', updated_at = ?1 WHERE id = ?2",
                                params![now, memory.id.as_str()],
                            )
                            .await?;
                        }
                    }
                    None => {
                        conn.execute(
                            "UPDATE memories SET status = 'pending', updated_at = ?1 WHERE id = ?2",
                            params![now, memory.id.as_str()],
                        )
                        .await?;
                    }
                }
            }
            Ok(ids)
        })
    })
    .await
}

pub async fn revert_triage_batch_on_close(db: &LocalDb, issue_id: &str) -> DbResult<Vec<String>> {
    let memories = triage_batch_memories_for_issue(db, issue_id).await?;
    let ids: Vec<String> = memories.into_iter().map(|memory| memory.id).collect();
    if ids.is_empty() {
        return Ok(ids);
    }
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let ids = ids.clone();
        Box::pin(async move {
            for id in &ids {
                conn.execute(
                    "UPDATE memories
                     SET status = 'pending', triage_decision = NULL, reason = NULL,
                         promoted_commit_sha = NULL, deferred_scope = NULL,
                         deferred_scope_value = NULL, updated_at = ?1
                     WHERE id = ?2",
                    params![now, id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await?;
    Ok(ids)
}

/// Distinct `(scope, scope_value)` pools among `pending` memories. Drives the
/// reconciliation sweep's scope discovery directly from DB state rather than
/// from a caller-supplied confirmed list.
pub async fn distinct_pending_scopes(db: &LocalDb) -> DbResult<Vec<(String, String)>> {
    db.query_all(
        "SELECT DISTINCT scope, scope_value FROM memories WHERE status = 'pending' \
         ORDER BY scope ASC, scope_value ASC",
        (),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
}

/// Terminal jobs (`complete`/`failed`) that still carry `draft` memories, paired
/// with each job's `memory_review_state`. The caller excludes jobs whose review
/// is still in flight before confirming the surviving drafts.
pub async fn terminal_jobs_with_draft_memories(
    db: &LocalDb,
) -> DbResult<Vec<(String, Option<String>)>> {
    db.query_all(
        "SELECT DISTINCT j.id, j.memory_review_state \
         FROM jobs j \
         JOIN memories m ON m.job_id = j.id AND m.status = 'draft' \
         WHERE j.status IN ('complete', 'failed') \
         ORDER BY j.id ASC",
        (),
        |row| Ok((row.text(0)?, row.opt_text(1)?)),
    )
    .await
}

/// Revert to `pending` every `claimed` memory with no row in
/// `memory_triage_issue_memories` — claimed but never linked to a triage issue
/// (the clean "no issue to begin with" signal, since the link is only written
/// after the triage issue is successfully created). Returns the reverted ids.
pub async fn revert_orphaned_claimed_memories(db: &LocalDb) -> DbResult<Vec<String>> {
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT m.id FROM memories m \
                     WHERE m.status = 'claimed' \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM memory_triage_issue_memories tm \
                         WHERE tm.memory_id = m.id) \
                     ORDER BY m.id ASC",
                    (),
                )
                .await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push(row.text(0)?);
            }
            for id in &ids {
                conn.execute(
                    "UPDATE memories SET status = 'pending', updated_at = ?1 \
                     WHERE id = ?2 AND status = 'claimed'",
                    params![now, id.as_str()],
                )
                .await?;
            }
            Ok(ids)
        })
    })
    .await
}

/// Reconciliation recovery for triage issues whose execution *failed* after the
/// claimed batch was already linked. A failed triage issue is terminal but never
/// merged or closed, so neither `resolve_triage_batch_on_merge` nor
/// `revert_triage_batch_on_close` ever runs for it: the batch would otherwise
/// stay `claimed` forever and — because the issue is neither merged nor closed —
/// keep `open_triage_issue_for_scope` reporting a live cover, wedging the scope
/// against every future spawn. Revert those still-`claimed` memories to
/// `pending`, clearing any decision recorded before the failure, so the batch
/// re-enters its pool. Returns the reverted ids.
pub async fn revert_claimed_for_failed_triage_issues(db: &LocalDb) -> DbResult<Vec<String>> {
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT m.id FROM memories m \
                     JOIN memory_triage_issue_memories tm ON tm.memory_id = m.id \
                     JOIN issues i ON i.id = tm.issue_id \
                     WHERE m.status = 'claimed' \
                       AND i.status = 'failed' \
                       AND i.merged_at IS NULL AND i.closed_at IS NULL \
                     ORDER BY m.id ASC",
                    (),
                )
                .await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push(row.text(0)?);
            }
            for id in &ids {
                conn.execute(
                    "UPDATE memories \
                     SET status = 'pending', triage_decision = NULL, reason = NULL, \
                         promoted_commit_sha = NULL, deferred_scope = NULL, \
                         deferred_scope_value = NULL, updated_at = ?1 \
                     WHERE id = ?2 AND status = 'claimed'",
                    params![now, id.as_str()],
                )
                .await?;
            }
            Ok(ids)
        })
    })
    .await
}

/// Issue ids of *merged* memory-triage issues that still own `claimed` memories —
/// batches whose merge never applied the recorded triage decisions (e.g. the
/// merge path bypassed the canon gate, or the resolve hook errored). The
/// reconcile sweep finalizes each by calling `resolve_triage_batch_on_merge`.
pub async fn merged_triage_issues_with_claimed_memories(db: &LocalDb) -> DbResult<Vec<String>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT tm.issue_id FROM memory_triage_issue_memories tm \
                     JOIN memories m ON m.id = tm.memory_id \
                     JOIN issues i ON i.id = tm.issue_id \
                     WHERE m.status = 'claimed' AND i.merged_at IS NOT NULL \
                     ORDER BY tm.issue_id ASC",
                    (),
                )
                .await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push(row.text(0)?);
            }
            Ok(ids)
        })
    })
    .await
}

/// Escape SQL `LIKE` metacharacters so a built prefix matches literally under
/// `ESCAPE '\'`.
fn escape_like_pattern(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Issue id of a *live* memory-triage issue seeded for the exact
/// `(scope, scope_value)`, matched on the deterministic triage title prefix.
/// "Live" means non-terminal: `merged_at` and `closed_at` both NULL **and** the
/// issue is not `failed`. Used to avoid spawning a duplicate triage issue when
/// one is already in flight — including an orphan issue whose execution start
/// failed and whose claimed batch was reverted (no link row survives, so a title
/// match is the only signal). A *failed* triage execution is terminal: it is
/// excluded here (otherwise it would wedge its scope against every future spawn)
/// and its stranded batch is recovered by `revert_claimed_for_failed_triage_issues`.
pub async fn open_triage_issue_for_scope(
    db: &LocalDb,
    scope: &str,
    scope_value: &str,
) -> DbResult<Option<String>> {
    let prefix = format!(
        "Memory triage: {}={} (",
        escape_like_pattern(scope),
        escape_like_pattern(scope_value)
    );
    let pattern = format!("{prefix}%");
    db.query_opt(
        "SELECT id FROM issues \
         WHERE title LIKE ?1 ESCAPE '\\' \
           AND merged_at IS NULL AND closed_at IS NULL \
           AND status != 'failed' \
         ORDER BY created_at DESC LIMIT 1",
        params![pattern],
        |row| row.text(0),
    )
    .await
}

#[derive(Debug, Clone)]
pub struct MemoryTriageNeighbor {
    pub memory: Memory,
    pub uri: String,
    pub similarity: f32,
    pub triage_issue_uri: Option<String>,
}

pub async fn build_node_memory_uri_for_memory(db: &LocalDb, memory: &Memory) -> DbResult<String> {
    let job_id = memory
        .job_id
        .as_deref()
        .ok_or_else(|| DbError::Row(format!("Memory {} has no job_id", memory.id)))?
        .to_string();
    let node_seq = memory
        .node_seq
        .ok_or_else(|| DbError::Row(format!("Memory {} has no node_seq", memory.id)))?;
    db.query_one(
        "SELECT p.key, i.number, e.seq, j.uri_segment
         FROM jobs j
         JOIN executions e ON e.id = j.execution_id
         JOIN issues i ON i.id = j.issue_id
         JOIN projects p ON p.id = j.project_id
         WHERE j.id = ?1
         LIMIT 1",
        params![job_id.as_str()],
        move |row| {
            let project_key = row.text(0)?;
            let number = row.i64(1)? as i32;
            let exec_seq = row.i64(2)? as i32;
            let node_id = row.text(3)?;
            Ok(cairn_common::uri::build_node_memory_uri(
                &project_key,
                number,
                exec_seq,
                &node_id,
                node_seq as i32,
            ))
        },
    )
    .await
}

pub async fn resolve_node_memory_id(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    memory_seq: i32,
) -> DbResult<Option<String>> {
    let project_key = project_key.to_uppercase();
    let node_id = node_id.to_string();
    db.query_opt(
        "SELECT m.id
         FROM memories m
         JOIN jobs j ON j.id = m.job_id
         JOIN executions e ON e.id = j.execution_id
         JOIN issues i ON i.id = j.issue_id
         JOIN projects p ON p.id = j.project_id
         WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
           AND j.uri_segment = ?4 AND m.node_seq = ?5
         LIMIT 1",
        params![
            project_key.as_str(),
            number as i64,
            exec_seq as i64,
            node_id.as_str(),
            memory_seq as i64
        ],
        |row| row.text(0),
    )
    .await
}

pub async fn similar_memory_neighbors(
    db: &LocalDb,
    query_memory: &Memory,
    query_uri: &str,
    excluded_memory_ids: &[String],
    min_similarity: f32,
    limit: usize,
) -> DbResult<Vec<MemoryTriageNeighbor>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let query_embedding =
        match crate::embeddings::queries::get_resource_embedding_async(db, query_uri).await? {
            Some(record) => vector::from_bytes(&record.embedding),
            None => return Ok(Vec::new()),
        };
    if query_embedding.is_empty() {
        return Ok(Vec::new());
    }

    let excluded: HashSet<&str> = excluded_memory_ids.iter().map(String::as_str).collect();
    let memories = load_all_memories(db, query_memory.project_id.as_deref()).await?;
    let mut candidates = Vec::new();
    for memory in memories {
        if memory.id == query_memory.id || excluded.contains(memory.id.as_str()) {
            continue;
        }
        let uri = build_node_memory_uri_for_memory(db, &memory).await?;
        let Some(record) =
            crate::embeddings::queries::get_resource_embedding_async(db, &uri).await?
        else {
            continue;
        };
        let embedding = vector::from_bytes(&record.embedding);
        if embedding.len() != query_embedding.len() {
            continue;
        }
        let similarity = vector::cosine_similarity(&query_embedding, &embedding);
        if similarity < min_similarity {
            continue;
        }
        candidates.push((memory, uri, similarity));
    }

    candidates.sort_by(|a, b| {
        b.2.partial_cmp(&a.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.created_at.cmp(&b.0.created_at))
            .then_with(|| a.0.id.cmp(&b.0.id))
    });
    candidates.truncate(limit);

    let mut out = Vec::new();
    for (memory, uri, similarity) in candidates {
        let triage_issue_uri = triage_issue_uri_for_memory(db, &memory.id).await?;
        out.push(MemoryTriageNeighbor {
            memory,
            uri,
            similarity,
            triage_issue_uri,
        });
    }
    Ok(out)
}

async fn triage_issue_uri_for_memory(db: &LocalDb, memory_id: &str) -> DbResult<Option<String>> {
    let memory_id = memory_id.to_string();
    db.query_opt(
        "SELECT p.key, i.number
         FROM memory_triage_issue_memories tm
         JOIN issues i ON i.id = tm.issue_id
         JOIN projects p ON p.id = i.project_id
         WHERE tm.memory_id = ?1
         ORDER BY tm.rowid DESC
         LIMIT 1",
        params![memory_id.as_str()],
        |row| {
            let key = row.text(0)?;
            let number = row.i64(1)? as i32;
            Ok(Some(cairn_common::uri::build_issue_uri(&key, number)))
        },
    )
    .await
    .map(Option::flatten)
}

pub async fn project_key_by_id(db: &LocalDb, project_id: &str) -> DbResult<String> {
    let project_id = project_id.to_string();
    db.query_one(
        "SELECT key FROM projects \
         WHERE id = ?1 OR (?1 = 'workspace' AND is_workspace = 1) \
         ORDER BY CASE WHEN id = ?1 THEN 0 ELSE 1 END LIMIT 1",
        params![project_id.as_str()],
        |row| row.text(0),
    )
    .await
}

pub async fn backfill_workspace_project_id(db: &LocalDb) -> DbResult<u64> {
    db.execute(
        "UPDATE memories SET project_id = 'workspace' WHERE project_id IS NULL",
        (),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn create_memory(
    db: &LocalDb,
    id: &str,
    name: Option<&str>,
    content: &str,
    project_id: Option<&str>,
    scope: &str,
    scope_value: &str,
    job_id: Option<&str>,
    node_seq: Option<i64>,
    provenance_uri: Option<&str>,
) -> DbResult<Memory> {
    let id = id.to_string();
    let content = content.to_string();
    let project_id = project_id.map(str::to_string);
    let name = name.map(str::to_string);
    let scope = scope.to_string();
    let scope_value = scope_value.to_string();
    let job_id = job_id.map(str::to_string);
    let provenance_uri = provenance_uri.map(str::to_string);
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let id = id.clone();
        let content = content.clone();
        let project_id = project_id.clone();
        let name = name.clone();
        let scope = scope.clone();
        let scope_value = scope_value.clone();
        let job_id = job_id.clone();
        let provenance_uri = provenance_uri.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO memories (
                    id, name, project_id, content, scope, scope_value,
                    job_id, node_seq, provenance_uri, created_at, updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)
                ",
                params![
                    id.as_str(),
                    name.as_deref(),
                    project_id.as_deref(),
                    content.as_str(),
                    scope.as_str(),
                    scope_value.as_str(),
                    job_id.as_deref(),
                    node_seq,
                    provenance_uri.as_deref(),
                    now
                ],
            )
            .await?;

            load_memory_conn(conn, &id).await
        })
    })
    .await
}

pub async fn update_memory(
    db: &LocalDb,
    id: &str,
    content: Option<&str>,
    status: Option<&str>,
) -> DbResult<Memory> {
    let id = id.to_string();
    let content = content.map(str::to_string);
    let status = status.map(str::to_string);
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let id = id.clone();
        let content = content.clone();
        let status = status.clone();
        Box::pin(async move {
            let mut updated = false;

            if let Some(content) = content.as_deref() {
                conn.execute(
                    "UPDATE memories SET content = ?1, updated_at = ?2 WHERE id = ?3",
                    params![content, now, id.as_str()],
                )
                .await?;
                updated = true;
            }
            if let Some(status) = status.as_deref() {
                conn.execute(
                    "UPDATE memories SET status = ?1, updated_at = ?2 WHERE id = ?3",
                    params![status, now, id.as_str()],
                )
                .await?;
                updated = true;
            }
            if !updated {
                return Err(DbError::internal("No fields to update"));
            }

            load_memory_conn(conn, &id).await
        })
    })
    .await
}

pub async fn delete_memory(db: &LocalDb, id: &str) -> DbResult<()> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute("DELETE FROM memories WHERE id = ?1", params![id.as_str()])
                .await?;
            Ok(())
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("memories-db-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace) VALUES ('workspace', 'default', 'Workspace', 'WKS', '/tmp/ws', 1, 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/prj', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT OR IGNORE INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-main', 'project-1', 42, 'Main', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT OR IGNORE INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('exec-main', 'recipe', 'issue-main', 'project-1', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT OR IGNORE INTO jobs (id, execution_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('job-main', 'exec-main', 'issue-main', 'project-1', 'running', 'builder', 'builder', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    async fn insert_memory(db: &LocalDb, id: &str, project_id: Option<&str>, created_at: i64) {
        db.write(|conn| {
            let id = id.to_string();
            let project_id = project_id.map(str::to_string);
            let (scope, scope_value) = match project_id.as_deref() {
                Some("workspace") | None => ("workspace".to_string(), "workspace".to_string()),
                Some(project_id) => ("project".to_string(), project_id.to_string()),
            };
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at) VALUES (?1, ?1, ?2, ?3, 'pending', ?4, ?5, 'job-main', ?6, ?6, ?6)",
                    params![id.as_str(), project_id.as_deref(), id.as_str(), scope.as_str(), scope_value.as_str(), created_at],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn memory_status(db: &LocalDb, id: &str) -> String {
        let id = id.to_string();
        db.query_one(
            "SELECT status FROM memories WHERE id = ?1",
            params![id.as_str()],
            |row| row.text(0),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn open_triage_issue_for_scope_ignores_failed_terminal_issue() {
        let db = test_db().await;
        // A failed triage execution is terminal even though merged_at/closed_at
        // are NULL; it must not count as a live cover for its scope.
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-failed', 'project-1', 9, 'Memory triage: project=project-1 (5 pending)', 'failed', 1, 1)",
            (),
        )
        .await
        .unwrap();
        assert!(open_triage_issue_for_scope(&db, "project", "project-1")
            .await
            .unwrap()
            .is_none());

        // A non-terminal issue for the same scope still counts as a live cover.
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-active', 'project-1', 10, 'Memory triage: project=project-1 (5 pending)', 'active', 2, 2)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            open_triage_issue_for_scope(&db, "project", "project-1")
                .await
                .unwrap()
                .as_deref(),
            Some("issue-active")
        );
    }

    #[tokio::test]
    async fn load_all_memories_without_project_includes_project_rows() {
        let db = test_db().await;
        insert_memory(&db, "workspace-memory", Some("workspace"), 1).await;
        insert_memory(&db, "project-memory", Some("project-1"), 2).await;

        let memories = load_all_memories(&db, None).await.unwrap();
        let ids = memories
            .iter()
            .map(|memory| memory.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert!(ids.contains("workspace-memory"));
        assert!(ids.contains("project-memory"));
    }

    #[tokio::test]
    async fn pending_queries_are_exact_scope_and_oldest_first() {
        let db = test_db().await;
        insert_memory(&db, "workspace-old", Some("workspace"), 1).await;
        insert_memory(&db, "project-old", Some("project-1"), 2).await;
        insert_memory(&db, "workspace-new", Some("workspace"), 3).await;
        set_memories_status(&db, &["workspace-new".to_string()], "claimed")
            .await
            .unwrap();

        assert_eq!(count_pending_memories(&db, "workspace").await.unwrap(), 1);
        assert_eq!(count_pending_memories(&db, "project-1").await.unwrap(), 1);

        let workspace = pending_memories_for_scope(&db, "workspace", "workspace", 10)
            .await
            .unwrap();
        assert_eq!(
            workspace.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["workspace-old"]
        );
    }

    #[tokio::test]
    async fn claim_requires_full_batch_and_marks_oldest_claimed() {
        let db = test_db().await;
        insert_memory(&db, "one", Some("workspace"), 1).await;
        assert!(
            claim_pending_memories_for_scope(&db, "workspace", "workspace", 2)
                .await
                .unwrap()
                .is_empty()
        );

        insert_memory(&db, "two", Some("workspace"), 2).await;
        insert_memory(&db, "three", Some("workspace"), 3).await;
        let claimed = claim_pending_memories_for_scope(&db, "workspace", "workspace", 2)
            .await
            .unwrap();
        assert_eq!(
            claimed.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["one", "two"]
        );
        assert_eq!(count_pending_memories(&db, "workspace").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn scoped_claim_pools_role_project_and_workspace_separately() {
        let db = test_db().await;
        insert_memory(&db, "project-one", Some("project-1"), 1).await;
        insert_memory(&db, "project-two", Some("project-1"), 2).await;
        db.execute(
            "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-2', 'default', 'Project 2', 'PR2', '/tmp/prj2', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at)
             VALUES ('role-one', 'role-one', 'project-1', 'role', 'pending', 'role', 'builder', 'job-main', 103, 3, 3)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at)
             VALUES ('role-two', 'role-two', 'project-1', 'role', 'pending', 'role', 'coordinator', 'job-main', 104, 4, 4)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at)
             VALUES ('role-other-project', 'role-other-project', 'project-2', 'role', 'pending', 'role', 'builder', 'job-main', 105, 5, 5)",
            (),
        )
        .await
        .unwrap();

        assert_eq!(
            count_pending_memories_for_scope(&db, "project", "project-1")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            count_pending_memories_for_scope(&db, "role", "builder")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            count_pending_memories_for_scope(&db, "role", "coordinator")
                .await
                .unwrap(),
            1
        );

        let claimed = claim_pending_memories_for_scope(&db, "project", "project-1", 2)
            .await
            .unwrap();
        assert_eq!(
            claimed.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["project-one", "project-two"]
        );
        assert_eq!(memory_status(&db, "role-one").await, "pending");
        assert_eq!(memory_status(&db, "role-two").await, "pending");

        let claimed = claim_pending_memories_for_scope(&db, "role", "builder", 2)
            .await
            .unwrap();
        assert_eq!(
            claimed.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["role-one", "role-other-project"]
        );
    }

    #[tokio::test]
    async fn similar_memory_neighbors_include_prior_resolution_metadata() {
        let db = test_db().await;
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-7', 'project-1', 7, 'Memory triage', 'active', 1, 1)",
            (),
        )
        .await
        .unwrap();
        let current = create_memory(
            &db,
            "current",
            Some("current"),
            "use cargo nextest for focused tests",
            Some("project-1"),
            "project",
            "project-1",
            Some("job-main"),
            Some(201),
            None,
        )
        .await
        .unwrap();
        let prior = create_memory(
            &db,
            "prior",
            Some("prior"),
            "prefer cargo nextest for targeted rust tests",
            Some("project-1"),
            "project",
            "project-1",
            Some("job-main"),
            Some(202),
            None,
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE memories SET status = 'promoted', promoted_commit_sha = 'abc123', reason = 'landed in project canon' WHERE id = 'prior'",
            (),
        )
        .await
        .unwrap();
        record_triage_issue_batch(&db, "issue-7", &["prior".to_string()])
            .await
            .unwrap();
        let current_uri = build_node_memory_uri_for_memory(&db, &current)
            .await
            .unwrap();
        let prior_uri = build_node_memory_uri_for_memory(&db, &prior).await.unwrap();
        crate::embeddings::queries::upsert_resource_embedding_async(
            &db,
            &current_uri,
            &crate::embeddings::vector::to_bytes(&[1.0, 0.0, 0.0]),
            "test",
            3,
        )
        .await
        .unwrap();
        crate::embeddings::queries::upsert_resource_embedding_async(
            &db,
            &prior_uri,
            &crate::embeddings::vector::to_bytes(&[0.95, 0.05, 0.0]),
            "test",
            3,
        )
        .await
        .unwrap();

        let neighbors = similar_memory_neighbors(
            &db,
            &current,
            &current_uri,
            &["current".to_string()],
            0.7,
            5,
        )
        .await
        .unwrap();

        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].memory.id, "prior");
        assert_eq!(
            neighbors[0].triage_issue_uri.as_deref(),
            Some("cairn://p/PRJ/7")
        );
        assert_eq!(
            neighbors[0].memory.promoted_commit_sha.as_deref(),
            Some("abc123")
        );
        assert_eq!(
            neighbors[0].memory.reason.as_deref(),
            Some("landed in project canon")
        );
    }

    #[tokio::test]
    async fn triage_batch_resolves_stored_decisions_on_merge_and_close() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-1', 'project-1', 1, 'Memory triage', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-2', 'project-1', 2, 'Memory triage close', 'active', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        for (seq, id) in [
            "promoted-memory",
            "discarded-memory",
            "deferred-memory",
            "rescoped-memory",
            "undecided-memory",
        ]
        .into_iter()
        .enumerate()
        {
            create_memory(
                &db,
                id,
                Some(id),
                id,
                Some("project-1"),
                "project",
                "project-1",
                Some("job-main"),
                Some(300 + seq as i64),
                None,
            )
            .await
            .unwrap();
        }
        let ids = vec![
            "promoted-memory".to_string(),
            "discarded-memory".to_string(),
            "deferred-memory".to_string(),
            "rescoped-memory".to_string(),
            "undecided-memory".to_string(),
        ];
        set_memories_status(&db, &ids, "claimed").await.unwrap();
        record_triage_issue_batch(&db, "issue-1", &ids)
            .await
            .unwrap();

        record_triage_decision(
            &db,
            "promoted-memory",
            MemoryTriageDecision::Promote,
            "canon-worthy",
            None,
            None,
        )
        .await
        .unwrap();
        set_memories_promoted_commit_sha(&db, &["promoted-memory".to_string()], "abc123")
            .await
            .unwrap();
        record_triage_decision(
            &db,
            "discarded-memory",
            MemoryTriageDecision::Discard,
            "too local",
            None,
            None,
        )
        .await
        .unwrap();
        record_triage_decision(
            &db,
            "deferred-memory",
            MemoryTriageDecision::Defer,
            "needs recurrence",
            None,
            None,
        )
        .await
        .unwrap();
        record_triage_decision(
            &db,
            "rescoped-memory",
            MemoryTriageDecision::Defer,
            "belongs to workspace",
            Some(MemoryScope::Workspace),
            Some("workspace"),
        )
        .await
        .unwrap();

        let resolved = resolve_triage_batch_on_merge(&db, "issue-1").await.unwrap();
        assert_eq!(resolved, ids);
        let promoted = load_memory(&db, "promoted-memory").await.unwrap();
        assert_eq!(promoted.status, MemoryStatus::Promoted);
        assert_eq!(promoted.promoted_commit_sha.as_deref(), Some("abc123"));
        assert_eq!(promoted.reason.as_deref(), Some("canon-worthy"));
        assert_eq!(memory_status(&db, "discarded-memory").await, "discarded");
        assert_eq!(memory_status(&db, "deferred-memory").await, "deferred");
        let rescoped = load_memory(&db, "rescoped-memory").await.unwrap();
        assert_eq!(rescoped.status, MemoryStatus::Pending);
        assert_eq!(rescoped.scope, MemoryScope::Workspace);
        assert_eq!(rescoped.scope_value, "workspace");
        assert_eq!(rescoped.project_id.as_deref(), Some("workspace"));
        assert_eq!(memory_status(&db, "undecided-memory").await, "pending");

        let close_ids = vec![
            "promoted-memory".to_string(),
            "discarded-memory".to_string(),
        ];
        set_memories_status(&db, &close_ids, "claimed")
            .await
            .unwrap();
        record_triage_decision(
            &db,
            "promoted-memory",
            MemoryTriageDecision::Promote,
            "abandoned",
            None,
            None,
        )
        .await
        .unwrap();
        set_memories_promoted_commit_sha(&db, &["promoted-memory".to_string()], "def456")
            .await
            .unwrap();
        record_triage_issue_batch(&db, "issue-2", &close_ids)
            .await
            .unwrap();
        let reverted = revert_triage_batch_on_close(&db, "issue-2").await.unwrap();
        assert_eq!(reverted, close_ids);
        for id in ["promoted-memory", "discarded-memory"] {
            let memory = load_memory(&db, id).await.unwrap();
            assert_eq!(memory.status, MemoryStatus::Pending);
            assert!(memory.triage_decision.is_none());
            assert!(memory.promoted_commit_sha.is_none());
            assert!(memory.reason.is_none());
            assert!(memory.deferred_scope.is_none());
            assert!(memory.deferred_scope_value.is_none());
        }
    }

    #[tokio::test]
    async fn backfill_workspace_project_id_replaces_null_scope() {
        let db = test_db().await;
        insert_memory(&db, "legacy", None, 1).await;
        assert_eq!(backfill_workspace_project_id(&db).await.unwrap(), 1);
        let memory = load_memory(&db, "legacy").await.unwrap();
        assert_eq!(memory.project_id.as_deref(), Some("workspace"));
    }

    #[tokio::test]
    async fn project_key_by_id_resolves_workspace_key_dynamically() {
        let db = test_db().await;
        assert_eq!(project_key_by_id(&db, "workspace").await.unwrap(), "WKS");
        assert_eq!(project_key_by_id(&db, "project-1").await.unwrap(), "PRJ");
    }
}
