//! Queries for the memories system.

use std::collections::HashSet;

use cairn_db::turso::params;

use crate::embeddings::vector;
use crate::models::{Memory, MemoryScope, MemoryStatus, MemoryTriageDecision};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

const MEMORY_COLUMNS: &str = "id, name, project_id, content, status, \
     scope, scope_value, job_id, node_seq, promoted_commit_sha, reason, \
     triage_decision, deferred_scope, deferred_scope_value, provenance_uri, \
     created_at, updated_at";

pub(crate) const MEMORY_COLUMNS_FOR_COMMANDS: &str = MEMORY_COLUMNS;

/// Predicate restricting a joined `tm` link row to the memory's MOST RECENT
/// triage-batch link — the batch that currently owns it.
///
/// `memory_triage_issue_memories` accumulates: a memory released back to
/// `pending` (undecided, or re-pooled by a defer) can be claimed into a later
/// batch while its earlier link rows survive as history. Ownership is therefore
/// the latest link, never merely "linked". Without this, a long-merged batch
/// re-applies its decision to a memory whose state has moved on, and every
/// historical link of one stuck memory reports as its own failing batch.
const LATEST_TRIAGE_LINK: &str = "tm.rowid = (SELECT MAX(tm2.rowid) \
     FROM memory_triage_issue_memories tm2 WHERE tm2.memory_id = tm.memory_id)";

fn memory_from_row(row: &cairn_db::turso::Row) -> DbResult<Memory> {
    memory_from_row_inner(row)
}

pub(crate) fn memory_from_row_for_commands(row: &cairn_db::turso::Row) -> DbResult<Memory> {
    memory_from_row_inner(row)
}

fn memory_from_row_inner(row: &cairn_db::turso::Row) -> DbResult<Memory> {
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

async fn load_memory_conn(conn: &cairn_db::turso::Connection, memory_id: &str) -> DbResult<Memory> {
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

/// Claim a batch of pending memories AND record the batch that owns them, in one
/// transaction.
///
/// Ownership has to be atomic with the claim. A memory that is `claimed` with no
/// batch link is a memory no batch owns, and every sweep that keys off `claimed`
/// — merge finalization, close release, orphan recovery — is then free to act on
/// it: a batch spawned while an earlier merged batch still linked the memory could
/// have that older batch apply its decision and take the memory back before the
/// new link existed. Callers therefore create the triage issue first and claim
/// through here, so a memory is `claimed` and owned together or neither.
///
/// `false` means the pool moved between the caller's read and this claim — at
/// least one memory is no longer `pending` — and nothing was written.
pub(crate) async fn claim_and_link_pending_batch(
    db: &LocalDb,
    issue_id: &str,
    memory_ids: &[String],
) -> DbResult<bool> {
    if memory_ids.is_empty() {
        return Ok(false);
    }
    let issue_id = issue_id.to_string();
    let memory_ids = memory_ids.to_vec();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        let memory_ids = memory_ids.clone();
        Box::pin(async move {
            // Verify the whole batch before writing any of it, so losing the race
            // leaves the pool exactly as it was.
            for memory_id in &memory_ids {
                let mut rows = conn
                    .query(
                        "SELECT status FROM memories WHERE id = ?1",
                        params![memory_id.as_str()],
                    )
                    .await?;
                let still_pending = match rows.next().await? {
                    Some(row) => row.text(0)? == "pending",
                    None => false,
                };
                if !still_pending {
                    return Ok(false);
                }
            }
            for memory_id in &memory_ids {
                conn.execute(
                    "UPDATE memories SET status = 'claimed', updated_at = ?1 WHERE id = ?2",
                    params![now, memory_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT OR IGNORE INTO memory_triage_issue_memories (issue_id, memory_id) \
                     VALUES (?1, ?2)",
                    params![issue_id.as_str(), memory_id.as_str()],
                )
                .await?;
            }
            Ok(true)
        })
    })
    .await
}

pub(crate) async fn set_memories_status(
    db: &LocalDb,
    ids: &[String],
    status: &str,
) -> DbResult<()> {
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

pub(crate) async fn load_memories_for_job(db: &LocalDb, job_id: &str) -> DbResult<Vec<Memory>> {
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
pub(crate) async fn role_for_job(db: &LocalDb, job_id: &str) -> DbResult<Option<String>> {
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
            // A job's drafts become `pending` only once the work that produced
            // them lands: the job's issue must be merged, or the job must have
            // no owning issue (chat / ad-hoc runs, where "merged" is undefined).
            // Until then survivors stay `draft` and never enter the triage pool.
            let mut eligibility_rows = conn
                .query(
                    "SELECT 1 FROM jobs j
                     WHERE j.id = ?1
                       AND (j.issue_id IS NULL
                         OR EXISTS (
                           SELECT 1 FROM issues i
                           WHERE i.id = j.issue_id AND i.merged_at IS NOT NULL
                         ))
                     LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            if eligibility_rows.next().await?.is_none() {
                return Ok(Vec::new());
            }

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

/// Test-only batch linker. Production establishes a batch link ONLY through
/// [`claim_and_link_pending_batch`], where the link lands in the same transaction
/// as the claim; this seeds already-claimed fixtures directly.
#[cfg(test)]
pub(crate) async fn record_triage_issue_batch(
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

/// The memories a triage batch still owns: `claimed`, with this issue as their
/// latest batch link (see [`LATEST_TRIAGE_LINK`]).
///
/// This is the set a batch may act on — the ledger it renders, the decisions its
/// merge applies, the claims its close releases. Memories that already reached a
/// terminal status, or that a later batch has since claimed, are deliberately
/// excluded even though their link rows remain.
/// The ownership query, on a caller-supplied connection.
///
/// A write path MUST select through this inside its own `db.write` transaction
/// rather than reading first and updating after: ownership can change between a
/// read and a later write, and an update keyed only by memory id would apply a
/// decision to a memory a newer batch had taken in between.
async fn claimed_batch_memories_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Vec<Memory>> {
    let sql = format!(
        "SELECT {MEMORY_COLUMNS} FROM memories m
         JOIN memory_triage_issue_memories tm ON tm.memory_id = m.id
         WHERE tm.issue_id = ?1 AND m.status = 'claimed' AND {LATEST_TRIAGE_LINK}
         ORDER BY tm.rowid ASC, m.created_at ASC, m.id ASC"
    );
    let mut rows = conn.query(&sql, params![issue_id]).await?;
    let mut memories = Vec::new();
    while let Some(row) = rows.next().await? {
        memories.push(memory_from_row(&row)?);
    }
    Ok(memories)
}

pub(crate) async fn claimed_batch_memories_for_issue(
    db: &LocalDb,
    issue_id: &str,
) -> DbResult<Vec<Memory>> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move { claimed_batch_memories_conn(conn, &issue_id).await })
    })
    .await
}

/// Resolve a project reference — either a `projects.id` or a project key such as
/// `CAIRN` — to the project id, or `None` when it names no project in this
/// database.
///
/// Both `memories.project_id` (an enforced foreign key) and a project-scope
/// memory's `scope_value` hold project *ids*, while agents and humans name
/// projects by key. Every project reference on its way into a memory row passes
/// through here so an id is what actually gets stored.
async fn project_id_for_reference_conn(
    conn: &cairn_db::turso::Connection,
    reference: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT id FROM projects WHERE id = ?1 OR key = ?1 LIMIT 1",
            params![reference],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.text(0)?)),
        None => Ok(None),
    }
}

pub(crate) async fn project_id_for_reference(
    db: &LocalDb,
    reference: &str,
) -> DbResult<Option<String>> {
    let reference = reference.to_string();
    db.read(|conn| {
        let reference = reference.clone();
        Box::pin(async move { project_id_for_reference_conn(conn, &reference).await })
    })
    .await
}

pub(crate) async fn record_triage_decision(
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

pub(crate) async fn clear_triage_decisions(db: &LocalDb, ids: &[String]) -> DbResult<()> {
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

pub(crate) async fn set_memories_promoted_commit_sha(
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

/// The corrected pool a `defer` decision returns a memory to.
struct DeferRescopeTarget {
    scope: MemoryScope,
    scope_value: String,
    project_id: String,
}

/// Where a `defer` decision sends the memory, or `None` when it should be parked
/// as `deferred` instead.
///
/// `None` covers two cases that are the same outcome: the integrator recorded no
/// corrected scope, or the scope it did record names no project in this database.
/// The second case must never be written: `memories.project_id` is an enforced
/// foreign key, so a dangling target aborts the whole batch transaction and the
/// reconcile sweep then retries and re-warns forever (CAIRN-3289). Parking is the
/// explicit, terminal, human-visible alternative.
async fn defer_rescope_target(
    conn: &cairn_db::turso::Connection,
    memory: &Memory,
) -> DbResult<Option<DeferRescopeTarget>> {
    let Some(scope) = memory.deferred_scope.clone() else {
        return Ok(None);
    };
    let recorded = memory
        .deferred_scope_value
        .as_deref()
        .unwrap_or(memory.scope_value.as_str());
    let (pool_value, project_reference) = match scope {
        // A project pool is keyed by project id, so its pool value and its owning
        // project are both the resolution below.
        MemoryScope::Project => (None, recorded.to_string()),
        MemoryScope::Workspace => (Some("workspace".to_string()), "workspace".to_string()),
        // CAIRN-1493 supplies role-pool routing; until then a role defer keeps the
        // memory's owning project as its home.
        MemoryScope::Role => (
            Some(recorded.to_string()),
            memory
                .project_id
                .clone()
                .unwrap_or_else(|| "workspace".to_string()),
        ),
    };
    let Some(project_id) = project_id_for_reference_conn(conn, &project_reference).await? else {
        log::warn!(
            "memory triage: memory {} defers to {scope}={project_reference}, which names no \
             project in this database; parking it as deferred",
            memory.id
        );
        return Ok(None);
    };
    Ok(Some(DeferRescopeTarget {
        scope,
        scope_value: pool_value.unwrap_or_else(|| project_id.clone()),
        project_id,
    }))
}

/// Apply a merged batch's recorded decisions to the memories that batch still
/// owns.
///
/// Ownership is selected inside the same transaction as the updates, so a memory
/// a newer batch has taken since cannot be resolved by this one. Every branch
/// writing `project_id` — an enforced foreign key — resolves it to a live project
/// first; an unresolvable defer target parks its memory rather than aborting its
/// siblings. Re-running this for the same issue is a no-op: resolved memories are
/// no longer `claimed`, so they are no longer owned.
pub(crate) async fn resolve_triage_batch_on_merge(
    db: &LocalDb,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let issue_id = issue_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        let mut ids = Vec::new();
        Box::pin(async move {
            let memories = claimed_batch_memories_conn(conn, &issue_id).await?;
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
                        match defer_rescope_target(conn, memory).await? {
                            // The decision is consumed by re-pooling, so it is
                            // cleared: a stale `defer` left on a `pending` row
                            // would be re-applied by the next merge even when the
                            // new batch recorded nothing. `reason` stays as the
                            // durable note of why the memory moved pools.
                            Some(target) => {
                                let scope = target.scope.to_string();
                                conn.execute(
                                    "UPDATE memories
                                     SET status = 'pending', scope = ?1, scope_value = ?2,
                                         project_id = ?3, triage_decision = NULL,
                                         deferred_scope = NULL, deferred_scope_value = NULL,
                                         updated_at = ?4
                                     WHERE id = ?5",
                                    params![
                                        scope.as_str(),
                                        target.scope_value.as_str(),
                                        target.project_id.as_str(),
                                        now,
                                        memory.id.as_str()
                                    ],
                                )
                                .await?;
                            }
                            None => {
                                conn.execute(
                                    "UPDATE memories SET status = 'deferred', updated_at = ?1 WHERE id = ?2",
                                    params![now, memory.id.as_str()],
                                )
                                .await?;
                            }
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

pub(crate) async fn draft_memory_job_ids_for_issue(
    db: &LocalDb,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let issue_id = issue_id.to_string();
    db.query_all(
        "SELECT DISTINCT m.job_id
         FROM memories m
         JOIN jobs j ON j.id = m.job_id
         WHERE m.status = 'draft' AND j.issue_id = ?1
         ORDER BY m.job_id ASC",
        params![issue_id.as_str()],
        |row| row.text(0),
    )
    .await
}

pub(crate) async fn discard_draft_memories_for_closed_issue(
    db: &LocalDb,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let issue_id = issue_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT m.id
                     FROM memories m
                     JOIN jobs j ON j.id = m.job_id
                     WHERE m.status = 'draft' AND j.issue_id = ?1
                     ORDER BY m.created_at ASC, m.id ASC",
                    params![issue_id.as_str()],
                )
                .await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push(row.text(0)?);
            }
            for id in &ids {
                conn.execute(
                    "UPDATE memories
                     SET status = 'discarded',
                         reason = COALESCE(reason, 'owning issue closed without merge'),
                         updated_at = ?1
                     WHERE id = ?2 AND status = 'draft'",
                    params![now, id.as_str()],
                )
                .await?;
            }
            Ok(ids)
        })
    })
    .await
}

pub(crate) async fn discard_draft_memories_for_closed_issues(
    db: &LocalDb,
) -> DbResult<Vec<String>> {
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT m.id
                     FROM memories m
                     JOIN jobs j ON j.id = m.job_id
                     JOIN issues i ON i.id = j.issue_id
                     WHERE m.status = 'draft' AND i.closed_at IS NOT NULL
                     ORDER BY m.created_at ASC, m.id ASC",
                    (),
                )
                .await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push(row.text(0)?);
            }
            for id in &ids {
                conn.execute(
                    "UPDATE memories
                     SET status = 'discarded',
                         reason = COALESCE(reason, 'owning issue closed without merge'),
                         updated_at = ?1
                     WHERE id = ?2 AND status = 'draft'",
                    params![now, id.as_str()],
                )
                .await?;
            }
            Ok(ids)
        })
    })
    .await
}

/// Release the claims a closing triage batch still holds, so its memories
/// re-enter their pending pool with no decision recorded. Ownership is selected
/// inside the releasing transaction: a memory this issue once linked but that has
/// since been resolved, or claimed by a later batch, is not this batch's to reset.
pub(crate) async fn revert_triage_batch_on_close(
    db: &LocalDb,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let issue_id = issue_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let memories = claimed_batch_memories_conn(conn, &issue_id).await?;
            let mut ids = Vec::new();
            for memory in &memories {
                ids.push(memory.id.clone());
                conn.execute(
                    "UPDATE memories
                     SET status = 'pending', triage_decision = NULL, reason = NULL,
                         promoted_commit_sha = NULL, deferred_scope = NULL,
                         deferred_scope_value = NULL, updated_at = ?1
                     WHERE id = ?2 AND status = 'claimed'",
                    params![now, memory.id.as_str()],
                )
                .await?;
            }
            Ok(ids)
        })
    })
    .await
}

/// Distinct `(scope, scope_value)` pools among `pending` memories. Drives the
/// reconciliation sweep's scope discovery directly from DB state rather than
/// from a caller-supplied confirmed list.
pub(crate) async fn distinct_pending_scopes(db: &LocalDb) -> DbResult<Vec<(String, String)>> {
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
pub(crate) async fn terminal_jobs_with_draft_memories(
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

/// Count open memory-triage issues that own a batch for this exact scope.
/// Open issues are not merged, closed, or failed.
pub(crate) async fn count_open_triage_issues_for_scope(
    db: &LocalDb,
    scope: &str,
    scope_value: &str,
) -> DbResult<i64> {
    db.query_one(
        "SELECT COUNT(DISTINCT i.id) FROM issues i \
         JOIN memory_triage_issue_memories tm ON tm.issue_id = i.id \
         JOIN memories m ON m.id = tm.memory_id \
         WHERE m.scope = ?1 AND m.scope_value = ?2 \
           AND i.merged_at IS NULL AND i.closed_at IS NULL AND i.status != 'failed'",
        params![scope, scope_value],
        |row| row.i64(0),
    )
    .await
}

/// Revert to `pending` every `claimed` memory with no row in
/// `memory_triage_issue_memories` — claimed but never linked to a triage issue
/// (the clean "no issue to begin with" signal, since the link is only written
/// after the triage issue is successfully created). Returns the reverted ids.
pub(crate) async fn revert_orphaned_claimed_memories(db: &LocalDb) -> DbResult<Vec<String>> {
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
/// stay `claimed` forever, stranding those memories outside their pending pool.
/// Revert those still-`claimed` memories to `pending`, clearing any decision
/// recorded before the failure, so the batch re-enters its pool. Returns the
/// reverted ids.
pub(crate) async fn revert_claimed_for_failed_triage_issues(db: &LocalDb) -> DbResult<Vec<String>> {
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        Box::pin(async move {
            let sql = format!(
                "SELECT m.id FROM memories m \
                 JOIN memory_triage_issue_memories tm ON tm.memory_id = m.id \
                 JOIN issues i ON i.id = tm.issue_id \
                 WHERE m.status = 'claimed' \
                   AND i.status = 'failed' \
                   AND i.merged_at IS NULL AND i.closed_at IS NULL \
                   AND {LATEST_TRIAGE_LINK} \
                 ORDER BY m.id ASC"
            );
            let mut rows = conn.query(&sql, ()).await?;
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
pub(crate) async fn merged_triage_issues_with_claimed_memories(
    db: &LocalDb,
) -> DbResult<Vec<String>> {
    db.read(|conn| {
        Box::pin(async move {
            let sql = format!(
                "SELECT DISTINCT tm.issue_id FROM memory_triage_issue_memories tm \
                 JOIN memories m ON m.id = tm.memory_id \
                 JOIN issues i ON i.id = tm.issue_id \
                 WHERE m.status = 'claimed' AND i.merged_at IS NOT NULL \
                   AND {LATEST_TRIAGE_LINK} \
                 ORDER BY tm.issue_id ASC"
            );
            let mut rows = conn.query(&sql, ()).await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push(row.text(0)?);
            }
            Ok(ids)
        })
    })
    .await
}

#[derive(Debug, Clone)]
pub struct MemoryTriageNeighbor {
    pub memory: Memory,
    pub uri: String,
    pub(crate) similarity: f32,
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

pub(crate) async fn project_key_by_id(db: &LocalDb, project_id: &str) -> DbResult<String> {
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

pub(crate) async fn project_name_by_id(db: &LocalDb, project_id: &str) -> DbResult<String> {
    let project_id = project_id.to_string();
    db.query_one(
        "SELECT name FROM projects \
         WHERE id = ?1 OR (?1 = 'workspace' AND is_workspace = 1) \
         ORDER BY CASE WHEN id = ?1 THEN 0 ELSE 1 END LIMIT 1",
        params![project_id.as_str()],
        |row| row.text(0),
    )
    .await
}

pub(crate) async fn backfill_workspace_project_id(db: &LocalDb) -> DbResult<u64> {
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
        insert_memory_with_status_and_job(db, id, project_id, "pending", "job-main", created_at)
            .await;
    }

    async fn insert_memory_with_status_and_job(
        db: &LocalDb,
        id: &str,
        project_id: Option<&str>,
        status: &str,
        job_id: &str,
        created_at: i64,
    ) {
        db.write(|conn| {
            let id = id.to_string();
            let project_id = project_id.map(str::to_string);
            let status = status.to_string();
            let job_id = job_id.to_string();
            let (scope, scope_value) = match project_id.as_deref() {
                Some("workspace") | None => ("workspace".to_string(), "workspace".to_string()),
                Some(project_id) => ("project".to_string(), project_id.to_string()),
            };
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at) VALUES (?1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?8)",
                    params![id.as_str(), project_id.as_deref(), id.as_str(), status.as_str(), scope.as_str(), scope_value.as_str(), job_id.as_str(), created_at],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn pool_ids(db: &LocalDb, scope: &str, scope_value: &str) -> Vec<String> {
        pending_memories_for_scope(db, scope, scope_value, 10)
            .await
            .unwrap()
            .iter()
            .map(|memory| memory.id.clone())
            .collect()
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
    async fn unmerged_issue_job_keeps_drafts_out_of_pending_pool() {
        let db = test_db().await;
        insert_memory_with_status_and_job(
            &db,
            "draft-open",
            Some("project-1"),
            "draft",
            "job-main",
            1,
        )
        .await;

        let confirmed = confirm_draft_memories_for_job(&db, "job-main")
            .await
            .unwrap();

        assert!(confirmed.is_empty());
        assert_eq!(memory_status(&db, "draft-open").await, "draft");
    }

    #[tokio::test]
    async fn no_issue_job_confirms_drafts_immediately() {
        let db = test_db().await;
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-chat', 'recipe', NULL, 'project-1', 'complete', 2, 2)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at)
             VALUES ('job-chat', 'exec-chat', NULL, 'project-1', 'complete', 'chat', 'chat', 2, 2)",
            (),
        )
        .await
        .unwrap();
        insert_memory_with_status_and_job(
            &db,
            "draft-chat",
            Some("project-1"),
            "draft",
            "job-chat",
            2,
        )
        .await;

        let confirmed = confirm_draft_memories_for_job(&db, "job-chat")
            .await
            .unwrap();

        assert_eq!(
            confirmed
                .iter()
                .map(|memory| memory.id.as_str())
                .collect::<Vec<_>>(),
            vec!["draft-chat"]
        );
        assert_eq!(memory_status(&db, "draft-chat").await, "pending");
    }

    #[tokio::test]
    async fn merged_issue_job_confirms_drafts() {
        let db = test_db().await;
        insert_memory_with_status_and_job(
            &db,
            "draft-merged",
            Some("project-1"),
            "draft",
            "job-main",
            1,
        )
        .await;
        db.execute(
            "UPDATE issues SET merged_at = 10 WHERE id = 'issue-main'",
            (),
        )
        .await
        .unwrap();

        let confirmed = confirm_draft_memories_for_job(&db, "job-main")
            .await
            .unwrap();

        assert_eq!(
            confirmed
                .iter()
                .map(|memory| memory.id.as_str())
                .collect::<Vec<_>>(),
            vec!["draft-merged"]
        );
        assert_eq!(memory_status(&db, "draft-merged").await, "pending");
    }

    #[tokio::test]
    async fn draft_job_lookup_and_close_discard_are_issue_scoped() {
        let db = test_db().await;
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-other', 'project-1', 43, 'Other', 'active', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-other', 'recipe', 'issue-other', 'project-1', 'complete', 2, 2)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at)
             VALUES ('job-other', 'exec-other', 'issue-other', 'project-1', 'complete', 'builder', 'builder', 2, 2)",
            (),
        )
        .await
        .unwrap();
        insert_memory_with_status_and_job(
            &db,
            "draft-main",
            Some("project-1"),
            "draft",
            "job-main",
            1,
        )
        .await;
        insert_memory_with_status_and_job(
            &db,
            "draft-other",
            Some("project-1"),
            "draft",
            "job-other",
            2,
        )
        .await;

        assert_eq!(
            draft_memory_job_ids_for_issue(&db, "issue-main")
                .await
                .unwrap(),
            vec!["job-main"]
        );

        let discarded = discard_draft_memories_for_closed_issue(&db, "issue-main")
            .await
            .unwrap();
        assert_eq!(discarded, vec!["draft-main"]);
        let memory = load_memory(&db, "draft-main").await.unwrap();
        assert_eq!(memory.status, MemoryStatus::Discarded);
        assert_eq!(
            memory.reason.as_deref(),
            Some("owning issue closed without merge")
        );
        assert_eq!(memory_status(&db, "draft-other").await, "draft");
    }

    #[tokio::test]
    async fn reconcile_close_discard_finds_closed_owning_issues() {
        let db = test_db().await;
        insert_memory_with_status_and_job(
            &db,
            "draft-closed",
            Some("project-1"),
            "draft",
            "job-main",
            1,
        )
        .await;
        db.execute(
            "UPDATE issues SET closed_at = 10 WHERE id = 'issue-main'",
            (),
        )
        .await
        .unwrap();

        let discarded = discard_draft_memories_for_closed_issues(&db).await.unwrap();

        assert_eq!(discarded, vec!["draft-closed"]);
        assert_eq!(memory_status(&db, "draft-closed").await, "discarded");
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

    /// The claim takes the batch it was given or writes nothing, and the ownership
    /// link lands with it in the same transaction.
    #[tokio::test]
    async fn claim_and_link_takes_the_whole_batch_or_none_of_it() {
        let db = test_db().await;
        insert_memory(&db, "one", Some("workspace"), 1).await;
        insert_memory(&db, "two", Some("workspace"), 2).await;
        insert_memory(&db, "three", Some("workspace"), 3).await;

        let candidates = pending_memories_for_scope(&db, "workspace", "workspace", 2)
            .await
            .unwrap();
        assert_eq!(
            candidates.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["one", "two"],
            "the pool is read oldest-first"
        );
        let ids: Vec<String> = candidates.iter().map(|m| m.id.clone()).collect();

        // Another claimer takes part of the batch before this claim lands.
        set_memories_status(&db, &["two".to_string()], "claimed")
            .await
            .unwrap();
        assert!(!claim_and_link_pending_batch(&db, "issue-main", &ids)
            .await
            .unwrap());
        assert_eq!(memory_status(&db, "one").await, "pending");
        assert!(claimed_batch_memories_for_issue(&db, "issue-main")
            .await
            .unwrap()
            .is_empty());

        set_memories_status(&db, &["two".to_string()], "pending")
            .await
            .unwrap();
        assert!(claim_and_link_pending_batch(&db, "issue-main", &ids)
            .await
            .unwrap());
        assert_eq!(memory_status(&db, "one").await, "claimed");
        assert_eq!(memory_status(&db, "two").await, "claimed");
        assert_eq!(memory_status(&db, "three").await, "pending");
        assert_eq!(
            claimed_batch_memories_for_issue(&db, "issue-main")
                .await
                .unwrap()
                .iter()
                .map(|memory| memory.id.clone())
                .collect::<Vec<_>>(),
            ids,
            "the claim is what makes this issue the batch's owner"
        );
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

        assert_eq!(pool_ids(&db, "role", "coordinator").await, vec!["role-two"]);

        let project_batch = pool_ids(&db, "project", "project-1").await;
        assert_eq!(project_batch, vec!["project-one", "project-two"]);
        assert!(
            claim_and_link_pending_batch(&db, "issue-main", &project_batch)
                .await
                .unwrap()
        );
        assert_eq!(memory_status(&db, "role-one").await, "pending");
        assert_eq!(memory_status(&db, "role-two").await, "pending");

        // A role pool spans projects: both `builder` memories claim together.
        let role_batch = pool_ids(&db, "role", "builder").await;
        assert_eq!(role_batch, vec!["role-one", "role-other-project"]);
        assert!(claim_and_link_pending_batch(&db, "issue-main", &role_batch)
            .await
            .unwrap());
        assert_eq!(memory_status(&db, "role-two").await, "pending");
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

    /// Seed a merged triage issue owning a two-memory batch: one `defer` carrying
    /// the given project target, and one `discard` sibling that proves the batch
    /// transaction survived.
    async fn merged_batch_with_project_defer(db: &LocalDb, target: &str) {
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at, merged_at)
             VALUES ('issue-merged', 'project-1', 9, 'Memory triage', 'merged', 1, 1, 10)",
            (),
        )
        .await
        .unwrap();
        for (seq, id) in ["rescoped", "sibling"].into_iter().enumerate() {
            create_memory(
                db,
                id,
                Some(id),
                id,
                Some("workspace"),
                "workspace",
                "workspace",
                Some("job-main"),
                Some(700 + seq as i64),
                None,
            )
            .await
            .unwrap();
        }
        let ids = vec!["rescoped".to_string(), "sibling".to_string()];
        set_memories_status(db, &ids, "claimed").await.unwrap();
        record_triage_issue_batch(db, "issue-merged", &ids)
            .await
            .unwrap();
        record_triage_decision(
            db,
            "rescoped",
            MemoryTriageDecision::Defer,
            "belongs to the project pool",
            Some(MemoryScope::Project),
            Some(target),
        )
        .await
        .unwrap();
        record_triage_decision(
            db,
            "sibling",
            MemoryTriageDecision::Discard,
            "noise",
            None,
            None,
        )
        .await
        .unwrap();
    }

    /// A defer naming its project by KEY finalizes into the project's id.
    /// `memories.project_id` is an enforced foreign key, so storing the key itself
    /// aborted the whole batch transaction and left every sibling `claimed`, which
    /// the reconcile sweep then re-attempted and re-warned on forever
    /// (CAIRN-3289).
    #[tokio::test]
    async fn merged_batch_finalizes_a_defer_that_names_its_project_by_key() {
        let db = test_db().await;
        merged_batch_with_project_defer(&db, "PRJ").await;

        let resolved = resolve_triage_batch_on_merge(&db, "issue-merged")
            .await
            .unwrap();

        assert_eq!(resolved, vec!["rescoped", "sibling"]);
        let rescoped = load_memory(&db, "rescoped").await.unwrap();
        assert_eq!(rescoped.status, MemoryStatus::Pending);
        assert_eq!(rescoped.scope, MemoryScope::Project);
        assert_eq!(rescoped.scope_value, "project-1");
        assert_eq!(rescoped.project_id.as_deref(), Some("project-1"));
        // The decision is consumed by the re-pooling, so a later merge cannot
        // re-apply it; the reason survives as the note of why it moved.
        assert!(rescoped.triage_decision.is_none());
        assert!(rescoped.deferred_scope.is_none());
        assert!(rescoped.deferred_scope_value.is_none());
        assert_eq!(
            rescoped.reason.as_deref(),
            Some("belongs to the project pool")
        );
        assert_eq!(memory_status(&db, "sibling").await, "discarded");
    }

    /// A defer target naming no live project is parked as `deferred` — explicit,
    /// terminal, and visible — instead of failing the batch's foreign key.
    #[tokio::test]
    async fn merged_batch_parks_a_defer_whose_target_project_is_missing() {
        let db = test_db().await;
        merged_batch_with_project_defer(&db, "ghost-project").await;

        let resolved = resolve_triage_batch_on_merge(&db, "issue-merged")
            .await
            .unwrap();

        assert_eq!(resolved, vec!["rescoped", "sibling"]);
        let rescoped = load_memory(&db, "rescoped").await.unwrap();
        assert_eq!(rescoped.status, MemoryStatus::Deferred);
        assert_eq!(
            rescoped.triage_decision,
            Some(MemoryTriageDecision::Defer),
            "a parked defer keeps its decision: the decision still stands"
        );
        assert_eq!(memory_status(&db, "sibling").await, "discarded");
        // Nothing is left claimed on the merged issue, so the sweep is done with it.
        assert!(merged_triage_issues_with_claimed_memories(&db)
            .await
            .unwrap()
            .is_empty());
    }

    /// Only the batch that currently owns a memory may resolve it. A memory
    /// released back to `pending` and re-claimed by a later batch keeps its older
    /// link rows, and those historical batches must not act on it — otherwise every
    /// merged batch a stuck memory ever belonged to reports as its own failing
    /// batch, which is what made one defect warn across dozens of issues per sweep.
    #[tokio::test]
    async fn a_historical_merged_batch_does_not_own_a_re_claimed_memory() {
        let db = test_db().await;
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at, merged_at)
             VALUES ('issue-old', 'project-1', 11, 'Memory triage', 'merged', 1, 1, 10)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-open', 'project-1', 12, 'Memory triage', 'active', 2, 2)",
            (),
        )
        .await
        .unwrap();
        create_memory(
            &db,
            "undecided",
            Some("undecided"),
            "undecided",
            Some("project-1"),
            "project",
            "project-1",
            Some("job-main"),
            Some(800),
            None,
        )
        .await
        .unwrap();
        let ids = vec!["undecided".to_string()];

        // The old batch merged with no decision recorded, so it released the
        // memory back to its pending pool.
        set_memories_status(&db, &ids, "claimed").await.unwrap();
        record_triage_issue_batch(&db, "issue-old", &ids)
            .await
            .unwrap();
        assert_eq!(
            resolve_triage_batch_on_merge(&db, "issue-old")
                .await
                .unwrap(),
            ids
        );
        assert_eq!(memory_status(&db, "undecided").await, "pending");

        // A later, still-open batch claims it.
        set_memories_status(&db, &ids, "claimed").await.unwrap();
        record_triage_issue_batch(&db, "issue-open", &ids)
            .await
            .unwrap();

        assert!(
            merged_triage_issues_with_claimed_memories(&db)
                .await
                .unwrap()
                .is_empty(),
            "the merged issue no longer owns the memory, so it is not pending finalization"
        );
        assert!(resolve_triage_batch_on_merge(&db, "issue-old")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            memory_status(&db, "undecided").await,
            "claimed",
            "the open batch keeps its claim"
        );
        assert_eq!(
            claimed_batch_memories_for_issue(&db, "issue-open")
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn project_reference_resolves_by_key_and_by_id() {
        let db = test_db().await;

        assert_eq!(
            project_id_for_reference(&db, "PRJ")
                .await
                .unwrap()
                .as_deref(),
            Some("project-1")
        );
        assert_eq!(
            project_id_for_reference(&db, "project-1")
                .await
                .unwrap()
                .as_deref(),
            Some("project-1")
        );
        assert!(project_id_for_reference(&db, "nope")
            .await
            .unwrap()
            .is_none());
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

    #[tokio::test]
    async fn project_name_by_id_resolves_workspace_name_dynamically() {
        let db = test_db().await;
        assert_eq!(
            project_name_by_id(&db, "workspace").await.unwrap(),
            "Workspace"
        );
        assert_eq!(
            project_name_by_id(&db, "project-1").await.unwrap(),
            "Project"
        );
    }
}
