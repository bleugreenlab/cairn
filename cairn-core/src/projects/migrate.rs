use cairn_common::ids::rekey_to_team;
use cairn_db::turso::Value;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::db::DbState;
use crate::projects::crud::{get_db, insert_project_route};
use crate::services::Clock;
use crate::storage::{DbError, LocalDb, RowExt, PROJECT_REKEY_MANIFEST};
use crate::CairnError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum MoveProjectGateFailure {
    TeamNotOpen { team_id: String },
    ProjectNotLocal { project_id: String },
    KeyCollision { key: String, team_id: String },
    NotQuiesced { blocking_jobs: Vec<BlockingJob> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlockingJob {
    pub id: String,
    pub status: String,
    pub issue_number: Option<i64>,
    pub node_name: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum MoveProjectError {
    #[error("move rejected: {0:?}")]
    Gate(MoveProjectGateFailure),
    #[error(transparent)]
    Cairn(#[from] CairnError),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("{0}")]
    Internal(String),
}

pub type MoveProjectResult<T> = Result<T, MoveProjectError>;

#[derive(Debug, Clone)]
struct TableCopySpec {
    table: &'static str,
    where_sql: &'static str,
    skip_copy: bool,
}

#[derive(Debug, Clone)]
struct DeferredReferenceUpdate {
    table: &'static str,
    column: String,
    id: String,
    value: String,
}

const COPY_ORDER: &[TableCopySpec] = &[
    spec("projects", "id = ?1"),
    skip("labels", "1 = 0"),
    spec("action_configs", "project_id = ?1"),
    spec("skill_configs", "project_id = ?1"),
    spec("issues", "project_id = ?1"),
    spec("comments", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("doc_references", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("issue_dependencies", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("issue_labels", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("issue_workspaces", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("executions", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("condition_evaluations", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("execution_history", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("jobs", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("artifact_content", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("artifacts", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("checkpoint_command_cache", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("checkpoint_runs", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("file_changes", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("job_browsers", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("job_terminals", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("memories", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("runs", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("turns", "run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("events", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("prompts", "run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("permission_requests", "run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("tool_invocations", "run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR event_id IN (SELECT id FROM events WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
    spec("tool_invocation_runs", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("token_rollup", "project_id = ?1 OR run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("token_rollup_runs", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("message_streams", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("message_stream_chunks", "stream_id IN (SELECT id FROM message_streams WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
    spec("sessions", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR chat_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("session_skyline_cache", "session_id IN (SELECT id FROM sessions WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("resource_surfacings", "session_id IN (SELECT id FROM sessions WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("execution_trigger_sources", "source_job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR triggered_execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("merge_requests", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("action_runs", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1) OR parent_job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("pr_node_port_fires", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("wake_subscriptions", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("suppressed_wakes", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR subscription_id IN (SELECT id FROM wake_subscriptions WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("todos", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("workflow_progress", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("attention_pushes", "recipient IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("attention_read_cursors", "recipient IN (SELECT id FROM jobs WHERE project_id = ?1) OR source IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("event_read_tokens", "event_id IN (SELECT id FROM events WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
    spec("event_vibes", "event_id IN (SELECT id FROM events WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
    spec("messages", "channel_id = ?1 OR channel_id IN (SELECT id FROM issues WHERE project_id = ?1) OR sender_run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR recipient_run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("queued_messages", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("memory_triage_issue_memories", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR memory_id IN (SELECT id FROM memories WHERE project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("search_outbox", "source_id = ?1 OR source_id IN (SELECT id FROM issues WHERE project_id = ?1) OR source_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR source_id IN (SELECT id FROM runs WHERE project_id = ?1) OR source_id IN (SELECT id FROM events WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
];

const fn spec(table: &'static str, where_sql: &'static str) -> TableCopySpec {
    TableCopySpec {
        table,
        where_sql,
        skip_copy: false,
    }
}

const fn skip(table: &'static str, where_sql: &'static str) -> TableCopySpec {
    TableCopySpec {
        table,
        where_sql,
        skip_copy: true,
    }
}

pub async fn move_project_to_team(
    dbs: &DbState,
    clock: &dyn Clock,
    project_id: &str,
    team_id: &str,
) -> MoveProjectResult<()> {
    let team_db = dbs.team_db(team_id).await.ok_or_else(|| {
        MoveProjectError::Gate(MoveProjectGateFailure::TeamNotOpen {
            team_id: team_id.to_string(),
        })
    })?;
    let local_project =
        get_db(&dbs.local, project_id)
            .await?
            .ok_or_else(|| CairnError::NotFound {
                entity: "project",
                id: project_id.to_string(),
            })?;
    if project_id.contains('~') {
        return Err(MoveProjectError::Gate(
            MoveProjectGateFailure::ProjectNotLocal {
                project_id: project_id.to_string(),
            },
        ));
    }
    let project_key = local_project.key.clone();
    let team_project_id = rekey_to_team(project_id, team_id)
        .map_err(|e| MoveProjectError::Internal(e.to_string()))?;
    if team_project_exists(&team_db, &project_key, &team_project_id).await? {
        return Err(MoveProjectError::Gate(
            MoveProjectGateFailure::KeyCollision {
                key: project_key,
                team_id: team_id.to_string(),
            },
        ));
    }
    let blocking_jobs = blocking_jobs(&dbs.local, project_id).await?;
    if !blocking_jobs.is_empty() {
        return Err(MoveProjectError::Gate(
            MoveProjectGateFailure::NotQuiesced { blocking_jobs },
        ));
    }

    copy_project_graph(&dbs.local, &team_db, project_id, team_id).await?;
    verify_team_prefixes(&team_db, team_id, &team_project_id).await?;
    insert_project_route(&dbs.local, clock, &local_project.key, Some(team_id)).await?;
    dbs.set_route(&local_project.key, Some(team_id.to_string()))
        .await;
    sweep_source(&dbs.local, project_id).await?;
    mark_search_pending(&team_db, &team_project_id).await?;
    let _ = team_db.push().await;
    Ok(())
}

async fn team_project_exists(db: &LocalDb, key: &str, expected_id: &str) -> Result<bool, DbError> {
    let key = key.to_string();
    let expected_id = expected_id.to_string();
    db.query_opt(
        "SELECT id FROM projects WHERE UPPER(key) = UPPER(?1) LIMIT 1",
        (key,),
        |row| row.text(0),
    )
    .await
    .map(|found| found.is_some_and(|id| id != expected_id))
}

async fn blocking_jobs(db: &LocalDb, project_id: &str) -> Result<Vec<BlockingJob>, DbError> {
    let project_id = project_id.to_string();
    db.query_all(
        "SELECT j.id, j.status, i.number, j.node_name
         FROM jobs j
         LEFT JOIN issues i ON i.id = j.issue_id
         WHERE j.project_id = ?1
           AND j.status NOT IN ('complete', 'failed', 'cancelled')
         ORDER BY j.updated_at DESC",
        (project_id,),
        |row| {
            Ok(BlockingJob {
                id: row.text(0)?,
                status: row.text(1)?,
                issue_number: row.opt_i64(2)?,
                node_name: row.opt_text(3)?,
            })
        },
    )
    .await
}

async fn copy_project_graph(
    source: &LocalDb,
    target: &LocalDb,
    project_id: &str,
    team_id: &str,
) -> MoveProjectResult<()> {
    let target_columns = table_columns(target).await?;
    let manifests = manifest_map();
    let mut deferred_updates = Vec::new();
    for spec in COPY_ORDER {
        if spec.skip_copy {
            continue;
        }
        let Some(columns) = target_columns.get(spec.table).cloned() else {
            continue;
        };
        let rows = load_rows(source, spec.table, spec.where_sql, project_id, &columns).await?;
        if rows.is_empty() {
            continue;
        }
        let id_columns = manifests.get(spec.table).ok_or_else(|| {
            MoveProjectError::Internal(format!("missing re-key manifest for {}", spec.table))
        })?;
        deferred_updates
            .extend(insert_rows(target, spec.table, &columns, rows, id_columns, team_id).await?);
    }
    apply_deferred_reference_updates(target, deferred_updates).await?;
    Ok(())
}

fn manifest_map() -> HashMap<&'static str, HashSet<&'static str>> {
    PROJECT_REKEY_MANIFEST
        .iter()
        .map(|entry| (entry.table, entry.id_columns.iter().copied().collect()))
        .collect()
}

async fn table_columns(db: &LocalDb) -> Result<HashMap<String, Vec<String>>, DbError> {
    db.read(|conn| {
        Box::pin(async move {
            let mut tables = conn
                .query("SELECT name FROM sqlite_master WHERE type='table'", ())
                .await?;
            let mut out = HashMap::new();
            while let Some(row) = tables.next().await? {
                let table = row.text(0)?;
                let pragma = format!("PRAGMA table_info({table})");
                let mut cols = conn.query(&pragma, ()).await?;
                let mut names = Vec::new();
                while let Some(col) = cols.next().await? {
                    names.push(col.text(1)?);
                }
                out.insert(table, names);
            }
            Ok(out)
        })
    })
    .await
}

async fn load_rows(
    db: &LocalDb,
    table: &str,
    where_sql: &str,
    project_id: &str,
    columns: &[String],
) -> Result<Vec<Vec<Value>>, DbError> {
    let select_columns = columns
        .iter()
        .map(|column| {
            if table == "projects" && column == "team_id" {
                "workspace_id AS team_id".to_string()
            } else {
                column.clone()
            }
        })
        .collect::<Vec<_>>();
    let sql = format!(
        "SELECT {} FROM {table} WHERE {where_sql}",
        select_columns.join(", ")
    );
    let project_id = project_id.to_string();
    let column_count = columns.len();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(&sql, (project_id.as_str(),)).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let mut values = Vec::with_capacity(column_count);
                for idx in 0..column_count {
                    values.push(row.get_value(idx)?);
                }
                out.push(values);
            }
            Ok(out)
        })
    })
    .await
}

async fn insert_rows(
    db: &LocalDb,
    table: &'static str,
    columns: &[String],
    rows: Vec<Vec<Value>>,
    id_columns: &HashSet<&'static str>,
    team_id: &str,
) -> MoveProjectResult<Vec<DeferredReferenceUpdate>> {
    let placeholders = (1..=columns.len())
        .map(|idx| format!("?{idx}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT OR REPLACE INTO {table} ({}) VALUES ({placeholders})",
        columns.join(", ")
    );
    let columns = columns.to_vec();
    let team_id = team_id.to_string();
    let id_columns = id_columns.clone();
    let deferred_columns = deferred_reference_columns(table);
    db.write(|conn| {
        let sql = sql.clone();
        let columns = columns.clone();
        let rows = rows.clone();
        let team_id = team_id.clone();
        let id_columns = id_columns.clone();
        let deferred_columns = deferred_columns.to_vec();
        Box::pin(async move {
            let id_idx = if deferred_columns.is_empty() {
                None
            } else {
                Some(
                    columns
                        .iter()
                        .position(|column| column == "id")
                        .ok_or_else(|| DbError::Internal(format!("{table} has no id column")))?,
                )
            };
            let mut deferred_updates = Vec::new();
            for mut values in rows {
                for (idx, column) in columns.iter().enumerate() {
                    if id_columns.contains(column.as_str()) {
                        if let Value::Text(value) = &values[idx] {
                            values[idx] =
                                Value::Text(rekey_to_team(value, &team_id).map_err(|e| {
                                    DbError::Internal(format!("re-keying {table}.{column}: {e}"))
                                })?);
                        }
                    }
                    if table == "projects" && column == "team_id" {
                        values[idx] = Value::Text(team_id.clone());
                    }
                    if table == "executions" && column == "snapshot" {
                        // The move only re-keys structural id columns plus this typed
                        // execution snapshot. Other JSON/text payloads are display or
                        // audit content for this slice and intentionally stay untouched.
                        if let Value::Text(json) = &values[idx] {
                            let mut snapshot = crate::config::snapshot_migrate::load(json)
                                .map_err(|e| {
                                    DbError::Internal(format!(
                                        "deserializing execution snapshot: {e}"
                                    ))
                                })?;
                            snapshot.rekey_to_team(&team_id).map_err(|e| {
                                DbError::Internal(format!("re-keying execution snapshot: {e}"))
                            })?;
                            values[idx] =
                                Value::Text(snapshot.to_json().map_err(DbError::Internal)?);
                        }
                    }
                    if deferred_columns.contains(&column.as_str()) {
                        if let Value::Text(value) = &values[idx] {
                            if !value.is_empty() {
                                let id_idx = id_idx.ok_or_else(|| {
                                    DbError::Internal(format!(
                                        "{table} deferred reference missing id column"
                                    ))
                                })?;
                                let id = match &values[id_idx] {
                                    Value::Text(id) => id.clone(),
                                    _ => {
                                        return Err(DbError::Internal(format!(
                                            "{table}.id is not text"
                                        )))
                                    }
                                };
                                deferred_updates.push(DeferredReferenceUpdate {
                                    table,
                                    column: column.clone(),
                                    id,
                                    value: value.clone(),
                                });
                                values[idx] = Value::Null;
                            }
                        }
                    }
                }
                conn.execute(&sql, values.clone())
                    .await
                    .map_err(|error| DbError::Internal(format!("copying {table}: {error}")))?;
            }
            Ok(deferred_updates)
        })
    })
    .await
    .map_err(MoveProjectError::Db)
}

fn deferred_reference_columns(table: &'static str) -> &'static [&'static str] {
    match table {
        // These columns can point to rows copied later in the same table or in a
        // later table. Turso enforces foreign keys immediately, so copy with the
        // reference temporarily cleared and restore it once the whole graph exists.
        "artifacts" => &["parent_version_id"],
        "issues" => &["parent_issue_id", "parent_job_id"],
        "jobs" => &["parent_job_id", "current_turn_id", "resume_session_id"],
        "sessions" => &["replaced_by_id", "parent_session_id"],
        "turns" => &["predecessor_id"],
        _ => &[],
    }
}

async fn apply_deferred_reference_updates(
    db: &LocalDb,
    updates: Vec<DeferredReferenceUpdate>,
) -> Result<(), DbError> {
    if updates.is_empty() {
        return Ok(());
    }
    db.write(|conn| {
        let updates = updates.clone();
        Box::pin(async move {
            for update in updates {
                let sql = format!(
                    "UPDATE {} SET {} = ?1 WHERE id = ?2",
                    update.table, update.column
                );
                let updated = conn
                    .execute(&sql, (update.value.as_str(), update.id.as_str()))
                    .await
                    .map_err(|error| {
                        DbError::Internal(format!(
                            "backfilling {}.{} for {}: {error}",
                            update.table, update.column, update.id
                        ))
                    })?;
                if updated != 1 {
                    return Err(DbError::Internal(format!(
                        "backfilling {}.{} for {} updated {updated} rows",
                        update.table, update.column, update.id
                    )));
                }
            }
            Ok(())
        })
    })
    .await
}

async fn verify_team_prefixes(
    db: &LocalDb,
    team_id: &str,
    project_id: &str,
) -> MoveProjectResult<()> {
    let manifests = manifest_map();
    for spec in COPY_ORDER {
        if spec.skip_copy {
            continue;
        }
        let Some(columns) = manifests.get(spec.table) else {
            continue;
        };
        for column in columns {
            let sql = format!(
                "SELECT {column} FROM {} WHERE {} AND {column} IS NOT NULL",
                spec.table, spec.where_sql
            );
            let values = db
                .query_all(sql, (project_id.to_string(),), |row| row.opt_text(0))
                .await?;
            for value in values.into_iter().flatten() {
                if value.starts_with(&format!("{team_id}~")) {
                    continue;
                }
                if value.contains('~') || is_canonical_uuid(&value) {
                    return Err(MoveProjectError::Internal(format!(
                        "{}.{column} contains mis-routed id {value}",
                        spec.table
                    )));
                }
            }
        }
    }
    Ok(())
}

fn is_canonical_uuid(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| uuid.to_string() == value)
}

async fn mark_search_pending(db: &LocalDb, project_id: &str) -> Result<(), DbError> {
    let project_id = project_id.to_string();
    db.execute(
        "UPDATE search_outbox SET status = 'pending'
         WHERE source_id = ?1
            OR source_id IN (SELECT id FROM issues WHERE project_id = ?1)
            OR source_id IN (SELECT id FROM jobs WHERE project_id = ?1)
            OR source_id IN (SELECT id FROM runs WHERE project_id = ?1)",
        (project_id,),
    )
    .await?;
    Ok(())
}

async fn sweep_source(db: &LocalDb, project_id: &str) -> Result<(), DbError> {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
        conn.execute("UPDATE jobs SET current_turn_id = NULL, resume_session_id = NULL WHERE project_id = ?1", (project_id.as_str(),)).await?;
        conn.execute("UPDATE turns SET predecessor_id = NULL WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR run_id IN (SELECT id FROM runs WHERE project_id = ?1)", (project_id.as_str(),)).await?;
        conn.execute("UPDATE sessions SET replaced_by_id = NULL, parent_session_id = NULL WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1)", (project_id.as_str(),)).await?;
        conn.execute("UPDATE artifacts SET parent_version_id = NULL WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1)", (project_id.as_str(),)).await?;
        for spec in COPY_ORDER.iter().rev() {
            if spec.table == "projects" || spec.table == "labels" { continue; }
            let sql = format!("DELETE FROM {} WHERE {}", spec.table, spec.where_sql);
            conn.execute(&sql, (project_id.as_str(),)).await.map_err(|error| {
                DbError::Internal(format!("sweeping {}: {error}", spec.table))
            })?;
        }
        conn.execute("DELETE FROM projects WHERE id = ?1", (project_id.as_str(),)).await.map_err(|error| {
            DbError::Internal(format!("sweeping projects: {error}"))
        })?;
        Ok(())
    })
    }).await
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::sync::Arc;

    use cairn_common::ids::rekey_to_team;
    use tempfile::tempdir;

    use super::*;
    use crate::storage::{MigrationRunner, SearchIndex, TEAM_MIGRATIONS, TURSO_MIGRATIONS};

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> i64 {
            1_704_067_200
        }

        fn now_u64(&self) -> u64 {
            1_704_067_200
        }
    }

    async fn migrated_db(name: &str, migrations: Vec<crate::storage::Migration>) -> LocalDb {
        let temp = tempdir().unwrap();
        let path = temp.keep().join(name);
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(migrations).run(&db).await.unwrap();
        db
    }

    #[tokio::test]
    async fn move_project_to_team_backfills_forward_self_references() {
        let team_id = "team-move-test";
        let project_id = "11111111-1111-4111-8111-111111111111";
        let issue_id = "22222222-2222-4222-8222-222222222222";
        let child_issue_id = "33333333-3333-4333-8333-333333333333";
        let execution_id = "44444444-4444-4444-8444-444444444444";
        let job_id = "55555555-5555-4555-8555-555555555555";
        let run_id = "66666666-6666-4666-8666-666666666666";
        let first_session_id = "77777777-7777-4777-8777-777777777777";
        let resumed_session_id = "88888888-8888-4888-8888-888888888888";
        let first_turn_id = "99999999-9999-4999-8999-999999999999";
        let current_turn_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        let artifact_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
        let artifact_version_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";

        let local = Arc::new(migrated_db("move-local.db", TURSO_MIGRATIONS.to_vec()).await);
        let team = Arc::new(migrated_db("move-team.db", TEAM_MIGRATIONS.to_vec()).await);
        team.execute(
            "INSERT INTO teams(id, name, created_at, updated_at)
             VALUES (?1, 'Move Test Team', 1, 1)",
            (team_id,),
        )
        .await
        .unwrap();

        local
            .execute(
                "INSERT INTO teams(id, name, sync_url, replica_path, created_at)
                 VALUES (?1, 'Move Test Team', 'http://localhost', '/tmp/team.db', 1)",
                (team_id,),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO workspaces(id, name, created_at, updated_at)
                 VALUES ('workspace-1', 'Workspace', 1, 1)",
                (),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES (?1, 'workspace-1', 'Move Project', 'MOVE', '/tmp/move', 1, 1)",
                (project_id,),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                 VALUES (?1, ?2, 1, 'Parent', 'closed', 1, 1)",
                (issue_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO issues(id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
                 VALUES (?1, ?2, 2, 'Child', 'closed', ?3, 1, 1)",
                (child_issue_id, project_id, issue_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, completed_at, seq)
                 VALUES (?1, 'recipe', ?2, ?3, 'completed', 1, 2, 1)",
                (execution_id, issue_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, status, created_at, updated_at, completed_at)
                 VALUES (?1, ?2, ?3, ?4, 'complete', 1, 2, 3)",
                (job_id, execution_id, issue_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "UPDATE issues SET parent_job_id = ?1 WHERE id = ?2",
                (job_id, child_issue_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO runs(id, issue_id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'completed', 'bare-session-id', 1, 2)",
                (run_id, issue_id, project_id, job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 1, 'complete', 1, 1)",
                (first_turn_id, first_session_id, run_id, job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, predecessor_id, state, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 2, ?5, 'complete', 2, 2)",
                (current_turn_id, resumed_session_id, run_id, job_id, first_turn_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES (?1, ?2, 'codex', 'closed', 1, 1, 2)",
                (first_session_id, job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO sessions(id, job_id, backend, status, parent_session_id, sequence, created_at, updated_at)
                 VALUES (?1, ?2, 'codex', 'closed', ?3, 2, 2, 3)",
                (resumed_session_id, job_id, first_session_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "UPDATE sessions SET replaced_by_id = ?1 WHERE id = ?2",
                (resumed_session_id, first_session_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "UPDATE jobs SET current_turn_id = ?1, resume_session_id = ?2 WHERE id = ?3",
                (current_turn_id, resumed_session_id, job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, created_at, updated_at)
                 VALUES (?1, ?2, 'plan', '{}', 1, 1)",
                (artifact_id, job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, parent_version_id, created_at, updated_at)
                 VALUES (?1, ?2, 'plan', '{}', ?3, 2, 2)",
                (artifact_version_id, job_id, artifact_id),
            )
            .await
            .unwrap();

        let search_dir = tempdir().unwrap();
        let dbs = DbState::new(
            local.clone(),
            Arc::new(SearchIndex::open_or_create(search_dir.path()).unwrap()),
        );
        dbs.insert_team_db_for_test(team_id, team.clone()).await;

        move_project_to_team(&dbs, &FixedClock, project_id, team_id)
            .await
            .unwrap();

        let team_project_id = rekey_to_team(project_id, team_id).unwrap();
        let team_job_id = rekey_to_team(job_id, team_id).unwrap();
        let team_first_session_id = rekey_to_team(first_session_id, team_id).unwrap();
        let team_resumed_session_id = rekey_to_team(resumed_session_id, team_id).unwrap();
        let team_first_turn_id = rekey_to_team(first_turn_id, team_id).unwrap();
        let team_current_turn_id = rekey_to_team(current_turn_id, team_id).unwrap();
        let team_artifact_id = rekey_to_team(artifact_id, team_id).unwrap();
        let team_artifact_version_id = rekey_to_team(artifact_version_id, team_id).unwrap();
        let team_child_issue_id = rekey_to_team(child_issue_id, team_id).unwrap();

        assert_eq!(
            team.query_text(
                "SELECT team_id FROM projects WHERE id = ?1",
                (team_project_id.clone(),)
            )
            .await
            .unwrap(),
            Some(team_id.to_string())
        );
        assert_eq!(
            team.query_text(
                "SELECT current_turn_id FROM jobs WHERE id = ?1",
                (team_job_id.clone(),)
            )
            .await
            .unwrap(),
            Some(team_current_turn_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT resume_session_id FROM jobs WHERE id = ?1",
                (team_job_id,)
            )
            .await
            .unwrap(),
            Some(team_resumed_session_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT replaced_by_id FROM sessions WHERE id = ?1",
                (team_first_session_id.clone(),)
            )
            .await
            .unwrap(),
            Some(team_resumed_session_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT parent_session_id FROM sessions WHERE id = ?1",
                (team_resumed_session_id.clone(),)
            )
            .await
            .unwrap(),
            Some(team_first_session_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT predecessor_id FROM turns WHERE id = ?1",
                (team_current_turn_id.clone(),)
            )
            .await
            .unwrap(),
            Some(team_first_turn_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT parent_version_id FROM artifacts WHERE id = ?1",
                (team_artifact_version_id,)
            )
            .await
            .unwrap(),
            Some(team_artifact_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT parent_job_id FROM issues WHERE id = ?1",
                (team_child_issue_id,)
            )
            .await
            .unwrap(),
            Some(rekey_to_team(job_id, team_id).unwrap())
        );
        assert_eq!(
            local
                .query_opt_text("SELECT id FROM projects WHERE id = ?1", (project_id,))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            local
                .query_text(
                    "SELECT team_id FROM project_routes WHERE project_key = 'MOVE'",
                    ()
                )
                .await
                .unwrap(),
            Some(team_id.to_string())
        );
    }
}
