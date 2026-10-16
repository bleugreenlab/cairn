use cairn_common::ids::rekey_to_team;
use cairn_db::turso::{Connection, Value};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::db::DbState;
use crate::projects::crud::{get_db, insert_project_route};
use crate::services::Clock;
use crate::storage::{DbError, LocalDb, RowExt, PROJECT_REKEY_MANIFEST, TABLE_SCOPES};
use crate::CairnError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum MoveProjectGateFailure {
    TeamNotOpen { team_id: String },
    ProjectNotLocal { project_id: String },
    KeyCollision { key: String, team_id: String },
    NotQuiesced { blocking_jobs: Vec<BlockingJob> },
}

#[cfg(all(test, feature = "test-utils"))]
pub(crate) async fn sweep_project_channel_surfaces(
    private_db: &LocalDb,
    project_key: &str,
) -> Result<(), DbError> {
    let project_key = cairn_common::uri::canonical_project(project_key);
    private_db
        .write(|conn| {
            let project_key = project_key.clone();
            Box::pin(async move { sweep_channel_surfaces(conn, project_key.as_str()).await })
        })
        .await
}

async fn sweep_channel_surfaces(conn: &Connection, project_key: &str) -> Result<(), DbError> {
    for spec in CHANNEL_KEY_ADDRESSED {
        let sql = format!("DELETE FROM {} WHERE {}", spec.table, spec.sweep_where_sql);
        conn.execute(&sql, (project_key,))
            .await
            .map_err(|error| DbError::Internal(format!("sweeping {}: {error}", spec.table)))?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct KeyAddressedSpec {
    table: &'static str,
    sweep_where_sql: &'static str,
}

/// Channel and Discord ledger rows that name a project by key, or by a
/// `cairn://p/<key>/…` URI, rather than through a foreign key to `projects`.
/// The schema-derived `COPY_ORDER` coverage test cannot see them by
/// construction, which is how they survived project deletion.
///
/// Every table here is private-lineage: its rows live in the per-install
/// database whichever database owns the project row. That is why this sweep
/// takes the private handle explicitly instead of riding the project graph's
/// owning-database connection, just as `DbState::remove_project_route` deletes
/// its route from `self.local`.
///
/// Each predicate binds `?1` to the canonical lowercase project key.
const CHANNEL_KEY_ADDRESSED: &[KeyAddressedSpec] = &[
    KeyAddressedSpec {
        table: "channel_state_card",
        sweep_where_sql: "project_key = ?1",
    },
    KeyAddressedSpec {
        table: "channel_conversation_binding",
        sweep_where_sql: "target_uri = 'cairn://p/' || ?1 OR substr(target_uri, 1, length(?1) + 11) = 'cairn://p/' || ?1 || '/'",
    },
    // `ensure_issue_surface` builds category → thread_channel/unmanaged_home →
    // issue_thread. Delete one level at a time because `parent_surface_id` is a
    // self-reference without an ON DELETE action and row order is unspecified.
    KeyAddressedSpec {
        table: "discord_surface",
        sweep_where_sql: "project_key = ?1 AND surface_kind = 'issue_thread'",
    },
    KeyAddressedSpec {
        table: "discord_surface",
        sweep_where_sql: "project_key = ?1 AND surface_kind IN ('thread_channel','unmanaged_home')",
    },
    KeyAddressedSpec {
        table: "discord_surface",
        sweep_where_sql: "project_key = ?1",
    },
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlockingJob {
    id: String,
    status: String,
    issue_number: Option<i64>,
    node_name: Option<String>,
}

impl BlockingJob {
    /// A short, user-facing label for one blocking item, e.g. `issue #12 · builder`
    /// or `job 1a2b3c4d (advancing)` when no issue backs it.
    fn label(&self) -> String {
        let mut label = match self.issue_number {
            Some(number) => format!("issue #{number}"),
            None => format!("job {}", &self.id[..self.id.len().min(8)]),
        };
        if let Some(node) = &self.node_name {
            label.push_str(" · ");
            label.push_str(node);
        }
        if self.status == ADVANCING_STATUS {
            label.push_str(" (advancing)");
        }
        label
    }
}

/// Synthetic `BlockingJob::status` for an execution with undelivered DAG
/// advancement (an outbox row, not a job). Distinguishes it in the surfaced list.
const ADVANCING_STATUS: &str = "advancing";

impl std::fmt::Display for MoveProjectGateFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The variant name stays at the head of every message: the frontend
        // matches on it as a stable token to pick a localized sentence.
        match self {
            Self::TeamNotOpen { team_id } => {
                write!(f, "TeamNotOpen: team {team_id} is not open for syncing")
            }
            Self::ProjectNotLocal { project_id } => {
                write!(f, "ProjectNotLocal: {project_id} is already a team project")
            }
            Self::KeyCollision { key, team_id } => write!(
                f,
                "KeyCollision: team {team_id} already has a project with key {key}"
            ),
            Self::NotQuiesced { blocking_jobs } => {
                // A bare enumerated list: the frontend owns the surrounding
                // "can't move until this finishes" sentence, so restating the
                // remedy here would read redundantly once wrapped.
                let labels: Vec<String> = blocking_jobs.iter().map(BlockingJob::label).collect();
                write!(f, "NotQuiesced: {}", labels.join(", "))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MoveProjectError {
    #[error("move rejected: {0}")]
    Gate(MoveProjectGateFailure),
    #[error(transparent)]
    Cairn(#[from] CairnError),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("{0}")]
    Internal(String),
}

pub type MoveProjectResult<T> = Result<T, MoveProjectError>;

/// Which database a manifest entry's rows physically live in.
///
/// Resolved from cairn-db's `TABLE_SCOPES`, the single declaration of lineage in
/// the codebase. The sweep derives lineage rather than restating it, so this
/// manifest stays one table list with two consumers instead of becoming a second
/// list that can drift out of agreement with the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableLineage {
    /// Lives in whichever database owns the project row: the private database
    /// for a local project, the team replica for a team project.
    Shared,
    /// Lives only in the per-install database, whichever database owns the
    /// project row. The team lineage is exactly the projection of the shared
    /// tables, so a team replica does not have this table at all.
    Private,
}

fn table_lineage(table: &str) -> Result<TableLineage, DbError> {
    TABLE_SCOPES
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, scope)| {
            if scope.lives_in_team() {
                TableLineage::Shared
            } else {
                TableLineage::Private
            }
        })
        .ok_or_else(|| {
            DbError::Internal(format!(
                "project sweep: `{table}` has no TABLE_SCOPES classification, so the \
                 database holding its rows is undeclared"
            ))
        })
}

/// A shared-lineage id set that private-lineage rows are addressed through.
///
/// Private tables name their owner with a bare id (`thread_id`, `job_id`,
/// `run_id`) and carry no `project_id`, so the only way to find their
/// project-owned rows is to ask the shared lineage which ids belong to the
/// project. For a LOCAL project both lineages are one database and that question
/// is a subquery; for a TEAM project the shared rows live in the replica while
/// the private rows stay here, so the ids are read from the replica first and
/// bound into the delete. Naming the id set — rather than writing the subquery
/// out — is what lets one predicate serve both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OwnerSet {
    Threads,
    Jobs,
    Runs,
    Prompts,
    PermissionRequests,
}

impl OwnerSet {
    const ALL: [OwnerSet; 5] = [
        OwnerSet::Threads,
        OwnerSet::Jobs,
        OwnerSet::Runs,
        OwnerSet::Prompts,
        OwnerSet::PermissionRequests,
    ];

    const fn table(self) -> &'static str {
        match self {
            OwnerSet::Threads => "threads",
            OwnerSet::Jobs => "jobs",
            OwnerSet::Runs => "runs",
            OwnerSet::Prompts => "prompts",
            OwnerSet::PermissionRequests => "permission_requests",
        }
    }

    /// The manifest's own answer to "which of this table's rows belong to the
    /// project". Reusing the shared entry's predicate keeps the id sets and the
    /// shared sweep from ever disagreeing about what the project owns.
    fn select_ids_sql(self) -> String {
        let spec = COPY_ORDER
            .iter()
            .find(|spec| spec.table == self.table())
            .expect("every OwnerSet names a COPY_ORDER table");
        format!(
            "SELECT id FROM {} WHERE {}",
            self.table(),
            spec.local_where_sql()
        )
    }
}

/// The marker an owner-addressed clause leaves for its id set.
const IDS_MARKER: &str = "{ids}";

/// One OR'd term of a private entry's sweep predicate.
#[derive(Debug, Clone, Copy)]
struct OwnerClause {
    /// The id set this term addresses rows through, or `None` for a term that
    /// names the project directly and binds `?1`.
    owner: Option<OwnerSet>,
    /// SQL containing exactly one [`IDS_MARKER`] when `owner` is `Some`.
    sql: &'static str,
}

/// How an entry selects the rows a project owns.
#[derive(Debug, Clone, Copy)]
enum SweepPredicate {
    /// Binds `?1` = the project id and may subquery any table that lives in the
    /// same database as the entry itself.
    Direct(&'static str),
    /// OR'd clauses, each addressed through one shared-lineage id set. Written
    /// once and expanded two ways — to subqueries when both lineages share a
    /// database, to bound id lists when they do not — so the two paths cannot
    /// drift apart.
    Owned(&'static [OwnerClause]),
}

#[derive(Debug, Clone)]
struct TableCopySpec {
    table: &'static str,
    sweep: SweepPredicate,
    copy_where_sql: Option<&'static str>,
}

#[derive(Debug, Clone)]
struct DeferredReferenceUpdate {
    table: &'static str,
    column: String,
    id: String,
    value: String,
}

// One ownership manifest serves both move-to-team and project deletion. Entries
// are parent-before-child so copy can walk forward and deletion can walk in
// reverse. Private-only tables have no copy predicate; mixed-scope tables may
// copy a narrower replicated subset while sweeping every locally owned row.
const COPY_ORDER: &[TableCopySpec] = &[
    spec("projects", "id = ?1"),
    delete_only("labels", "1 = 0"),
    spec("action_configs", "project_id = ?1"),
    spec("skill_configs", "project_id = ?1"),
    spec("threads", "project_id = ?1"),
    owned("thread_read_positions", &[owner(OwnerSet::Threads, "thread_id IN {ids}")]),
    owned("wake_schedules", &[owner(OwnerSet::Threads, "home_kind = 'thread' AND home_id IN {ids}")]),
    owned("wake_conditions", &[owner(OwnerSet::Threads, "home_kind = 'thread' AND home_id IN {ids}")]),
    spec("issues", "project_id = ?1"),
    spec("image_refs", "project_id = ?1"),
    delete_only("attachments", "project_id = ?1 OR thread_id IN (SELECT id FROM threads WHERE project_id = ?1)"),
    split_scope(
        "comments",
        "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)",
        "issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR thread_id IN (SELECT id FROM threads WHERE project_id = ?1)",
    ),
    spec("doc_references", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("issue_labels", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("issue_workspaces", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("executions", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    spec("condition_evaluations", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("execution_history", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("jobs", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    // Runner-local rebuild notices, addressed through their recipient jobs.
    // Their private cross-scope foreign key is why the private half must run
    // before the shared half deletes those jobs; the two-half ordering is what
    // guarantees that, so no pre-sweep special case is needed.
    owned("build_change_notifications", &[owner(OwnerSet::Jobs, "recipient IN {ids}")]),
    owned("feed_cursors", &[owner(OwnerSet::Threads, "(home_kind = 'thread' AND home_id IN {ids})"), owner(OwnerSet::Jobs, "(home_kind = 'job' AND home_id IN {ids})")]),
    spec("thread_branches", "thread_id IN (SELECT id FROM threads WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)))"),
    spec("turn_end_check_attempts", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("artifact_content", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("artifacts", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("checkpoint_command_cache", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("checkpoint_runs", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("file_changes", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("job_browsers", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("job_repls", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("repl_exchanges", "project_id = ?1 OR repl_id IN (SELECT id FROM job_repls WHERE project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("job_terminals", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("memories", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("runs", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    owned("codex_watchdog_leases", &[owner(OwnerSet::Runs, "run_id IN {ids}"), owner(OwnerSet::Runs, "successor_run_id IN {ids}")]),
    owned("codex_watchdog_lifecycle", &[owner(OwnerSet::Runs, "run_id IN {ids}"), owner(OwnerSet::Runs, "successor_run_id IN {ids}")]),
    spec("turns", "run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("provider_limit_holds", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))) OR run_id IN (SELECT id FROM runs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("events", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("prompts", "run_id IN (SELECT id FROM runs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)))) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)))"),
    spec("permission_requests", "run_id IN (SELECT id FROM runs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)))) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)))"),
    owned("channel_outbound", &[owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    // Terminal ask rows whose prompt has already been pruned are intentionally
    // unreachable here. They are inert; live ask rows retain either their
    // prompt or permission request and are swept through that owner.
    owned("channel_ask_resolution", &[owner(OwnerSet::Prompts, "action_ref IN {ids}"), owner(OwnerSet::PermissionRequests, "action_ref IN {ids}")]),
    owned("channel_ask_action", &[owner(OwnerSet::Prompts, "action_ref IN {ids}"), owner(OwnerSet::PermissionRequests, "action_ref IN {ids}")]),
    spec("tool_invocations", "run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR event_id IN (SELECT id FROM events WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
    spec("tool_invocation_runs", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("token_rollup", "project_id = ?1 OR run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("token_rollup_runs", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("message_streams", "run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    spec("message_stream_chunks", "stream_id IN (SELECT id FROM message_streams WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
    spec("sessions", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR chat_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    delete_only("agent_waits", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR session_id IN (SELECT id FROM sessions WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("session_skyline_cache", "session_id IN (SELECT id FROM sessions WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("resource_surfacings", "session_id IN (SELECT id FROM sessions WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("execution_trigger_sources", "source_job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR triggered_execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    spec("merge_requests", "project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    delete_only("merge_attempt_events", "project_id = ?1 OR merge_request_id IN (SELECT id FROM merge_requests WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
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
    split_scope(
        "messages",
        "channel_id = ?1 OR channel_id IN (SELECT id FROM issues WHERE project_id = ?1) OR sender_run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR recipient_run_id IN (SELECT id FROM runs WHERE project_id = ?1)",
        "channel_id = ?1 OR channel_id IN (SELECT id FROM issues WHERE project_id = ?1) OR channel_id IN (SELECT id FROM threads WHERE project_id = ?1) OR sender_run_id IN (SELECT id FROM runs WHERE project_id = ?1) OR recipient_run_id IN (SELECT id FROM runs WHERE project_id = ?1)",
    ),
    spec("queued_messages", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    spec("memory_triage_issue_memories", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR memory_id IN (SELECT id FROM memories WHERE project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    spec("search_outbox", "source_id = ?1 OR source_id IN (SELECT id FROM issues WHERE project_id = ?1) OR source_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR source_id IN (SELECT id FROM runs WHERE project_id = ?1) OR source_id IN (SELECT id FROM events WHERE run_id IN (SELECT id FROM runs WHERE project_id = ?1))"),
    delete_only("check_result_observations", "project_id = ?1"),
    delete_only("check_test_results", "observation_id IN (SELECT id FROM check_result_observations WHERE project_id = ?1)"),
    delete_only("check_result_commit_aliases", "project_id = ?1 OR source_observation_id IN (SELECT id FROM check_result_observations WHERE project_id = ?1)"),
    delete_only("check_result_cache", "project_id = ?1 OR source_observation_id IN (SELECT id FROM check_result_observations WHERE project_id = ?1)"),
    delete_only("codemap_cache", "project_id = ?1"),
    delete_only("jj_reconcile_intents", "project_id = ?1"),
    delete_only("jj_reconcile_items", "intent_id IN (SELECT id FROM jj_reconcile_intents WHERE project_id = ?1)"),
    delete_only("jj_reconcile_incoming_files", "intent_id IN (SELECT id FROM jj_reconcile_intents WHERE project_id = ?1)"),
    delete_only("jj_reconcile_quarantines", "project_id = ?1"),
    delete_only("pack_catalog", "project_id = ?1"),
    delete_only("pack_catalog_references", "project_id = ?1"),
    delete_only("posts", "project_id = ?1"),
    delete_only("post_comments", "post_id IN (SELECT id FROM posts WHERE project_id = ?1)"),
    delete_only("contended_identifier_obligations", "project_id = ?1"),
];

const fn spec(table: &'static str, where_sql: &'static str) -> TableCopySpec {
    TableCopySpec {
        table,
        sweep: SweepPredicate::Direct(where_sql),
        copy_where_sql: Some(where_sql),
    }
}

const fn delete_only(table: &'static str, sweep_where_sql: &'static str) -> TableCopySpec {
    TableCopySpec {
        table,
        sweep: SweepPredicate::Direct(sweep_where_sql),
        copy_where_sql: None,
    }
}

/// A private-lineage entry whose rows are addressed through shared-lineage id
/// sets. Never copied: nothing in the private lineage is replicated.
const fn owned(table: &'static str, clauses: &'static [OwnerClause]) -> TableCopySpec {
    TableCopySpec {
        table,
        sweep: SweepPredicate::Owned(clauses),
        copy_where_sql: None,
    }
}

const fn owner(set: OwnerSet, sql: &'static str) -> OwnerClause {
    OwnerClause {
        owner: Some(set),
        sql,
    }
}

const fn split_scope(
    table: &'static str,
    copy_where_sql: &'static str,
    sweep_where_sql: &'static str,
) -> TableCopySpec {
    TableCopySpec {
        table,
        sweep: SweepPredicate::Direct(sweep_where_sql),
        copy_where_sql: Some(copy_where_sql),
    }
}

impl TableCopySpec {
    fn lineage(&self) -> Result<TableLineage, DbError> {
        table_lineage(self.table)
    }

    /// The `DELETE`s this private entry issues against the per-install database.
    ///
    /// One statement when the id sets are readable as subqueries; one statement
    /// per clause per chunk of bound ids when they are not. Both expansions come
    /// from the same clause list, so what the two paths delete cannot diverge.
    fn private_statements(
        &self,
        project_id: &str,
        owner_ids: &OwnerIds,
    ) -> Result<Vec<(String, Vec<Value>)>, DbError> {
        let project_bind = || vec![Value::Text(project_id.to_string())];
        let sets = match owner_ids {
            OwnerIds::SameDatabase => {
                return Ok(vec![(
                    format!(
                        "DELETE FROM {} WHERE {}",
                        self.table,
                        self.local_where_sql()
                    ),
                    project_bind(),
                )])
            }
            OwnerIds::Materialized(sets) => sets,
        };
        let clauses = match self.sweep {
            // A private entry that names the project directly, or reaches its
            // rows only through other private tables, needs no translation: the
            // rows it subqueries are in this same database.
            SweepPredicate::Direct(sql) => {
                return Ok(vec![(
                    format!("DELETE FROM {} WHERE {sql}", self.table),
                    project_bind(),
                )])
            }
            SweepPredicate::Owned(clauses) => clauses,
        };
        let mut statements = Vec::new();
        for clause in clauses {
            let Some(set) = clause.owner else {
                statements.push((
                    format!("DELETE FROM {} WHERE {}", self.table, clause.sql),
                    project_bind(),
                ));
                continue;
            };
            let ids = sets.get(&set).ok_or_else(|| {
                DbError::Internal(format!(
                    "project sweep: no materialized {} ids for {}",
                    set.table(),
                    self.table
                ))
            })?;
            for chunk in ids.chunks(OWNER_ID_CHUNK) {
                let placeholders = (1..=chunk.len())
                    .map(|position| format!("?{position}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                statements.push((
                    format!(
                        "DELETE FROM {} WHERE {}",
                        self.table,
                        clause.sql.replace(IDS_MARKER, &format!("({placeholders})"))
                    ),
                    chunk.iter().cloned().map(Value::Text).collect(),
                ));
            }
        }
        Ok(statements)
    }

    /// The predicate to use when this entry's rows and every id set it addresses
    /// them through are in the same database.
    fn local_where_sql(&self) -> String {
        match self.sweep {
            SweepPredicate::Direct(sql) => sql.to_string(),
            SweepPredicate::Owned(clauses) => clauses
                .iter()
                .map(|clause| match clause.owner {
                    None => clause.sql.to_string(),
                    Some(set) => clause
                        .sql
                        .replace(IDS_MARKER, &format!("({})", set.select_ids_sql())),
                })
                .collect::<Vec<_>>()
                .join(" OR "),
        }
    }
}

pub async fn move_project_to_team(
    dbs: &DbState,
    clock: &dyn Clock,
    project_id: &str,
    team_id: &str,
    live_job_ids: &HashSet<String>,
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
    let blocking_jobs = blocking_work(&dbs.local, project_id, live_job_ids).await?;
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

/// The move-to-team quiesce gate's liveness query. The move re-roots every id in
/// the project graph with the team prefix (`copy_project_graph`) and then sweeps
/// the source, so any concurrent writer or in-memory handle still keyed to the
/// pre-move ids would corrupt or dangle. The gate therefore blocks the move on
/// GENUINELY live work — never on the durable non-terminal job-row population
/// (`pending` / `blocked` / `running`) that every real project accumulates and
/// keeps forever: parked resumable host waits and unstarted recipe nodes are a
/// designed steady state, not a signal of activity (see
/// `docs/turn-process-recovery.md`). Gating on those rows made the feature
/// unusable on any project that had ever done real work.
///
/// Two populations are genuinely live and block:
///
/// 1. **Jobs with a live in-memory agent process.** `live_job_ids` is threaded
///    in from the caller's `Orchestrator::process_state`, because in-memory
///    process liveness — not `job.status` — is the source of truth for "actually
///    running". EVERY registry entry counts, warm (live + idle) processes
///    included: a warm process still holds a backend session handle keyed to the
///    pre-move ids, so letting the move re-key and sweep beneath it would strand
///    that handle. Blocking on warm processes (rather than evicting them before
///    the copy) is the deliberate, fail-closed choice — it needs no
///    evict-then-copy race window, and a warm process idles out of existence on
///    its own via the warm-process GC, after which the move passes.
///
/// 2. **Executions with undelivered DAG advancement** — a non-terminal
///    (`pending` / `running`) `advance_dag` outbox row. That row is claimed or
///    queued advancement work between agent processes, and `effect_outbox` is
///    deliberately absent from `COPY_ORDER`, so proceeding would drop the
///    advance from the team copy and strand it against a swept-away source
///    execution. This closes the between-processes window where a DAG worker
///    advances an execution with no live process attached. A quiesced project
///    has none: the drain worker carries every advance to `done`, and startup
///    resets any `running` rows back to `pending` for replay.
async fn blocking_work(
    db: &LocalDb,
    project_id: &str,
    live_job_ids: &HashSet<String>,
) -> Result<Vec<BlockingJob>, DbError> {
    let mut blocking = Vec::new();
    for job_id in live_job_ids {
        if let Some(job) = live_process_job(db, project_id, job_id).await? {
            blocking.push(job);
        }
    }
    blocking.extend(advancing_executions(db, project_id).await?);
    Ok(blocking)
}

/// Resolve one live-process job id to a [`BlockingJob`], but only if it belongs
/// to the project being moved. The membership predicate mirrors the `jobs`
/// entry in [`COPY_ORDER`]: a job joins the move via its own `project_id`, its
/// issue, or its execution, so a live process on any of those blocks.
async fn live_process_job(
    db: &LocalDb,
    project_id: &str,
    job_id: &str,
) -> Result<Option<BlockingJob>, DbError> {
    db.query_opt(
        "SELECT j.id, j.status, i.number, j.node_name
         FROM jobs j
         LEFT JOIN issues i ON i.id = j.issue_id
         WHERE j.id = ?2
           AND (j.project_id = ?1
                OR j.issue_id IN (SELECT id FROM issues WHERE project_id = ?1)
                OR j.execution_id IN (
                    SELECT id FROM executions
                    WHERE project_id = ?1
                       OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1)))",
        (project_id.to_string(), job_id.to_string()),
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

/// Executions with undelivered DAG advancement queued in the effect outbox
/// (`advance_dag` rows are keyed by execution id). Reported as [`BlockingJob`]s
/// with the synthetic [`ADVANCING_STATUS`] so the surfaced list names them.
async fn advancing_executions(db: &LocalDb, project_id: &str) -> Result<Vec<BlockingJob>, DbError> {
    let project_id = project_id.to_string();
    db.query_all(
        "SELECT DISTINCT eo.dedupe_key, i.number
         FROM effect_outbox eo
         JOIN executions e ON e.id = eo.dedupe_key
         LEFT JOIN issues i ON i.id = e.issue_id
         WHERE eo.kind = 'advance_dag'
           AND eo.state IN ('pending', 'running')
           AND (e.project_id = ?1
                OR e.issue_id IN (SELECT id FROM issues WHERE project_id = ?1))",
        (project_id,),
        |row| {
            Ok(BlockingJob {
                id: row.text(0)?,
                status: ADVANCING_STATUS.to_string(),
                issue_number: row.opt_i64(1)?,
                node_name: None,
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
        let Some(copy_where_sql) = spec.copy_where_sql else {
            continue;
        };
        let Some(columns) = target_columns.get(spec.table).cloned() else {
            continue;
        };
        let rows = load_rows(source, spec.table, copy_where_sql, project_id, &columns).await?;
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
        "jobs" => &[
            "parent_job_id",
            "current_turn_id",
            "resume_session_id",
            "turn_end_check_attempt_id",
        ],
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
        let Some(copy_where_sql) = spec.copy_where_sql else {
            continue;
        };
        let Some(columns) = manifests.get(spec.table) else {
            continue;
        };
        for column in columns {
            let sql = format!(
                "SELECT {column} FROM {} WHERE {} AND {column} IS NOT NULL",
                spec.table, copy_where_sql
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

/// Sweep a project whose private and shared rows are both in `db`: every local
/// project, and the move path's source database.
pub(crate) async fn sweep_project(db: &LocalDb, project_id: &str) -> Result<(), DbError> {
    sweep_project_with_route(db, project_id, None).await
}

pub(crate) async fn sweep_project_with_route(
    db: &LocalDb,
    project_id: &str,
    project_key: Option<&str>,
) -> Result<(), DbError> {
    let project_id = project_id.to_string();
    let project_key = project_key.map(cairn_common::uri::canonical_project);
    db.write(|conn| {
        let project_id = project_id.clone();
        let project_key = project_key.clone();
        Box::pin(async move {
            // One database, so one transaction carries both halves and the
            // id sets are readable as subqueries.
            sweep_private_half(
                conn,
                project_id.as_str(),
                &OwnerIds::SameDatabase,
                project_key.as_deref(),
                project_key.as_deref(),
            )
            .await?;
            sweep_shared_half(conn, project_id.as_str()).await
        })
    })
    .await
}

/// Sweep only the shared half against a database that holds nothing else.
///
/// The team replica's lineage is exactly the shared tables, so this is the
/// complete and correct sweep for a team-scoped project id handed a bare team
/// handle — the project-create rollback path, where the project is new and owns
/// no private rows yet. A team project's accumulated private rows are swept by
/// [`sweep_team_project`], which has both databases to work with.
pub(crate) async fn sweep_project_shared_only(
    db: &LocalDb,
    project_id: &str,
) -> Result<(), DbError> {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move { sweep_shared_half(conn, project_id.as_str()).await })
    })
    .await
}

/// Delete a TEAM project's rows across the two databases that hold them.
///
/// There is no cross-database transaction, so the halves commit independently
/// and the ORDER is chosen for what a partial failure leaves behind. The private
/// half runs first: if it fails the team replica is untouched, and if the shared
/// half then fails the project row, its route, and the owner id sets are all
/// still there, so re-running delete finds the project, re-materializes the same
/// ids, no-ops through the already-empty private half, and retries the shared
/// one. The reverse order cannot recover — once the replica has dropped the
/// project row, `delete_routed` returns `NotFound` and the private rows it never
/// reached are unreachable forever.
pub(crate) async fn sweep_team_project(
    private_db: &LocalDb,
    team_db: &LocalDb,
    project_id: &str,
    project_key: &str,
) -> Result<(), DbError> {
    let owner_ids = materialize_owner_ids(team_db, project_id).await?;
    let id = project_id.to_string();
    let project_key = cairn_common::uri::canonical_project(project_key);
    private_db
        .write(|conn| {
            let id = id.clone();
            let owner_ids = owner_ids.clone();
            let project_key = project_key.clone();
            Box::pin(async move {
                sweep_private_half(
                    conn,
                    id.as_str(),
                    &owner_ids,
                    Some(project_key.as_str()),
                    None,
                )
                .await
            })
        })
        .await?;
    let id = project_id.to_string();
    team_db
        .write(|conn| {
            let id = id.clone();
            Box::pin(async move { sweep_shared_half(conn, id.as_str()).await })
        })
        .await
}

/// Read every owner id set from the database that holds the shared lineage.
async fn materialize_owner_ids(db: &LocalDb, project_id: &str) -> Result<OwnerIds, DbError> {
    let mut sets = HashMap::new();
    for set in OwnerSet::ALL {
        let project_id = project_id.to_string();
        let ids = db
            .query_all(set.select_ids_sql(), (project_id,), |row| row.text(0))
            .await
            .map_err(|error| {
                DbError::Internal(format!("materializing owned {} ids: {error}", set.table()))
            })?;
        sets.insert(set, ids);
    }
    Ok(OwnerIds::Materialized(sets))
}

/// Where the sweep reads owner id sets from.
#[derive(Debug, Clone)]
enum OwnerIds {
    /// Both lineages are one database: address owned rows by subquery.
    SameDatabase,
    /// The shared lineage is a different database: its ids were read up front
    /// and are bound into each delete.
    Materialized(HashMap<OwnerSet, Vec<String>>),
}

/// How many ids one statement binds. Deleting a large project's private rows
/// costs a few more round trips rather than one statement that could exceed the
/// engine's bound-parameter limit.
const OWNER_ID_CHUNK: usize = 500;

async fn sweep_source(db: &LocalDb, project_id: &str) -> Result<(), DbError> {
    sweep_project(db, project_id).await
}

/// Delete every private-lineage row the project owns. Always runs against the
/// per-install database, whichever database owns the project row.
async fn sweep_private_half(
    conn: &Connection,
    project_id: &str,
    owner_ids: &OwnerIds,
    channel_project_key: Option<&str>,
    route_project_key: Option<&str>,
) -> Result<(), DbError> {
    for spec in COPY_ORDER.iter().rev() {
        if spec.lineage()? != TableLineage::Private {
            continue;
        }
        for (sql, params) in spec.private_statements(project_id, owner_ids)? {
            conn.execute(&sql, params)
                .await
                .map_err(|error| DbError::Internal(format!("sweeping {}: {error}", spec.table)))?;
        }
    }
    // Key-addressed channel state is private too, but cannot be discovered from
    // the project-id graph. Sweep it in this same recoverable transaction before
    // either the project row or its route can disappear. The move path passes no
    // key, so moving a project deliberately preserves these rows.
    if let Some(project_key) = channel_project_key {
        sweep_channel_surfaces(conn, project_key).await?;
    }
    // The routing root is private too, and deleting it is what releases the key
    // for reuse. Only the local-delete path asks for it: a team project's route
    // is removed through `DbState::remove_project_route`, which also evicts the
    // in-memory routing entry.
    if let Some(project_key) = route_project_key {
        conn.execute(
            "DELETE FROM project_routes WHERE project_key = ?1",
            (project_key,),
        )
        .await
        .map_err(|error| DbError::Internal(format!("sweeping project_routes: {error}")))?;
    }
    Ok(())
}

/// Delete every shared-lineage row the project owns, ending with the project
/// row itself. Runs against whichever database owns the project.
async fn sweep_shared_half(conn: &Connection, project_id: &str) -> Result<(), DbError> {
    conn.execute("UPDATE jobs SET current_turn_id = NULL, resume_session_id = NULL, turn_end_check_attempt_id = NULL WHERE project_id = ?1", (project_id,)).await?;
    conn.execute("UPDATE turns SET predecessor_id = NULL WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1) OR run_id IN (SELECT id FROM runs WHERE project_id = ?1)", (project_id,)).await?;
    conn.execute("UPDATE sessions SET replaced_by_id = NULL, parent_session_id = NULL WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1)", (project_id,)).await?;
    conn.execute("UPDATE artifacts SET parent_version_id = NULL WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1)", (project_id,)).await?;
    for spec in COPY_ORDER.iter().rev() {
        if spec.table == "projects" || spec.table == "labels" {
            continue;
        }
        if spec.lineage()? != TableLineage::Shared {
            continue;
        }
        let sql = format!(
            "DELETE FROM {} WHERE {}",
            spec.table,
            spec.local_where_sql()
        );
        conn.execute(&sql, (project_id,))
            .await
            .map_err(|error| DbError::Internal(format!("sweeping {}: {error}", spec.table)))?;
    }
    conn.execute("DELETE FROM projects WHERE id = ?1", (project_id,))
        .await
        .map_err(|error| DbError::Internal(format!("sweeping projects: {error}")))?;
    Ok(())
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use cairn_common::ids::rekey_to_team;
    use tempfile::tempdir;

    use super::*;
    use crate::channels::discord::{DiscordApi, DiscordGuildPermissions, DiscordRemoteChannel};
    use crate::channels::discord_surfaces::DiscordSurfaceReconciler;
    use crate::storage::{
        MigrationRunner, SearchIndex, TableScope, TABLE_SCOPES, TEAM_MIGRATIONS, TURSO_MIGRATIONS,
    };

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

    async fn project_fk_tables(db: &LocalDb) -> Vec<String> {
        let mut tables = db
            .query_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND sql LIKE '%REFERENCES projects%'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        tables.sort();
        tables
    }

    #[derive(Default)]
    struct CountingDiscordApi {
        calls: AtomicUsize,
    }

    impl CountingDiscordApi {
        fn called(&self) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl DiscordApi for CountingDiscordApi {
        async fn inspect_guild_permissions(
            &self,
            _guild_id: u64,
        ) -> Result<DiscordGuildPermissions, String> {
            self.called();
            Ok(DiscordGuildPermissions {
                manage_channels: true,
                manage_threads: true,
                manage_messages: true,
                send_messages_in_threads: true,
            })
        }

        async fn inspect_channel(&self, channel_id: u64) -> Result<DiscordRemoteChannel, String> {
            self.called();
            Ok(DiscordRemoteChannel {
                channel_id,
                parent_id: None,
                topic: None,
                archived: false,
                locked: false,
            })
        }

        async fn find_channel_by_marker(
            &self,
            _guild_id: u64,
            _marker: &str,
        ) -> Result<Option<DiscordRemoteChannel>, String> {
            self.called();
            Ok(None)
        }

        async fn create_category(
            &self,
            _guild_id: u64,
            _name: &str,
            _marker: &str,
        ) -> Result<u64, String> {
            self.called();
            Ok(1)
        }

        async fn create_text_channel(
            &self,
            _guild_id: u64,
            _parent_id: u64,
            _name: &str,
            _marker: &str,
        ) -> Result<u64, String> {
            self.called();
            Ok(2)
        }

        async fn send_message(&self, _channel_id: u64, _body: &str) -> Result<u64, String> {
            self.called();
            Ok(3)
        }

        async fn create_public_thread(
            &self,
            _channel_id: u64,
            _seed_message_id: u64,
            _name: &str,
        ) -> Result<u64, String> {
            self.called();
            Ok(4)
        }

        async fn edit_message(
            &self,
            _channel_id: u64,
            _message_id: u64,
            _body: &str,
        ) -> Result<(), String> {
            self.called();
            Ok(())
        }

        async fn set_thread_archived(
            &self,
            _channel_id: u64,
            _archived: bool,
        ) -> Result<(), String> {
            self.called();
            Ok(())
        }

        async fn lock_thread(&self, _channel_id: u64) -> Result<(), String> {
            self.called();
            Ok(())
        }
    }

    async fn seed_key_addressed_channel_state(db: &LocalDb, project_key: &str) {
        db.execute_batch(&format!(
            r#"
            INSERT INTO discord_surface(id, guild_id, surface_kind, project_key, updated_at)
              VALUES (101, '1', 'project_category', '{project_key}', 1);
            INSERT INTO discord_surface(id, guild_id, surface_kind, project_key, target_uri, parent_surface_id, updated_at)
              VALUES (102, '1', 'thread_channel', '{project_key}', 'cairn://p/{project_key}/general', 101, 1);
            INSERT INTO discord_surface(id, guild_id, surface_kind, project_key, target_uri, parent_surface_id, updated_at)
              VALUES (103, '1', 'issue_thread', '{project_key}', 'cairn://p/{project_key}/1', 102, 1);
            INSERT INTO channel_state_card(provider, conversation, subject_uri, target_uri, project_key, job_id, created_at, updated_at)
              VALUES ('discord', 'discord:1/103', 'cairn://p/{project_key}/1', 'cairn://p/{project_key}/1', '{project_key}', 'channel-job', 1, 1);
            INSERT INTO channel_conversation_binding(provider, conversation, target_uri, binding_kind, message_classes, followed_at)
              VALUES ('discord', 'discord:1/103', 'cairn://p/{project_key}/1', 'structural', 7, 1);
            "#
        ))
        .await
        .unwrap();
    }

    async fn count_rows(db: &LocalDb, table: &str) -> i64 {
        db.query_opt_i64(format!("SELECT COUNT(*) FROM {table}"), ())
            .await
            .unwrap()
            .unwrap()
    }

    async fn seed_complete_project_graph(db: &LocalDb) {
        db.execute_batch(
            r#"
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws-delete', 'Delete', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('project-delete', 'ws-delete', 'Delete', 'DELETE', '/tmp/delete', 1, 1);
            INSERT INTO action_configs(id, name, project_id, created_at, updated_at)
              VALUES ('action-config-delete', 'action', 'project-delete', 1, 1);
            INSERT INTO skill_configs(id, name, prompt, project_id, created_at, updated_at)
              VALUES ('skill-config-delete', 'skill', 'prompt', 'project-delete', 1, 1);
            INSERT INTO threads(id, project_id, name, created_at, updated_at)
              VALUES ('thread-delete', 'project-delete', 'thread', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at, parent_thread_id)
              VALUES ('issue-delete', 'project-delete', 1, 'Issue', 1, 1, 'thread-delete');
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES ('execution-delete', 'recipe', 'issue-delete', 'project-delete', 'completed', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, thread_id, status, created_at, updated_at)
              VALUES ('job-delete', 'execution-delete', 'issue-delete', 'project-delete', 'thread-delete', 'complete', 1, 1);
            INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
              VALUES ('run-delete', 'issue-delete', 'project-delete', 'job-delete', 'completed', 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
              VALUES ('run-successor-delete', 'project-delete', 'job-delete', 'completed', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES ('session-delete', 'job-delete', 'codex', 'closed', 1, 1, 1);
            INSERT INTO codex_watchdog_leases(run_id, session_id, provider_turn_id, generation, runner_boot_id, phase, phase_deadline_at, last_provider_progress_at, last_worker_heartbeat_at, armed_at, updated_at, state, successor_run_id)
              VALUES ('run-delete', 'session-delete', 'provider-turn', 'generation', 'boot', 'provider_progress', 1, 1, 1, 1, 1, 'terminalized', 'run-successor-delete');
            INSERT INTO codex_watchdog_lifecycle(id, run_id, session_id, provider_turn_id, generation, kind, successor_run_id, created_at)
              VALUES ('watchdog-event-delete', 'run-delete', 'session-delete', 'provider-turn', 'generation', 'successor_started', 'run-successor-delete', 1);
            INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at, parent_job_id)
              VALUES ('action-run-delete', 'execution-delete', 'node', 'action-config-delete', 'issue-delete', 'project-delete', 'completed', 1, 'job-delete');
            INSERT INTO attachments(project_id, scope, scope_key, ordinal, content_hash, kind, file_name, created_at)
              VALUES ('project-delete', 'project', '', 1, 'attachment-hash', 'file', 'file.txt', 1);
            INSERT INTO check_result_observations(id, project_id, commit_sha, tree_hash, check_name, input_hash, environment_fingerprint, exit_code, verdict, complete, reusable, parser_version, result_schema_version, ran_at, duration_ms, cadence, output_tail)
              VALUES ('observation-delete', 'project-delete', 'commit', 'tree', 'check', 'input', 'env', 0, 'passed', 1, 1, 1, 1, 1, 1, 'write', 'ok');
            INSERT INTO check_test_results(observation_id, test_id, status, flaky)
              VALUES ('observation-delete', 'test', 'passed', 0);
            INSERT INTO check_result_commit_aliases(project_id, commit_sha, check_name, environment_fingerprint, result_schema_version, source_observation_id, tree_hash, input_hash, disposition, evaluated_at)
              VALUES ('project-delete', 'commit', 'check', 'env', 1, 'observation-delete', 'tree', 'input', 'fresh', 1);
            INSERT INTO check_result_cache(project_id, tree_hash, input_hash, check_name, source_observation_id, exit_code, passed, output_tail, duration_ms, ran_at)
              VALUES ('project-delete', 'tree', 'input', 'check', 'observation-delete', 0, 1, 'ok', 1, 1);
            INSERT INTO codemap_cache(project_id, base_commit_sha, computed_at, file_count, edge_count, codemap_json)
              VALUES ('project-delete', 'base', 1, 1, 0, '{}');
            INSERT INTO image_refs(project_id, issue_number, ordinal, content_hash, created_at)
              VALUES ('project-delete', 1, 1, 'image-hash', 1);
            INSERT INTO jj_reconcile_intents(id, project_id, store_path, target_branch, destination_commit, created_at, updated_at)
              VALUES ('intent-delete', 'project-delete', '/store', 'main', 'dest', 1, 1);
            INSERT INTO jj_reconcile_items(intent_id, bookmark, status, updated_at)
              VALUES ('intent-delete', 'bookmark', 'completed', 1);
            INSERT INTO jj_reconcile_quarantines(project_id, store_path, bookmark, failure_kind, fingerprint, created_at, updated_at)
              VALUES ('project-delete', '/store', 'bookmark', 'permanent', 'fingerprint', 1, 1);
            INSERT INTO job_browsers(id, job_id, project_id, slug, webview_label, status, created_at)
              VALUES ('browser-delete', 'job-delete', 'project-delete', 'browser', 'browser', 'closed', 1);
            INSERT INTO job_repls(id, job_id, project_id, slug, interpreter, generation, status, created_at)
              VALUES ('repl-delete', 'job-delete', 'project-delete', 'repl', 'python', 1, 'exited', 1);
            INSERT INTO repl_exchanges(id, repl_id, project_id, generation, seq, origin, code, status, started_at)
              VALUES ('exchange-delete', 'repl-delete', 'project-delete', 1, 1, 'agent', '1', 'complete', 1);
            INSERT INTO job_terminals(id, job_id, project_id, run_id, session_id, command, status, created_at)
              VALUES ('terminal-delete', 'job-delete', 'project-delete', 'run-delete', 'session', 'true', 'exited', 1);
            INSERT INTO memories(id, project_id, content, job_id, node_seq, created_at, updated_at)
              VALUES ('memory-delete', 'project-delete', 'memory', 'job-delete', 1, 1, 1);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, opened_at, updated_at)
              VALUES ('merge-delete', 'job-delete', 'project-delete', 'issue-delete', 'MR', 'source', 'main', 1, 1);
            INSERT INTO merge_attempt_events(event_id, attempt_id, sequence, project_id, merge_request_id, job_id, state, target_ref, source_ref, policy_json, created_at)
              VALUES ('merge-event-delete', 'attempt', 1, 'project-delete', 'merge-delete', 'job-delete', 'succeeded', 'main', 'source', '{}', 1);
            INSERT INTO pack_catalog(content_hash, project_id, repository_id, object_format, byte_count, pack_checksum, object_count, kind, tip_commit, created_at, publication_state)
              VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'project-delete', 'repo', 'sha1', 1, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 1, 'reachable', 'tip', 1, 'published');
            INSERT INTO pack_catalog_references(content_hash, project_id, repository_id, object_format, owner_kind, owner_id, created_at)
              VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'project-delete', 'repo', 'sha1', 'mutation_delta', 'owner', 1);
            INSERT INTO posts(project_id, content, author_principal_json, appearance_snapshot_json)
              VALUES ('project-delete', 'post', '{}', '{}');
            INSERT INTO post_comments(post_id, content, author_principal_json, appearance_snapshot_json)
              VALUES ((SELECT id FROM posts WHERE project_id = 'project-delete'), 'comment', '{}', '{}');
            INSERT INTO comments(id, thread_id, content, source, created_at)
              VALUES ('comment-delete', 'thread-delete', 'comment', 'user', 1);
            INSERT INTO thread_read_positions(thread_id, acknowledged_event_rowid, updated_at)
              VALUES ('thread-delete', 0, 1);
            INSERT INTO thread_branches(id, thread_id, number, stage, git_ref, job_id, created_at, moved_at)
              VALUES ('branch-delete', 'thread-delete', 1, 'accumulating', 'agent/delete', 'job-delete', 1, 1);
            INSERT INTO wake_schedules(id, home_kind, home_id, every_ms, anchor_at, reason, created_by)
              VALUES ('schedule-delete', 'thread', 'thread-delete', 1000, 1, 'reason', 'user');
            INSERT INTO wake_conditions(id, home_kind, home_id, sql, reason, created_by)
              VALUES ('condition-delete', 'thread', 'thread-delete', 'SELECT 1', 'reason', 'user');
            INSERT INTO feed_cursors(home_kind, home_id) VALUES ('thread', 'thread-delete');
            INSERT INTO contended_identifier_obligations(project_id, store_path, bookmark, identifier, delta, basis_fork_value, basis_branch_value, basis_base_value, created_at, updated_at)
              VALUES ('project-delete', '/store', 'bookmark', 'issue', 1, 0, 1, 0, 1, 1);
            "#,
        )
        .await
        .unwrap();
    }

    async fn assert_project_graph_seeded(db: &LocalDb, tables: &[String]) {
        for table in tables {
            let sql = format!("SELECT COUNT(*) FROM {table} WHERE project_id = 'project-delete'");
            let count = db.query_opt_i64(sql, ()).await.unwrap().unwrap();
            assert!(count > 0, "fixture did not seed {table}");
        }
    }

    async fn assert_project_graph_gone(db: &LocalDb, tables: &[String]) {
        assert!(db
            .query_opt_text("SELECT id FROM projects WHERE id = 'project-delete'", ())
            .await
            .unwrap()
            .is_none());
        for table in tables {
            let sql = format!("SELECT COUNT(*) FROM {table} WHERE project_id = 'project-delete'");
            let count = db.query_opt_i64(sql, ()).await.unwrap().unwrap();
            assert_eq!(count, 0, "{table} retained a project reference");
        }
        for table in [
            "check_test_results",
            "codex_watchdog_leases",
            "codex_watchdog_lifecycle",
            "comments",
            "feed_cursors",
            "jj_reconcile_items",
            "pack_catalog_references",
            "post_comments",
            "thread_branches",
            "thread_read_positions",
            "wake_conditions",
            "wake_schedules",
        ] {
            let count = db
                .query_opt_i64(format!("SELECT COUNT(*) FROM {table}"), ())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(count, 0, "{table} retained project-owned rows");
        }
    }

    #[tokio::test]
    async fn sweep_manifest_covers_every_table_referencing_projects() {
        let db = migrated_db("project-sweep-coverage.db", TURSO_MIGRATIONS.to_vec()).await;
        let schema_tables = project_fk_tables(&db).await;
        let mut manifest_tables = COPY_ORDER
            .iter()
            .map(|spec| spec.table.to_string())
            .collect::<Vec<_>>();
        manifest_tables.sort();
        let missing = schema_tables
            .iter()
            .filter(|table| !manifest_tables.contains(table))
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "project sweep manifest misses {missing:?}"
        );
    }

    #[tokio::test]
    async fn channel_key_manifest_covers_private_project_key_columns() {
        let db = migrated_db("channel-key-coverage.db", TURSO_MIGRATIONS.to_vec()).await;
        let private_tables = TABLE_SCOPES
            .iter()
            .filter_map(|(table, scope)| matches!(scope, TableScope::Private(_)).then_some(*table));
        let manifest_tables = CHANNEL_KEY_ADDRESSED
            .iter()
            .map(|spec| spec.table)
            .collect::<HashSet<_>>();

        let mut key_addressed = Vec::new();
        for table in private_tables {
            let columns = db
                .query_all(format!("PRAGMA table_info({table})"), (), |row| row.text(1))
                .await
                .unwrap();
            if columns.iter().any(|column| column == "project_key") {
                key_addressed.push(table);
                assert!(
                    table == "project_routes" || manifest_tables.contains(table),
                    "private table {table} has project_key but is absent from the channel sweep"
                );
            }
        }
        key_addressed.sort_unstable();
        assert_eq!(
            key_addressed,
            ["channel_state_card", "discord_surface", "project_routes"]
        );
        // `project_routes` is intentionally swept by the routing path because
        // local and team deletion have different route-removal transactions.
    }

    #[test]
    fn channel_key_manifest_lists_uri_addressed_tables_explicitly() {
        // The schema cannot distinguish a project URI in `target_uri` from any
        // other string, so this part of the manifest is necessarily maintained
        // by hand rather than discovered through a column or foreign key.
        assert!(CHANNEL_KEY_ADDRESSED
            .iter()
            .any(|spec| spec.table == "channel_conversation_binding"));
    }

    #[tokio::test]
    async fn sweep_manifest_covers_transitive_non_cascade_dependencies() {
        let db = migrated_db("project-sweep-transitive.db", TURSO_MIGRATIONS.to_vec()).await;
        let tables = db
            .query_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        let mut edges = Vec::new();
        for child in &tables {
            let child_name = child.clone();
            let child_edges = db
                .query_all(
                    format!("PRAGMA foreign_key_list({child})"),
                    (),
                    move |row| {
                        Ok((
                            child_name.clone(),
                            row.text(2)?,
                            row.text(6)?.to_ascii_uppercase(),
                        ))
                    },
                )
                .await
                .unwrap();
            edges.extend(child_edges);
        }

        let explicit = COPY_ORDER
            .iter()
            .map(|spec| spec.table)
            .collect::<HashSet<_>>();
        let mut reachable = HashSet::from(["projects".to_string()]);
        loop {
            let discovered = edges
                .iter()
                .filter(|(child, parent, _)| {
                    reachable.contains(parent) && !reachable.contains(child)
                })
                .map(|(child, _, _)| child.clone())
                .collect::<Vec<_>>();
            if discovered.is_empty() {
                break;
            }
            reachable.extend(discovered);
        }
        for (child, parent, on_delete) in &edges {
            if !reachable.contains(parent) {
                continue;
            }
            assert!(
                explicit.contains(child.as_str()) || on_delete == "CASCADE",
                "{child} reaches project-owned {parent} through {on_delete}, but is not in the sweep manifest"
            );
        }
    }

    /// The tables a predicate reads through `FROM <table>`, including a
    /// `DELETE FROM` target.
    fn tables_named_by(sql: &str) -> Vec<String> {
        let mut named = Vec::new();
        let mut rest = sql;
        while let Some(at) = rest.find("FROM ") {
            rest = &rest[at + "FROM ".len()..];
            let table: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !table.is_empty() {
                named.push(table);
            }
        }
        named
    }

    /// The manifest's lineage claims are the schema's, and no statement the
    /// sweep issues crosses a lineage boundary.
    ///
    /// Everything here is derived: the classification from `TABLE_SCOPES`, the
    /// truth it is checked against from a real team-lineage database, and the
    /// statements from the manifest itself. Adding an entry whose scope is
    /// undeclared, or whose predicate reaches into the other lineage, fails here
    /// rather than at a customer's `no such table`.
    #[tokio::test]
    async fn sweep_statements_are_issued_only_to_the_lineage_that_holds_them() {
        let team = migrated_db("sweep-lineage.db", TEAM_MIGRATIONS.to_vec()).await;
        let team_tables = team
            .query_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
            .into_iter()
            .collect::<HashSet<String>>();

        for spec in COPY_ORDER {
            let lineage = spec
                .lineage()
                .unwrap_or_else(|error| panic!("{}: {error}", spec.table));
            assert_eq!(
                lineage == TableLineage::Shared,
                team_tables.contains(spec.table),
                "`{}` is declared {lineage:?} in TABLE_SCOPES but the team lineage \
                 {} it",
                spec.table,
                if team_tables.contains(spec.table) {
                    "has"
                } else {
                    "lacks"
                }
            );

            let read = match spec.sweep {
                SweepPredicate::Direct(sql) => tables_named_by(sql),
                SweepPredicate::Owned(clauses) => clauses
                    .iter()
                    .flat_map(|clause| tables_named_by(clause.sql))
                    .collect(),
            };
            for table in read {
                assert_eq!(
                    table_lineage(&table).unwrap(),
                    lineage,
                    "`{}` is {lineage:?} but subqueries `{table}`, which lives in the \
                     other lineage; for a team project the two are different \
                     databases and the subquery would silently match nothing",
                    spec.table
                );
            }
        }

        // The team path's private half runs against the per-install database.
        // Every statement it would issue must name only tables that database has.
        let owner_ids = OwnerIds::Materialized(
            OwnerSet::ALL
                .iter()
                .map(|set| (*set, vec!["owned-id".to_string()]))
                .collect(),
        );
        for spec in COPY_ORDER {
            if spec.lineage().unwrap() != TableLineage::Private {
                continue;
            }
            for (sql, _) in spec.private_statements("project", &owner_ids).unwrap() {
                for table in tables_named_by(&sql) {
                    assert_eq!(
                        table_lineage(&table).unwrap(),
                        TableLineage::Private,
                        "the team path would issue `{sql}`, naming shared table \
                         `{table}`, against the private database"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn delete_db_sweeps_complete_project_graph() {
        let db = migrated_db("project-delete-complete.db", TURSO_MIGRATIONS.to_vec()).await;
        seed_complete_project_graph(&db).await;
        let tables = project_fk_tables(&db).await;
        assert_project_graph_seeded(&db, &tables).await;

        crate::projects::crud::delete_db(&db, "project-delete")
            .await
            .unwrap();

        assert_project_graph_gone(&db, &tables).await;
    }

    #[tokio::test]
    async fn routed_delete_sweeps_channel_state_and_quiets_discord() {
        let db =
            Arc::new(migrated_db("project-delete-channels.db", TURSO_MIGRATIONS.to_vec()).await);
        seed_complete_project_graph(&db).await;
        seed_key_addressed_channel_state(&db, "delete").await;
        db.execute_batch(
            r#"
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('project-other', 'ws-delete', 'Other', 'OTHER', '/tmp/other', 1, 1);
            INSERT INTO jobs(id, issue_id, project_id, status, created_at, updated_at)
              VALUES ('job-issue-only', 'issue-delete', 'project-other', 'complete', 1, 1);
            INSERT INTO runs(id, issue_id, job_id, status, created_at, updated_at)
              VALUES ('run-issue-only', 'issue-delete', 'job-issue-only', 'completed', 1, 1);
            INSERT INTO prompts(id, run_id, questions, job_id, created_at)
              VALUES ('prompt-delete', 'run-issue-only', '[]', 'job-issue-only', 1);
            INSERT INTO permission_requests(id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
              VALUES ('permission-delete', 'run-issue-only', 'tool', 'write', '{}', 'pending', 1);
            INSERT INTO channel_outbound(id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, status, created_at)
              VALUES ('outbound-delete', 'discord', 'question', 'prompt-delete:0', 'discord:1/103', 'job-issue-only', 'Question?', 'text', 'pending', 1);
            INSERT INTO channel_ask_action(action_ref, kind)
              VALUES ('prompt-delete', 'question'), ('permission-delete', 'permission');
            INSERT INTO channel_ask_resolution(binding_ref, action_ref, kind, answer, winner_provider, winner_conversation, resolved_at)
              VALUES ('prompt-delete:0', 'prompt-delete', 'question', 'yes', 'discord', 'discord:1/103', 1),
                     ('permission-delete', 'permission-delete', 'permission', 'allow', 'discord', 'discord:1/103', 1);
            "#,
        )
        .await
        .unwrap();
        let binding_ref = db
            .query_text(
                "SELECT binding_ref FROM channel_outbound WHERE id = 'outbound-delete'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(binding_ref.strip_suffix(":0"), Some("prompt-delete"));

        let search = Arc::new(SearchIndex::open_or_create(tempdir().unwrap().keep()).unwrap());
        let dbs = DbState::new(db.clone(), search);
        crate::projects::crud::delete_routed(&dbs, "project-delete")
            .await
            .unwrap();

        for table in [
            "discord_surface",
            "channel_state_card",
            "channel_conversation_binding",
            "channel_outbound",
            "channel_ask_action",
            "channel_ask_resolution",
        ] {
            assert_eq!(count_rows(&db, table).await, 0, "{table} retained rows");
        }
        assert!(crate::channels::discord_surfaces::list_due(&db, 2)
            .await
            .unwrap()
            .is_empty());

        let api = Arc::new(CountingDiscordApi::default());
        let reconciler = DiscordSurfaceReconciler::new(db, api.clone());
        assert_eq!(reconciler.reconcile_due(2).await.unwrap(), 0);
        assert_eq!(api.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failed_channel_sweep_leaves_local_delete_re_runnable() {
        let db = Arc::new(
            migrated_db("project-delete-channel-retry.db", TURSO_MIGRATIONS.to_vec()).await,
        );
        seed_complete_project_graph(&db).await;
        seed_key_addressed_channel_state(&db, "delete").await;
        let state_card_ddl = db
            .query_opt_text(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'channel_state_card'",
                (),
            )
            .await
            .unwrap()
            .expect("the private lineage has a channel_state_card table");
        exclusive_ddl(&db, "DROP TABLE channel_state_card").await;

        let search = Arc::new(SearchIndex::open_or_create(tempdir().unwrap().keep()).unwrap());
        let dbs = DbState::new(db.clone(), search);
        let error = crate::projects::crud::delete_routed(&dbs, "project-delete")
            .await
            .expect_err("the missing channel table must fail the private sweep");
        assert!(
            error.to_string().contains("channel_state_card"),
            "unexpected failure: {error}"
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM projects WHERE id = 'project-delete'"
            )
            .await,
            1,
            "the project must survive so deletion remains retryable"
        );
        assert!(
            count_rows(&db, "discord_surface").await > 0,
            "the encompassing local transaction must roll back"
        );

        exclusive_ddl(&db, &state_card_ddl).await;
        crate::projects::crud::delete_routed(&dbs, "project-delete")
            .await
            .expect("re-running delete finishes the channel sweep");

        assert_eq!(count(&db, "SELECT COUNT(*) FROM projects").await, 0);
        assert_eq!(count_rows(&db, "discord_surface").await, 0);
        assert_eq!(count_rows(&db, "channel_conversation_binding").await, 0);
    }

    #[tokio::test]
    async fn channel_surface_sweep_uses_the_private_database_for_team_projects() {
        let db = migrated_db(
            "team-project-private-channels.db",
            TURSO_MIGRATIONS.to_vec(),
        )
        .await;
        seed_key_addressed_channel_state(&db, "mixedcase").await;
        db.execute(
            "INSERT INTO channel_conversation_binding(provider, conversation, target_uri, binding_kind, message_classes, followed_at)
             VALUES ('discord', 'discord:1/999', 'cairn://p/mixedcase_/1', 'structural', 7, 1)",
            (),
        )
        .await
        .unwrap();

        sweep_project_channel_surfaces(&db, "MixedCase")
            .await
            .unwrap();

        assert_eq!(count_rows(&db, "discord_surface").await, 0);
        assert_eq!(count_rows(&db, "channel_state_card").await, 0);
        assert_eq!(count_rows(&db, "channel_conversation_binding").await, 1);
        assert_eq!(
            db.query_text("SELECT target_uri FROM channel_conversation_binding", ())
                .await
                .unwrap(),
            Some("cairn://p/mixedcase_/1".to_string())
        );
    }

    #[tokio::test]
    async fn move_source_sweep_removes_private_project_rows() {
        let db = migrated_db("project-move-source-sweep.db", TURSO_MIGRATIONS.to_vec()).await;
        seed_complete_project_graph(&db).await;
        let tables = project_fk_tables(&db).await;
        assert_project_graph_seeded(&db, &tables).await;

        sweep_source(&db, "project-delete").await.unwrap();

        assert_project_graph_gone(&db, &tables).await;
    }

    #[tokio::test]
    async fn turso_enforces_declared_foreign_key_delete_actions() {
        let db = migrated_db(
            "project-delete-fk-enforcement.db",
            TURSO_MIGRATIONS.to_vec(),
        )
        .await;
        seed_complete_project_graph(&db).await;

        let error = db
            .execute("DELETE FROM projects WHERE id = 'project-delete'", ())
            .await
            .unwrap_err();
        let diagnostic = error.to_string().to_ascii_lowercase();
        assert!(
            diagnostic.contains("foreign key") && diagnostic.contains("constraint"),
            "unexpected delete error: {diagnostic}"
        );

        db.execute(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-cascade', 'ws-delete', 'Cascade', 'CASCADE', '/tmp/cascade', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO action_configs(id, name, project_id, created_at, updated_at)
             VALUES ('action-cascade', 'action', 'project-cascade', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute("DELETE FROM projects WHERE id = 'project-cascade'", ())
            .await
            .unwrap();
        assert!(
            db.query_opt_text(
                "SELECT id FROM action_configs WHERE id = 'action-cascade'",
                (),
            )
            .await
            .unwrap()
            .is_none(),
            "ON DELETE CASCADE must remove the child row"
        );
    }

    /// A team project split across two databases, produced the way production
    /// produces it, and the ids each half is left addressing.
    struct TeamProjectFixture {
        dbs: DbState,
        local: Arc<LocalDb>,
        team: Arc<LocalDb>,
        project_id: String,
    }

    /// Turso refuses DDL inside the concurrent write transaction `execute` uses.
    async fn exclusive_ddl(db: &LocalDb, sql: &str) {
        let sql = sql.to_string();
        db.exclusive(|conn| {
            let sql = sql.clone();
            Box::pin(async move {
                conn.execute(&sql, ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Build a team project the way `move_project_to_team` does, then seed the
    /// private rows that a team project actually accumulates on this host.
    ///
    /// Private tables with cross-lineage foreign keys cannot hold team-project
    /// rows on this host because their parents live in the replica. The channel
    /// ledger and the untyped wake/feed tables deliberately carry no such foreign
    /// keys, so they can address team-side ids while remaining in the private
    /// database; key-addressed Discord state likewise stays private.
    async fn team_project_fixture(name: &str) -> TeamProjectFixture {
        // A team id is guarded to `[A-Za-z0-9]`, so no hyphen here.
        let team_id = "teamsweep";
        let project_id = "11111111-1111-4111-8111-111111111111";
        let issue_id = "22222222-2222-4222-8222-222222222222";
        let execution_id = "33333333-3333-4333-8333-333333333333";
        let job_id = "44444444-4444-4444-8444-444444444444";
        let run_id = "55555555-5555-4555-8555-555555555555";

        let local =
            Arc::new(migrated_db(&format!("{name}-local.db"), TURSO_MIGRATIONS.to_vec()).await);
        let team =
            Arc::new(migrated_db(&format!("{name}-team.db"), TEAM_MIGRATIONS.to_vec()).await);
        team.execute(
            "INSERT INTO teams(id, name, created_at, updated_at) VALUES (?1, 'Sweep', 1, 1)",
            (team_id,),
        )
        .await
        .unwrap();
        local
            .execute(
                "INSERT INTO teams(id, name, sync_url, replica_path, created_at)
                 VALUES (?1, 'Sweep', 'http://localhost', '/tmp/team.db', 1)",
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
                 VALUES (?1, 'workspace-1', 'Sweep Project', 'SWEEP', '/tmp/sweep', 1, 1)",
                (project_id,),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                 VALUES (?1, ?2, 1, 'Issue', 'closed', 1, 1)",
                (issue_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES (?1, 'recipe', ?2, ?3, 'completed', 1, 1)",
                (execution_id, issue_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'complete', 1, 1)",
                (job_id, execution_id, issue_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'completed', 1, 1)",
                (run_id, issue_id, project_id, job_id),
            )
            .await
            .unwrap();

        let search_dir = tempdir().unwrap();
        let dbs = DbState::new(
            local.clone(),
            Arc::new(SearchIndex::open_or_create(search_dir.keep()).unwrap()),
        );
        dbs.insert_team_db_for_test(team_id, team.clone()).await;
        move_project_to_team(&dbs, &FixedClock, project_id, team_id, &HashSet::new())
            .await
            .unwrap();

        let team_project_id = rekey_to_team(project_id, team_id).unwrap();
        let team_job_id = rekey_to_team(job_id, team_id).unwrap();
        let team_run_id = rekey_to_team(run_id, team_id).unwrap();
        let team_prompt_id =
            rekey_to_team("77777777-7777-4777-8777-777777777777", team_id).unwrap();
        let team_permission_id =
            rekey_to_team("88888888-8888-4888-8888-888888888888", team_id).unwrap();
        // A thread created after the move: it is minted into the team lineage,
        // which is where a team project's threads live.
        let team_thread_id =
            rekey_to_team("66666666-6666-4666-8666-666666666666", team_id).unwrap();
        team.execute(
            "INSERT INTO threads(id, project_id, name, created_at, updated_at)
             VALUES (?1, ?2, 'thread', 1, 1)",
            (team_thread_id.clone(), team_project_id.clone()),
        )
        .await
        .unwrap();
        team.execute(
            "INSERT INTO thread_branches(id, thread_id, number, stage, git_ref, job_id, created_at, moved_at)
             VALUES ('team-branch', ?1, 1, 'pr-open', 'agent/thread/1', ?2, 1, 1)",
            (team_thread_id.clone(), team_job_id.clone()),
        )
        .await
        .unwrap();

        local
            .execute(
                "INSERT INTO wake_schedules(id, home_kind, home_id, every_ms, anchor_at, reason, created_by)
                 VALUES ('schedule-team', 'thread', ?1, 1000, 1, 'reason', 'user')",
                (team_thread_id.clone(),),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO wake_conditions(id, home_kind, home_id, sql, reason, created_by)
                 VALUES ('condition-team', 'thread', ?1, 'SELECT 1', 'reason', 'user')",
                (team_thread_id.clone(),),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO feed_cursors(home_kind, home_id) VALUES ('thread', ?1)",
                (team_thread_id.clone(),),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO feed_cursors(home_kind, home_id) VALUES ('job', ?1)",
                (team_job_id.clone(),),
            )
            .await
            .unwrap();
        team.execute(
            "INSERT INTO prompts(id, run_id, questions, job_id, created_at)
                 VALUES (?1, ?2, '[]', ?3, 1)",
            (
                team_prompt_id.clone(),
                team_run_id.clone(),
                team_job_id.clone(),
            ),
        )
        .await
        .unwrap();
        team
            .execute(
                "INSERT INTO permission_requests(id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
                 VALUES (?1, ?2, 'tool', 'write', '{}', 'pending', 1)",
                (team_permission_id.clone(), team_run_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO channel_outbound(id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, status, created_at)
                 VALUES ('outbound-team', 'discord', 'question', ?1, 'discord:1/103', ?2, 'Question?', 'text', 'pending', 1)",
                (format!("{team_prompt_id}:0"), team_job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO channel_ask_action(action_ref, kind) VALUES (?1, 'question'), (?2, 'permission')",
                (team_prompt_id.clone(), team_permission_id.clone()),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO channel_ask_resolution(binding_ref, action_ref, kind, answer, winner_provider, winner_conversation, resolved_at)
                 VALUES (?1, ?2, 'question', 'yes', 'discord', 'discord:1/103', 1),
                        (?3, ?3, 'permission', 'allow', 'discord', 'discord:1/103', 1)",
                (
                    format!("{team_prompt_id}:0"),
                    team_prompt_id,
                    team_permission_id,
                ),
            )
            .await
            .unwrap();
        seed_key_addressed_channel_state(&local, "sweep").await;

        TeamProjectFixture {
            dbs,
            local,
            team,
            project_id: team_project_id,
        }
    }

    async fn count(db: &LocalDb, sql: &str) -> i64 {
        db.query_opt_i64(sql.to_string(), ())
            .await
            .unwrap()
            .unwrap()
    }

    async fn private_row_count(fixture: &TeamProjectFixture) -> i64 {
        count(&fixture.local, "SELECT COUNT(*) FROM wake_schedules").await
            + count(&fixture.local, "SELECT COUNT(*) FROM wake_conditions").await
            + count(&fixture.local, "SELECT COUNT(*) FROM feed_cursors").await
    }

    /// Deleting a team project removes its rows from BOTH databases.
    ///
    /// Before the split this failed outright: the sweep ran the whole manifest
    /// against the team replica, which has no `wake_schedules` table, so delete
    /// died on `no such table` and left every row in place.
    #[tokio::test]
    async fn delete_routed_sweeps_a_team_project_across_both_databases() {
        let fixture = team_project_fixture("team-delete").await;
        assert_eq!(
            private_row_count(&fixture).await,
            4,
            "fixture did not seed the private rows a team project accumulates"
        );
        assert!(count(&fixture.team, "SELECT COUNT(*) FROM jobs").await > 0);
        assert_eq!(
            count(&fixture.team, "SELECT COUNT(*) FROM thread_branches").await,
            1
        );
        for table in [
            "discord_surface",
            "channel_state_card",
            "channel_conversation_binding",
            "channel_outbound",
            "channel_ask_action",
            "channel_ask_resolution",
        ] {
            assert!(
                count_rows(&fixture.local, table).await > 0,
                "fixture did not seed {table}"
            );
        }

        crate::projects::crud::delete_routed(&fixture.dbs, &fixture.project_id)
            .await
            .unwrap();

        assert_eq!(
            private_row_count(&fixture).await,
            0,
            "private rows addressed through team-side ids survived the delete"
        );
        for table in [
            "discord_surface",
            "channel_state_card",
            "channel_conversation_binding",
            "channel_outbound",
            "channel_ask_action",
            "channel_ask_resolution",
        ] {
            assert_eq!(
                count_rows(&fixture.local, table).await,
                0,
                "{table} retained team-project rows"
            );
        }
        for table in [
            "projects",
            "issues",
            "executions",
            "jobs",
            "runs",
            "threads",
            "thread_branches",
        ] {
            assert_eq!(
                count(&fixture.team, &format!("SELECT COUNT(*) FROM {table}")).await,
                0,
                "{table} retained rows in the team replica"
            );
        }
        assert!(
            fixture
                .local
                .query_opt_text("SELECT project_key FROM project_routes", ())
                .await
                .unwrap()
                .is_none(),
            "the durable route outlived the project"
        );
    }

    /// The two halves commit independently, so the order has to be the one whose
    /// partial failure a re-run can finish. Private first: when the shared half
    /// fails, the project row, its route, and the owner id sets are all still
    /// there, so delete can simply be run again.
    #[tokio::test]
    async fn a_failed_shared_half_leaves_a_team_delete_re_runnable() {
        let fixture = team_project_fixture("team-delete-partial").await;
        let todos_ddl = fixture
            .team
            .query_opt_text(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'todos'",
                (),
            )
            .await
            .unwrap()
            .expect("the team lineage has a todos table");
        exclusive_ddl(&fixture.team, "DROP TABLE todos").await;

        let error = crate::projects::crud::delete_routed(&fixture.dbs, &fixture.project_id)
            .await
            .expect_err("the shared half must fail with todos missing");
        assert!(
            error.to_string().contains("todos"),
            "unexpected failure: {error}"
        );
        assert_eq!(
            private_row_count(&fixture).await,
            0,
            "the private half runs first and commits on its own"
        );
        assert!(
            count(&fixture.team, "SELECT COUNT(*) FROM projects").await > 0,
            "the project row must survive so the re-run can find it"
        );
        assert!(
            fixture
                .local
                .query_opt_text("SELECT project_key FROM project_routes", ())
                .await
                .unwrap()
                .is_some(),
            "the route must survive so the key is not released early"
        );

        exclusive_ddl(&fixture.team, &todos_ddl).await;
        crate::projects::crud::delete_routed(&fixture.dbs, &fixture.project_id)
            .await
            .expect("re-running delete finishes the half that failed");

        assert_eq!(private_row_count(&fixture).await, 0);
        assert_eq!(
            count(&fixture.team, "SELECT COUNT(*) FROM projects").await,
            0
        );
        assert!(fixture
            .local
            .query_opt_text("SELECT project_key FROM project_routes", ())
            .await
            .unwrap()
            .is_none());
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
        let check_attempt_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
        let thread_id = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
        let branch_id = "ffffffff-ffff-4fff-8fff-ffffffffffff";
        let branch_job_id = "00000000-0000-4000-8000-000000000001";

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
                "INSERT INTO threads(id, project_id, name, next_branch_number, created_at, updated_at)
                 VALUES (?1, ?2, 'branch-thread', 8, 1, 1)",
                (thread_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO jobs(id, thread_id, project_id, status, uri_segment, branch, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'complete', 'branch-7', 'agent/branch-thread/7', 1, 1)",
                (branch_job_id, thread_id, project_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO thread_branches(id, thread_id, number, title, stage, git_ref, job_id, created_at, moved_at)
                 VALUES (?1, ?2, 7, 'Preserve this branch', 'pr-open', 'agent/branch-thread/7', ?3, 1, 2)",
                (branch_id, thread_id, branch_job_id),
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
                "INSERT INTO turn_end_check_attempts(id, job_id, state, created_at, updated_at)
                 VALUES (?1, ?2, 'settled', 1, 2)",
                (check_attempt_id, job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "UPDATE jobs SET turn_end_check_attempt_id = ?1 WHERE id = ?2",
                (check_attempt_id, job_id),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO build_change_notifications(
                    id, recipient, from_version, from_build_id, to_version, to_build_id, booted_at
                 ) VALUES ('build-note', ?1, '1', 'old', '2', 'new', 1)",
                (job_id,),
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
        seed_key_addressed_channel_state(&local, "move").await;

        let search_dir = tempdir().unwrap();
        let dbs = DbState::new(
            local.clone(),
            Arc::new(SearchIndex::open_or_create(search_dir.path()).unwrap()),
        );
        dbs.insert_team_db_for_test(team_id, team.clone()).await;

        move_project_to_team(&dbs, &FixedClock, project_id, team_id, &HashSet::new())
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
        let team_check_attempt_id = rekey_to_team(check_attempt_id, team_id).unwrap();
        let team_thread_id = rekey_to_team(thread_id, team_id).unwrap();
        let team_branch_job_id = rekey_to_team(branch_job_id, team_id).unwrap();
        assert_eq!(
            team.query_one(
                "SELECT thread_id, job_id, number, title, stage, git_ref, created_at, moved_at
                 FROM thread_branches WHERE id = ?1",
                (rekey_to_team(branch_id, team_id).unwrap(),),
                |row| Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.i64(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.text(5)?,
                    row.i64(6)?,
                    row.i64(7)?
                )),
            )
            .await
            .unwrap(),
            (
                team_thread_id.clone(),
                team_branch_job_id.clone(),
                7,
                "Preserve this branch".to_string(),
                "pr-open".to_string(),
                "agent/branch-thread/7".to_string(),
                1,
                2
            )
        );
        assert_eq!(
            team.query_opt_i64(
                "SELECT next_branch_number FROM threads WHERE id = ?1",
                (team_thread_id,)
            )
            .await
            .unwrap(),
            Some(8)
        );
        assert_eq!(
            crate::jobs::queries::home_uri_for_job(&team, &team_branch_job_id)
                .await
                .unwrap()
                .as_deref(),
            Some("cairn://p/move/branch-thread/branches/7")
        );
        assert_eq!(
            count(&local, "SELECT COUNT(*) FROM thread_branches").await,
            0
        );
        assert_eq!(count(&local, "SELECT COUNT(*) FROM threads").await, 0);

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
                (team_job_id.clone(),)
            )
            .await
            .unwrap(),
            Some(team_resumed_session_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT turn_end_check_attempt_id FROM jobs WHERE id = ?1",
                (team_job_id.clone(),)
            )
            .await
            .unwrap(),
            Some(team_check_attempt_id.clone())
        );
        assert_eq!(
            team.query_text(
                "SELECT job_id FROM turn_end_check_attempts WHERE id = ?1",
                (team_check_attempt_id,)
            )
            .await
            .unwrap(),
            Some(team_job_id)
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
                .query_opt_text(
                    "SELECT id FROM build_change_notifications WHERE id = 'build-note'",
                    ()
                )
                .await
                .unwrap(),
            None,
            "runner-local build notifications are discarded with their swept recipient"
        );
        assert_eq!(
            local
                .query_text(
                    "SELECT team_id FROM project_routes WHERE project_key = 'move'",
                    ()
                )
                .await
                .unwrap(),
            Some(team_id.to_string())
        );
        for table in [
            "discord_surface",
            "channel_state_card",
            "channel_conversation_binding",
        ] {
            assert!(
                count_rows(&local, table).await > 0,
                "moving a project must preserve private channel state in {table}"
            );
        }
    }

    // === Quiesce-gate (`blocking_work`) tests ===

    /// A migrated local db seeded with one project (`proj`, key `GATE`).
    async fn gate_db() -> LocalDb {
        let db = migrated_db("gate-local.db", TURSO_MIGRATIONS.to_vec()).await;
        db.execute(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES ('ws', 'Workspace', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'ws', 'Gate', 'GATE', '/tmp/gate', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db
    }

    async fn insert_issue(db: &LocalDb, id: &str, number: i64, status: &str) {
        db.execute(
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES (?1, 'proj', ?2, 'Issue', ?3, 1, 1)",
            (id.to_string(), number, status.to_string()),
        )
        .await
        .unwrap();
    }

    async fn insert_job(db: &LocalDb, id: &str, issue_id: &str, status: &str, node_name: &str) {
        db.execute(
            "INSERT INTO jobs(id, issue_id, project_id, status, node_name, created_at, updated_at)
             VALUES (?1, ?2, 'proj', ?3, ?4, 1, 1)",
            (
                id.to_string(),
                issue_id.to_string(),
                status.to_string(),
                node_name.to_string(),
            ),
        )
        .await
        .unwrap();
    }

    async fn insert_execution(db: &LocalDb, id: &str, issue_id: &str) {
        db.execute(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES (?1, 'recipe', ?2, 'proj', 'running', 1, 1)",
            (id.to_string(), issue_id.to_string()),
        )
        .await
        .unwrap();
    }

    async fn insert_advance_outbox(db: &LocalDb, id: &str, execution_id: &str, state: &str) {
        db.execute(
            "INSERT INTO effect_outbox(id, kind, dedupe_key, state, created_at, updated_at)
             VALUES (?1, 'advance_dag', ?2, ?3, 1, 1)",
            (id.to_string(), execution_id.to_string(), state.to_string()),
        )
        .await
        .unwrap();
    }

    fn live_set(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    #[tokio::test]
    async fn gate_passes_on_inert_nonterminal_rows() {
        // The reported regression: every job row is non-terminal (running /
        // pending / blocked) but the work is entirely inert — no live process,
        // no queued advance. A `running` row parked on a resumable wait and a
        // never-started recipe node are a designed steady state, so the move
        // must pass regardless of how many such rows exist.
        let db = gate_db().await;
        insert_issue(&db, "iss", 1, "merged").await;
        insert_job(&db, "job-running", "iss", "running", "builder").await;
        insert_job(&db, "job-pending", "iss", "pending", "planner").await;
        insert_job(&db, "job-blocked", "iss", "blocked", "reviewer").await;

        let blocking = blocking_work(&db, "proj", &HashSet::new()).await.unwrap();
        assert!(
            blocking.is_empty(),
            "inert non-terminal rows must not block, got {blocking:?}"
        );
    }

    #[tokio::test]
    async fn gate_blocks_on_live_process_and_names_it() {
        // A live in-memory process on one job blocks the move; the surfaced
        // BlockingJob names that job (issue number + node) so the user can act.
        // The other running job with no live process does not block.
        let db = gate_db().await;
        insert_issue(&db, "iss", 7, "active").await;
        insert_job(&db, "job-live", "iss", "running", "builder").await;
        insert_job(&db, "job-idle", "iss", "running", "planner").await;

        let blocking = blocking_work(&db, "proj", &live_set(&["job-live"]))
            .await
            .unwrap();
        assert_eq!(blocking.len(), 1);
        assert_eq!(blocking[0].id, "job-live");
        assert_eq!(blocking[0].issue_number, Some(7));
        assert_eq!(blocking[0].node_name.as_deref(), Some("builder"));
    }

    #[tokio::test]
    async fn gate_ignores_live_process_outside_project() {
        // A live job id that does not belong to the project being moved never
        // blocks it — liveness is scoped to the project graph, not global.
        let db = gate_db().await;
        insert_issue(&db, "iss", 1, "merged").await;
        insert_job(&db, "job-mine", "iss", "running", "builder").await;

        let blocking = blocking_work(&db, "proj", &live_set(&["job-elsewhere"]))
            .await
            .unwrap();
        assert!(blocking.is_empty(), "got {blocking:?}");
    }

    #[tokio::test]
    async fn gate_blocks_on_pending_advance_then_passes_when_drained() {
        // An undelivered advance_dag outbox row for one of the project's
        // executions blocks the move: effect_outbox is not copied, so the
        // advance would be lost against a swept-away source. Once the row
        // reaches a terminal state, the move passes. Pins the mid-advance-window
        // decision.
        let db = gate_db().await;
        insert_issue(&db, "iss", 3, "active").await;
        insert_execution(&db, "exec", "iss").await;
        insert_advance_outbox(&db, "eob-1", "exec", "running").await;

        let blocking = blocking_work(&db, "proj", &HashSet::new()).await.unwrap();
        assert_eq!(blocking.len(), 1);
        assert_eq!(blocking[0].id, "exec");
        assert_eq!(blocking[0].status, ADVANCING_STATUS);
        assert_eq!(blocking[0].issue_number, Some(3));

        db.execute(
            "UPDATE effect_outbox SET state = 'done' WHERE id = 'eob-1'",
            (),
        )
        .await
        .unwrap();
        let after = blocking_work(&db, "proj", &HashSet::new()).await.unwrap();
        assert!(
            after.is_empty(),
            "a drained advance must not block, got {after:?}"
        );
    }

    #[test]
    fn notquiesced_display_names_the_blocking_work() {
        // The gate failure renders a self-contained, user-facing sentence that
        // keeps the `NotQuiesced` token (matched by the frontend) and lists the
        // blocking work.
        let failure = MoveProjectGateFailure::NotQuiesced {
            blocking_jobs: vec![
                BlockingJob {
                    id: "job-live".to_string(),
                    status: "running".to_string(),
                    issue_number: Some(12),
                    node_name: Some("builder".to_string()),
                },
                BlockingJob {
                    id: "exec-abc12345def".to_string(),
                    status: ADVANCING_STATUS.to_string(),
                    issue_number: None,
                    node_name: None,
                },
            ],
        };
        let rendered = failure.to_string();
        assert!(rendered.starts_with("NotQuiesced:"), "got {rendered}");
        assert!(rendered.contains("issue #12 · builder"), "got {rendered}");
        assert!(
            rendered.contains("job exec-abc (advancing)"),
            "got {rendered}"
        );
    }
}
