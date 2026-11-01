//! Retention: one stated rule for every table, and the paged evictor that
//! enforces the rules that delete.
//!
//! The live database is bounded by rules, not by the project's age (tenet
//! 1303). [`TABLE_RETENTION`] names a rule for every table either lineage
//! creates, the way `TABLE_SCOPES` names a scope, and
//! `every_table_has_a_retention_rule` fails the build when a migration adds a
//! table without one. A rule says what bounds the table:
//!
//! - [`Retention::Keep`]: kept for good, because it is small, authored, or the
//!   long-term record.
//! - [`Retention::Owned`]: deleted with its parent, by foreign-key cascade or
//!   by the parent's owner delete.
//! - [`Retention::Tiered`]: moves out of the hot database with resolved
//!   history (the cold tier, cairn/5586 and its eviction child).
//! - [`Retention::Evict`]: deleted by this module's evictor.
//! - [`Retention::OwnedBy`]: pruned by an existing bespoke owner elsewhere.
//! - [`Retention::Undecided`]: no rule yet. A named owner holds the decision,
//!   and the proposal records what the census suggests.
//!
//! Every entry also carries a byte budget, which the daily census
//! (cairn/5588) checks.
//!
//! # The evictor
//!
//! [`run_pass`] applies each [`Eviction`] in turn. One page is a short read
//! that selects up to [`PAGE_ROWS`] rowids (fewer for tables whose rows are
//! large), then one short write that deletes exactly those rowids. Pages are
//! separate transactions, so the checkpoint can fold the logical log between
//! them. When the log has grown past [`EvictorBudget::fold_at_log_bytes`], the
//! pass folds it before the next page, and ends the pass if the fold fails
//! (tenet 1260). No delete is an `IN (SELECT ...)` (cairn/5332): the rowids
//! are selected first and bound as a literal list.
//!
//! The journals (`effect_outbox`, `search_outbox`, `message_streams`) are
//! walked in rowid order from the start of the table. Their rows are inserted
//! in time order and `created_at` is stamped once at insert, so the walk stops
//! at the first page whose oldest row was created after the cutoff. Rows the
//! rule keeps that are older than the cutoff, such as a stuck open stream, are
//! stepped over. Once a table has caught up, a pass therefore reads about one
//! window of inflow whatever the table's age, and needs no index on the age
//! column. That matters because the obvious index costs too much to build: the
//! columns sit after a large payload, and building `effect_outbox(state,
//! updated_at)` took 39 s on the 10-30 production clone.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::storage::{DbResult, DrainBudget, LocalDb, RowExt, TrackedConnection};
use crate::turso::Value;

/// What bounds a table's size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    /// Kept for good. The reason says why that is bounded enough.
    Keep(&'static str),
    /// Deleted with its parent row, by foreign-key cascade or by the parent's
    /// owner delete (job cleanup, issue or project deletion).
    Owned { parent: &'static str },
    /// Moves to the cold tier with resolved history; `owner` builds the move.
    Tiered { owner: &'static str },
    /// Deleted by this module's evictor.
    Evict(Eviction),
    /// Pruned by an existing owner outside this module.
    OwnedBy {
        owner: &'static str,
        rule: &'static str,
    },
    /// No deleting rule yet. `owner` (an issue or thread) holds the decision.
    Undecided {
        owner: &'static str,
        proposal: &'static str,
    },
}

/// One table's rule and the payload budget the daily census holds it to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableRetention {
    pub table: &'static str,
    pub rule: Retention,
    /// Payload megabytes above which the census flags the table.
    pub budget_mb: u32,
}

/// The budget of a table that states none: well above every small table in
/// the 10-30 census, so crossing it is news.
pub const DEFAULT_BUDGET_MB: u32 = 128;

impl TableRetention {
    const fn budget(self, budget_mb: u32) -> Self {
        Self { budget_mb, ..self }
    }
}

const fn rule(table: &'static str, rule: Retention) -> TableRetention {
    TableRetention {
        table,
        rule,
        budget_mb: DEFAULT_BUDGET_MB,
    }
}

const fn keep(table: &'static str, reason: &'static str) -> TableRetention {
    rule(table, Retention::Keep(reason))
}

const fn owned(table: &'static str, parent: &'static str) -> TableRetention {
    rule(table, Retention::Owned { parent })
}

const fn tiered(table: &'static str) -> TableRetention {
    rule(
        table,
        Retention::Tiered {
            owner: "cairn/5586",
        },
    )
}

const fn evict(table: &'static str, eviction: Eviction) -> TableRetention {
    rule(table, Retention::Evict(eviction))
}

const fn owned_by(
    table: &'static str,
    owner: &'static str,
    rule_text: &'static str,
) -> TableRetention {
    rule(
        table,
        Retention::OwnedBy {
            owner,
            rule: rule_text,
        },
    )
}

const fn undecided(
    table: &'static str,
    owner: &'static str,
    proposal: &'static str,
) -> TableRetention {
    rule(table, Retention::Undecided { owner, proposal })
}

const SKELETON: &str = "skeleton: keeps every cairn:// URI resolvable while its history tiers";
const CONFIG: &str = "configuration, bounded by what the operator configures";
const IDENTITY: &str = "identity and credentials; a handful of rows";
const MARKER: &str = "single-row state marker";
const SMALL: &str = "small and slow-growing; the census budget flags it if that changes";
const AUTHORED: &str = "authored content, deleted only by its author or with its project";
const ROLLUP: &str = "rollup: the long-term record history rolls up into";
const SECURITY: &str = "security record, kept for audit";
const TEAM_FLEET: &str = "team-replica fleet state, one row per member or intent";

/// Who decides the check-verdict history tables (verdict reuse is the testing
/// thread's).
const TESTING: &str = "cairn://p/cairn/testing";

/// Every table either lineage creates, with its rule. Proven exhaustive and
/// free of stale entries by `every_table_has_a_retention_rule`.
pub const TABLE_RETENTION: &[TableRetention] = &[
    // Skeleton: small rows that make URIs resolve.
    keep("projects", SKELETON),
    keep("issues", SKELETON),
    keep("jobs", SKELETON),
    keep("runs", SKELETON),
    keep("sessions", SKELETON),
    keep("turns", SKELETON),
    keep("threads", SKELETON),
    keep("merge_requests", SKELETON),
    keep("pack_catalog", SKELETON),
    keep(
        "executions",
        "skeleton; delegated packets live in their own table (cairn/5597)",
    )
    .budget(1_200),
    keep("workspaces", "structural root"),
    keep("teams", "structural root"),
    keep("project_routes", "structural root: the project router"),
    keep("cairn_schema_migrations", "the migration ledger"),
    // History that moves to the cold tier.
    tiered("events").budget(8_000),
    tiered("tool_invocations").budget(256),
    tiered("tool_invocation_runs"),
    tiered("turn_embeddings").budget(1_024),
    tiered("session_skyline_cache").budget(768),
    tiered("execution_history").budget(1_100),
    tiered("archival_blobs"),
    owned("event_read_tokens", "events"),
    owned("event_vibes", "events"),
    // Evicted here.
    evict("effect_outbox", Eviction::DoneOutbox).budget(64),
    evict("search_outbox", Eviction::AppliedSearchOutbox).budget(32),
    evict("message_streams", Eviction::TerminalStreams).budget(64),
    evict("message_stream_chunks", Eviction::TerminalStreams).budget(256),
    evict("codemap_cache", Eviction::InvalidatedCodemaps),
    evict("artifacts", Eviction::SupersededArcVersions).budget(768),
    // Pruned by their own owners.
    owned_by(
        "runner_events",
        "cairn-core fleet::runner_events",
        "14 days or 200,000 rows",
    ),
    owned_by(
        "fact_log",
        "cairn-db storage::fact_log::prune",
        "per-source retention",
    ),
    owned_by(
        "fact_log_wakes",
        "cairn-db storage::fact_log::prune_wakes",
        "closed wakes",
    ),
    owned_by(
        "page_calls",
        "cairn-core pages::log",
        "newest N calls per page",
    ),
    owned_by(
        "command_outcome_history",
        "cairn-core fleet::reliability",
        "finished-at window",
    ),
    owned_by(
        "service_log",
        "cairn-core project_services::log",
        "newest 5,000 rows per service",
    ),
    owned_by(
        "response_invocations",
        "cairn-db storage::response_invocations",
        "newest RESPONSE_INVOCATION_RETENTION per response",
    ),
    owned_by(
        "route_firings",
        "cairn-db storage::route_firings",
        "newest ROUTE_FIRING_RETENTION per route",
    ),
    owned_by(
        "ref_store_samples",
        "cairn-core vcs_maintenance",
        "measurements older than its RETENTION",
    ),
    owned_by(
        "ingress_events",
        "cairn-core ingress::prune_events",
        "the fact log's ingress window",
    ),
    owned_by(
        "vibe_axis_samples",
        "cairn-core embeddings::vibe_recompute",
        "replaced on recompute",
    ),
    owned_by(
        "queued_messages",
        "cairn-core messages::queued",
        "deleted on delivery",
    ),
    owned_by(
        "executor_preparation_refusals",
        "cairn-core fleet::preparation",
        "cleared when the executor prepares",
    ),
    owned_by(
        "main_cadence_interruptions",
        "cairn-core execution::checks_main",
        "cleared when the cadence resumes",
    ),
    owned_by(
        "node_scratches",
        "cairn-core node_scratch::lifecycle",
        "deleted when the scratch's owner resolves",
    ),
    owned_by(
        "node_scratch_replicas",
        "cairn-core node_scratch::owner",
        "deleted with its scratch",
    ),
    owned_by(
        "workflow_call",
        "cairn-workflow journal",
        "deleted with its workflow run",
    ),
    // Undecided, each with an owner (the tables cairn/5585's census named).
    undecided(
        "cas_cache",
        "cairn/4648",
        "not a cache on a private database: 99% of rows (944 of 949 MB on 10-30) are the \
         authoritative bytes of local attachments; only rows 4648's reference inventory \
         proves unreferenced may go",
    )
    .budget(1_024),
    undecided(
        "check_result_cache",
        TESTING,
        "keep the newest row per check and input plus 90 days",
    )
    .budget(1_400),
    undecided(
        "check_result_observations",
        TESTING,
        "window observations (newest per check and input plus 90 days); children cascade",
    )
    .budget(256),
    owned("check_test_results", "check_result_observations").budget(1_100),
    owned("check_deferred_tests", "check_result_observations"),
    owned("check_result_commit_aliases", "check_result_observations"),
    owned("check_observation_relabels", "check_result_observations"),
    undecided(
        "pr_resolution_attributions",
        TESTING,
        "trim lane_snapshot (~215 KB a row) once the merge resolves, or cap per merge request",
    )
    .budget(640),
    undecided(
        "runtime_admission_requests",
        "cairn://p/cairn/execution",
        "delete settled requests 30 days after they settle; prepared launches and calls cascade",
    ),
    owned("prepared_ordinary_launches", "runtime_admission_requests").budget(384),
    owned("prepared_runtime_calls", "runtime_admission_requests"),
    owned(
        "team_ordinary_admission_authority",
        "prepared_ordinary_launches",
    ),
    undecided(
        "codex_watchdog_lifecycle",
        "cairn://p/cairn/harness",
        "window: 30 days on created_at",
    )
    .budget(256),
    undecided(
        "execution_queue",
        "cairn/5543",
        "14 days by newest row per entry (cairn/5542 has landed)",
    )
    .budget(512),
    undecided(
        "execution_queue_closed_incarnations",
        "cairn/5543",
        "one row per runner start; delete an incarnation once the queue rule has deleted its entries",
    ),
    undecided(
        "job_file_activity",
        "cairn://p/cairn/performance",
        "delete with its job; no foreign key carries it today",
    ),
    // Deleted with their parent.
    owned("agent_waits", "jobs"),
    owned("attention_pushes", "jobs"),
    owned("attention_read_cursors", "jobs"),
    owned("build_change_notifications", "jobs"),
    owned("checkpoint_command_cache", "jobs"),
    owned("checkpoint_runs", "jobs"),
    owned("file_changes", "jobs"),
    owned("job_artifact_inputs", "jobs"),
    owned("job_browsers", "jobs"),
    owned("job_repls", "jobs"),
    owned("job_terminals", "jobs"),
    owned("provider_limit_holds", "jobs"),
    owned("suppressed_wakes", "jobs"),
    owned("todos", "jobs"),
    owned("turn_end_check_attempts", "jobs"),
    owned("wake_subscriptions", "jobs"),
    owned("thread_compaction_entries", "jobs"),
    owned("thread_compaction_marks", "jobs"),
    owned("thread_compactions", "jobs"),
    owned("thread_era_children", "jobs"),
    owned("thread_eras", "jobs"),
    owned("repl_exchanges", "job_repls"),
    owned("codex_watchdog_leases", "sessions"),
    owned("permission_requests", "runs"),
    owned("prompts", "runs"),
    owned("write_replay_ledger", "runs"),
    owned("action_runs", "executions"),
    owned("artifact_content", "executions"),
    owned("condition_evaluations", "executions"),
    // Kept with their execution, finished or not: an archived execution's
    // packets are what its delegated children's lineage reads.
    owned("delegated_packets", "executions"),
    owned("execution_gates", "executions"),
    owned("execution_trigger_sources", "executions"),
    owned("pr_node_port_fires", "executions"),
    owned("pr_publish_attempts", "action_runs"),
    owned("pack_catalog_backfill_attempts", "execution_history"),
    owned("pack_catalog_references", "pack_catalog"),
    owned("merge_attempt_events", "merge_requests"),
    owned("human_base_merge_policy_events", "projects"),
    owned("doc_references", "issues"),
    owned("issue_labels", "issues"),
    owned("issue_workspaces", "issues"),
    owned("memory_triage_issue_memories", "issues"),
    owned("thread_branches", "threads"),
    owned("thread_read_positions", "threads"),
    owned("thread_services", "threads"),
    owned("service_schedules", "projects"),
    owned("wake_conditions", "threads"),
    owned("wake_schedules", "threads"),
    owned("jj_reconcile_incoming_files", "jj_reconcile_intents"),
    owned("jj_reconcile_items", "jj_reconcile_intents"),
    owned("tenet_evidence", "tenet_evidence_events"),
    // Kept: authored, configuration, identity, rollups and small state.
    keep("comments", AUTHORED),
    keep("messages", AUTHORED).budget(256),
    keep("memories", AUTHORED),
    keep("posts", AUTHORED),
    keep("post_comments", AUTHORED),
    keep("tenets", AUTHORED),
    keep("tenet_ruling_imports", AUTHORED),
    keep("tenet_evidence_events", AUTHORED),
    keep(
        "attachments",
        "authored attachments; their bytes live in cas_cache",
    ),
    keep(
        "image_refs",
        "authored images; their bytes live in cas_cache",
    ),
    keep("labels", CONFIG),
    keep("action_configs", CONFIG),
    keep("skill_configs", CONFIG),
    keep("config_disables", CONFIG),
    keep("inference_recipes", CONFIG),
    keep("inference_deployments", CONFIG),
    keep("inference_pools", CONFIG),
    keep("inference_bginf_link", CONFIG),
    keep(
        "inference_deployment_state",
        "one row per inference deployment the runner has acted on",
    ),
    owned_by(
        "inference_deployment_loads",
        "cairn-core orchestrator::inference_control",
        "the newest loads per deployment, pruned on insert",
    ),
    keep("inference_pool_control", CONFIG),
    keep(
        "inference_requests",
        "the pool controller deletes answered requests a week after they were answered",
    ),
    keep("ingress_endpoints", CONFIG),
    keep("account", IDENTITY),
    keep("anon_device", IDENTITY),
    keep("account_credential_events", IDENTITY),
    keep("authority_grants", IDENTITY),
    keep("authorization_events", IDENTITY),
    keep("subscription_periods", IDENTITY),
    keep("github_app", IDENTITY),
    keep("github_installations", IDENTITY),
    keep("relay_channel", IDENTITY),
    keep("executor_enrollment_grants", IDENTITY),
    keep("executor_enrollments", IDENTITY),
    keep("operator_notification_dismissals", IDENTITY),
    keep("disclosure_incidents", SECURITY),
    keep("disclosure_actions", SECURITY),
    keep("disclosure_affected_records", SECURITY),
    keep("quarantined_records", SECURITY),
    keep("token_rollup", ROLLUP),
    keep("token_rollup_runs", ROLLUP),
    keep("tool_analytics_hourly", ROLLUP),
    keep("run_activity_minutes", ROLLUP),
    keep(
        "resource_embeddings",
        "expensive per-resource vectors, one per resource",
    )
    .budget(192),
    keep(
        "command_resource_profiles",
        "bounded by command-key cardinality; samples capped in the row",
    ),
    keep(
        "command_duration_profiles",
        "bounded by command-key cardinality; samples capped in the row",
    ),
    keep(
        "command_contention_profiles",
        "bounded by command-key cardinality; samples capped in the row",
    ),
    keep("analytics_rollup_backfill_state", MARKER),
    keep("app_boot_state", MARKER),
    keep("archival_backfill_state", MARKER),
    keep("delegated_packet_backfill_state", MARKER),
    keep("attention_push_retirement_backfill", MARKER),
    keep("embedding_state", MARKER),
    keep("integrity_sweep_state", MARKER),
    keep("trigger_accumulator_state", MARKER),
    keep("runtime_admission_cursors", MARKER),
    keep("channel_cursor", MARKER),
    keep("ci_logs_cache", "refetchable CI logs; empty on 10-30"),
    keep("vibe_axes", SMALL),
    keep("session_positions", SMALL),
    keep("feed_cursors", SMALL),
    keep("discord_surface", SMALL),
    keep("channel_outbound", SMALL),
    keep("channel_state_card", SMALL),
    keep("channel_inbound", SMALL),
    keep("channel_conversation_binding", SMALL),
    keep("channel_ask_resolution", SMALL),
    keep("channel_ask_action", SMALL),
    keep("compile_facts", SMALL),
    keep("execution_supersessions", SMALL),
    keep("executor_desktop_automation", SMALL),
    keep("executor_repairs", SMALL),
    keep("held_publications", SMALL),
    keep("jj_reconcile_intents", SMALL),
    keep("jj_reconcile_quarantines", SMALL),
    keep("job_presence", SMALL),
    keep("landing_allocations", SMALL),
    keep("pending_injections", SMALL),
    keep("purpose_debts", SMALL),
    keep("ref_store_maintenance", SMALL),
    keep("resource_surfacings", SMALL),
    keep("workflow_journal", SMALL),
    keep("workflow_progress", SMALL),
    keep("workflow_run", SMALL),
    keep("device_presence", TEAM_FLEET),
    keep("remote_intents", TEAM_FLEET),
    keep("executor_registry", TEAM_FLEET),
];

/// The rule for `table`, if it has one.
pub fn retention_for(table: &str) -> Option<&'static TableRetention> {
    TABLE_RETENTION.iter().find(|entry| entry.table == table)
}

/// The most rows one evictor write deletes.
pub const PAGE_ROWS: usize = 500;

/// How often the orchestrator runs a pass.
pub const PASS_INTERVAL: Duration = Duration::from_secs(10 * 60);

const DAY_S: i64 = 86_400;

/// Done outbox entries older than this are gone.
const DONE_OUTBOX_AGE_S: i64 = DAY_S;
/// Applied search-outbox entries older than this are gone.
const APPLIED_SEARCH_OUTBOX_AGE_S: i64 = DAY_S;
/// A finalized or aborted stream's content lives in its event; the stream row
/// and its chunks go this long after it ends.
const TERMINAL_STREAM_AGE_S: i64 = DAY_S;
/// Invalidated code maps older than this are gone, except each project's
/// newest map.
const INVALIDATED_CODEMAP_AGE_S: i64 = DAY_S;
/// Versions of an arc document kept per thread job. Every patch stores the
/// whole document again, and only the newest version is read, except a
/// version a consumer pinned in `job_artifact_inputs`, which is always kept.
pub const ARC_VERSIONS_KEPT: i64 = 5;

/// Code-map rows are about 1 MB each, and Turso reads a whole record to read
/// any column of it, so this rule walks and deletes a few rows at a time: on
/// the 10-30 clone a 500-row walk took 1 s at p50 and 6.5 s at worst, and
/// folding 1,403 deleted maps took 144 s.
const CODEMAP_PAGE_ROWS: usize = 4;
const CODEMAP_WALK_ROWS: usize = 2 * CODEMAP_PAGE_ROWS;
/// Arc versions reach 0.5 MB each; this keeps one write near 4 MB.
const ARC_PAGE_ROWS: usize = 8;
/// Arc chains examined per chain read.
const ARC_CHAIN_BATCH: i64 = 64;

pub(crate) const OUTBOX_WALK: &str = "SELECT rowid, state, updated_at, created_at \
     FROM effect_outbox WHERE rowid > ?1 ORDER BY rowid LIMIT ?2";
pub(crate) const SEARCH_OUTBOX_WALK: &str = "SELECT rowid, status, created_at \
     FROM search_outbox WHERE rowid > ?1 ORDER BY rowid LIMIT ?2";
pub(crate) const STREAM_WALK: &str = "SELECT rowid, id, status, finalized_at, created_at \
     FROM message_streams WHERE rowid > ?1 ORDER BY rowid LIMIT ?2";
pub(crate) const STREAM_CHUNKS: &str =
    "SELECT rowid FROM message_stream_chunks WHERE stream_id = ?1 LIMIT ?2";
pub(crate) const CODEMAP_WALK: &str = "SELECT rowid, project_id, computed_at, invalidated_at \
     FROM codemap_cache WHERE rowid > ?1 ORDER BY rowid LIMIT ?2";
pub(crate) const CODEMAP_NEWEST: &str =
    "SELECT max(computed_at) FROM codemap_cache WHERE project_id = ?1";
pub(crate) const ARC_CHAINS: &str = "SELECT job_id, max(version) FROM artifacts \
     WHERE output_name = 'arc' AND job_id > ?1 GROUP BY job_id ORDER BY job_id LIMIT ?2";
pub(crate) const ARC_SUPERSEDED: &str = "SELECT rowid, id, artifact_type, version FROM artifacts \
     WHERE output_name = 'arc' AND job_id = ?1 AND version <= ?2 AND version > ?3 \
     ORDER BY version LIMIT ?4";
pub(crate) const ARC_PINNED: &str = "SELECT version FROM job_artifact_inputs \
     WHERE producer_job_id = ?1 AND output_name = 'arc'";

/// One deleting rule of the evictor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Eviction {
    /// `effect_outbox` entries `done` more than a day ago.
    DoneOutbox,
    /// `search_outbox` entries `applied` more than a day ago.
    AppliedSearchOutbox,
    /// Finalized or aborted `message_streams` that ended more than a day ago,
    /// with their `message_stream_chunks`.
    TerminalStreams,
    /// `codemap_cache` rows invalidated more than a day ago, except each
    /// project's newest map.
    InvalidatedCodemaps,
    /// `artifacts` arc versions older than the newest [`ARC_VERSIONS_KEPT`].
    SupersededArcVersions,
}

impl Eviction {
    /// Every rule, in the order a pass applies them: most reclaimed first.
    pub const ALL: [Eviction; 5] = [
        Eviction::DoneOutbox,
        Eviction::TerminalStreams,
        Eviction::InvalidatedCodemaps,
        Eviction::SupersededArcVersions,
        Eviction::AppliedSearchOutbox,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Eviction::DoneOutbox => "done_outbox",
            Eviction::AppliedSearchOutbox => "applied_search_outbox",
            Eviction::TerminalStreams => "terminal_streams",
            Eviction::InvalidatedCodemaps => "invalidated_codemaps",
            Eviction::SupersededArcVersions => "superseded_arc_versions",
        }
    }

    /// The tables this rule deletes from.
    pub fn tables(self) -> &'static [&'static str] {
        match self {
            Eviction::DoneOutbox => &["effect_outbox"],
            Eviction::AppliedSearchOutbox => &["search_outbox"],
            Eviction::TerminalStreams => &["message_streams", "message_stream_chunks"],
            Eviction::InvalidatedCodemaps => &["codemap_cache"],
            Eviction::SupersededArcVersions => &["artifacts"],
        }
    }

    async fn page(self, db: &LocalDb, now: i64, cursor: &mut Cursor) -> DbResult<Page> {
        let started = Instant::now();
        let selection = match self {
            Eviction::DoneOutbox => select_done_outbox(db, now, cursor).await?,
            Eviction::AppliedSearchOutbox => select_applied_search_outbox(db, now, cursor).await?,
            Eviction::TerminalStreams => select_terminal_streams(db, now, cursor).await?,
            Eviction::InvalidatedCodemaps => select_invalidated_codemaps(db, now, cursor).await?,
            Eviction::SupersededArcVersions => select_superseded_arcs(db, cursor).await?,
        };
        let select = started.elapsed();
        let rows = selection.rows();
        let started = Instant::now();
        if rows > 0 && !selection.unlink_parents.is_empty() {
            delete_versions(db, &selection.unlink_parents, &selection.deletes).await?;
        } else if rows > 0 {
            let deletes = selection.deletes.clone();
            db.write(move |conn| {
                let deletes = deletes.clone();
                Box::pin(async move {
                    for (table, rowids) in deletes {
                        delete_rowids(conn, table, &rowids).await?;
                    }
                    Ok(())
                })
            })
            .await?;
        }
        Ok(Page {
            rows,
            select,
            write: started.elapsed(),
            done: selection.done,
        })
    }
}

/// Where a rule's walk stands within one pass.
#[derive(Debug, Default)]
struct Cursor {
    rowid: i64,
    /// Arc chains: the job being trimmed, its newest version, and the last
    /// version examined in it.
    job: Option<(String, i64)>,
    after_job: String,
    after_version: i64,
}

/// What one page read chose to delete.
#[derive(Debug, Default)]
struct Selection {
    /// Rowids per table, deleted in this order.
    deletes: Vec<(&'static str, Vec<i64>)>,
    /// Artifact ids whose children's `parent_version_id` is cleared first.
    unlink_parents: Vec<String>,
    /// The rule has nothing more to delete in this pass.
    done: bool,
}

impl Selection {
    fn rows(&self) -> usize {
        self.deletes.iter().map(|(_, rowids)| rowids.len()).sum()
    }
}

#[derive(Debug)]
struct Page {
    rows: usize,
    select: Duration,
    write: Duration,
    done: bool,
}

fn placeholders(count: usize) -> String {
    (1..=count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Delete exactly `rowids` from `table`, a literal list so the statement is a
/// rowid seek per entry and never a subquery.
async fn delete_rowids(conn: &TrackedConnection, table: &str, rowids: &[i64]) -> DbResult<u64> {
    let sql = format!(
        "DELETE FROM {table} WHERE rowid IN ({})",
        placeholders(rowids.len())
    );
    let values: Vec<Value> = rowids.iter().copied().map(Value::Integer).collect();
    Ok(conn.execute(&sql, values).await?)
}

pub(crate) const ARC_UNLINK: &str = "UPDATE artifacts SET parent_version_id = NULL \
     WHERE parent_version_id IN";
pub(crate) const ARC_DANGLING: &str = "SELECT count(*) FROM artifacts WHERE parent_version_id IN";

/// Delete artifact versions without the engine's self-reference check.
///
/// `artifacts.parent_version_id` references `artifacts(id)`, and the pinned
/// Turso checks a self-referencing key by scanning the table for every deleted
/// row, ignoring `idx_artifacts_parent_version` (an ordinary child key's index
/// is used). On the 10-30 clone that made 8-row writes take 1.7 s at p50 and
/// 8.7 s at worst. So the write runs on a dedicated connection with
/// enforcement off, as `MigrationRunner::run_fk_off` does, and states the
/// invariant itself: it clears the pointers that reference the deleted
/// versions, deletes them, and before committing proves through the index that
/// no row still points at one. Nothing else references `artifacts`, and a new
/// version only ever points at the newest, which this rule never deletes.
async fn delete_versions(
    db: &LocalDb,
    ids: &[String],
    deletes: &[(&'static str, Vec<i64>)],
) -> DbResult<()> {
    let conn = db.connect().await?;
    // Before BEGIN: the pragma is a no-op inside an open transaction. The
    // connection is outside the pool and dropped here, so it cannot leak.
    conn.execute("PRAGMA foreign_keys = OFF", ()).await?;
    let id_list = || -> (String, Vec<Value>) {
        (
            format!("({})", placeholders(ids.len())),
            ids.iter().cloned().map(Value::Text).collect(),
        )
    };
    let outcome: DbResult<()> = async {
        conn.execute(db.concurrent_begin(), ()).await?;
        let (list, values) = id_list();
        conn.execute(&format!("{ARC_UNLINK} {list}"), values)
            .await?;
        for (table, rowids) in deletes {
            delete_rowids(&conn, table, rowids).await?;
        }
        let (list, values) = id_list();
        let mut rows = conn
            .query(&format!("{ARC_DANGLING} {list}"), values)
            .await?;
        let dangling = match rows.next().await? {
            Some(row) => row.i64(0)?,
            None => 0,
        };
        drop(rows);
        if dangling != 0 {
            return Err(crate::storage::DbError::internal(format!(
                "{dangling} artifacts still reference a version this page deletes"
            )));
        }
        conn.execute("COMMIT", ()).await?;
        Ok(())
    }
    .await;
    if outcome.is_err() {
        let _ = conn.execute("ROLLBACK", ()).await;
    }
    let restored = conn.execute("PRAGMA foreign_keys = ON", ()).await;
    outcome?;
    restored?;
    Ok(())
}

/// One walked journal row: its rowid, whether the rule deletes it, and when it
/// was created.
struct JournalRow {
    rowid: i64,
    delete: bool,
    created_at: i64,
}

/// Shared walk of an append-ordered journal: delete what `classify` marks,
/// step over what it keeps, and stop at the first row created after the
/// cutoff.
fn journal_page(
    table: &'static str,
    rows: Vec<JournalRow>,
    cutoff: i64,
    cursor: &mut Cursor,
) -> Selection {
    let walked_all = rows.len() < PAGE_ROWS;
    let mut rowids = Vec::new();
    let mut reached_new = false;
    for row in rows {
        if row.created_at >= cutoff {
            reached_new = true;
            break;
        }
        cursor.rowid = row.rowid;
        if row.delete {
            rowids.push(row.rowid);
        }
    }
    let done = rowids.is_empty() && (reached_new || walked_all);
    Selection {
        deletes: if rowids.is_empty() {
            Vec::new()
        } else {
            vec![(table, rowids)]
        },
        unlink_parents: Vec::new(),
        done,
    }
}

async fn select_done_outbox(db: &LocalDb, now: i64, cursor: &mut Cursor) -> DbResult<Selection> {
    let cutoff = now - DONE_OUTBOX_AGE_S;
    let after = cursor.rowid;
    let rows = db
        .query_all(OUTBOX_WALK, (after, PAGE_ROWS as i64), move |row| {
            Ok(JournalRow {
                rowid: row.i64(0)?,
                delete: row.text(1)? == "done" && row.i64(2)? < cutoff,
                created_at: row.i64(3)?,
            })
        })
        .await?;
    Ok(journal_page("effect_outbox", rows, cutoff, cursor))
}

async fn select_applied_search_outbox(
    db: &LocalDb,
    now: i64,
    cursor: &mut Cursor,
) -> DbResult<Selection> {
    let cutoff = now - APPLIED_SEARCH_OUTBOX_AGE_S;
    let after = cursor.rowid;
    let rows = db
        .query_all(SEARCH_OUTBOX_WALK, (after, PAGE_ROWS as i64), move |row| {
            Ok(JournalRow {
                rowid: row.i64(0)?,
                delete: row.text(1)? == "applied",
                created_at: row.i64(2)?,
            })
        })
        .await?;
    Ok(journal_page("search_outbox", rows, cutoff, cursor))
}

/// Terminal streams and their chunks, at most [`PAGE_ROWS`] rows in all. A
/// stream is deleted only in the page that also deletes its last chunks, so a
/// write never relies on the cascade to remove an unbounded number of rows. A
/// stream with more chunks than fit is trimmed over several pages and revisited
/// until it is empty.
async fn select_terminal_streams(
    db: &LocalDb,
    now: i64,
    cursor: &mut Cursor,
) -> DbResult<Selection> {
    let cutoff = now - TERMINAL_STREAM_AGE_S;
    let after = cursor.rowid;
    let (selection, next) = db
        .read(move |conn| {
            Box::pin(async move {
                let mut walked = Vec::new();
                let mut rows = conn.query(STREAM_WALK, (after, PAGE_ROWS as i64)).await?;
                while let Some(row) = rows.next().await? {
                    walked.push((
                        row.i64(0)?,
                        row.text(1)?,
                        row.text(2)?,
                        row.opt_i64(3)?,
                        row.i64(4)?,
                    ));
                }
                drop(rows);
                let walked_all = walked.len() < PAGE_ROWS;
                let mut next = after;
                let mut chunks: Vec<i64> = Vec::new();
                let mut streams: Vec<i64> = Vec::new();
                let mut reached_new = false;
                let mut full = false;
                for (rowid, id, status, finalized_at, created_at) in walked {
                    if created_at >= cutoff {
                        reached_new = true;
                        break;
                    }
                    let terminal = matches!(status.as_str(), "finalized" | "aborted");
                    if !terminal || finalized_at.is_none_or(|at| at >= cutoff) {
                        next = rowid;
                        continue;
                    }
                    let room = PAGE_ROWS - chunks.len() - streams.len();
                    if room == 0 {
                        full = true;
                        break;
                    }
                    let mut found = Vec::new();
                    let mut chunk_rows = conn.query(STREAM_CHUNKS, (id, room as i64)).await?;
                    while let Some(chunk) = chunk_rows.next().await? {
                        found.push(chunk.i64(0)?);
                    }
                    drop(chunk_rows);
                    let emptied = found.len() < room;
                    chunks.extend(found);
                    if emptied {
                        streams.push(rowid);
                        next = rowid;
                    } else {
                        // Revisit this stream on the next page.
                        full = true;
                        break;
                    }
                }
                let mut deletes = Vec::new();
                if !chunks.is_empty() {
                    deletes.push(("message_stream_chunks", chunks));
                }
                if !streams.is_empty() {
                    deletes.push(("message_streams", streams));
                }
                let done = deletes.is_empty() && !full && (reached_new || walked_all);
                Ok((
                    Selection {
                        deletes,
                        unlink_parents: Vec::new(),
                        done,
                    },
                    next,
                ))
            })
        })
        .await?;
    cursor.rowid = next;
    Ok(selection)
}

/// Invalidated code maps, never a project's newest. The table is walked whole
/// once per pass; this rule is what keeps it at a few rows per project.
async fn select_invalidated_codemaps(
    db: &LocalDb,
    now: i64,
    cursor: &mut Cursor,
) -> DbResult<Selection> {
    let cutoff = now - INVALIDATED_CODEMAP_AGE_S;
    let after = cursor.rowid;
    let (selection, next) = db
        .read(move |conn| {
            Box::pin(async move {
                let mut walked = Vec::new();
                let mut rows = conn
                    .query(CODEMAP_WALK, (after, CODEMAP_WALK_ROWS as i64))
                    .await?;
                while let Some(row) = rows.next().await? {
                    walked.push((row.i64(0)?, row.text(1)?, row.i64(2)?, row.opt_i64(3)?));
                }
                drop(rows);
                let walked_all = walked.len() < CODEMAP_WALK_ROWS;
                let mut newest: HashMap<String, i64> = HashMap::new();
                let mut rowids = Vec::new();
                let mut next = after;
                let mut full = false;
                for (rowid, project_id, computed_at, invalidated_at) in walked {
                    if rowids.len() == CODEMAP_PAGE_ROWS {
                        full = true;
                        break;
                    }
                    next = rowid;
                    if invalidated_at.is_none_or(|at| at >= cutoff) {
                        continue;
                    }
                    let project_newest = match newest.get(&project_id) {
                        Some(value) => *value,
                        None => {
                            let mut max = conn.query(CODEMAP_NEWEST, (project_id.clone(),)).await?;
                            let value = match max.next().await? {
                                Some(row) => row.opt_i64(0)?.unwrap_or(i64::MIN),
                                None => i64::MIN,
                            };
                            drop(max);
                            newest.insert(project_id.clone(), value);
                            value
                        }
                    };
                    if computed_at < project_newest {
                        rowids.push(rowid);
                    }
                }
                let done = rowids.is_empty() && !full && walked_all;
                let deletes = if rowids.is_empty() {
                    Vec::new()
                } else {
                    vec![("codemap_cache", rowids)]
                };
                Ok((
                    Selection {
                        deletes,
                        unlink_parents: Vec::new(),
                        done,
                    },
                    next,
                ))
            })
        })
        .await?;
    cursor.rowid = next;
    Ok(selection)
}

/// Arc versions older than the newest [`ARC_VERSIONS_KEPT`], oldest first. The
/// version a deleted row's child points at is cleared in the same write, so the
/// self-reference never dangles.
async fn select_superseded_arcs(db: &LocalDb, cursor: &mut Cursor) -> DbResult<Selection> {
    if cursor.job.is_none() {
        let after = cursor.after_job.clone();
        let chains = db
            .query_all(ARC_CHAINS, (after, ARC_CHAIN_BATCH), |row| {
                Ok((row.text(0)?, row.i64(1)?))
            })
            .await?;
        let exhausted = (chains.len() as i64) < ARC_CHAIN_BATCH;
        let mut chosen = None;
        for (job_id, newest) in chains {
            cursor.after_job = job_id.clone();
            if newest > ARC_VERSIONS_KEPT {
                chosen = Some((job_id, newest));
                break;
            }
        }
        match chosen {
            Some(chain) => {
                cursor.job = Some(chain);
                cursor.after_version = 0;
            }
            None => {
                return Ok(Selection {
                    done: exhausted,
                    ..Selection::default()
                })
            }
        }
    }
    let Some((job_id, newest)) = cursor.job.clone() else {
        return Ok(Selection::default());
    };
    let keep_from = newest - ARC_VERSIONS_KEPT;
    let after_version = cursor.after_version;
    let (victims, last_version, examined) = db
        .read(move |conn| {
            Box::pin(async move {
                let mut pinned = Vec::new();
                let mut rows = conn.query(ARC_PINNED, (job_id.clone(),)).await?;
                while let Some(row) = rows.next().await? {
                    pinned.push(row.i64(0)?);
                }
                drop(rows);
                let mut victims = Vec::new();
                let mut last_version = after_version;
                let mut examined = 0usize;
                let mut rows = conn
                    .query(
                        ARC_SUPERSEDED,
                        (job_id, keep_from, after_version, ARC_PAGE_ROWS as i64),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    examined += 1;
                    let version = row.i64(3)?;
                    last_version = version;
                    if row.text(2)? == "arc" && !pinned.contains(&version) {
                        victims.push((row.i64(0)?, row.text(1)?));
                    }
                }
                Ok((victims, last_version, examined))
            })
        })
        .await?;
    cursor.after_version = last_version;
    if examined < ARC_PAGE_ROWS {
        cursor.job = None;
    }
    let (rowids, ids): (Vec<i64>, Vec<String>) = victims.into_iter().unzip();
    Ok(Selection {
        deletes: if rowids.is_empty() {
            Vec::new()
        } else {
            vec![("artifacts", rowids)]
        },
        unlink_parents: ids,
        done: false,
    })
}

/// How much one pass may do before it yields to the next.
#[derive(Debug, Clone, Copy)]
pub struct EvictorBudget {
    /// Wall time after which the pass stops starting pages.
    pub pass_time: Duration,
    /// Rows after which the pass stops starting pages.
    pub pass_rows: usize,
    /// Logical-log size at which the pass folds before its next page.
    pub fold_at_log_bytes: u64,
    /// The fold work (excluding the drain) the pass lets its unfolded pages
    /// accumulate before it folds them, or `None` to fold on log size alone.
    /// See [`EvictorState`].
    pub fold_work_target: Option<Duration>,
    /// Folds one pass may ask for; past it the pass ends and leaves the rest
    /// to the next pass.
    pub folds_per_pass: usize,
}

impl EvictorBudget {
    /// The orchestrator's pass, every [`PASS_INTERVAL`]. Each fold closes the
    /// connection gate for its drain plus about half a second of work.
    ///
    /// One fold a pass, not the eight the clone rehearsal sized: the first
    /// build carrying the evictor wedged twice in the minute a pass folded
    /// (cairn/5644), and the engine condition behind it is not yet named. The
    /// checkpoint's attempt bound now caps what a stuck fold costs, and this
    /// caps how often a pass can meet the condition. A pass that needs a second
    /// fold ends `FoldBudget` and leaves its unfolded pages to the next pass,
    /// which folds them before it deletes more. Raise it once the condition
    /// is named and fixed; a backlog clears more slowly meanwhile, steady state
    /// (a page or two per rule) not at all.
    pub const PRODUCTION: Self = Self {
        pass_time: Duration::from_secs(90),
        pass_rows: 200_000,
        fold_at_log_bytes: 64 * 1024 * 1024,
        fold_work_target: Some(Duration::from_millis(500)),
        folds_per_pass: 1,
    };
}

/// The environment variable an operator sets to stop the evictor without a
/// rebuild. Read once, when the evictor's loop is spawned.
pub const EVICTOR_ENV: &str = "CAIRN_RETENTION_EVICTOR";

/// Whether the evictor runs, given [`EVICTOR_ENV`]'s value. It runs unless
/// the operator turned it off explicitly; an unset or unrecognised value
/// leaves it on, because eviction is what keeps the database bounded.
pub fn evictor_enabled(setting: Option<&str>) -> bool {
    !matches!(
        setting
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("off" | "0" | "false" | "no")
    )
}

/// The most pages one fold covers, however cheap a rule's pages fold.
const MAX_PAGES_PER_FOLD: usize = 64;

/// What the evictor has learned about folding, kept across passes.
///
/// A fold's cost follows the payload the deletes freed, not the log they
/// wrote: on the 10-30 clone 677k outbox deletes (2.5 GB of payload) wrote
/// about 90 MiB of log and the fold took 50 s, and 1,403 code-map deletes
/// (1.3 GB) wrote under 32 MiB and took 144 s. A log-size threshold therefore
/// cannot bound a fold. Instead the evictor measures each rule's fold work per
/// deleted page, and folds before the pages it has not folded are expected to
/// cost more than [`EvictorBudget::fold_work_target`]. A rule it has not
/// measured yet folds after its first page.
#[derive(Debug, Default)]
pub struct EvictorState {
    /// Fold work per deleted page, per rule.
    fold_ms_per_page: HashMap<Eviction, f64>,
    /// Deleted pages no fold has covered yet, and the rule that wrote them.
    unfolded: Option<(Eviction, usize)>,
}

impl EvictorState {
    /// How many unfolded pages of `eviction` one fold may cover.
    fn pages_per_fold(&self, eviction: Eviction, target: Duration) -> usize {
        match self.fold_ms_per_page.get(&eviction) {
            Some(ms) if *ms > 0.0 => {
                ((target.as_millis() as f64 / ms) as usize).clamp(1, MAX_PAGES_PER_FOLD)
            }
            Some(_) => MAX_PAGES_PER_FOLD,
            None => 1,
        }
    }

    /// Whether the unfolded pages must be folded before `next` deletes more.
    fn fold_due(&self, next: Eviction, target: Duration) -> bool {
        match self.unfolded {
            None => false,
            // Fold windows hold one rule, so each fold measures one rule.
            Some((rule, _)) if rule != next => true,
            Some((rule, pages)) => pages >= self.pages_per_fold(rule, target),
        }
    }

    fn deleted_page(&mut self, eviction: Eviction) {
        self.unfolded = match self.unfolded {
            Some((rule, pages)) if rule == eviction => Some((rule, pages + 1)),
            _ => Some((eviction, 1)),
        };
    }

    fn folded(&mut self, work: Duration) {
        if let Some((rule, pages)) = self.unfolded.take() {
            let sample = work.as_millis() as f64 / pages as f64;
            let estimate = match self.fold_ms_per_page.get(&rule) {
                Some(previous) => (previous + sample) / 2.0,
                None => sample,
            };
            self.fold_ms_per_page.insert(rule, estimate);
        }
    }
}

/// What one rule did in one pass.
#[derive(Debug, Clone)]
pub struct RuleReport {
    pub eviction: Eviction,
    pub pages: usize,
    pub rows: usize,
    /// Rows deleted by the largest single write.
    pub max_page_rows: usize,
    pub select_ms: Vec<u64>,
    pub write_ms: Vec<u64>,
    /// The rule ran out of work before the pass ended.
    pub finished: bool,
    pub error: Option<String>,
}

/// Why a pass ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassEnd {
    /// Every rule ran out of work.
    Complete,
    TimeBudget,
    RowBudget,
    FoldBudget,
    /// A fold failed; deleting more would only grow the log.
    FoldFailed,
}

/// One fold the pass asked for.
#[derive(Debug, Clone)]
pub struct FoldRecord {
    pub log_bytes_before: u64,
    pub log_bytes_after: u64,
    /// The whole checkpoint, drain included.
    pub duration: Duration,
    /// The drain: the stall every other database call paid before the fold.
    pub drain: Duration,
    pub succeeded: bool,
}

impl FoldRecord {
    /// The fold's own work, the part that scales with what was deleted.
    pub fn work(&self) -> Duration {
        self.duration.saturating_sub(self.drain)
    }
}

/// What one pass did.
#[derive(Debug, Clone)]
pub struct PassReport {
    pub rules: Vec<RuleReport>,
    pub folds: Vec<FoldRecord>,
    pub log_peak_bytes: u64,
    pub duration: Duration,
    pub end: PassEnd,
}

impl PassReport {
    pub fn rows(&self) -> usize {
        self.rules.iter().map(|rule| rule.rows).sum()
    }
}

/// The `q`-quantile of `values` (nearest rank), or 0 when there are none.
pub fn quantile(values: &[u64], q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

impl std::fmt::Display for PassReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "end={:?} rows={} duration_ms={} folds={} fold_work_ms_max={} fold_drain_ms_max={} \
             log_peak_mib={:.1}",
            self.end,
            self.rows(),
            self.duration.as_millis(),
            self.folds.len(),
            self.folds
                .iter()
                .map(|fold| fold.work().as_millis())
                .max()
                .unwrap_or(0),
            self.folds
                .iter()
                .map(|fold| fold.drain.as_millis())
                .max()
                .unwrap_or(0),
            self.log_peak_bytes as f64 / (1024.0 * 1024.0),
        )?;
        for rule in &self.rules {
            write!(
                f,
                "; {} rows={} pages={} write_ms_p50={} p99={} max={}{}",
                rule.eviction.name(),
                rule.rows,
                rule.pages,
                quantile(&rule.write_ms, 0.5),
                quantile(&rule.write_ms, 0.99),
                rule.write_ms.iter().max().copied().unwrap_or(0),
                match &rule.error {
                    Some(error) => format!(" error={error}"),
                    None if rule.finished => " finished".to_string(),
                    None => String::new(),
                }
            )?;
        }
        Ok(())
    }
}

/// Run every [`Eviction`] once over `db` at `now` (unix seconds), within
/// `budget`. Each rule's error is recorded in its report and the pass moves to
/// the next rule. `state` carries what earlier passes learned and any pages
/// they left unfolded.
pub async fn run_pass(
    db: &LocalDb,
    now: i64,
    budget: EvictorBudget,
    state: &mut EvictorState,
) -> PassReport {
    run_rules(db, now, budget, state, &Eviction::ALL).await
}

/// [`run_pass`] restricted to `rules`, in the order given.
pub async fn run_rules(
    db: &LocalDb,
    now: i64,
    budget: EvictorBudget,
    state: &mut EvictorState,
    rules: &[Eviction],
) -> PassReport {
    let started = Instant::now();
    let mut report = PassReport {
        rules: Vec::new(),
        folds: Vec::new(),
        log_peak_bytes: db.logical_log_bytes(),
        duration: Duration::ZERO,
        end: PassEnd::Complete,
    };
    let mut rows_total = 0usize;
    'rules: for &eviction in rules {
        let mut rule = RuleReport {
            eviction,
            pages: 0,
            rows: 0,
            max_page_rows: 0,
            select_ms: Vec::new(),
            write_ms: Vec::new(),
            finished: false,
            error: None,
        };
        let mut cursor = Cursor::default();
        loop {
            if started.elapsed() >= budget.pass_time {
                report.end = PassEnd::TimeBudget;
            } else if rows_total >= budget.pass_rows {
                report.end = PassEnd::RowBudget;
            } else if let Some(end) = fold_if_due(db, budget, state, eviction, &mut report).await {
                report.end = end;
            }
            if report.end != PassEnd::Complete {
                report.rules.push(rule);
                break 'rules;
            }
            match eviction.page(db, now, &mut cursor).await {
                Ok(page) => {
                    rule.select_ms.push(page.select.as_millis() as u64);
                    if page.rows > 0 {
                        state.deleted_page(eviction);
                        rule.pages += 1;
                        rule.rows += page.rows;
                        rule.max_page_rows = rule.max_page_rows.max(page.rows);
                        rule.write_ms.push(page.write.as_millis() as u64);
                        rows_total += page.rows;
                    }
                    report.log_peak_bytes = report.log_peak_bytes.max(db.logical_log_bytes());
                    if page.done {
                        rule.finished = true;
                        break;
                    }
                }
                Err(error) => {
                    rule.error = Some(error.to_string());
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
        report.rules.push(rule);
    }
    report.duration = started.elapsed();
    report
}

/// Fold before `next` deletes another page when the log has passed the
/// budget's threshold or the unfolded pages would cost more than the fold
/// target. Returns why the pass must end, if it must.
async fn fold_if_due(
    db: &LocalDb,
    budget: EvictorBudget,
    state: &mut EvictorState,
    next: Eviction,
    report: &mut PassReport,
) -> Option<PassEnd> {
    let log_bytes = db.logical_log_bytes();
    report.log_peak_bytes = report.log_peak_bytes.max(log_bytes);
    let cost_due = budget
        .fold_work_target
        .is_some_and(|target| state.fold_due(next, target));
    if log_bytes < budget.fold_at_log_bytes && !cost_due {
        return None;
    }
    if report.folds.len() >= budget.folds_per_pass {
        return Some(PassEnd::FoldBudget);
    }
    let outcome = db
        .checkpoint(3, Duration::from_millis(50), DrainBudget::PRODUCTION)
        .await;
    let (record, succeeded) = match outcome {
        Ok(checkpoint) => {
            let succeeded = checkpoint.succeeded();
            (
                FoldRecord {
                    log_bytes_before: checkpoint.log_bytes_before,
                    log_bytes_after: checkpoint.log_bytes_after,
                    duration: checkpoint.duration,
                    drain: checkpoint.drain.waited,
                    succeeded,
                },
                succeeded,
            )
        }
        Err(_) => (
            FoldRecord {
                log_bytes_before: log_bytes,
                log_bytes_after: db.logical_log_bytes(),
                duration: Duration::ZERO,
                drain: Duration::ZERO,
                succeeded: false,
            },
            false,
        ),
    };
    if succeeded {
        state.folded(record.work());
    }
    report.folds.push(record);
    (!succeeded).then_some(PassEnd::FoldFailed)
}

#[cfg(test)]
mod tests;
