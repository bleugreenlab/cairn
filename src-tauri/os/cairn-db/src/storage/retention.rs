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

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
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

impl Retention {
    /// The rule's class, for grouping tables by what bounds them.
    pub fn kind(&self) -> &'static str {
        match self {
            Retention::Keep(_) => "keep",
            Retention::Owned { .. } => "owned",
            Retention::Tiered { .. } => "tiered",
            Retention::Evict(_) => "evict",
            Retention::OwnedBy { .. } => "owned_by",
            Retention::Undecided { .. } => "undecided",
        }
    }

    /// One line naming what deletes the table's rows, for census reports.
    pub fn summary(&self) -> String {
        match self {
            Retention::Keep(_) => "kept".to_string(),
            Retention::Owned { parent } => format!("deleted with {parent}"),
            Retention::Tiered { owner } => format!("tiered with history ({owner})"),
            Retention::Evict(eviction) => format!("evicted: {}", eviction.name()),
            Retention::OwnedBy { owner, rule } => format!("{rule} ({owner})"),
            Retention::Undecided { owner, .. } => format!("undecided ({owner})"),
        }
    }
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
    // Evicted with its execution but not packed: a rebuildable cache, as every
    // history pack's manifest states.
    tiered("session_skyline_cache").budget(768),
    keep(
        "execution_history",
        "pointers: an execution's base/tip and its range pack's hash; the pack lives in the archive",
    )
    .budget(16),
    owned_by(
        "archival_blobs",
        "cairn-core archival::offload",
        "drains into the file archive; dropped once production reports it empty",
    ),
    owned("event_read_tokens", "events"),
    owned("event_vibes", "events"),
    // Evicted here.
    evict("effect_outbox", Eviction::DoneOutbox).budget(64),
    evict("search_outbox", Eviction::AppliedSearchOutbox).budget(32),
    evict("message_streams", Eviction::TerminalStreams).budget(64),
    evict("message_stream_chunks", Eviction::TerminalStreams).budget(256),
    evict("codemap_cache", Eviction::InvalidatedCodemaps),
    evict("artifacts", Eviction::SupersededArcVersions).budget(768),
    evict("agent_waits", Eviction::SettledWaits).budget(64),
    evict("codex_watchdog_lifecycle", Eviction::WatchdogLifecycle).budget(256),
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
        "model_requests",
        "cairn-db storage::served_api",
        "rows older than MODEL_REQUEST_RETENTION_MS (90 days), on each insert",
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
    owned_by(
        "storage_census",
        "cairn-db storage::census::record",
        "the newest KEPT_CENSUSES censuses",
    )
    .budget(32),
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
    owned("manual_check_handles", "projects"),
    owned("check_test_results", "check_result_observations").budget(1_100),
    owned("check_deferred_tests", "check_result_observations"),
    owned("check_audit_findings", "check_result_observations"),
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
    owned("run_batch_detached", "jobs"),
    owned("todos", "jobs"),
    owned("turn_end_check_attempts", "jobs"),
    owned("wake_subscriptions", "jobs"),
    owned("thread_compaction_entries", "jobs"),
    owned("thread_compaction_marks", "jobs"),
    owned("thread_compactions", "jobs"),
    owned("thread_era_children", "jobs"),
    owned("thread_eras", "jobs"),
    owned("ask_conditions", "jobs"),
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
    // The judgment thread's labels: kept as long as the execution they
    // describe, and deleted with it.
    owned("recipe_resolutions", "executions"),
    owned("execution_agent_edits", "executions"),
    // One row per distinct recipe set a project's resolutions chose among.
    owned("recipe_option_sets", "projects"),
    keep(
        "queue_lane_turns",
        "one row per queue lane that has started work: bounded by the threads and parent issues there are",
    ),
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
    keep("inference_profiles", CONFIG),
    keep("inference_deployments", CONFIG),
    keep("inference_machines", CONFIG),
    keep("model_pool_preferences", CONFIG),
    keep("model_pins", "one row per pinned model resident"),
    keep("inference_models", CONFIG),
    owned_by(
        "inference_verifications",
        "cairn-core orchestrator::inference_verify",
        "the newest verifications per profile and pool, pruned on insert",
    ),
    keep(
        "inference_deployment_state",
        "one row per inference deployment the runner has acted on",
    ),
    owned_by(
        "inference_deployment_loads",
        "cairn-core orchestrator::inference_control",
        "the newest loads per deployment, pruned on insert",
    ),
    keep(
        "inference_requests",
        "placement deletes answered model requests a week after they were answered",
    ),
    keep(
        "inference_generations",
        "one row per profile generation the operator started by hand from Settings > Inference",
    ),
    keep("ingress_endpoints", CONFIG),
    keep("account", IDENTITY),
    keep("anon_device", IDENTITY),
    keep("account_credential_events", IDENTITY),
    keep("authority_grants", IDENTITY),
    keep("authorization_events", IDENTITY),
    keep("served_api_clients", IDENTITY),
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
        "suite_memory_evidence",
        "bounded by suite and machine-context cardinality; one high-water row each",
    ),
    keep(
        "resident_kills_learned",
        "pruned to two days of out-of-memory kills as rows are added",
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
    keep("archival_progress", "one row per maintenance pass"),
    keep(
        "history_tier",
        "the catalog of history packs: the only list of them, and what sends reads to one",
    ),
    keep(
        "history_tier_runs",
        "the catalog of history packs, by run: the read seam's source lookup",
    ),
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
    keep("executor_repairs", SMALL),
    keep("held_publications", SMALL),
    keep("jj_reconcile_intents", SMALL),
    keep("jj_reconcile_quarantines", SMALL),
    keep("jj_branch_recoveries", SMALL),
    keep("job_presence", SMALL),
    keep("landing_allocations", SMALL),
    keep("pending_injections", SMALL),
    // At most one row per job or thread, deleted with either by cascade.
    keep("placement_defaults", SMALL),
    keep("project_learned_requirements", SMALL),
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

/// Every table `db` holds, as `sqlite_master` lists it, less the engine's own.
pub async fn table_names(db: &LocalDb) -> DbResult<Vec<String>> {
    db.query_all(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso%'",
        (),
        |row| row.text(0),
    )
    .await
}

/// How [`TABLE_RETENTION`] covers a set of tables.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct RetentionAudit {
    /// Tables in the set that have no rule.
    pub unruled: Vec<String>,
    /// Rules for tables not in the set.
    pub stale: Vec<&'static str>,
    /// Tables with more than one rule.
    pub duplicated: Vec<&'static str>,
}

/// Audit the registry against `tables`. Over the tables both lineages create,
/// a clean audit is the registry's invariant (`every_table_has_a_retention_rule`).
pub fn audit(tables: &BTreeSet<String>) -> RetentionAudit {
    let mut seen = BTreeSet::new();
    let mut audit = RetentionAudit::default();
    for entry in TABLE_RETENTION {
        if !seen.insert(entry.table) {
            audit.duplicated.push(entry.table);
        }
        if !tables.contains(entry.table) {
            audit.stale.push(entry.table);
        }
    }
    audit.unruled = tables
        .iter()
        .filter(|table| !seen.contains(table.as_str()))
        .cloned()
        .collect();
    audit
}

/// [`audit`] of the tables one database holds. Only `unruled` speaks for a
/// single database: a rule for a table only the other lineage creates shows
/// here as `stale`.
pub async fn audit_tables(db: &LocalDb) -> DbResult<RetentionAudit> {
    Ok(audit(&table_names(db).await?.into_iter().collect()))
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
/// A settled wait is read only while a resume or replay of its turn can reach
/// it: a parked turn resumes within the batch ceiling (6 h) or at the next
/// boot. A week is far past both.
const SETTLED_WAIT_AGE_S: i64 = 7 * DAY_S;
/// Routine watchdog evidence lasts a week; every other kind lasts a month.
/// Both windows must exceed RECOVERY_SUCCESSOR_WINDOW_SECS (30 minutes):
/// reserve_watchdog_successor counts recent successor_reserved rows to enforce
/// the live recovery budget. Older evidence is forensic, not recovery state.
const WATCHDOG_ROUTINE_AGE_S: i64 = 7 * DAY_S;
const WATCHDOG_RECOVERY_AGE_S: i64 = 30 * DAY_S;
/// Wait rows carry the settled batch's output: about 3 KB on average and up to
/// 3.3 MB on the 11-02 database, so a typical write stays near 400 KB.
const WAIT_PAGE_ROWS: usize = 128;
/// The resolution `durable_suspend::relinquish_to_inner_wait` writes when a
/// permission takes over a call's continuation. Withdrawing that permission
/// moves the row back to `pending` (`reclaim_from_inner_wait`), so such a row is
/// live state, not history, while the permission is pending, however old it is.
pub(crate) const TRANSFERRED_TO_INNER_WAIT: &str = r#"{"outcome":"transferred_to_inner_wait"}"#;

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
/// Settled waits in settle order, read from `idx_agent_waits_settled` alone.
/// The state list is spelled as the index's predicate is, or the planner does
/// not substitute it.
pub(crate) const SETTLED_WAIT_WALK: &str = "SELECT rowid, resolved_at FROM agent_waits \
     WHERE state IN ('resolved', 'cancelled') AND resolved_at > ?1 AND resolved_at < ?2 \
     ORDER BY resolved_at LIMIT ?3";
/// Settled waits that carry no settle time, aged by creation instead.
pub(crate) const UNSTAMPED_WAIT_WALK: &str = "SELECT rowid, created_at FROM agent_waits \
     WHERE state IN ('resolved', 'cancelled') AND resolved_at IS NULL \
     AND created_at > ?1 AND created_at < ?2 ORDER BY created_at LIMIT ?3";
/// Transferred waits whose permission is still pending, driven from the few
/// pending permissions. A permission owns exactly one call, the
/// `(run_id, tool_use_id)` it shares with the wait it took over.
pub(crate) const HELD_TRANSFERRED_WAITS: &str = "SELECT w.rowid FROM permission_requests p \
     JOIN agent_waits w ON w.run_id = p.run_id AND w.tool_use_id = p.tool_use_id \
     WHERE p.status = 'pending' AND w.state = 'resolved' AND w.resolution_json = ?1";

/// The covering age index bounds the read before the shortest window and
/// evaluates kind without fetching payloads. No insertion-order assumption:
/// watchdog timestamps can move backwards between adjacent rowids.
pub(crate) const WATCHDOG_WALK: &str = "SELECT rowid FROM codex_watchdog_lifecycle \
     WHERE created_at < ?1 AND (created_at < ?2 \
     OR kind IN ('phase_transition', 'armed', 'disarmed')) \
     ORDER BY created_at LIMIT ?3";

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
    /// `agent_waits` resolved or cancelled more than a week ago, except a wait
    /// a still-pending permission took over. Filtered on state and age, never
    /// on `kind`, which rows from before it existed lack. MCP prompts bound to
    /// a deleted wait go with it by `prompts.mcp_wait_id`'s cascade: they
    /// belong to their wait, and a wait this old has none still open.
    SettledWaits,
    /// Routine watchdog events older than seven days; all other kinds older
    /// than thirty days, including any kinds added in the future.
    WatchdogLifecycle,
}

impl Eviction {
    /// The initial order before budget-driven rotation. Watchdog evidence gets
    /// the first runtime turn rather than waiting behind payload-heavy journals.
    pub const ALL: [Eviction; 7] = [
        Eviction::WatchdogLifecycle,
        Eviction::DoneOutbox,
        Eviction::TerminalStreams,
        Eviction::InvalidatedCodemaps,
        Eviction::SupersededArcVersions,
        Eviction::SettledWaits,
        Eviction::AppliedSearchOutbox,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Eviction::DoneOutbox => "done_outbox",
            Eviction::AppliedSearchOutbox => "applied_search_outbox",
            Eviction::TerminalStreams => "terminal_streams",
            Eviction::InvalidatedCodemaps => "invalidated_codemaps",
            Eviction::SupersededArcVersions => "superseded_arc_versions",
            Eviction::SettledWaits => "settled_waits",
            Eviction::WatchdogLifecycle => "watchdog_lifecycle",
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
            Eviction::SettledWaits => &["agent_waits"],
            Eviction::WatchdogLifecycle => &["codex_watchdog_lifecycle"],
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
            Eviction::SettledWaits => select_settled_waits(db, now, cursor).await?,
            Eviction::WatchdogLifecycle => select_watchdog_lifecycle(db, now).await?,
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
    /// Settled waits: whether the walk over rows with no settle time is
    /// finished, and the time the walk has stepped past in its current phase.
    waits_stamped: bool,
    after_at: Option<i64>,
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

async fn select_watchdog_lifecycle(db: &LocalDb, now: i64) -> DbResult<Selection> {
    let rowids = db
        .query_all(
            WATCHDOG_WALK,
            (
                now - WATCHDOG_ROUTINE_AGE_S,
                now - WATCHDOG_RECOVERY_AGE_S,
                PAGE_ROWS as i64,
            ),
            |row| row.i64(0),
        )
        .await?;
    Ok(Selection {
        done: rowids.len() < PAGE_ROWS,
        deletes: vec![("codex_watchdog_lifecycle", rowids)],
        ..Selection::default()
    })
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

/// Settled waits older than [`SETTLED_WAIT_AGE_S`], oldest first: first those
/// with no settle time (aged by creation), then the rest by settle time. Both
/// walks read only `idx_agent_waits_settled`, never a row's payload.
///
/// The walk steps past what it read only when a page held a row back. A page
/// that deletes everything it read leaves nothing behind it, so the next page
/// starts where this one did and rows that share a settle time are never
/// split. After a held row, rows tied with the page's last settle time wait for
/// the next pass.
///
/// Reading the held set and deleting in a later write cannot race a reclaim: a
/// row is reclaimable only while its permission is pending, which holds it here,
/// and a permission never returns to pending. A row the transfer retires now is
/// a week too young for either walk.
async fn select_settled_waits(db: &LocalDb, now: i64, cursor: &mut Cursor) -> DbResult<Selection> {
    let cutoff = (now - SETTLED_WAIT_AGE_S) * 1_000;
    let stamped = cursor.waits_stamped;
    let walk = if stamped {
        SETTLED_WAIT_WALK
    } else {
        UNSTAMPED_WAIT_WALK
    };
    let after = cursor.after_at.unwrap_or(i64::MIN);
    let (walked, held) = db
        .read(move |conn| {
            Box::pin(async move {
                let mut walked = Vec::new();
                let mut rows = conn
                    .query(walk, (after, cutoff, WAIT_PAGE_ROWS as i64))
                    .await?;
                while let Some(row) = rows.next().await? {
                    walked.push((row.i64(0)?, row.i64(1)?));
                }
                drop(rows);
                let mut held = HashSet::new();
                if !walked.is_empty() {
                    let mut rows = conn
                        .query(
                            HELD_TRANSFERRED_WAITS,
                            (TRANSFERRED_TO_INNER_WAIT.to_string(),),
                        )
                        .await?;
                    while let Some(row) = rows.next().await? {
                        held.insert(row.i64(0)?);
                    }
                }
                Ok((walked, held))
            })
        })
        .await?;
    let walked_all = walked.len() < WAIT_PAGE_ROWS;
    let last_at = walked.last().map(|(_, at)| *at);
    let mut rowids = Vec::new();
    let mut held_any = false;
    for (rowid, _) in walked {
        if held.contains(&rowid) {
            held_any = true;
        } else {
            rowids.push(rowid);
        }
    }
    let mut done = false;
    if walked_all && stamped {
        done = true;
    } else if walked_all {
        cursor.waits_stamped = true;
        cursor.after_at = None;
    } else if held_any {
        cursor.after_at = last_at;
    }
    Ok(Selection {
        deletes: if rowids.is_empty() {
            Vec::new()
        } else {
            vec![("agent_waits", rowids)]
        },
        unlink_parents: Vec::new(),
        done,
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
    /// The fold work (excluding the drain and the fold's fixed cost) the pass
    /// lets its unfolded pages accumulate before it folds them, or `None` to
    /// fold on log size alone. See [`EvictorState`].
    pub fold_work_target: Option<Duration>,
    /// Folds one pass may ask for; past it the pass ends and leaves the rest
    /// to the next pass.
    pub folds_per_pass: usize,
    /// How long one page's write may take before the pass stops starting pages.
    pub slow_write: Duration,
}

impl EvictorBudget {
    /// The orchestrator's pass, every [`PASS_INTERVAL`]. Each fold closes the
    /// connection gate for its drain plus about half a second of work.
    ///
    /// Eight folds keep one pass's fold work under about 10 s: the first fold
    /// settles whatever log the pass inherited (up to the periodic fold's
    /// 64 MiB, usually 1 to 4 s in production), and each of the other seven
    /// costs about the half-second target plus the fold's fixed cost. Nothing
    /// bounds a fold that runs long, and production has seen one at 19 s over
    /// 34 MiB. Under a CPU and fsync storm on a clone of production, twelve
    /// passes at this cap did at most 5.6 s of fold work a pass, with no fold
    /// over 1.6 s, none abandoned, and no caller refused (cairn/6041).
    ///
    /// A page's write taking a second means the host is saturated. Eviction is
    /// background work, so the pass stops adding to it rather than finishing
    /// its budget.
    pub const PRODUCTION: Self = Self {
        pass_time: Duration::from_secs(90),
        pass_rows: 200_000,
        fold_at_log_bytes: super::FOLD_AT_LOG_BYTES,
        fold_work_target: Some(Duration::from_millis(500)),
        folds_per_pass: 8,
        slow_write: Duration::from_secs(1),
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

/// How many times larger than the window it was last measured over a rule's
/// next window may be. Once the floor is taken out, one low sample can read as
/// a page costing nothing, and the fold that disproves it should not be
/// [`MAX_PAGES_PER_FOLD`] pages of code maps.
const MAX_WINDOW_GROWTH: usize = 4;

/// How many measured folds the floor is the least of: about five passes, so it
/// follows the host's condition rather than its best moment.
const FLOOR_SAMPLES: usize = 32;

/// One rule's measured fold cost.
#[derive(Debug, Clone, Copy)]
struct FoldCost {
    ms_per_page: f64,
    /// The pages of the window last measured.
    window: usize,
}

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
///
/// It learns only from folds whose window began at the pass's own previous
/// fold. A pass's first fold covers whatever log other writers left since the
/// last fold, up to the periodic fold's 64 MiB, and any pages carried over from
/// an earlier pass. In production that log, not the pass's page, is where the
/// fold's work went: about 1 s at 30 MiB, against 70-300 ms over a log near
/// empty. Charging it to the page made a rule look many times its cost
/// (cairn/5655, cairn/6041). That fold settles the log and teaches nothing.
///
/// A fold also has a fixed cost that no page caused. Counting that against the
/// pages made the estimate depend on the window, and stopped a window growing
/// at all once the fixed cost reached the target, so the estimate is the work
/// above a floor, and the target bounds what the pages add. The floor is the
/// least of the recent measured folds. Each of those is the true fixed cost
/// plus that fold's own pages, so the floor errs high and understates a page,
/// which errs toward a larger window. When one-page folds set the floor, the
/// understatement is one page's worth of the window: about a quarter at four
/// pages, less beyond, and all of it at one page, where the sample can read as
/// zero. `MAX_WINDOW_GROWTH` and `MAX_PAGES_PER_FOLD` are what bound that.
#[derive(Debug, Default)]
pub struct EvictorState {
    /// Fold work per deleted page above the floor, per rule.
    costs: HashMap<Eviction, FoldCost>,
    /// The work of the last [`FLOOR_SAMPLES`] measured folds, in milliseconds.
    recent_work_ms: VecDeque<u64>,
    /// Deleted pages no fold has covered yet, and the rule that wrote them.
    unfolded: Option<(Eviction, usize)>,
    /// The index in [`Eviction::ALL`] the next [`run_pass`] starts at.
    start: usize,
}

impl EvictorState {
    /// The fold work per page learned for `eviction`, above the floor, in
    /// milliseconds, or `None` before its first measured fold.
    pub fn fold_ms_per_page(&self, eviction: Eviction) -> Option<f64> {
        self.costs.get(&eviction).map(|cost| cost.ms_per_page)
    }

    /// How many unfolded pages of `eviction` one fold may cover.
    pub fn pages_per_fold(&self, eviction: Eviction, target: Duration) -> usize {
        let Some(cost) = self.costs.get(&eviction) else {
            return 1;
        };
        let affordable = if cost.ms_per_page > 0.0 {
            (target.as_millis() as f64 / cost.ms_per_page) as usize
        } else {
            MAX_PAGES_PER_FOLD
        };
        affordable
            .min(cost.window * MAX_WINDOW_GROWTH)
            .clamp(1, MAX_PAGES_PER_FOLD)
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
        let Some((rule, pages)) = self.unfolded.take() else {
            return;
        };
        let work_ms = work.as_millis() as u64;
        let floor_ms = self.recent_work_ms.iter().min().copied().unwrap_or(0);
        let sample = work_ms.saturating_sub(floor_ms) as f64 / pages as f64;
        let ms_per_page = match self.costs.get(&rule) {
            Some(previous) => (previous.ms_per_page + sample) / 2.0,
            None => sample,
        };
        self.costs.insert(
            rule,
            FoldCost {
                ms_per_page,
                window: pages,
            },
        );
        self.recent_work_ms.push_back(work_ms);
        if self.recent_work_ms.len() > FLOOR_SAMPLES {
            self.recent_work_ms.pop_front();
        }
    }

    /// A fold covered the unfolded pages without measuring them.
    fn settled(&mut self) {
        self.unfolded = None;
    }

    /// Where the pass after `report` starts, given the order `rules` this one
    /// ran in. A pass that finished starts the next from the first rule. One
    /// that ended early starts the next at the rule it was serving, so a rule
    /// cut off mid-backlog resumes first, unless that rule is the one this pass
    /// began with: then it alone spent the budget, and the next pass starts at
    /// the rule after it. However small the budget, every rule is served within
    /// [`Eviction::ALL`]`.len()` passes.
    fn advance(&mut self, rules: &[Eviction], report: &PassReport) {
        self.start = match (report.end, report.rules.last()) {
            (PassEnd::Complete, _) | (_, None) => 0,
            (_, Some(last)) => {
                let index = Eviction::ALL
                    .iter()
                    .position(|rule| *rule == last.eviction)
                    .unwrap_or(0);
                if rules.first() == Some(&last.eviction) {
                    (index + 1) % Eviction::ALL.len()
                } else {
                    index
                }
            }
        };
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
    /// A page's write took at least [`EvictorBudget::slow_write`].
    SlowWrite,
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
/// the next rule. `state` carries what earlier passes learned, any pages they
/// left unfolded, and the rule this pass starts at.
///
/// The order rotates rather than always starting at the first rule, because a
/// pass that ends early otherwise ends in the same rule every time. With one
/// fold a pass that was all of them: every pass from 10-31 to 11-03 served
/// `done_outbox` alone, while `message_stream_chunks` grew to 1.4 GB against a
/// 256 MB budget (cairn/5655). Rotating keeps a fold window to one rule, which
/// is what lets each fold measure one rule's cost.
pub async fn run_pass(
    db: &LocalDb,
    now: i64,
    budget: EvictorBudget,
    state: &mut EvictorState,
) -> PassReport {
    let mut rules = Eviction::ALL;
    rules.rotate_left(state.start % Eviction::ALL.len());
    let report = run_rules(db, now, budget, state, &rules).await;
    state.advance(&rules, &report);
    report
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
    let mut slow_write = false;
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
            if slow_write {
                // The pass is over whichever way this goes, unless the fold it
                // owes fails, which is the worse news to report.
                let fold = fold_if_due(db, budget, state, eviction, &mut report).await;
                report.end = match fold {
                    Some(PassEnd::FoldFailed) => PassEnd::FoldFailed,
                    _ => PassEnd::SlowWrite,
                };
            } else if started.elapsed() >= budget.pass_time {
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
                        slow_write = page.write >= budget.slow_write;
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
/// target. The pass's first fold counts against the fold budget like any other
/// but does not train the estimate (see [`EvictorState`]). Returns why the pass
/// must end, if it must.
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
    if succeeded && report.folds.is_empty() {
        state.settled();
    } else if succeeded {
        state.folded(record.work());
    }
    report.folds.push(record);
    (!succeeded).then_some(PassEnd::FoldFailed)
}

#[cfg(test)]
mod tests;
