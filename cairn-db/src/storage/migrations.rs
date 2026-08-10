use super::migration::RebuildCheck;
use super::Migration;

/// Composes a migration lineage from its head migrations plus the shared tail.
///
/// The private (`TURSO_MIGRATIONS`) and team (`TEAM_MIGRATIONS`) lineages diverge
/// only at their heads — the private head is the frozen 0001.. history rooted at
/// `workspaces`; the team head is a one-time snapshot of the same shared tables
/// re-rooted at `teams`. Shipped private history can never be rewritten, so that
/// one-time divergence is unavoidable. From here forward, every FUTURE
/// shared-table change is written ONCE as its own `shared_tail*!` macro below and
/// both lineages compose them IN ORDER — the single source of truth that the
/// schema-equivalence test enforces. Each shared migration is a separate
/// single-expression macro because a `macro_rules!` invocation in array-element
/// position must expand to exactly one expression, not a multi-item list.
macro_rules! shared_tail {
    () => {
        // ── SHARED_TAIL ─────────────────────────────────────────────────
        // CAIRN-2188 is the FIRST shared-tail migration: `execution_history.
        // pack_hash` is a pointer to the per-execution range pack in the shared
        // per-team content store. It is a shared-table change (both the private
        // and team `execution_history` gain the column identically), so it is
        // written once here. The SQL file lives in `turso_migrations/`; it is
        // numbered 0084 to follow the private head (0082 + the 0083 cas_cache
        // private head), and the team lineage records it after its 0002 head.
        Migration::new(
            "0084",
            "archival_pack_hash",
            include_str!("../../../../turso_migrations/0084_archival_pack_hash.sql"),
        )
    };
}

/// CAIRN-3810: recency ordering for the semantic lane's turn sweep. `turns` is
/// shared project state, so the index is composed once into both lineages.
macro_rules! shared_tail_turns_created_at_index {
    () => {
        Migration::new(
            "0163",
            "turns_created_at_index",
            include_str!("../../../../turso_migrations/0163_turns_created_at_index.sql"),
        )
    };
}

/// Private-only: an authority grant is this operator's decision about this
/// install and must never replicate to a teammate's machine. See the SQL
/// file's header for why that is an authorization property, not a storage one.
macro_rules! private_authority_grants {
    () => {
        Migration::new(
            "0166",
            "authority_grants",
            include_str!("../../../../turso_migrations/0166_authority_grants.sql"),
        )
    };
}

/// CAIRN-3861: thread ownership was never stamped on a delegated child, so a
/// task a thread spawned could not be listed or opened from the thread pane.
/// Carries the inheritance job creation now performs back over existing rows.
macro_rules! shared_tail_inherit_thread_id_for_child_jobs {
    () => {
        Migration::new(
            "0167",
            "inherit_thread_id_for_child_jobs",
            include_str!("../../../../turso_migrations/0167_inherit_thread_id_for_child_jobs.sql"),
        )
    };
}

macro_rules! shared_tail_thread_title_retires {
    () => {
        Migration::new(
            "0160",
            "thread_title_retires",
            include_str!("../../../../turso_migrations/0160_thread_title_retires.sql"),
        )
    };
}

macro_rules! shared_tail_migrate_issue_threads {
    () => {
        Migration::new(
            "0157",
            "migrate_issue_threads",
            include_str!("../../../../turso_migrations/0157_migrate_issue_threads.sql"),
        )
    };
}

macro_rules! shared_tail_threads_entity {
    () => {
        Migration::new(
            "0156",
            "threads_entity",
            include_str!("../../../../turso_migrations/0156_threads_entity.sql"),
        )
    };
}

/// CAIRN-3823: one unit for `check_result_cache.ran_at`. The observation
/// projection writes milliseconds; the pre-observation writer wrote seconds.
macro_rules! shared_tail_check_result_cache_ran_at_millis {
    () => {
        Migration::new(
            "0165",
            "check_result_cache_ran_at_millis",
            include_str!("../../../../turso_migrations/0165_check_result_cache_ran_at_millis.sql"),
        )
    };
}

macro_rules! shared_tail_verdict_reuse_facts {
    () => {
        Migration::new(
            "0155",
            "verdict_reuse_facts",
            include_str!("../../../../turso_migrations/0155_verdict_reuse_facts.sql"),
        )
    };
}

macro_rules! shared_tail_repair_check_observation_public_handle {
    () => {
        Migration::new(
            "0154",
            "repair_check_observation_public_handle",
            include_str!(
                "../../../../turso_migrations/0154_repair_check_observation_public_handle.sql"
            ),
        )
    };
}

macro_rules! private_route_firing_content {
    () => {
        Migration::new(
            "0159",
            "route_firing_content",
            include_str!("../../../../turso_migrations/0159_route_firing_content.sql"),
        )
    };
}

macro_rules! private_channel_outbound_route_kind {
    () => {
        Migration::new(
            "0153",
            "channel_outbound_route_kind",
            include_str!("../../../../turso_migrations/0153_channel_outbound_route_kind.sql"),
        )
    };
}

macro_rules! shared_tail_rebase_replay_status {
    () => {
        Migration::new(
            "0150",
            "rebase_replay_status",
            include_str!("../../../../turso_migrations/0150_rebase_replay_status.sql"),
        )
    };
}

macro_rules! private_command_contention_profiles {
    () => {
        Migration::new(
            "0149",
            "command_contention_profiles",
            include_str!("../../../../turso_migrations/0149_command_contention_profiles.sql"),
        )
    };
}

macro_rules! private_channel_thread_suppression {
    () => {
        Migration::new(
            "0148",
            "channel_thread_suppression",
            include_str!("../../../../turso_migrations/0148_channel_thread_suppression.sql"),
        )
    };
}

macro_rules! shared_tail_check_observation_public_handle {
    () => {
        Migration::new(
            "0146",
            "add_check_observation_public_handle",
            include_str!(
                "../../../../turso_migrations/0146_add_check_observation_public_handle.sql"
            ),
        )
    };
}

macro_rules! shared_tail_pr_resolution_attribution {
    () => {
        Migration::new(
            "0145",
            "pr_resolution_attribution",
            include_str!("../../../../turso_migrations/0145_pr_resolution_attribution.sql"),
        )
    };
}

macro_rules! private_channel_thread_state {
    () => {
        Migration::new(
            "0144",
            "channel_thread_state",
            include_str!("../../../../turso_migrations/0144_channel_thread_state.sql"),
        )
    };
}

macro_rules! shared_tail_virtual_reconcile_coordinates {
    () => {
        // `jobs` loses two columns in place via ALTER TABLE DROP COLUMN;
        // `jj_reconcile_items` is copied whole out of its `_legacy` rename. Both
        // carry every row.
        Migration::rebuild_fk_off(
            "0118",
            "virtual_reconcile_coordinates",
            include_str!("../../../../turso_migrations/0118_virtual_reconcile_coordinates.sql"),
            &[
                RebuildCheck::Conserved("jobs"),
                RebuildCheck::Conserved("jj_reconcile_items"),
            ],
        )
    };
}

macro_rules! shared_tail_jj_reconcile_quarantines {
    () => {
        Migration::new(
            "0116",
            "add_jj_reconcile_quarantines",
            include_str!("../../../../turso_migrations/0116_add_jj_reconcile_quarantines.sql"),
        )
    };
}

macro_rules! shared_tail_agent_waits {
    () => {
        Migration::new(
            "0115",
            "add_agent_waits",
            include_str!("../../../../turso_migrations/0115_add_agent_waits.sql"),
        )
    };
}

/// Canonical, synced pack metadata and durable references. This is deliberately
/// shared between lineages: the team replica is the authority consumed by the
/// API-owned mark-and-sweep, while local projects retain the same data model.
macro_rules! shared_tail_pack_catalog {
    () => {
        Migration::new(
            "0112",
            "pack_catalog",
            include_str!("../../../../turso_migrations/0112_pack_catalog.sql"),
        )
    };
}

/// Terminal rows persist the lifetime lease fence and process generation used by
/// the executor-hosted PTY transport.
macro_rules! shared_tail_terminal_lifetime_lease {
    () => {
        Migration::new(
            "0113",
            "bind_agent_terminals_to_lifetime_leases",
            include_str!(
                "../../../../turso_migrations/0113_bind_agent_terminals_to_lifetime_leases.sql"
            ),
        )
    };
}

macro_rules! shared_tail_jj_reconcile_intents {
    () => {
        Migration::new(
            "0114",
            "add_jj_reconcile_intents",
            include_str!("../../../../turso_migrations/0114_add_jj_reconcile_intents.sql"),
        )
    };
}

/// CAIRN-2270: re-grain token_rollup from the UTC-day floor to the UTC-hour floor
/// (the `day` column becomes `bucket_start`). token_rollup is a project-scoped
/// SHARED table — present in both lineages, with `team_schema_matches_private`
/// enforcing identical schema — so the drop-and-recreate is written once here and
/// reaches both. The private-only backfill-marker reset that forces the
/// historical fold to re-derive every run lives in the sibling private migration
/// 0088_reopen_analytics_backfill.
macro_rules! shared_tail_token_rollup_hourly {
    () => {
        Migration::new(
            "0087",
            "token_rollup_hourly",
            include_str!("../../../../turso_migrations/0087_token_rollup_hourly.sql"),
        )
    };
}

/// CAIRN-2251: sync-on-write check result cache, keyed by (project, sealed tree,
/// check name). A project-scoped SHARED table (present in both lineages), so it is
/// written once here and appended last in each lineage. Numbered 0089 to follow
/// main's analytics migrations (0086-0088) after this branch rebased onto #2037.
macro_rules! shared_tail_check_result_cache {
    () => {
        Migration::new(
            "0089",
            "check_result_cache",
            include_str!("../../../../turso_migrations/0089_check_result_cache.sql"),
        )
    };
}

/// CAIRN-2281: re-key check_result_cache by each check's impact-scoped input hash
/// (the content of just the files matching its globs) instead of the whole sealed
/// tree, so a commit touching none of a check's inputs reuses its cached verdict.
/// A project-scoped SHARED table change, so it is written once here and appended
/// after 0089 in each lineage.
macro_rules! shared_tail_check_result_input_hash {
    () => {
        Migration::new(
            "0090",
            "check_result_cache_input_hash",
            include_str!("../../../../turso_migrations/0090_check_result_cache_input_hash.sql"),
        )
    };
}

/// CAIRN-2348: add result timestamps to the tool-invocation rollup so duration
/// analytics can derive tool-call wall time from assistant/tool_result event
/// pairs. `tool_invocations` and its watermark table are project-scoped shared
/// tables, so this schema change is appended to both lineages.
macro_rules! shared_tail_tool_invocation_durations {
    () => {
        Migration::new(
            "0092",
            "tool_invocation_durations",
            include_str!("../../../../turso_migrations/0092_tool_invocation_durations.sql"),
        )
    };
}

/// CAIRN-2368: add a durable per-job listing index and cached/fresh stamp to
/// check_result_cache. The table is project-scoped shared state, so this append-only
/// migration lands in both lineages; the cache key remains input-hash based.
macro_rules! shared_tail_check_result_job_id {
    () => {
        Migration::new(
            "0093",
            "check_result_cache_job_id",
            include_str!("../../../../turso_migrations/0093_check_result_cache_job_id.sql"),
        )
    };
}

/// CAIRN-2386: relink first-class PR-node merge request rows from action-run or
/// otherwise dangling ids back to the producing builder job, when that job is an
/// unambiguous match for the PR source branch.
macro_rules! shared_tail_relink_merge_request_jobs {
    () => {
        Migration::new(
            "0095",
            "relink_merge_request_jobs",
            include_str!("../../../../turso_migrations/0095_relink_merge_request_jobs.sql"),
        )
    };
}

/// CAIRN-2460: per-job child-routing mode. `jobs` is a project-scoped shared
/// table, so this append-only ADD COLUMN lands in both lineages. The column
/// carries the resolved `childBase` of the agent node that produced the job.
///
/// DORMANT as of CAIRN-2475: the childBase mechanism was deleted (ambient
/// no-worktree coordinators route their children to the default branch
/// naturally, so the flag was redundant). Nothing reads or writes this column
/// anymore. It is kept in place rather than dropped because `jobs` is a synced
/// shared table, and dropping a column from a synced replica table (the
/// create-new/copy/drop/rename rebuild) is exactly what the sync-safety rules
/// forbid. A nullable, unwritten column is inert at runtime, and the migration
/// stays in the lineage so no already-applied database carries a recorded
/// migration the composed list no longer contains.
macro_rules! shared_tail_jobs_child_base {
    () => {
        Migration::new(
            "0096",
            "jobs_child_base",
            include_str!("../../../../turso_migrations/0096_jobs_child_base.sql"),
        )
    };
}

/// CAIRN-2476: mark a task job that owns a throwaway ephemeral worktree. `jobs`
/// is a project-scoped shared table, so this append-only ADD COLUMN lands in both
/// lineages. The column is set to 1 for a task delegated by an ambient
/// (no-worktree) parent — such a task gets its own worktree off the default
/// branch, reclaimed when the task job terminalizes — and read back by the
/// finalize reclaim and the worktree GC backstop.
macro_rules! shared_tail_jobs_owns_ephemeral_worktree {
    () => {
        Migration::new(
            "0097",
            "jobs_owns_ephemeral_worktree",
            include_str!("../../../../turso_migrations/0097_jobs_owns_ephemeral_worktree.sql"),
        )
    };
}

/// CAIRN-2481: schema + workflow-tag carriers for the ephemeral call primitive.
/// `jobs.output_contract` gives node-less runs (ephemeral calls and, going
/// forward, child tasks) a per-run output schema the artifact-write handler can
/// resolve without a recipe node; `runs.label`/`runs.phase` are durable workflow
/// tags. Both `jobs` and `runs` are project-scoped SHARED tables, so this change
/// is written once here and appended after 0097 in each lineage.
macro_rules! shared_tail_call_output_contract {
    () => {
        Migration::new(
            "0098",
            "call_output_contract_and_run_tags",
            include_str!("../../../../turso_migrations/0098_call_output_contract_and_run_tags.sql"),
        )
    };
}

/// CAIRN-2499: durable phase/log progress timeline for a workflow node, the
/// reader the workflow monitoring panel renders. A project-scoped SHARED table
/// (a new CREATE TABLE), so it is written once here and appended after 0098 in
/// both lineages -- it lives alongside the jobs/runs the monitor joins it with,
/// and its progress syncs so teammates see the panel.
macro_rules! shared_tail_workflow_progress {
    () => {
        Migration::new(
            "0101",
            "workflow_progress",
            include_str!("../../../../turso_migrations/0101_workflow_progress.sql"),
        )
    };
}

/// CAIRN-2623: classify a FAILING check's terminal outcome (timeout vs spawn
/// failure vs signal kill) so a slow-but-healthy suite killed at its budget
/// renders AS a timeout, not an opaque `exit -1`. A project-scoped SHARED table
/// ADD COLUMN, written once here and appended after 0101 in both lineages.
macro_rules! shared_tail_check_result_failure_kind {
    () => {
        Migration::new(
            "0102",
            "check_result_cache_failure_kind",
            include_str!("../../../../turso_migrations/0102_check_result_cache_failure_kind.sql"),
        )
    };
}

/// CAIRN-2629: stamp the owning machine on an execution. `executions` is a
/// project-scoped SHARED table, so this append-only ADD COLUMN lands in both
/// lineages, written once here and appended after 0102. The synced migration
/// ledger handles a legacy replica (it simply runs 0103 on next open), so unlike
/// the head-snapshot columns (is_workspace) this shared-tail column needs no
/// bespoke runtime repair.
macro_rules! shared_tail_executions_runner_device_id {
    () => {
        Migration::new(
            "0103",
            "executions_runner_device_id",
            include_str!("../../../../turso_migrations/0103_executions_runner_device_id.sql"),
        )
    };
}

/// CAIRN-2580: repair invalid empty/root worktree assignments left on historical
/// child jobs. `jobs` is shared, so both private and team replicas apply the same
/// data cleanup and represent these worktree-less records canonically as NULL.
macro_rules! shared_tail_clear_invalid_job_worktree_paths {
    () => {
        Migration::new(
            "0104",
            "clear_invalid_job_worktree_paths",
            include_str!("../../../../turso_migrations/0104_clear_invalid_job_worktree_paths.sql"),
        )
    };
}

macro_rules! shared_tail_add_turn_end_reason {
    () => {
        Migration::new(
            "0105",
            "add_turn_end_reason",
            include_str!("../../../../turso_migrations/0105_add_turn_end_reason.sql"),
        )
    };
}

macro_rules! shared_tail_index_hot_gui_status_queries {
    () => {
        Migration::new(
            "0106",
            "index_hot_gui_status_queries",
            include_str!("../../../../turso_migrations/0106_index_hot_gui_status_queries.sql"),
        )
    };
}
/// CAIRN-2804: retain executor attribution and the runner-local toolchain claim
/// alongside each cached verdict. The cache is shared project state, so this
/// additive migration is composed once into both lineages.
macro_rules! shared_tail_check_result_cache_provenance {
    () => {
        Migration::new(
            "0109",
            "check_result_cache_provenance",
            include_str!("../../../../turso_migrations/0109_check_result_cache_provenance.sql"),
        )
    };
}

/// CAIRN-3108: index the `(check_name, ran_at, tree_hash)` recency ranking that
/// selects each check's latest verdict. The cache is shared project state, so
/// this index-only migration is composed once into both lineages.
macro_rules! shared_tail_check_result_cache_recency_index {
    () => {
        Migration::new(
            "0121",
            "check_result_cache_recency_index",
            include_str!("../../../../turso_migrations/0121_check_result_cache_recency_index.sql"),
        )
    };
}

/// CAIRN-3167: pin a durable session to its selected provider account. Sessions
/// are shared project state, so this additive migration reaches both lineages.
macro_rules! shared_tail_session_account {
    () => {
        Migration::new(
            "0122",
            "session_account",
            include_str!("../../../../turso_migrations/0122_session_account.sql"),
        )
    };
}

/// CAIRN-3152: the durable identity of a REPL. A REPL was registry-only
/// in-memory state, which is why its GUI could not be invalidated and why a dead
/// session became indistinguishable from one that never existed. The row makes it
/// an entity with a lifecycle, and it is project-scoped shared state alongside
/// the jobs it hangs off — so it is written once here and composed into both
/// lineages, exactly like `job_terminals`.
macro_rules! shared_tail_job_repls {
    () => {
        Migration::new(
            "0123",
            "job_repls",
            include_str!("../../../../turso_migrations/0123_job_repls.sql"),
        )
    };
}

/// CAIRN-3152: the durable REPL transcript, replacing the in-memory 200-entry
/// exchange ring. Project-scoped shared state hanging off `job_repls`, so it is
/// composed into both lineages immediately after it.
macro_rules! shared_tail_repl_exchanges {
    () => {
        Migration::new(
            "0124",
            "repl_exchanges",
            include_str!("../../../../turso_migrations/0124_repl_exchanges.sql"),
        )
    };
}

/// CAIRN-3112: a terminal is a process inside an execution environment, so its
/// durable fence becomes (holder, incarnation, cell epoch). `job_terminals` is
/// carried by the synced team lineage, so this reaches both.
macro_rules! shared_tail_rebind_terminals_to_residencies {
    () => {
        Migration::new(
            "0125",
            "rebind_terminals_to_residencies",
            include_str!("../../../../turso_migrations/0125_rebind_terminals_to_residencies.sql"),
        )
    };
}

/// CAIRN-3232: re-key the active-suspension bound from the job to the call, so
/// a turn can park every one of its concurrent long-running calls instead of
/// only the first. `agent_waits` is project-scoped shared state, so this
/// index-only migration is composed once here and reaches both lineages.
macro_rules! shared_tail_agent_waits_concurrent_calls {
    () => {
        Migration::new(
            "0126",
            "agent_waits_concurrent_calls",
            include_str!("../../../../turso_migrations/0126_agent_waits_concurrent_calls.sql"),
        )
    };
}

/// CAIRN-3242: the reference rows that give a stored image a human address. The
/// blob's sha256 stays its storage identity in the content store; these rows map
/// a scoped ordinal onto it. Project-scoped shared state — a teammate opening a
/// synced issue must resolve the same image URIs — so it reaches both lineages.
macro_rules! shared_tail_image_refs {
    () => {
        Migration::new(
            "0127",
            "image_refs",
            include_str!("../../../../turso_migrations/0127_image_refs.sql"),
        )
    };
}

/// CAIRN-3264: the write-replay ledger, which makes a second delivery of one
/// `write` call a no-op instead of a second application of its patch. Host-local
/// delivery state, so it is a private-lineage migration rather than a shared
/// tail — a teammate has no in-flight socket of ours to deduplicate.
macro_rules! private_write_replay_ledger {
    () => {
        Migration::new(
            "0128",
            "write_replay_ledger",
            include_str!("../../../../turso_migrations/0128_write_replay_ledger.sql"),
        )
    };
}

/// CAIRN-3265: ordinary prompt rows used for external MCP input carry an
/// explicit durable-wait owner and one-time consumption marker. Both referenced
/// tables are project-scoped shared state, so both lineages receive the column.
macro_rules! shared_tail_mcp_continuation_prompts {
    () => {
        Migration::new(
            "0129",
            "mcp_continuation_prompts",
            include_str!("../../../../turso_migrations/0129_mcp_continuation_prompts.sql"),
        )
    };
}

/// CAIRN-3293: delete the snapshotted child-attention subscriptions now that the
/// recipient is derived from the parent edge. The table is shared schema, so both
/// lineages converge on having none; it is a no-op wherever none were written.
macro_rules! shared_tail_retire_snapshot_child_wakes {
    () => {
        Migration::new(
            "0130",
            "retire_snapshot_child_wakes",
            include_str!("../../../../turso_migrations/0130_retire_snapshot_child_wakes.sql"),
        )
    };
}

/// CAIRN-3290: `events(run_id, sequence)` becomes UNIQUE. Repairs the duplicate
/// slots that hand-rolled sequence counters wrote, then constrains the column so
/// a regression fails loudly instead of silently corrupting transcript order.
/// `events` is project-scoped shared state, so both lineages receive it.
macro_rules! shared_tail_unique_event_sequence {
    () => {
        Migration::new(
            "0131",
            "unique_event_sequence",
            include_str!("../../../../turso_migrations/0131_unique_event_sequence.sql"),
        )
    };
}

/// CAIRN-3245: bound the re-execution of an infrastructure-failing check. The
/// consecutive-failure counter and its one-shot escalation stamp live on the
/// cache row whose key is already the suppressed triple, so this is additive to
/// shared project state and composed once into both lineages.
macro_rules! shared_tail_check_result_cache_infra_suppression {
    () => {
        Migration::new(
            "0132",
            "check_result_cache_infra_suppression",
            include_str!(
                "../../../../turso_migrations/0132_check_result_cache_infra_suppression.sql"
            ),
        )
    };
}

macro_rules! shared_tail_check_result_observations {
    () => {
        Migration::rebuild_fk_off(
            "0134",
            "check_result_observations",
            include_str!("../../../../turso_migrations/0134_check_result_observations.sql"),
            &[RebuildCheck::Conserved("check_result_cache")],
        )
    };
}

macro_rules! shared_tail_check_definition_provenance {
    () => {
        Migration::new(
            "0135",
            "check_definition_provenance",
            include_str!("../../../../turso_migrations/0135_check_definition_provenance.sql"),
        )
    };
}

/// The conflict diagnostic captured before a rolled-back rebase, plus the
/// incoming change's normalized file inventory. Shared because the reconcile
/// tables it extends are shared; a team replica carries the same data model.
macro_rules! shared_tail_conflict_resolution_sessions {
    () => {
        Migration::new(
            "0136",
            "conflict_resolution_sessions",
            include_str!("../../../../turso_migrations/0136_conflict_resolution_sessions.sql"),
        )
    };
}

/// The discriminator that says what a row in `issues` is: an ordinary issue or a
/// thread. `issues` is a project-scoped table both lineages carry, so the column
/// is written once and composed into both.
macro_rules! shared_tail_issue_kind {
    () => {
        Migration::new(
            "0138",
            "issue_kind",
            include_str!("../../../../turso_migrations/0138_issue_kind.sql"),
        )
    };
}

/// External channel delivery state is tied to this runner's provider process and
/// personal messaging account. It must not replicate into a team database.
macro_rules! private_channel_persistence {
    () => {
        Migration::new(
            "0137",
            "channel_persistence",
            include_str!("../../../../turso_migrations/0137_channel_persistence.sql"),
        )
    };
}

/// Rolling compaction state for thread sessions. A thread's compaction is a
/// property of how THIS runner reseeds its local agent session -- the marks are a
/// work queue, and the generations and entries are derivable again from the
/// append-only transcript that stays in the shared tables. None of it is
/// collaboration data a teammate's replica needs.
/// Warmth-stratified execution durations learned from this runner's own
/// executors. Machine-local observation aggregates, safely rebuildable, and
/// never team-replicated -- the same reasoning that keeps
/// `command_resource_profiles` private.
macro_rules! private_command_duration_profiles {
    () => {
        Migration::new(
            "0143",
            "command_duration_profiles",
            include_str!("../../../../turso_migrations/0143_command_duration_profiles.sql"),
        )
    };
}

macro_rules! private_route_firings {
    () => {
        Migration::new(
            "0152",
            "route_firings",
            include_str!("../../../../turso_migrations/0152_route_firings.sql"),
        )
    };
}

macro_rules! private_response_invocations {
    () => {
        Migration::new(
            "0151",
            "response_invocations",
            include_str!("../../../../turso_migrations/0151_response_invocations.sql"),
        )
    };
}

macro_rules! private_quarantine_saturation_memory_profiles {
    () => {
        Migration::new(
            "0147",
            "quarantine_saturation_memory_profiles",
            include_str!(
                "../../../../turso_migrations/0147_quarantine_saturation_memory_profiles.sql"
            ),
        )
    };
}

macro_rules! private_thread_compaction {
    () => {
        Migration::new(
            "0139",
            "thread_compaction",
            include_str!("../../../../turso_migrations/0139_thread_compaction.sql"),
        )
    };
}

/// The abandoned-intent state the live-tap channel needs. Channel delivery state
/// never replicates into a team database, so this rebuild stays private.
macro_rules! private_channel_outbound_expired {
    () => {
        Migration::new(
            "0142",
            "channel_outbound_expired",
            include_str!("../../../../turso_migrations/0142_channel_outbound_expired.sql"),
        )
    };
}

macro_rules! team_lineage {
    ($($head:expr),* $(,)?) => {
        &[
            $($head,)*
            shared_tail!(),
            shared_tail_token_rollup_hourly!(),
            shared_tail_check_result_cache!(),
            shared_tail_check_result_input_hash!(),
            shared_tail_tool_invocation_durations!(),
            shared_tail_check_result_job_id!(),
            shared_tail_relink_merge_request_jobs!(),
            shared_tail_jobs_child_base!(),
            shared_tail_jobs_owns_ephemeral_worktree!(),
            shared_tail_call_output_contract!(),
            shared_tail_workflow_progress!(),
            shared_tail_check_result_failure_kind!(),
            shared_tail_executions_runner_device_id!(),
            shared_tail_clear_invalid_job_worktree_paths!(),
            shared_tail_add_turn_end_reason!(),
            shared_tail_index_hot_gui_status_queries!(),
            shared_tail_check_result_cache_provenance!(),
            shared_tail_pack_catalog!(),
            shared_tail_terminal_lifetime_lease!(),
            shared_tail_jj_reconcile_intents!(),
            shared_tail_agent_waits!(),
            shared_tail_jj_reconcile_quarantines!(),
            shared_tail_virtual_reconcile_coordinates!(),
            shared_tail_check_result_cache_recency_index!(),
            shared_tail_session_account!(),
            shared_tail_job_repls!(),
            shared_tail_repl_exchanges!(),
            shared_tail_rebind_terminals_to_residencies!(),
            shared_tail_agent_waits_concurrent_calls!(),
            shared_tail_image_refs!(),
            shared_tail_mcp_continuation_prompts!(),
            shared_tail_retire_snapshot_child_wakes!(),
            shared_tail_unique_event_sequence!(),
            shared_tail_check_result_cache_infra_suppression!(),
            shared_tail_check_result_observations!(),
            shared_tail_check_definition_provenance!(),
            shared_tail_conflict_resolution_sessions!(),
            shared_tail_issue_kind!(),
            shared_tail_pr_resolution_attribution!(),
            shared_tail_check_observation_public_handle!(),
            shared_tail_repair_check_observation_public_handle!(),
            shared_tail_verdict_reuse_facts!(),
            shared_tail_rebase_replay_status!(),
            shared_tail_threads_entity!(),
            shared_tail_migrate_issue_threads!(),
            shared_tail_thread_title_retires!(),
            shared_tail_turns_created_at_index!(),
            shared_tail_check_result_cache_ran_at_millis!(),
            shared_tail_inherit_thread_id_for_child_jobs!(),
            // ── TEAM_TAIL ───────────────────────────────────────────────────
            // Intentionally empty for now. CAIRN-2277's team-side removal of
            // `projects.server_id` lives in the team snapshot instead of a
            // post-snapshot rebuild: destructive create-copy-drop-rename rebuilds
            // on synced replica tables break Turso sync triggers. The unfenced
            // `turso_sync_roundtrip` lane failed 8/15 tests when this was modeled
            // as `0003_drop_projects_server_id`, so do not re-add that tail entry.
        ]
    };
}

macro_rules! private_lineage {
    ($($head:expr),* $(,)?) => {
        &[
            $($head,)*
            shared_tail!(),
            shared_tail_token_rollup_hourly!(),
            // ── PRIVATE_TAIL ────────────────────────────────────────────────
            // Private-only migrations that must apply after the shared tail go
            // here. They are intentionally absent from `TEAM_MIGRATIONS`.
            //
            // CAIRN-2223: per-machine repository clone path for team projects.
            // The synced `projects.repo_path` is the creator's path and cannot be
            // overwritten by teammates; this private router column is the
            // effective local path on this machine.
            Migration::new(
                "0085",
                "project_routes_local_path",
                include_str!("../../../../turso_migrations/0085_project_routes_local_path.sql"),
            ),
            // CAIRN-2252: one-time marker for the historical analytics-rollup
            // backfill. Per-install runner-transient state (mirrors
            // archival_backfill_state); never synced to a team replica.
            Migration::new(
                "0086",
                "analytics_rollup_backfill_state",
                include_str!(
                    "../../../../turso_migrations/0086_analytics_rollup_backfill_state.sql"
                ),
            ),
            // CAIRN-2270: reopen the one-time historical analytics-rollup backfill
            // so it re-folds every run into the hour-grain token_rollup the shared
            // 0087 migration recreated empty. Private-only: it resets
            // analytics_rollup_backfill_state (migration 0086), which exists only
            // in this lineage.
            Migration::new(
                "0088",
                "reopen_analytics_backfill",
                include_str!("../../../../turso_migrations/0088_reopen_analytics_backfill.sql"),
            ),
            shared_tail_check_result_cache!(),
            shared_tail_check_result_input_hash!(),
            // CAIRN-2277: drop the dead `projects.server_id` column, its FK to the
            // local `servers` table, and then that now-orphaned `servers` table. A
            // rebuild_fk_off because many tables FK-reference `projects(id)` and
            // `projects` itself referenced `servers`. Private-only: the team
            // snapshot schema never carries the local `servers` column/table.
            Migration::rebuild_fk_off(
                "0091",
                "drop_projects_server_id_and_servers",
                include_str!(
                    "../../../../turso_migrations/0091_drop_projects_server_id_and_servers.sql"
                ),
                // `servers` is dropped outright, not rebuilt; `projects` copies
                // every row minus the `server_id` column.
                &[RebuildCheck::Conserved("projects")],
            ),
            shared_tail_tool_invocation_durations!(),
            shared_tail_check_result_job_id!(),
            // CAIRN-2385: one-time repair marker for historical
            // tool_invocations.result_ts rows. Private-only because the marker
            // table is runner-transient state, not team-replicated data.
            Migration::new(
                "0094",
                "tool_invocation_result_backfill_state",
                include_str!(
                    "../../../../turso_migrations/0094_tool_invocation_result_backfill_state.sql"
                ),
            ),
            shared_tail_relink_merge_request_jobs!(),
            shared_tail_jobs_child_base!(),
            shared_tail_jobs_owns_ephemeral_worktree!(),
            shared_tail_call_output_contract!(),
            shared_tail_workflow_progress!(),
            shared_tail_check_result_failure_kind!(),
            shared_tail_executions_runner_device_id!(),
            shared_tail_clear_invalid_job_worktree_paths!(),
            // CAIRN-2487: durable journal for the workflow harness's agent()
            // calls. A per-machine runner-transient replay cache (never synced),
            // so it lives ONLY in the private lineage's tail -- absent from
            // TEAM_MIGRATIONS.
            Migration::new(
                "0099",
                "workflow_journal",
                include_str!("../../../../turso_migrations/0099_workflow_journal.sql"),
            ),
            // CAIRN-2498: workflow restart durability. workflow_run records the
            // durable spawn params for startup re-dispatch; workflow_call links
            // an in-flight ephemeral call back to its journal key. Both are
            // per-machine runner-transient replay state (never synced), so they
            // live ONLY in the private lineage's tail -- absent from
            // TEAM_MIGRATIONS, like workflow_journal above.
            Migration::new(
                "0100",
                "workflow_restart_durability",
                include_str!(
                    "../../../../turso_migrations/0100_workflow_restart_durability.sql"
                ),
            ),
            shared_tail_add_turn_end_reason!(),
            shared_tail_index_hot_gui_status_queries!(),
            // Token usage rows and analytics backfill state are local/private.
            // Re-normalize OpenAI-style history and force a complete rollup fold.
            Migration::new(
                "0107",
                "normalize_openai_token_usage_components",
                include_str!(
                    "../../../../turso_migrations/0107_normalize_openai_token_usage_components.sql"
                ),
            ),
            // Executor enrollment grants and credentials establish trust with
            // this runner device. They are host-local identity/credential state,
            // never collaboration data synced into a team replica.
            Migration::new(
                "0108",
                "executor_enrollment",
                include_str!("../../../../turso_migrations/0108_executor_enrollment.sql"),
            ),
            shared_tail_check_result_cache_provenance!(),
            Migration::new(
                "0110",
                "executor_enrollment_expiry",
                include_str!("../../../../turso_migrations/0110_executor_enrollment_expiry.sql"),
            ),
            // Machine-local resource learning contains executor observations and
            // must never replicate into a team database.
            Migration::new(
                "0111",
                "command_resource_profiles",
                include_str!("../../../../turso_migrations/0111_command_resource_profiles.sql"),
            ),
            shared_tail_pack_catalog!(),
            shared_tail_terminal_lifetime_lease!(),
            shared_tail_jj_reconcile_intents!(),
            shared_tail_agent_waits!(),
            shared_tail_jj_reconcile_quarantines!(),
            // Workflow restart records are runner-local state, so their executor
            // anchor migration belongs to the private lineage with workflow_run.
            Migration::new(
                "0117",
                "workflow_executor_anchor",
                include_str!("../../../../turso_migrations/0117_workflow_executor_anchor.sql"),
            ),
            shared_tail_virtual_reconcile_coordinates!(),
            Migration::new(
                "0119",
                "virtual_workflow_coordinates",
                include_str!("../../../../turso_migrations/0119_virtual_workflow_coordinates.sql"),
            ),
            // CAIRN-3103: cadence marker for the whole-database integrity sweep,
            // which replaced the per-migration integrity_check. Per-install
            // runner-transient state (mirrors archival_backfill_state); never
            // synced to a team replica.
            Migration::new(
                "0120",
                "integrity_sweep_state",
                include_str!("../../../../turso_migrations/0120_integrity_sweep_state.sql"),
            ),
            shared_tail_check_result_cache_recency_index!(),
            shared_tail_session_account!(),
            shared_tail_job_repls!(),
            shared_tail_repl_exchanges!(),
            shared_tail_rebind_terminals_to_residencies!(),
            shared_tail_agent_waits_concurrent_calls!(),
            shared_tail_image_refs!(),
            private_write_replay_ledger!(),
            shared_tail_mcp_continuation_prompts!(),
            shared_tail_retire_snapshot_child_wakes!(),
            shared_tail_unique_event_sequence!(),
            shared_tail_check_result_cache_infra_suppression!(),
            // The public name an executor answers to is host-local enrollment
            // state, exactly like the credential beside it, and never
            // collaboration data synced into a team replica.
            Migration::new(
                "0133",
                "executor_enrollment_names",
                include_str!("../../../../turso_migrations/0133_executor_enrollment_names.sql"),
            ),
            shared_tail_check_result_observations!(),
            shared_tail_check_definition_provenance!(),
            shared_tail_conflict_resolution_sessions!(),
            // Composition order, not file number, is what applies (see the
            // PRIVATE_TAIL note above). Thread compaction only creates new
            // tables of its own, so its position among the unrelated tail
            // entries around it carries no dependency.
            private_thread_compaction!(),
            private_command_duration_profiles!(),
            private_quarantine_saturation_memory_profiles!(),
            private_channel_persistence!(),
            private_channel_outbound_expired!(),
            private_channel_thread_state!(),
            private_channel_thread_suppression!(),
            shared_tail_issue_kind!(),
            shared_tail_pr_resolution_attribution!(),
            shared_tail_check_observation_public_handle!(),
            shared_tail_repair_check_observation_public_handle!(),
            shared_tail_verdict_reuse_facts!(),
            private_command_contention_profiles!(),
            // Keep the shared rebase state at 0150 before private Response history at 0151.
            shared_tail_rebase_replay_status!(),
            // Response observability is runner-local and intentionally not team replicated.
            private_response_invocations!(),
            private_route_firings!(),
            private_channel_outbound_route_kind!(),
            // Must follow both: it adds columns to `route_firings` and backfills
            // them from `channel_outbound`.
            private_route_firing_content!(),
            shared_tail_threads_entity!(),
            shared_tail_migrate_issue_threads!(),
            Migration::new(
                "0158",
                "executor_desktop_automation",
                include_str!("../../../../turso_migrations/0158_executor_desktop_automation.sql"),
            ),
            shared_tail_thread_title_retires!(),
            // Must follow `private_thread_compaction!()`: they alter that table.
            Migration::new(
                "0140",
                "thread_compaction_seed_bytes",
                include_str!(
                    "../../../../turso_migrations/0140_thread_compaction_seed_bytes.sql"
                ),
            ),
            Migration::new(
                "0141",
                "thread_compaction_capacity_trigger",
                include_str!(
                    "../../../../turso_migrations/0141_thread_compaction_capacity_trigger.sql"
                ),
            ),
            // Data-only clear of a derived cache. The table is shared with the
            // team lineage, but nothing here is schema, and a team replica's
            // stale rows self-heal: an unparseable row is a cache miss that
            // rebuilds in the compact shape.
            Migration::new(
                "0161",
                "clear_skyline_cache_for_compact_bars",
                include_str!(
                    "../../../../turso_migrations/0161_clear_skyline_cache_for_compact_bars.sql"
                ),
            ),
            // CAIRN-3810: per-turn semantic vectors backing the search lane.
            // Private-only for the same reason `resource_embeddings` is.
            Migration::new(
                "0162",
                "turn_embeddings",
                include_str!("../../../../turso_migrations/0162_turn_embeddings.sql"),
            ),
            shared_tail_turns_created_at_index!(),
            // CAIRN-3810: separates the sweep's consent boundary from its
            // reconciliation floor. Private-only, like `turn_embeddings`.
            Migration::new(
                "0164",
                "turn_embedding_state",
                include_str!("../../../../turso_migrations/0164_turn_embedding_state.sql"),
            ),
            shared_tail_check_result_cache_ran_at_millis!(),
            private_authority_grants!(),
            shared_tail_inherit_thread_id_for_child_jobs!(),
        ]
    };
}

pub const TURSO_MIGRATIONS: &[Migration] = private_lineage![
    Migration::new(
        "0001",
        "initial_schema",
        include_str!("../../../../turso_migrations/0001_initial_schema.sql"),
    ),
    Migration::new(
        "0002",
        "search_outbox",
        include_str!("../../../../turso_migrations/0002_search_outbox.sql"),
    ),
    Migration::new(
        "0003",
        "seed_default_workspace",
        include_str!("../../../../turso_migrations/0003_seed_default_workspace.sql"),
    ),
    Migration::new(
        "0004",
        "add_issue_dependencies",
        include_str!("../../../../turso_migrations/0004_add_issue_dependencies.sql"),
    ),
    Migration::new(
        "0005",
        "change_preview_events",
        include_str!("../../../../turso_migrations/0005_change_preview_events.sql"),
    ),
    Migration::new(
        "0006",
        "uri_segments",
        include_str!("../../../../turso_migrations/0006_uri_segments.sql"),
    ),
    Migration::new(
        "0007",
        "add_uri_segment_to_prompts",
        include_str!("../../../../turso_migrations/0007_add_uri_segment_to_prompts.sql"),
    ),
    Migration::new(
        "0008",
        "add_job_id_to_prompts",
        include_str!("../../../../turso_migrations/0008_add_job_id_to_prompts.sql"),
    ),
    Migration::new(
        "0009",
        "cohere_embeddings",
        include_str!("../../../../turso_migrations/0009_cohere_embeddings.sql"),
    ),
    Migration::new(
        "0010",
        "anon_device",
        include_str!("../../../../turso_migrations/0010_anon_device.sql"),
    ),
    Migration::new(
        "0011",
        "session_current_pos",
        include_str!("../../../../turso_migrations/0011_session_current_pos.sql"),
    ),
    Migration::new(
        "0012",
        "resource_surfacings",
        include_str!("../../../../turso_migrations/0012_resource_surfacings.sql"),
    ),
    Migration::new(
        "0013",
        "memory_redux",
        include_str!("../../../../turso_migrations/0013_memory_redux.sql"),
    ),
    Migration::new(
        "0014",
        "add_tool_use_id_to_prompts",
        include_str!("../../../../turso_migrations/0014_add_tool_use_id_to_prompts.sql"),
    ),
    Migration::new(
        "0015",
        "add_artifact_confirmed",
        include_str!("../../../../turso_migrations/0015_add_artifact_confirmed.sql"),
    ),
    Migration::new(
        "0016",
        "remove_ready_status",
        include_str!("../../../../turso_migrations/0016_remove_ready_status.sql"),
    ),
    Migration::new(
        "0017",
        "messages_delivered_at",
        include_str!("../../../../turso_migrations/0017_messages_delivered_at.sql"),
    ),
    Migration::new(
        "0018",
        "pr_node_port_fires",
        include_str!("../../../../turso_migrations/0018_pr_node_port_fires.sql"),
    ),
    Migration::new(
        "0019",
        "merge_request_owner",
        include_str!("../../../../turso_migrations/0019_merge_request_owner.sql"),
    ),
    Migration::new(
        "0020",
        "add_uri_segment_to_action_runs",
        include_str!("../../../../turso_migrations/0020_add_uri_segment_to_action_runs.sql"),
    ),
    Migration::new(
        "0021",
        "vibe_axes",
        include_str!("../../../../turso_migrations/0021_vibe_axes.sql"),
    ),
    Migration::new(
        "0022",
        "add_segments_to_permission_requests",
        include_str!("../../../../turso_migrations/0022_add_segments_to_permission_requests.sql"),
    ),
    Migration::new(
        "0023",
        "add_labels",
        include_str!("../../../../turso_migrations/0023_add_labels.sql"),
    ),
    Migration::new(
        "0024",
        "add_parent_issue",
        include_str!("../../../../turso_migrations/0024_add_parent_issue.sql"),
    ),
    Migration::rebuild_fk_off(
        "0025",
        "remove_managers",
        include_str!("../../../../turso_migrations/0025_remove_managers.sql"),
        // Five unfiltered rebuilds dropping `manager_id`. The four manager tables
        // are dropped, not rebuilt.
        &[
            RebuildCheck::Conserved("issues"),
            RebuildCheck::Conserved("jobs"),
            RebuildCheck::Conserved("turns"),
            RebuildCheck::Conserved("messages"),
            RebuildCheck::Conserved("merge_requests"),
        ],
    ),
    Migration::new(
        "0026",
        "child_side_channel_notices",
        include_str!("../../../../turso_migrations/0026_child_side_channel_notices.sql"),
    ),
    Migration::new(
        "0027",
        "session_channel_cursor",
        include_str!("../../../../turso_migrations/0027_session_channel_cursor.sql"),
    ),
    Migration::new(
        "0028",
        "issue_parent_job",
        include_str!("../../../../turso_migrations/0028_issue_parent_job.sql"),
    ),
    Migration::new(
        "0029",
        "queued_messages",
        include_str!("../../../../turso_migrations/0029_queued_messages.sql"),
    ),
    Migration::new(
        "0030",
        "checkpoint_runs",
        include_str!("../../../../turso_migrations/0030_checkpoint_runs.sql"),
    ),
    Migration::rebuild_fk_off(
        "0031",
        "drop_dead_chats_table",
        include_str!("../../../../turso_migrations/0031_drop_dead_chats_table.sql"),
        // Only the FK reference to `chats` goes away; `chat_id` survives as a
        // vestigial column and every row is copied.
        &[
            RebuildCheck::Conserved("runs"),
            RebuildCheck::Conserved("sessions"),
        ],
    ),
    Migration::new(
        "0032",
        "drop_workspaces_timezone_column",
        include_str!("../../../../turso_migrations/0032_drop_workspaces_timezone_column.sql"),
    ),
    Migration::new(
        "0033",
        "annotations",
        include_str!("../../../../turso_migrations/0033_annotations.sql"),
    ),
    Migration::new(
        "0034",
        "annotation_message_links",
        include_str!("../../../../turso_migrations/0034_annotation_message_links.sql"),
    ),
    Migration::new(
        "0035",
        "annotation_uri_seq",
        include_str!("../../../../turso_migrations/0035_annotation_uri_seq.sql"),
    ),
    Migration::new(
        "0036",
        "wake_subscriptions",
        include_str!("../../../../turso_migrations/0036_wake_subscriptions.sql"),
    ),
    Migration::new(
        "0037",
        "unify_side_channel_notices",
        include_str!("../../../../turso_migrations/0037_unify_side_channel_notices.sql"),
    ),
    Migration::rebuild_fk_off(
        "0038",
        "drop_annotation_tables",
        include_str!("../../../../turso_migrations/0038_drop_annotation_tables.sql"),
        // Three bare `DROP TABLE IF EXISTS`; nothing is rebuilt, so there is
        // nothing to verify.
        &[],
    ),
    Migration::new(
        "0039",
        "memory_intake_ledger",
        include_str!("../../../../turso_migrations/0039_memory_intake_ledger.sql"),
    ),
    Migration::new(
        "0040",
        "add_is_workspace_to_projects",
        include_str!("../../../../turso_migrations/0040_add_is_workspace_to_projects.sql"),
    ),
    Migration::new(
        "0041",
        "memory_triage_batches_and_drop_when_to_use",
        include_str!(
            "../../../../turso_migrations/0041_memory_triage_batches_and_drop_when_to_use.sql"
        ),
    ),
    // Column values transformed in place by CASE expressions; no WHERE filter.
    Migration::rebuild_fk_off(
        "0042",
        "memory_scope_node_id_and_status_lattice",
        include_str!(
            "../../../../turso_migrations/0042_memory_scope_node_id_and_status_lattice.sql"
        ),
        &[RebuildCheck::Conserved("memories")],
    ),
    Migration::new(
        "0043",
        "memory_triage_decision",
        include_str!("../../../../turso_migrations/0043_memory_triage_decision.sql"),
    ),
    Migration::new(
        "0044",
        "jobs_memory_review_state",
        include_str!("../../../../turso_migrations/0044_jobs_memory_review_state.sql"),
    ),
    Migration::rebuild_fk_off(
        "0045",
        "memory_canon_v2_consolidation",
        include_str!("../../../../turso_migrations/0045_memory_canon_v2_consolidation.sql"),
        // Deliberately lossy: pre-v2 rows are not copied at all (the new table is
        // created empty) and `memory_triage_issue_memories` is emptied first.
        &[RebuildCheck::Reshaped("memories")],
    ),
    Migration::rebuild_fk_off(
        "0046",
        "memory_review_sent_state",
        include_str!("../../../../turso_migrations/0046_memory_review_sent_state.sql"),
        // `memory_review_state` value remapped; every row copied.
        &[RebuildCheck::Conserved("jobs")],
    ),
    Migration::new(
        "0047",
        "add_message_urgency",
        include_str!("../../../../turso_migrations/0047_add_message_urgency.sql"),
    ),
    Migration::new(
        "0048",
        "add_event_thinking_tokens",
        include_str!("../../../../turso_migrations/0048_add_event_thinking_tokens.sql"),
    ),
    Migration::new(
        "0049",
        "event_vibes_session_id",
        include_str!("../../../../turso_migrations/0049_event_vibes_session_id.sql"),
    ),
    Migration::new(
        "0050",
        "session_skyline_cache_vibe_watermark",
        include_str!("../../../../turso_migrations/0050_session_skyline_cache_vibe_watermark.sql"),
    ),
    Migration::new(
        "0051",
        "clear_skyline_cache_for_content_bar_decomposition",
        include_str!(
            "../../../../turso_migrations/0051_clear_skyline_cache_for_content_bar_decomposition.sql"
        ),
    ),
    Migration::new(
        "0052",
        "clear_skyline_cache_for_visual_height_sizing",
        include_str!(
            "../../../../turso_migrations/0052_clear_skyline_cache_for_visual_height_sizing.sql"
        ),
    ),
    Migration::new(
        "0053",
        "job_pack_anchor",
        include_str!("../../../../turso_migrations/0053_job_pack_anchor.sql"),
    ),
    Migration::new(
        "0054",
        "archival_storage",
        include_str!("../../../../turso_migrations/0054_archival_storage.sql"),
    ),
    Migration::new(
        "0055",
        "archival_backfill_state",
        include_str!("../../../../turso_migrations/0055_archival_backfill_state.sql"),
    ),
    Migration::new(
        "0056",
        "archival_blobs",
        include_str!("../../../../turso_migrations/0056_archival_blobs.sql"),
    ),
    Migration::new(
        "0057",
        "event_read_tokens",
        include_str!("../../../../turso_migrations/0057_event_read_tokens.sql"),
    ),
    Migration::new(
        "0058",
        "terminal_exit_wakes",
        include_str!("../../../../turso_migrations/0058_terminal_exit_wakes.sql"),
    ),
    Migration::new(
        "0059",
        "jobs_needs_fresh_session",
        include_str!("../../../../turso_migrations/0059_jobs_needs_fresh_session.sql"),
    ),
    Migration::new(
        "0060",
        "attention_items",
        include_str!("../../../../turso_migrations/0060_attention_items.sql"),
    ),
    Migration::new(
        "0061",
        "attention_escalate_at",
        include_str!("../../../../turso_migrations/0061_attention_escalate_at.sql"),
    ),
    Migration::new(
        "0062",
        "attention_fingerprint",
        include_str!("../../../../turso_migrations/0062_attention_fingerprint.sql"),
    ),
    Migration::new(
        "0063",
        "comment_seq",
        include_str!("../../../../turso_migrations/0063_comment_seq.sql"),
    ),
    Migration::new(
        "0064",
        "clear_skyline_cache_for_system_event_filter",
        include_str!(
            "../../../../turso_migrations/0064_clear_skyline_cache_for_system_event_filter.sql"
        ),
    ),
    Migration::new(
        "0065",
        "merge_request_is_local",
        include_str!("../../../../turso_migrations/0065_merge_request_is_local.sql"),
    ),
    Migration::new(
        "0066",
        "config_disables",
        include_str!("../../../../turso_migrations/0066_config_disables.sql"),
    ),
    Migration::new(
        "0067",
        "tool_invocations",
        include_str!("../../../../turso_migrations/0067_tool_invocations.sql"),
    ),
    Migration::new(
        "0068",
        "job_browsers",
        include_str!("../../../../turso_migrations/0068_job_browsers.sql"),
    ),
    Migration::new(
        "0069",
        "add_event_cost_usd",
        include_str!("../../../../turso_migrations/0069_add_event_cost_usd.sql"),
    ),
    Migration::new(
        "0070",
        "attention_pushes",
        include_str!("../../../../turso_migrations/0070_attention_pushes.sql"),
    ),
    Migration::new(
        "0071",
        "attention_push_fingerprint",
        include_str!("../../../../turso_migrations/0071_attention_push_fingerprint.sql"),
    ),
    Migration::new(
        "0072",
        "merge_request_head_sha",
        include_str!("../../../../turso_migrations/0072_merge_request_head_sha.sql"),
    ),
    Migration::new(
        "0073",
        "attention_read_cursors",
        include_str!("../../../../turso_migrations/0073_attention_read_cursors.sql"),
    ),
    Migration::new(
        "0074",
        "drop_attention_ledger",
        include_str!("../../../../turso_migrations/0074_drop_attention_ledger.sql"),
    ),
    Migration::new(
        "0075",
        "drop_messages_delivered_at",
        include_str!("../../../../turso_migrations/0075_drop_messages_delivered_at.sql"),
    ),
    Migration::new(
        "0076",
        "terminal_output_wakes",
        include_str!("../../../../turso_migrations/0076_terminal_output_wakes.sql"),
    ),
    Migration::new(
        "0077",
        "event_content_change_id",
        include_str!("../../../../turso_migrations/0077_event_content_change_id.sql"),
    ),
    Migration::new(
        "0078",
        "browser_last_active_at",
        include_str!("../../../../turso_migrations/0078_browser_last_active_at.sql"),
    ),
    Migration::new(
        "0079",
        "index_runs_session_id_created_at",
        include_str!("../../../../turso_migrations/0079_index_runs_session_id_created_at.sql"),
    ),
    Migration::new(
        "0080",
        "token_rollup",
        include_str!("../../../../turso_migrations/0080_token_rollup.sql"),
    ),
    Migration::new(
        "0081",
        "drop_runs_backend",
        include_str!("../../../../turso_migrations/0081_drop_runs_backend.sql"),
    ),
    Migration::new(
        "0082",
        "team_routing",
        include_str!("../../../../turso_migrations/0082_team_routing.sql"),
    ),
    // PRIVATE-ONLY (CAIRN-2188): the local read-through cache for content-store
    // objects (team-run packs/blobs fetched by hash). Fetched bytes must never
    // land on the synced team replica, so this is a private head entry, not a
    // SHARED_TAIL change. Classified `Private(PrivateReason::RebuildableCache)` in
    // `TABLE_SCOPES`; the `team_schema_matches_private` projection test proves it
    // stays out of the team lineage.
    Migration::new(
        "0083",
        "cas_cache",
        include_str!("../../../../turso_migrations/0083_cas_cache.sql"),
    ),
];

/// Team-DB migration lineage (the team-rooted counterpart of `TURSO_MIGRATIONS`).
///
/// `TEAM_HEAD` is a single snapshot migration (`turso_migrations_team/0001`) of
/// the FINAL schema of every project-scoped table, re-anchored from `workspaces`
/// to a `teams` root. It composes the same `SHARED_TAIL` as the private lineage,
/// so a future shared-table change written once in a `shared_tail*!` macro reaches
/// both. Beyond the shared tail it carries its own `TEAM_TAIL` — team-only
/// migrations with no private counterpart, the mirror of `private_lineage!`'s
/// PRIVATE_TAIL — hence the dedicated `team_lineage!` composer. The
/// `team_schema_matches_private` test proves the two lineages stay byte-equivalent
/// (after whitespace normalization) for every shared table except the four
/// intentional re-rootings.
pub const TEAM_MIGRATIONS: &[Migration] = team_lineage![
    Migration::new(
        "0001",
        "team_initial_schema",
        include_str!("../../../../turso_migrations_team/0001_team_initial_schema.sql"),
    ),
    // Catch-up: the team head (0001) omitted `labels` (team-scoped label
    // management is deferred), but the routed issue-content paths JOIN it for
    // read-resolution. This adds the EMPTY table so that JOIN resolves uniformly
    // across both lineages instead of failing `no such table: labels`
    // (CAIRN-2186). Team-only by design: the private lineage already has `labels`
    // from 0023, so this is not a SHARED_TAIL change.
    Migration::new(
        "0002",
        "labels_read_completeness",
        include_str!("../../../../turso_migrations_team/0002_labels_read_completeness.sql"),
    ),
    // CAIRN-2629: per-machine device presence for team runner selection. Team-only
    // (no private counterpart): the runner picker, clone validation, and
    // "waiting for <device>" UX all read it. `CREATE TABLE IF NOT EXISTS` makes it
    // self-sufficient against the migration-vs-sync race; the idempotent
    // `ensure_device_presence_table` repair at team-open backstops it.
    Migration::new(
        "0003",
        "device_presence",
        include_str!("../../../../turso_migrations_team/0003_device_presence.sql"),
    ),
    // Team-only durable inbox for remote clients. Canonical effects remain in
    // shared tables; only the synced delivery envelope is team-specific.
    Migration::new(
        "0004",
        "remote_intents",
        include_str!("../../../../turso_migrations_team/0004_remote_intents.sql"),
    ),
    // Team-only non-secret executor advertisements. Enrollment credentials and
    // revocation state remain private; only fleet availability is replicated.
    Migration::new(
        "0005",
        "executor_registry",
        include_str!("../../../../turso_migrations_team/0005_executor_registry.sql"),
    ),
    // CAIRN-2870: inventory is emergent executor health, not advertised capacity.
    Migration::new(
        "0006",
        "elastic_executor_inventory",
        include_str!("../../../../turso_migrations_team/0006_elastic_executor_inventory.sql"),
    ),
];

// ── Table scope: the single source of truth (CAIRN-2210) ────────────────────
//
// Scope is a property of the data, declared ONCE per table here. This one
// declaration drives schema derivation (the team schema is the projection of the
// ProjectScoped tables), the sync filter, and the write router. There is no
// second place that encodes the same fact: the deleted CAIRN-2186 allowlist used
// to, and that is exactly the drift this replaces. The `team_schema_matches_private`
// test below proves the team lineage IS this projection.

/// Which physical lineage a table currently lives in. Distinct from a table's
/// eventual scope: a `SharedContent` table names where it lives TODAY so the
/// schema projection stays exact until CAIRN-2188 moves it to the shared store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lineage {
    /// The private/per-install database only.
    Private,
    /// The team replica (and, for a local project, the private DB — the team
    /// lineage is the projection).
    Team,
}

/// The eventual target scope of a `DeferredShared` table — what it WILL become
/// once its tracked owner does the work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeTarget {
    ProjectScoped,
    SharedContent,
}

/// Why a table is Private. Every Private classification carries a re-justified
/// reason rather than an undifferentiated "doesn't sync" bucket, so a genuinely
/// private credential is never confused with a rebuildable cache or a table whose
/// lean is shared but whose move is deferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateReason {
    /// Identity and credentials (account, device, GitHub app/installations,
    /// webhook staging).
    IdentityCredential,
    /// A structural root or the router itself (the private `workspaces` lineage
    /// root, the `project_routes` catalog).
    StructuralRoot,
    /// A host-local runner-transient work queue (effect outbox, injection queue,
    /// trigger accumulation, archival-backfill progress).
    RunnerTransient,
    /// A rebuildable / refetchable cache (CI logs).
    RebuildableCache,
    /// Private today, but its lean is to be shared; the move is DEFERRED to a
    /// named owner. Recorded as an owned, documented exception, never an
    /// anonymous allowlist line.
    DeferredShared {
        issue: &'static str,
        target: ScopeTarget,
    },
}

/// A table's scope: the single classification that drives schema, sync, and
/// routing. Independent of `RouteScope` (where an *id* routes) — a local issue
/// lives in a `ProjectScoped` table but has a bare, Local-routing id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableScope {
    /// Lives only in the local/per-install database; never synced.
    Private(PrivateReason),
    /// Durable shared collaboration data owned by a project/team. Lives in BOTH
    /// lineages: the private DB for a local project, the team replica for a team
    /// project. The team schema is exactly the projection of these tables.
    ProjectScoped,
    /// Heavy content-addressed objects fetched on demand from a per-team shared
    /// store (CAIRN-2188). The scope model NAMES the category; 2188 builds the
    /// store and moves these. Until then each stays in its current lineage.
    SharedContent { current: Lineage },
}

impl TableScope {
    /// Whether a table with this scope physically appears in the team lineage
    /// today (and therefore must be present in the team schema).
    pub fn lives_in_team(&self) -> bool {
        matches!(
            self,
            TableScope::ProjectScoped
                | TableScope::SharedContent {
                    current: Lineage::Team
                }
        )
    }
}

/// Every table the private lineage creates, classified exactly once. The
/// `team_schema_matches_private` test proves this is exhaustive (no private table
/// unclassified), free of duplicate/stale entries, and that the team lineage is
/// its projection. `teams` is the team-only root and has no private counterpart,
/// so it is intentionally absent here and special-cased in the test.
pub const TABLE_SCOPES: &[(&str, TableScope)] = &[
    // ── ProjectScoped: the durable shared collaboration surface ──────────────
    ("action_configs", TableScope::ProjectScoped),
    ("action_runs", TableScope::ProjectScoped),
    ("agent_waits", TableScope::ProjectScoped),
    ("artifact_content", TableScope::ProjectScoped),
    ("artifacts", TableScope::ProjectScoped),
    ("attention_pushes", TableScope::ProjectScoped),
    ("attention_read_cursors", TableScope::ProjectScoped),
    // Authorization state is this operator's decision about this install.
    // Replicating a grant would authorize a teammate's agents against their own
    // workspace configuration, which is the blast-radius expansion the
    // authorization subsystem exists to gate.
    (
        "authority_grants",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "authorization_events",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    ("check_result_cache", TableScope::ProjectScoped),
    ("check_result_commit_aliases", TableScope::ProjectScoped),
    ("check_result_observations", TableScope::ProjectScoped),
    ("check_test_results", TableScope::ProjectScoped),
    ("checkpoint_command_cache", TableScope::ProjectScoped),
    ("checkpoint_runs", TableScope::ProjectScoped),
    ("comments", TableScope::ProjectScoped),
    ("condition_evaluations", TableScope::ProjectScoped),
    ("doc_references", TableScope::ProjectScoped),
    ("event_read_tokens", TableScope::ProjectScoped),
    ("event_vibes", TableScope::ProjectScoped),
    ("events", TableScope::ProjectScoped),
    ("execution_trigger_sources", TableScope::ProjectScoped),
    ("executions", TableScope::ProjectScoped),
    ("file_changes", TableScope::ProjectScoped),
    ("image_refs", TableScope::ProjectScoped),
    ("issue_dependencies", TableScope::ProjectScoped),
    ("issue_labels", TableScope::ProjectScoped),
    ("issue_workspaces", TableScope::ProjectScoped),
    ("issues", TableScope::ProjectScoped),
    ("job_browsers", TableScope::ProjectScoped),
    ("job_repls", TableScope::ProjectScoped),
    ("job_terminals", TableScope::ProjectScoped),
    ("jj_reconcile_incoming_files", TableScope::ProjectScoped),
    ("jj_reconcile_intents", TableScope::ProjectScoped),
    ("jj_reconcile_items", TableScope::ProjectScoped),
    ("jj_reconcile_quarantines", TableScope::ProjectScoped),
    ("jobs", TableScope::ProjectScoped),
    ("labels", TableScope::ProjectScoped),
    ("memories", TableScope::ProjectScoped),
    ("memory_triage_issue_memories", TableScope::ProjectScoped),
    ("merge_requests", TableScope::ProjectScoped),
    ("pr_resolution_attributions", TableScope::ProjectScoped),
    ("message_stream_chunks", TableScope::ProjectScoped),
    ("message_streams", TableScope::ProjectScoped),
    ("messages", TableScope::ProjectScoped),
    ("permission_requests", TableScope::ProjectScoped),
    ("pack_catalog", TableScope::ProjectScoped),
    ("pack_catalog_backfill_attempts", TableScope::ProjectScoped),
    ("pack_catalog_references", TableScope::ProjectScoped),
    ("pr_node_port_fires", TableScope::ProjectScoped),
    ("projects", TableScope::ProjectScoped),
    ("prompts", TableScope::ProjectScoped),
    ("queued_messages", TableScope::ProjectScoped),
    ("repl_exchanges", TableScope::ProjectScoped),
    ("resource_surfacings", TableScope::ProjectScoped),
    ("runs", TableScope::ProjectScoped),
    ("search_outbox", TableScope::ProjectScoped),
    ("session_skyline_cache", TableScope::ProjectScoped),
    ("sessions", TableScope::ProjectScoped),
    ("skill_configs", TableScope::ProjectScoped),
    ("suppressed_wakes", TableScope::ProjectScoped),
    ("threads", TableScope::ProjectScoped),
    ("todos", TableScope::ProjectScoped),
    ("token_rollup", TableScope::ProjectScoped),
    ("token_rollup_runs", TableScope::ProjectScoped),
    ("tool_invocation_runs", TableScope::ProjectScoped),
    ("tool_invocations", TableScope::ProjectScoped),
    ("turns", TableScope::ProjectScoped),
    ("wake_subscriptions", TableScope::ProjectScoped),
    ("workflow_progress", TableScope::ProjectScoped),
    // ── SharedContent: content-addressed, owned by CAIRN-2188 ────────────────
    // Named here so the category exists; 2188 builds the store and moves them.
    // Each stays in its CURRENT lineage until then so the projection is exact.
    (
        "archival_blobs",
        TableScope::SharedContent {
            current: Lineage::Private,
        },
    ),
    (
        "execution_history",
        TableScope::SharedContent {
            current: Lineage::Team,
        },
    ),
    // Learned command resource profiles are machine-local, safely rebuildable
    // aggregates over executor observations.
    (
        "command_resource_profiles",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // Learned execution durations, keyed by machine context and cache warmth.
    // Machine-local observations of how long work takes HERE; nothing a
    // teammate's replica could use, and rebuildable by running the work again.
    (
        "command_duration_profiles",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // Machine-local slowdown curves learned from executions with stated start
    // load (0149). Same category as the duration/resource profiles above:
    // observations of how work behaves HERE, rebuildable by running it again.
    (
        "command_contention_profiles",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // The desktop-automation capabilities this runner last probed off each
    // enrolled executor (0158). A cached answer about machines THIS runner can
    // reach, refreshed by probing again; a teammate's replica would describe a
    // different fleet.
    (
        "executor_desktop_automation",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // ── Private: identity & credentials ──────────────────────────────────────
    (
        "account",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "anon_device",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // Enrollment binds executor credentials and one-time grants to this local
    // runner device; neither table is team-replicated collaboration state.
    (
        "executor_enrollment_grants",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "executor_enrollments",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "github_app",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "github_installations",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "webhook_events",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // ── Private: structural roots & the router ───────────────────────────────
    (
        "project_routes",
        TableScope::Private(PrivateReason::StructuralRoot),
    ),
    (
        "workspaces",
        TableScope::Private(PrivateReason::StructuralRoot),
    ),
    // ── Private: runner-transient work queues ────────────────────────────────
    (
        "analytics_rollup_backfill_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "archival_backfill_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "effect_outbox",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "integrity_sweep_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "pending_injections",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "trigger_accumulator_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "workflow_journal",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "workflow_run",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "workflow_call",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // Runner-local observability history (0151/0152): what THIS runner's
    // Response invocations and route firings did. Intentionally not
    // team-replicated, same category as the workflow journal above.
    (
        "response_invocations",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "route_firings",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "write_replay_ledger",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "channel_outbound",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "channel_cursor",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "channel_inbound",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "channel_thread_follow",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "channel_thread_focus",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // A terminal child makes parent turns compactable; the mark sits in this
    // queue until an expiry or ratio trigger applies it. Local to the runner
    // that owns the thread's agent session.
    (
        "thread_compaction_marks",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // ── Private: rebuildable / refetchable caches ────────────────────────────
    (
        "cas_cache",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // An applied compaction and its table of contents are a projection of the
    // job's append-only transcript, which stays in the shared tables. Losing
    // them costs entry stability across generations, never content: the next
    // composition rebuilds the same chapters from the same events.
    (
        "thread_compactions",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    (
        "thread_compaction_entries",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    (
        "ci_logs_cache",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // ── Private: deferred-shared (lean is shared, move tracked by an owner) ───
    // resource_embeddings: remotely computed (expensive to regenerate); lean is
    // compute-once-per-team. Sharing needs routing the embed worker + a mechanism
    // choice (sync rows vs the 2188 store), so it is deferred, not anonymous.
    (
        "resource_embeddings",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-2210",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
    // turn_embeddings: the same store one granularity finer, so the same
    // deferral. Sharing it needs the identical routing + mechanism decision,
    // and splitting the two apart would answer that question twice.
    (
        "turn_embeddings",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-2210",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
    // turn_embedding_state: this host's sweep progress, alongside the
    // archival-backfill progress the same reason already covers. It describes
    // when THIS install began participating, so it is meaningless elsewhere.
    (
        "turn_embedding_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // config_disables: a host-side resolution override; team-config propagation is
    // a separate cross-scope feature, deferred with a named owner.
    (
        "config_disables",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-2210",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
];

/// Declarative re-key manifest for moving a local project into a team replica.
///
/// Each `ProjectScoped` table must appear exactly once. `id_columns` are the
/// structural columns whose values are Cairn routable ids and must be transformed
/// from a bare local id to `{team}~{uuid}` during a private-to-team move. Columns
/// intentionally absent from this list, such as `runs.session_id` or provider
/// `tool_use_id` values, stay bare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RekeyTableManifest {
    pub table: &'static str,
    pub id_columns: &'static [&'static str],
}

pub const PROJECT_REKEY_MANIFEST: &[RekeyTableManifest] = &[
    RekeyTableManifest {
        table: "action_configs",
        id_columns: &["id", "project_id"],
    },
    RekeyTableManifest {
        table: "action_runs",
        id_columns: &[
            "id",
            "execution_id",
            "action_config_id",
            "issue_id",
            "project_id",
            "parent_job_id",
        ],
    },
    RekeyTableManifest {
        table: "agent_waits",
        id_columns: &[
            "id",
            "job_id",
            "run_id",
            "session_id",
            "predecessor_turn_id",
            "successor_turn_id",
        ],
    },
    RekeyTableManifest {
        table: "artifact_content",
        id_columns: &["id", "execution_id", "job_id"],
    },
    RekeyTableManifest {
        table: "artifacts",
        id_columns: &["id", "job_id", "parent_version_id"],
    },
    RekeyTableManifest {
        table: "attention_pushes",
        id_columns: &["id", "recipient", "delivered_event_id"],
    },
    RekeyTableManifest {
        table: "attention_read_cursors",
        id_columns: &["recipient", "source"],
    },
    RekeyTableManifest {
        table: "check_result_cache",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "check_result_commit_aliases",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "check_result_observations",
        id_columns: &["id", "project_id"],
    },
    RekeyTableManifest {
        table: "check_test_results",
        id_columns: &["observation_id"],
    },
    RekeyTableManifest {
        table: "checkpoint_command_cache",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "checkpoint_runs",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "comments",
        id_columns: &["id", "issue_id", "thread_id"],
    },
    RekeyTableManifest {
        table: "condition_evaluations",
        id_columns: &["id", "execution_id"],
    },
    RekeyTableManifest {
        table: "doc_references",
        id_columns: &["id", "issue_id"],
    },
    RekeyTableManifest {
        table: "event_read_tokens",
        id_columns: &["event_id"],
    },
    RekeyTableManifest {
        table: "event_vibes",
        id_columns: &["event_id"],
    },
    RekeyTableManifest {
        table: "events",
        id_columns: &["id", "run_id", "turn_id"],
    },
    RekeyTableManifest {
        table: "execution_history",
        id_columns: &["execution_id"],
    },
    RekeyTableManifest {
        table: "execution_trigger_sources",
        id_columns: &["id", "source_job_id", "triggered_execution_id"],
    },
    RekeyTableManifest {
        table: "executions",
        id_columns: &["id", "issue_id", "project_id"],
    },
    RekeyTableManifest {
        table: "file_changes",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "image_refs",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "issue_dependencies",
        id_columns: &["issue_id"],
    },
    RekeyTableManifest {
        table: "issue_labels",
        id_columns: &["issue_id", "label_id"],
    },
    RekeyTableManifest {
        table: "issue_workspaces",
        id_columns: &["issue_id", "execution_id"],
    },
    RekeyTableManifest {
        table: "issues",
        id_columns: &[
            "id",
            "project_id",
            "parent_issue_id",
            "parent_job_id",
            "parent_thread_id",
        ],
    },
    RekeyTableManifest {
        table: "jj_reconcile_incoming_files",
        id_columns: &["intent_id"],
    },
    RekeyTableManifest {
        table: "jj_reconcile_intents",
        id_columns: &["id", "project_id"],
    },
    RekeyTableManifest {
        table: "jj_reconcile_items",
        id_columns: &["intent_id"],
    },
    RekeyTableManifest {
        table: "jj_reconcile_quarantines",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "job_browsers",
        id_columns: &["id", "job_id", "project_id"],
    },
    RekeyTableManifest {
        table: "job_repls",
        id_columns: &["id", "job_id", "project_id"],
    },
    RekeyTableManifest {
        table: "job_terminals",
        id_columns: &["id", "job_id", "project_id", "run_id"],
    },
    RekeyTableManifest {
        table: "jobs",
        id_columns: &[
            "id",
            "execution_id",
            "thread_id",
            "parent_job_id",
            "issue_id",
            "project_id",
            "current_turn_id",
            "resume_session_id",
        ],
    },
    RekeyTableManifest {
        table: "labels",
        id_columns: &["id"],
    },
    RekeyTableManifest {
        table: "memories",
        id_columns: &["id", "project_id", "job_id"],
    },
    RekeyTableManifest {
        table: "memory_triage_issue_memories",
        id_columns: &["issue_id", "memory_id"],
    },
    RekeyTableManifest {
        table: "merge_requests",
        id_columns: &["id", "job_id", "project_id", "issue_id"],
    },
    RekeyTableManifest {
        table: "pr_resolution_attributions",
        id_columns: &["id", "merge_request_id"],
    },
    RekeyTableManifest {
        table: "message_stream_chunks",
        id_columns: &["id", "stream_id"],
    },
    RekeyTableManifest {
        table: "message_streams",
        id_columns: &["id", "run_id", "turn_id", "final_event_id"],
    },
    RekeyTableManifest {
        table: "messages",
        id_columns: &["id", "channel_id", "sender_run_id", "recipient_run_id"],
    },
    RekeyTableManifest {
        table: "pack_catalog",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "pack_catalog_backfill_attempts",
        id_columns: &["execution_id"],
    },
    RekeyTableManifest {
        table: "pack_catalog_references",
        id_columns: &["project_id", "owner_id"],
    },
    RekeyTableManifest {
        table: "permission_requests",
        id_columns: &["id", "run_id", "turn_id", "job_id"],
    },
    RekeyTableManifest {
        table: "pr_node_port_fires",
        id_columns: &["id", "execution_id"],
    },
    RekeyTableManifest {
        table: "projects",
        id_columns: &["id"],
    },
    RekeyTableManifest {
        table: "prompts",
        id_columns: &["id", "run_id", "turn_id", "job_id"],
    },
    RekeyTableManifest {
        table: "queued_messages",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "repl_exchanges",
        id_columns: &["id", "repl_id", "project_id"],
    },
    RekeyTableManifest {
        table: "resource_surfacings",
        id_columns: &["id"],
    },
    RekeyTableManifest {
        table: "runs",
        id_columns: &["id", "issue_id", "project_id", "job_id", "chat_id"],
    },
    RekeyTableManifest {
        table: "search_outbox",
        id_columns: &["id", "source_id"],
    },
    RekeyTableManifest {
        table: "session_skyline_cache",
        id_columns: &["session_id"],
    },
    RekeyTableManifest {
        table: "sessions",
        id_columns: &[
            "id",
            "job_id",
            "chat_id",
            "replaced_by_id",
            "parent_session_id",
        ],
    },
    RekeyTableManifest {
        table: "skill_configs",
        id_columns: &["id", "project_id"],
    },
    RekeyTableManifest {
        table: "suppressed_wakes",
        id_columns: &["id", "subscription_id", "job_id"],
    },
    RekeyTableManifest {
        table: "threads",
        id_columns: &["id", "project_id"],
    },
    RekeyTableManifest {
        table: "todos",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "token_rollup",
        id_columns: &["id", "project_id", "run_id", "job_id"],
    },
    RekeyTableManifest {
        table: "token_rollup_runs",
        id_columns: &["run_id"],
    },
    RekeyTableManifest {
        table: "tool_invocation_runs",
        id_columns: &["run_id"],
    },
    RekeyTableManifest {
        table: "tool_invocations",
        id_columns: &["id", "event_id", "run_id"],
    },
    RekeyTableManifest {
        table: "turns",
        id_columns: &["id", "run_id", "job_id", "predecessor_id"],
    },
    RekeyTableManifest {
        table: "wake_subscriptions",
        id_columns: &["id", "job_id"],
    },
    // workflow_progress.id is the composite `{job_id}:{seq}` (a non-uuid, so
    // rekey_to_team leaves it as-is); job_id is the routable column that gets
    // re-prefixed to the team scope on a local->team promotion.
    RekeyTableManifest {
        table: "workflow_progress",
        id_columns: &["id", "job_id"],
    },
];

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::{DbError, DbResult, LocalDb, MigrationRunner, RowExt};

    #[test]
    fn project_rekey_manifest_covers_project_scoped_tables() {
        let scoped = TABLE_SCOPES
            .iter()
            .filter_map(|(table, scope)| {
                matches!(scope, TableScope::ProjectScoped).then_some(*table)
            })
            .collect::<std::collections::BTreeSet<_>>();
        let manifest = PROJECT_REKEY_MANIFEST
            .iter()
            .map(|entry| entry.table)
            .collect::<std::collections::BTreeSet<_>>();

        let mut expected = scoped.clone();
        expected.insert("execution_history");
        assert_eq!(manifest, expected);
    }

    async fn migrated_db() -> DbResult<LocalDb> {
        let temp = tempdir()?;
        let path = temp.keep().join("cairn-real-turso-schema.db");
        let db = LocalDb::open(path).await?;
        let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await?;
        assert_eq!(
            applied,
            vec![
                "0001_initial_schema".to_string(),
                "0002_search_outbox".to_string(),
                "0003_seed_default_workspace".to_string(),
                "0004_add_issue_dependencies".to_string(),
                "0005_change_preview_events".to_string(),
                "0006_uri_segments".to_string(),
                "0007_add_uri_segment_to_prompts".to_string(),
                "0008_add_job_id_to_prompts".to_string(),
                "0009_cohere_embeddings".to_string(),
                "0010_anon_device".to_string(),
                "0011_session_current_pos".to_string(),
                "0012_resource_surfacings".to_string(),
                "0013_memory_redux".to_string(),
                "0014_add_tool_use_id_to_prompts".to_string(),
                "0015_add_artifact_confirmed".to_string(),
                "0016_remove_ready_status".to_string(),
                "0017_messages_delivered_at".to_string(),
                "0018_pr_node_port_fires".to_string(),
                "0019_merge_request_owner".to_string(),
                "0020_add_uri_segment_to_action_runs".to_string(),
                "0021_vibe_axes".to_string(),
                "0022_add_segments_to_permission_requests".to_string(),
                "0023_add_labels".to_string(),
                "0024_add_parent_issue".to_string(),
                "0025_remove_managers".to_string(),
                "0026_child_side_channel_notices".to_string(),
                "0027_session_channel_cursor".to_string(),
                "0028_issue_parent_job".to_string(),
                "0029_queued_messages".to_string(),
                "0030_checkpoint_runs".to_string(),
                "0031_drop_dead_chats_table".to_string(),
                "0032_drop_workspaces_timezone_column".to_string(),
                "0033_annotations".to_string(),
                "0034_annotation_message_links".to_string(),
                "0035_annotation_uri_seq".to_string(),
                "0036_wake_subscriptions".to_string(),
                "0037_unify_side_channel_notices".to_string(),
                "0038_drop_annotation_tables".to_string(),
                "0039_memory_intake_ledger".to_string(),
                "0040_add_is_workspace_to_projects".to_string(),
                "0041_memory_triage_batches_and_drop_when_to_use".to_string(),
                "0042_memory_scope_node_id_and_status_lattice".to_string(),
                "0043_memory_triage_decision".to_string(),
                "0044_jobs_memory_review_state".to_string(),
                "0045_memory_canon_v2_consolidation".to_string(),
                "0046_memory_review_sent_state".to_string(),
                "0047_add_message_urgency".to_string(),
                "0048_add_event_thinking_tokens".to_string(),
                "0049_event_vibes_session_id".to_string(),
                "0050_session_skyline_cache_vibe_watermark".to_string(),
                "0051_clear_skyline_cache_for_content_bar_decomposition".to_string(),
                "0052_clear_skyline_cache_for_visual_height_sizing".to_string(),
                "0053_job_pack_anchor".to_string(),
                "0054_archival_storage".to_string(),
                "0055_archival_backfill_state".to_string(),
                "0056_archival_blobs".to_string(),
                "0057_event_read_tokens".to_string(),
                "0058_terminal_exit_wakes".to_string(),
                "0059_jobs_needs_fresh_session".to_string(),
                "0060_attention_items".to_string(),
                "0061_attention_escalate_at".to_string(),
                "0062_attention_fingerprint".to_string(),
                "0063_comment_seq".to_string(),
                "0064_clear_skyline_cache_for_system_event_filter".to_string(),
                "0065_merge_request_is_local".to_string(),
                "0066_config_disables".to_string(),
                "0067_tool_invocations".to_string(),
                "0068_job_browsers".to_string(),
                "0069_add_event_cost_usd".to_string(),
                "0070_attention_pushes".to_string(),
                "0071_attention_push_fingerprint".to_string(),
                "0072_merge_request_head_sha".to_string(),
                "0073_attention_read_cursors".to_string(),
                "0074_drop_attention_ledger".to_string(),
                "0075_drop_messages_delivered_at".to_string(),
                "0076_terminal_output_wakes".to_string(),
                "0077_event_content_change_id".to_string(),
                "0078_browser_last_active_at".to_string(),
                "0079_index_runs_session_id_created_at".to_string(),
                "0080_token_rollup".to_string(),
                "0081_drop_runs_backend".to_string(),
                "0082_team_routing".to_string(),
                "0083_cas_cache".to_string(),
                "0084_archival_pack_hash".to_string(),
                // CAIRN-2270: the shared 0087 re-grain lives in the SHARED_TAIL,
                // which the macro emits BEFORE the private tail (0085/0086), so it
                // applies here between 0084 and 0085 despite its higher number.
                "0087_token_rollup_hourly".to_string(),
                "0085_project_routes_local_path".to_string(),
                "0086_analytics_rollup_backfill_state".to_string(),
                "0088_reopen_analytics_backfill".to_string(),
                "0089_check_result_cache".to_string(),
                "0090_check_result_cache_input_hash".to_string(),
                "0091_drop_projects_server_id_and_servers".to_string(),
                "0092_tool_invocation_durations".to_string(),
                "0093_check_result_cache_job_id".to_string(),
                "0094_tool_invocation_result_backfill_state".to_string(),
                "0095_relink_merge_request_jobs".to_string(),
                "0096_jobs_child_base".to_string(),
                "0097_jobs_owns_ephemeral_worktree".to_string(),
                "0098_call_output_contract_and_run_tags".to_string(),
                "0101_workflow_progress".to_string(),
                "0102_check_result_cache_failure_kind".to_string(),
                "0103_executions_runner_device_id".to_string(),
                "0104_clear_invalid_job_worktree_paths".to_string(),
                "0099_workflow_journal".to_string(),
                "0100_workflow_restart_durability".to_string(),
                "0105_add_turn_end_reason".to_string(),
                "0106_index_hot_gui_status_queries".to_string(),
                "0107_normalize_openai_token_usage_components".to_string(),
                "0108_executor_enrollment".to_string(),
                "0109_check_result_cache_provenance".to_string(),
                "0110_executor_enrollment_expiry".to_string(),
                "0111_command_resource_profiles".to_string(),
                "0112_pack_catalog".to_string(),
                "0113_bind_agent_terminals_to_lifetime_leases".to_string(),
                "0114_add_jj_reconcile_intents".to_string(),
                "0115_add_agent_waits".to_string(),
                "0116_add_jj_reconcile_quarantines".to_string(),
                "0117_workflow_executor_anchor".to_string(),
                "0118_virtual_reconcile_coordinates".to_string(),
                "0119_virtual_workflow_coordinates".to_string(),
                "0120_integrity_sweep_state".to_string(),
                "0121_check_result_cache_recency_index".to_string(),
                "0122_session_account".to_string(),
                "0123_job_repls".to_string(),
                "0124_repl_exchanges".to_string(),
                "0125_rebind_terminals_to_residencies".to_string(),
                "0126_agent_waits_concurrent_calls".to_string(),
                "0127_image_refs".to_string(),
                "0128_write_replay_ledger".to_string(),
                "0129_mcp_continuation_prompts".to_string(),
                "0130_retire_snapshot_child_wakes".to_string(),
                "0131_unique_event_sequence".to_string(),
                "0132_check_result_cache_infra_suppression".to_string(),
                "0133_executor_enrollment_names".to_string(),
                "0134_check_result_observations".to_string(),
                "0135_check_definition_provenance".to_string(),
                "0136_conflict_resolution_sessions".to_string(),
                "0139_thread_compaction".to_string(),
                "0143_command_duration_profiles".to_string(),
                "0147_quarantine_saturation_memory_profiles".to_string(),
                "0137_channel_persistence".to_string(),
                "0142_channel_outbound_expired".to_string(),
                "0144_channel_thread_state".to_string(),
                "0148_channel_thread_suppression".to_string(),
                "0138_issue_kind".to_string(),
                "0145_pr_resolution_attribution".to_string(),
                "0146_add_check_observation_public_handle".to_string(),
                "0154_repair_check_observation_public_handle".to_string(),
                "0155_verdict_reuse_facts".to_string(),
                "0149_command_contention_profiles".to_string(),
                "0150_rebase_replay_status".to_string(),
                "0151_response_invocations".to_string(),
                "0152_route_firings".to_string(),
                "0153_channel_outbound_route_kind".to_string(),
                "0159_route_firing_content".to_string(),
                "0156_threads_entity".to_string(),
                "0157_migrate_issue_threads".to_string(),
                "0158_executor_desktop_automation".to_string(),
                "0160_thread_title_retires".to_string(),
                "0140_thread_compaction_seed_bytes".to_string(),
                "0141_thread_compaction_capacity_trigger".to_string(),
                "0161_clear_skyline_cache_for_compact_bars".to_string(),
                "0162_turn_embeddings".to_string(),
                "0163_turns_created_at_index".to_string(),
                "0164_turn_embedding_state".to_string(),
                "0165_check_result_cache_ran_at_millis".to_string(),
                "0166_authority_grants".to_string(),
                "0167_inherit_thread_id_for_child_jobs".to_string(),
            ]
        );
        Ok(db)
    }

    #[tokio::test]
    async fn repairs_comment_only_public_handle_migration_on_an_applied_database() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("observation-handle-repair.turso.db"))
            .await
            .unwrap();

        // Model a live database whose registry includes the no-op 0146 and every
        // subsequently shipped migration, but not the new repair.
        let previously_shipped = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version != "0154")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(previously_shipped)
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('check_result_observations') WHERE name = 'public_handle'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM issue_workspaces WHERE issue_id='thread'"
            )
            .await
            .unwrap(),
            0
        );

        let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied,
            vec!["0154_repair_check_observation_public_handle".to_string()]
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('check_result_observations') WHERE name = 'public_handle'"
            )
            .await
            .unwrap(),
            1
        );
    }

    /// CAIRN-3293: the retirement removes every seeded child-attention default
    /// that would keep a job in the watcher set — `active` and `muted` alike,
    /// since a `muted` one still draws passive pushes for children the job may no
    /// longer own. Rows that only ever suppress (`unsubscribed`), rows a node
    /// created deliberately, and the non-derivable `user`/`peer`/`process` sources
    /// all survive.
    #[tokio::test]
    async fn snapshot_child_wakes_are_retired_without_touching_deliberate_rows() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("retire-child-wakes.turso.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version != "0130")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before).run(&db).await.unwrap();
        const DEFAULT_KINDS: &str = r#"["message","permission","question","resolved","review"]"#;
        db.execute_script(&format!(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (id, project_id, status, created_at, updated_at)
             VALUES ('stale', 'project', 'complete', 1, 1), ('live', 'project', 'running', 2, 2);
             INSERT INTO wake_subscriptions
               (id, job_id, source_kind, source_ref, fact_kinds_json, state, created_by, created_at, updated_at, one_shot)
             VALUES
               ('seeded', 'stale', 'issue', 'cairn://p/PRJ/2', '{DEFAULT_KINDS}', 'active', 'system', 1, 1, 0),
               ('muted-seed', 'stale', 'issue', 'cairn://p/PRJ/3', '{DEFAULT_KINDS}', 'muted', 'system', 1, 1, 0),
               ('refused-seed', 'stale', 'issue', 'cairn://p/PRJ/5', '{DEFAULT_KINDS}', 'unsubscribed', 'system', 1, 1, 0),
               ('agent-muted', 'live', 'issue', 'cairn://p/PRJ/6', NULL, 'muted', 'agent', 1, 1, 0),
               ('agent-watch', 'live', 'issue', 'cairn://p/PRJ/4', NULL, 'active', 'agent', 1, 1, 0),
               ('job-default', 'live', 'user', NULL, NULL, 'active', 'system', 1, 1, 0),
               ('terminal', 'live', 'process', 'cairn://p/PRJ/4/1/builder/terminal/dev', '[\"terminal_exit\"]', 'active', 'system', 1, 1, 1);"
        ))
        .await
        .unwrap();

        let retirement = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version == "0130")
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            MigrationRunner::new(retirement).run(&db).await.unwrap(),
            vec!["0130_retire_snapshot_child_wakes".to_string()]
        );

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM wake_subscriptions WHERE id IN ('seeded', 'muted-seed')"
            )
            .await
            .unwrap(),
            0,
            "every snapshot default that would keep the job watching is gone, \
             muted included"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM wake_subscriptions
                 WHERE id IN ('refused-seed', 'agent-muted', 'agent-watch', 'job-default', 'terminal')"
            )
            .await
            .unwrap(),
            5,
            "suppressing refusals, deliberate rows, and non-derivable sources survive"
        );
    }

    /// CAIRN-3290: the duplicate repair separates colliding slots in place
    /// without reordering or dropping anything, and leaves clean runs untouched.
    ///
    /// The `run-pinned` fixture is the row set observed on run
    /// `3f68480e-fb40-4b05-831c-a216ff4d2672`: sequence 7 held twice (a finalized
    /// `assistant` stamped with its stream's OPEN time, and the `tool_result`
    /// that was actually inserted first), with 8 absent.
    #[tokio::test]
    async fn duplicate_event_sequences_are_separated_before_the_unique_index() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("unique-event-sequence.turso.db"))
            .await
            .unwrap();
        let before_0131 = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version != "0131")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before_0131).run(&db).await.unwrap();

        // (id, run, sequence, created_at)
        let rows: &[(&str, &str, i64, i64)] = &[
            ("p0", "run-pinned", 0, 100),
            ("p1", "run-pinned", 1, 101),
            // The finalized assistant carries its stream's open time, so it sorts
            // first within the collided slot even though it landed second.
            ("p7-assistant", "run-pinned", 7, 152),
            ("p7-tool-result", "run-pinned", 7, 153),
            ("p9", "run-pinned", 9, 160),
            ("p12", "run-pinned", 12, 170),
            // Three rows on one slot: every extra must find its own.
            ("t0", "run-triple", 0, 200),
            ("t1-a", "run-triple", 1, 201),
            ("t1-b", "run-triple", 1, 202),
            ("t1-c", "run-triple", 1, 203),
            ("t2", "run-triple", 2, 204),
            // A run with no collisions must come through byte-identical.
            ("c0", "run-clean", 0, 300),
            ("c5", "run-clean", 5, 301),
        ];

        let mut script = String::from(
            "INSERT INTO runs(id, status, created_at, updated_at) VALUES
             ('run-pinned','exited',1,1),('run-triple','exited',1,1),('run-clean','exited',1,1);",
        );
        for (id, run, sequence, created_at) in rows {
            script.push_str(&format!(
                "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                 VALUES ('{id}','{run}',{sequence},{created_at},'assistant','{{}}',{created_at});"
            ));
        }
        db.execute_script(&script).await.unwrap();

        let repair = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version == "0131")
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            MigrationRunner::new(repair).run(&db).await.unwrap(),
            vec!["0131_unique_event_sequence".to_string()]
        );

        let sequence_of = |id: &'static str| {
            let db = &db;
            async move {
                db.query_one("SELECT sequence FROM events WHERE id = ?1", (id,), |row| {
                    row.i64(0)
                })
                .await
                .unwrap()
            }
        };

        // Order preserved, collision separated into the next slot, and the rows
        // above it shift by exactly the number of extras below them.
        assert_eq!(sequence_of("p0").await, 0);
        assert_eq!(sequence_of("p1").await, 1);
        assert_eq!(sequence_of("p7-assistant").await, 7);
        assert_eq!(sequence_of("p7-tool-result").await, 8);
        assert_eq!(sequence_of("p9").await, 10);
        assert_eq!(sequence_of("p12").await, 13);

        assert_eq!(sequence_of("t0").await, 0);
        assert_eq!(sequence_of("t1-a").await, 1);
        assert_eq!(sequence_of("t1-b").await, 2);
        assert_eq!(sequence_of("t1-c").await, 3);
        assert_eq!(sequence_of("t2").await, 4);

        assert_eq!(sequence_of("c0").await, 0, "a clean run is not renumbered");
        assert_eq!(sequence_of("c5").await, 5, "a clean run keeps its gaps");

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM events").await.unwrap(),
            rows.len() as i64,
            "the repair moves rows, it never deletes them"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type='table' AND name LIKE '_event_sequence%'"
            )
            .await
            .unwrap(),
            0,
            "the repair's scratch tables are dropped"
        );

        // The constraint is live: a duplicate slot is now rejected outright.
        let duplicate = db
            .execute(
                "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                 VALUES ('c0-dup','run-clean',0,1,'assistant','{}',1)",
                (),
            )
            .await;
        assert!(
            duplicate.is_err(),
            "events(run_id, sequence) must reject a duplicate slot"
        );
    }

    #[tokio::test]
    async fn invalid_job_worktree_paths_are_cleared() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("invalid-worktree-paths.turso.db"))
            .await
            .unwrap();
        let before_0104 = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version != "0104")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before_0104).run(&db).await.unwrap();
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (id, project_id, status, worktree_path, branch, created_at, updated_at)
             VALUES ('root', 'project', 'complete', '/', 'agent/root', 1, 1),
                    ('empty', 'project', 'complete', '  ', 'agent/empty', 1, 1),
                    ('valid', 'project', 'complete', '/managed/worktree', 'agent/valid', 1, 1);",
        )
        .await
        .unwrap();

        let cleanup = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version == "0104")
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            MigrationRunner::new(cleanup).run(&db).await.unwrap(),
            vec!["0104_clear_invalid_job_worktree_paths".to_string()]
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM jobs WHERE id IN ('root', 'empty') AND worktree_path IS NULL AND branch IS NULL"
            )
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            db.query_text("SELECT worktree_path FROM jobs WHERE id = 'valid'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("/managed/worktree")
        );
    }

    #[tokio::test]
    async fn virtual_coordinates_preserve_job_and_reconcile_state() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("virtual-coordinates.turso.db"))
            .await
            .unwrap();
        let before_0118 = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version != "0118")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before_0118).run(&db).await.unwrap();
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (
                 id, project_id, status, worktree_path, owns_ephemeral_worktree,
                 branch, base_branch, base_commit, created_at, updated_at
             ) VALUES (
                 'job', 'project', 'running', '/legacy/job', 1,
                 'agent/job', 'main', 'base-sha', 1, 1
             );
             INSERT INTO jj_reconcile_intents (
                 id, project_id, store_path, target_branch, destination_commit,
                 created_at, updated_at
             ) VALUES ('intent', 'project', '/store', 'main', 'destination-sha', 1, 1);
             INSERT INTO jj_reconcile_items (
                 intent_id, bookmark, workspace_path, observed_tip, workspace_lineage,
                 status, updated_at
             ) VALUES ('intent', 'agent/job', '/legacy/job', 'observed-sha', 'lineage', 'pending', 1);",
        )
        .await
        .unwrap();

        let migration = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version == "0118")
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            MigrationRunner::new(migration).run(&db).await.unwrap(),
            vec!["0118_virtual_reconcile_coordinates".to_string()]
        );
        assert_eq!(
            db.query_text(
                "SELECT branch || '|' || base_branch || '|' || base_commit FROM jobs WHERE id = 'job'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("agent/job|main|base-sha")
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name IN ('worktree_path', 'owns_ephemeral_worktree')",
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            db.query_text(
                "SELECT bookmark || '|' || observed_tip || '|' || status FROM jj_reconcile_items WHERE intent_id = 'intent'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("agent/job|observed-sha|pending")
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jj_reconcile_items') WHERE name IN ('workspace_path', 'workspace_lineage')",
            )
            .await
            .unwrap(),
            0
        );
    }

    async fn explain_plan(db: &LocalDb, sql: &str) -> Vec<String> {
        db.query_all(format!("EXPLAIN QUERY PLAN {sql}"), (), |row| row.text(3))
            .await
            .unwrap()
    }

    /// 0078 indexes the session-transcript loader's hottest query. This asserts
    /// the planner actually changes its plan when the index is present, against
    /// the real schema, so the index cannot silently become dead weight: without
    /// it the query is a full scan plus a sort; with it the query is an index
    /// seek that also satisfies the ORDER BY.
    #[tokio::test]
    async fn migration_0079_indexes_session_runs_query() {
        const SESSION_RUNS: &str =
            "SELECT id FROM runs WHERE session_id = 'x' ORDER BY created_at ASC";

        // Without the 0079 index migration (every other migration applied): no
        // session_id index, so the planner does a full table scan and sorts for
        // ORDER BY. Filter by version rather than slicing the last migration, so
        // the test stays valid as later migrations are appended.
        let before = {
            let temp = tempdir().unwrap();
            let path = temp.keep().join("cairn-runs-index-before.db");
            let db = LocalDb::open(path).await.unwrap();
            let without_index: Vec<_> = TURSO_MIGRATIONS
                .iter()
                .filter(|m| m.version != "0079")
                .copied()
                .collect();
            MigrationRunner::new(without_index).run(&db).await.unwrap();
            explain_plan(&db, SESSION_RUNS).await
        };
        assert!(
            before.iter().any(|step| step.contains("SCAN runs"))
                && before.iter().any(|step| step.contains("SORTER")),
            "expected full scan + sort before the index, got {before:?}"
        );

        // After 0078: the composite (session_id, created_at) index turns the
        // query into an index seek that also satisfies the ORDER BY.
        let db = migrated_db().await.unwrap();
        let after = explain_plan(&db, SESSION_RUNS).await;
        assert!(
            after
                .iter()
                .any(|step| step.contains("SEARCH runs USING INDEX idx_runs_session_id_created_at")),
            "expected an index seek after 0078, got {after:?}"
        );
        assert!(
            !after.iter().any(|step| step.contains("SORTER")),
            "the index should satisfy ORDER BY without a sort, got {after:?}"
        );
    }

    async fn query_i64(db: &LocalDb, sql: &'static str) -> DbResult<i64> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, ()).await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("missing integer row".to_string()))?;
                row.i64(0)
            })
        })
        .await
    }

    async fn query_text(db: &LocalDb, sql: &'static str) -> DbResult<String> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, ()).await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("missing text row".to_string()))?;
                row.text(0)
            })
        })
        .await
    }

    #[tokio::test]
    async fn migration_0095_relinks_action_run_owned_merge_request_jobs() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("mr-relink.turso.db"))
            .await
            .unwrap();
        let before_0095: Vec<_> = TURSO_MIGRATIONS
            .iter()
            .filter(|m| m.version != "0095")
            .copied()
            .collect();
        MigrationRunner::new(before_0095).run(&db).await.unwrap();

        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-mr', 'default', 'Project', 'PMR', '/tmp/pmr', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-mr', 'proj-mr', 1, 'Issue', 'active', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-mr', 'recipe', 'issue-mr', 'proj-mr', 'running', 1, 1);

            INSERT INTO jobs(id, execution_id, issue_id, project_id, branch, status, created_at, updated_at)
             VALUES ('job-good', 'exec-mr', 'issue-mr', 'proj-mr', 'agent/PMR-1-builder', 'complete', 100, 110);
            INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, parent_job_id, created_at)
             VALUES ('pr-action-run', 'exec-mr', 'pr', 'builtin:pr', 'issue-mr', 'proj-mr', 'blocked', 'job-good', 115);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-good', 'pr-action-run', 'proj-mr', 'issue-mr', 'PR', 'agent/PMR-1-builder', 'main', 'open', 120, 120);

            INSERT INTO jobs(id, execution_id, issue_id, project_id, branch, status, created_at, updated_at)
             VALUES ('job-other', 'exec-mr', 'issue-mr', 'proj-mr', 'agent/PMR-1-other', 'complete', 90, 90);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-untouched', 'dangling-not-action-run', 'proj-mr', 'issue-mr', 'Unmatched PR', 'agent/PMR-1-other', 'main', 'open', 100, 100);
            ",
        )
        .await
        .unwrap();

        let relink = TURSO_MIGRATIONS
            .iter()
            .filter(|m| m.version == "0095")
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            MigrationRunner::new(relink).run(&db).await.unwrap(),
            vec!["0095_relink_merge_request_jobs".to_string()]
        );

        assert_eq!(
            query_text(
                &db,
                "SELECT job_id FROM merge_requests WHERE id = 'mr-good'"
            )
            .await
            .unwrap(),
            "job-good"
        );
        assert_eq!(
            query_text(
                &db,
                "SELECT job_id FROM merge_requests WHERE id = 'mr-untouched'"
            )
            .await
            .unwrap(),
            "dangling-not-action-run"
        );
    }

    #[tokio::test]
    async fn route_firing_content_backfills_delivered_text_from_the_ledger() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("firing-content.turso.db"))
            .await
            .unwrap();

        // Every migration before the content columns, so the seed writes the
        // schema a running installation actually has. Filter by version rather
        // than slicing: composition order is not file-number order.
        let head = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version < "0159")
            .cloned()
            .collect::<Vec<_>>();
        MigrationRunner::new(head).run(&db).await.unwrap();

        // Two channel firings: one whose ledger row survives, one whose does not.
        db.execute_batch(
            "INSERT INTO route_firings
                 (id,route_id,scope_key,seq,trigger_source,fact_identity,status,sink_kind,sink_ref,created_at)
                 VALUES ('f1','followed-thread-stream','workspace',1,'thread_stream',
                         'cairn://p/CAIRN/3404:event:99','fired','channel','channel_outbound:o1',10),
                        ('f2','followed-thread-stream','workspace',2,'thread_stream',
                         'cairn://p/CAIRN/3404:event:100','fired','channel','channel_outbound:gone',20);
             INSERT INTO channel_outbound
                 (id,channel,kind,binding_ref,conversation,rendered_text,rendering,status,created_at)
                 VALUES ('o1','imessage','route',
                         'route:followed-thread-stream:cairn://p/CAIRN/3404:event:99',
                         'c','nineteen merges today','text','sent',10);",
        )
        .await
        .unwrap();

        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        assert_eq!(
            query_text(&db, "SELECT payload_text FROM route_firings WHERE id='f1'")
                .await
                .unwrap(),
            "nineteen merges today",
            "a firing whose ledger row survives recovers what it delivered"
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM route_firings WHERE id='f2' AND payload_text IS NULL",
                (),
            )
            .await
            .unwrap(),
            Some(1),
            "a firing with no surviving ledger row stays unrecorded rather than wrong"
        );
    }

    #[tokio::test]
    async fn drop_server_id_preserves_project_data() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("survival.turso.db"))
            .await
            .unwrap();

        // Apply every private migration before the 0091 drop, so the seed sees
        // the pre-drop schema with `projects.server_id` and the `servers` table
        // still present. Filter by version rather than slicing the tail: shared
        // migrations can be appended after 0091 in array order.
        let head = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version < "0091")
            .cloned()
            .collect::<Vec<_>>();
        MigrationRunner::new(head).run(&db).await.unwrap();

        // A server, a workspace, a project referencing that server, and a child
        // issue. FK enforcement is ON here, so the server row must exist first.
        db.execute_batch(
            "INSERT INTO servers (id, name, url, created_at, updated_at)
                 VALUES ('srv1', 's', 'u', 0, 0);
             INSERT INTO workspaces (id, name, created_at, updated_at)
                 VALUES ('ws1', 'w', 0, 0);
             INSERT INTO projects
                 (id, workspace_id, name, key, repo_path, server_id, created_at, updated_at)
                 VALUES ('proj1', 'ws1', 'p', 'P', '/tmp/p', 'srv1', 0, 0);
             INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                 VALUES ('iss1', 'proj1', 1, 't', 0, 0);",
        )
        .await
        .unwrap();

        // Apply the full lineage, which now includes the 0090 FK-off rebuild.
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        // The project and its child issue survive the rebuild intact.
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM projects WHERE id = 'proj1'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM issues WHERE id = 'iss1'")
                .await
                .unwrap(),
            1
        );
        // The dead column is gone from the rebuilt table.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('projects') WHERE name = 'server_id'"
            )
            .await
            .unwrap(),
            0
        );
        // The dead local `servers` table is dropped.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'servers'"
            )
            .await
            .unwrap(),
            0
        );
    }

    /// CAIRN-3434: widening a CHECK-constrained enum is a full table rebuild, so
    /// an ask already on the operator's phone has to survive it with the GUID a
    /// reply binds to -- and the new terminal state has to actually be accepted
    /// once the rebuild lands.
    #[tokio::test]
    async fn channel_outbound_rebuild_preserves_intents_and_admits_expired() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("channel-expired.turso.db"))
            .await
            .unwrap();

        // Stop at the migration under test in COMPOSITION order; the private tail
        // interleaves, so a version comparison would apply the wrong neighbors.
        let head = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version != "0140")
            .cloned()
            .collect::<Vec<_>>();
        MigrationRunner::new(head).run(&db).await.unwrap();

        db.execute_batch(
            "INSERT INTO channel_outbound
                 (id, channel, kind, binding_ref, conversation, rendered_text, rendering, status, provider_guid, created_at, sent_at)
                 VALUES ('sent-1', 'imessage', 'question', 'prompt-1:0', '+15551234567', 'Which path?', 'text', 'sent', 'guid-1', 10, 11);",
        )
        .await
        .unwrap();

        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        assert_eq!(
            query_text(
                &db,
                "SELECT provider_guid FROM channel_outbound WHERE id = 'sent-1'"
            )
            .await
            .unwrap(),
            "guid-1"
        );
        // The rebuild recreates every index the dropped table carried.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_channel_outbound%'"
            )
            .await
            .unwrap(),
            3
        );
        db.execute(
            "UPDATE channel_outbound SET status = 'expired' WHERE id = 'sent-1'",
            (),
        )
        .await
        .unwrap();
        assert!(
            db.execute(
                "UPDATE channel_outbound SET status = 'abandoned' WHERE id = 'sent-1'",
                (),
            )
            .await
            .is_err(),
            "the widened CHECK still fences off statuses the ledger does not define"
        );
    }

    #[tokio::test]
    async fn migrated_memories_default_to_draft_intake() {
        let db = migrated_db().await.unwrap();

        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Memory issue', 'active', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'issue-1', 'project-1', 'builder', 'builder', 'builder', 'running', 1, 1);
            INSERT INTO memories(id, content, job_id, node_seq, created_at, updated_at)
             VALUES ('capture', 'what happened and where', 'job-1', 1, 1, 1);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            query_text(&db, "SELECT status FROM memories WHERE id = 'capture'")
                .await
                .unwrap(),
            "draft"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_memories_pending_created'"
            )
            .await
            .unwrap(),
            1
        );
    }

    /// 0074 drops the dead attention ledger tables; 0075 drops the retired
    /// messages.delivered_at column and its partial index.
    #[tokio::test]
    async fn migrations_0074_0075_drop_attention_ledger_and_delivered_at() {
        let db = migrated_db().await.unwrap();

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='attention_items'"
            )
            .await
            .unwrap(),
            0,
            "attention_items should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='attention_seen'"
            )
            .await
            .unwrap(),
            0,
            "attention_seen should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='attention_evaluations'"
            )
            .await
            .unwrap(),
            0,
            "attention_evaluations should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name='delivered_at'"
            )
            .await
            .unwrap(),
            0,
            "messages.delivered_at should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_pending_directs'"
            )
            .await
            .unwrap(),
            0,
            "idx_messages_pending_directs should be dropped"
        );
    }

    /// 0031 drops the dead project-chat `chats` table. The `chat_id` foreign-key
    /// columns on `runs` and `sessions` survive as vestigial, always-NULL
    /// columns. This proves that with the parent table gone, inserts into both
    /// child tables (chat_id NULL) still succeed under the enforced
    /// `PRAGMA foreign_keys = ON` — i.e. the now-dangling FK does not break the
    /// hot insert paths.
    #[tokio::test]
    async fn migration_0031_drops_chats_and_keeps_child_inserts_working() {
        let db = migrated_db().await.unwrap();

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'chats'"
            )
            .await
            .unwrap(),
            0
        );

        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-x', 'default', 'Project', 'PX', '/tmp/px', 1, 1);
            INSERT INTO runs(id, project_id, created_at, updated_at)
             VALUES ('run-x', 'proj-x', 1, 1);
            INSERT INTO jobs(id, project_id, status, created_at, updated_at)
             VALUES ('job-x', 'proj-x', 'running', 1, 1);
            INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('sess-x', 'job-x', 1, 1);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM runs WHERE id = 'run-x'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM sessions WHERE id = 'sess-x'")
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn real_migrations_apply_once_under_mvcc() {
        let db = migrated_db().await.unwrap();

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM cairn_schema_migrations")
                .await
                .unwrap(),
            TURSO_MIGRATIONS.len() as i64
        );

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('workspaces') WHERE name = 'timezone'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('projects') WHERE name = 'is_workspace'"
            )
            .await
            .unwrap(),
            1
        );
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('default-project', 'default', 'Default Project', 'DP', '/tmp/dp', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(
            query_i64(
                &db,
                "SELECT is_workspace FROM projects WHERE id = 'default-project'"
            )
            .await
            .unwrap(),
            0
        );

        // 0025: the manager stack is physically removed. Manager tables gone,
        // manager columns gone, but every non-manager column/index/trigger on
        // the rebuilt tables is preserved. (The aggregate manager-table check
        // is asserted below via `name LIKE '%manager%'`.)
        //
        // manager_id / recipient_manager_id columns are gone from every table.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('issues') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('turns') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('merge_requests') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'recipient_manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        // Non-manager columns survive the rebuild.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('issues') WHERE name = 'parent_issue_id'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = 'uri_segment'"
            )
            .await
            .unwrap(),
            1
        );
        // Manager indexes are gone; non-manager indexes and parent index survive.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN ('idx_jobs_manager_id', 'idx_turns_manager_id', 'idx_messages_recipient_manager_id', 'idx_mr_manager')"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_issues_parent_id'"
            )
            .await
            .unwrap(),
            1
        );
        // Search triggers dropped with the issues/messages rebuilds are restored.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name IN ('search_issues_insert', 'search_issues_update', 'search_issues_delete', 'search_messages_insert', 'search_messages_update', 'search_messages_delete')"
            )
            .await
            .unwrap(),
            6
        );
        // No FK in the whole schema still points at a manager table, and no
        // leftover rebuild scratch tables remain.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '%manager%'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%_new' OR name LIKE '%_old'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('issues') WHERE name = 'parent_issue_id'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_index_list('issues') WHERE name = 'idx_issues_parent_id'"
            )
            .await
            .unwrap(),
            1
        );
        // 0022: permission_requests gains job_id + uri_segment for addressability.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('permission_requests') WHERE name IN ('job_id', 'uri_segment')"
            )
            .await
            .unwrap(),
            2
        );
        // 0021: event_vibes recreated with PHASE/FRICTION coordinates, no locus.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('event_vibes') WHERE name IN ('phase', 'friction')"
            )
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('event_vibes') WHERE name IN ('locus', 'similarity')"
            )
            .await
            .unwrap(),
            0
        );
        // 0015: artifacts gains the `confirmed` resolution column.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('artifacts') WHERE name = 'confirmed'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = 'uri_segment'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('action_runs') WHERE name = 'uri_segment'"
            )
            .await
            .unwrap(),
            1
        );
        // 0031 dropped the dead project-chat `chats` table entirely.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'chats'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_issue_execution_uri_segment'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM workspaces WHERE id = 'default'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '%_fts%'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_text(&db, "PRAGMA journal_mode").await.unwrap(),
            "mvcc"
        );

        // Memory intake ledger: applicability text was retired; triggers table
        // and the legacy keywords column is gone.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name = 'when_to_use'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name = 'keywords'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'memory_triggers'"
            )
            .await
            .unwrap(),
            0
        );

        let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        assert!(applied.is_empty());
    }

    #[tokio::test]
    async fn uri_segment_backfill_handles_natural_suffix_collisions() {
        let temp = tempdir().unwrap();
        let path = temp.keep().join("cairn-uri-collision.db");
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS[..5].to_vec())
            .run(&db)
            .await
            .unwrap();

        db.execute_script(
            "
            INSERT OR IGNORE INTO workspaces(id, name, created_at, updated_at)
             VALUES ('default', 'Default', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PROJ', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Issue', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe-1', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at)
             VALUES ('parent-1', 'exec-1', 'parent', 'issue-1', 'project-1', 'Parent', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at)
             VALUES ('unsafe-parent', 'exec-1', 'unsafe', 'issue-1', 'project-1', 'Build / Test?#', 'running', 2, 2);
            INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, task_index, created_at, updated_at)
             VALUES ('task-1', 'exec-1', 'parent-1', 'issue-1', 'project-1', 'Explore', 'running', 0, 3, 3);
            INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, task_index, created_at, updated_at)
             VALUES ('task-2', 'exec-1', 'parent-1', 'issue-1', 'project-1', 'Explore', 'running', 1, 4, 4);
            INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, task_index, created_at, updated_at)
             VALUES ('task-3', 'exec-1', 'parent-1', 'issue-1', 'project-1', 'Explore 2', 'running', 2, 5, 5);
            INSERT INTO chats(id, project_id, status, created_at, updated_at)
             VALUES ('chat-1', 'project-1', 'running', 6, 6);
            INSERT INTO chats(id, project_id, status, created_at, updated_at)
             VALUES ('chat-2', 'project-1', 'running', 7, 7);
            ",
        )
        .await
        .unwrap();

        MigrationRunner::new(vec![TURSO_MIGRATIONS[5]])
            .run(&db)
            .await
            .unwrap();

        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'parent-1'")
                .await
                .unwrap(),
            "parent"
        );
        assert_eq!(
            query_text(
                &db,
                "SELECT uri_segment FROM jobs WHERE id = 'unsafe-parent'"
            )
            .await
            .unwrap(),
            "build-test"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM jobs WHERE uri_segment GLOB '*[^a-z0-9-]*'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(DISTINCT uri_segment) FROM jobs WHERE parent_job_id = 'parent-1'"
            )
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'task-1'")
                .await
                .unwrap(),
            "explore"
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'task-2'")
                .await
                .unwrap(),
            "explore-task-2"
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'task-3'")
                .await
                .unwrap(),
            "explore-2"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'parent-1' AND uri_segment IS NOT NULL"
            )
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM chats WHERE id = 'chat-1'")
                .await
                .unwrap(),
            "chat"
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM chats WHERE id = 'chat-2'")
                .await
                .unwrap(),
            "chat-chat-2"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(DISTINCT uri_segment) FROM chats WHERE project_id = 'project-1'"
            )
            .await
            .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn real_schema_search_outbox_tracks_committed_writes_only() {
        let db = migrated_db().await.unwrap();

        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Turso migration', 'Index me', 1, 1);
            INSERT INTO comments(id, issue_id, content, source, created_at)
             VALUES ('comment-1', 'issue-1', 'Committed comment', 'user', 2);
            INSERT INTO messages(id, channel_type, channel_id, sender_name, content, created_at)
             VALUES ('message-1', 'issue', 'issue-1', 'system', 'Committed message', 3);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM search_outbox WHERE status = 'pending'"
            )
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM search_outbox WHERE source_table IN ('issues', 'comments', 'messages')"
            )
            .await
            .unwrap(),
            3
        );

        let error = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                         VALUES ('rolled-back-issue', 'project-1', 2, 'Rollback', 'Do not index', 4, 4)",
                        (),
                    )
                    .await?;
                    Err::<(), DbError>(DbError::internal("force rollback"))
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::Internal(_)));

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM issues WHERE id = 'rolled-back-issue'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM search_outbox")
                .await
                .unwrap(),
            3
        );
    }

    #[tokio::test]
    async fn migration_0042_backfills_memory_spine_columns() {
        let temp = tempdir().unwrap();
        let path = temp.keep().join("cairn-memory-0042.db");
        let db = LocalDb::open(path).await.unwrap();

        let pre = MigrationRunner::new(TURSO_MIGRATIONS[..41].to_vec())
            .run(&db)
            .await
            .unwrap();
        assert_eq!(pre.len(), 41);

        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
             VALUES ('workspace', 'default', 'Workspace', 'WKS', '/tmp/workspace', 1, 1, 1);
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/project', 1, 1);
            INSERT INTO memories(id, project_id, content, confidence, status, provenance_uri, created_at, updated_at, surfaced_count, active)
             VALUES ('workspace-memory', NULL, 'workspace content', 'tentative', 'handled', NULL, 1, 1, 0, 1);
            INSERT INTO memories(id, project_id, content, confidence, status, provenance_uri, created_at, updated_at, surfaced_count, active)
             VALUES ('project-memory', 'project-1', 'project content', 'tentative', 'pending', NULL, 2, 2, 0, 1);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Memory triage', 'active', 1, 1);
            INSERT INTO memory_triage_issue_memories(issue_id, memory_id)
             VALUES ('issue-1', 'project-memory');
            ",
        )
        .await
        .unwrap();

        let applied = MigrationRunner::new(vec![TURSO_MIGRATIONS[41]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied,
            vec!["0042_memory_scope_node_id_and_status_lattice".to_string()]
        );

        assert_eq!(
            query_text(
                &db,
                "SELECT status FROM memories WHERE id = 'workspace-memory'"
            )
            .await
            .unwrap(),
            "claimed"
        );
        assert_eq!(
            query_text(
                &db,
                "SELECT project_id || ':' || scope || ':' || scope_value FROM memories WHERE id = 'workspace-memory'"
            )
            .await
            .unwrap(),
            "workspace:workspace:workspace"
        );
        let applied_0043 = MigrationRunner::new(vec![TURSO_MIGRATIONS[42]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied_0043,
            vec!["0043_memory_triage_decision".to_string()]
        );

        assert_eq!(
            query_text(
                &db,
                "SELECT scope || ':' || scope_value FROM memories WHERE id = 'project-memory'"
            )
            .await
            .unwrap(),
            "project:project-1"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM memories WHERE status = 'handled'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM memories WHERE job_id IS NULL AND node_seq IS NULL AND promoted_commit_sha IS NULL AND reason IS NULL"
            )
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name IN ('scope', 'scope_value', 'job_id', 'node_seq', 'promoted_commit_sha', 'reason')"
            )
            .await
            .unwrap(),
            6
        );
        assert_eq!(
            query_text(
                &db,
                "SELECT dflt_value FROM pragma_table_info('memories') WHERE name = 'status'"
            )
            .await
            .unwrap(),
            "'draft'"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_memories_job_node_seq'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM memory_triage_issue_memories WHERE issue_id = 'issue-1' AND memory_id = 'project-memory'"
            )
            .await
            .unwrap(),
            1
        );

        let applied_0044 = MigrationRunner::new(vec![TURSO_MIGRATIONS[43]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied_0044,
            vec!["0044_jobs_memory_review_state".to_string()]
        );
        let applied_0045 = MigrationRunner::new(vec![TURSO_MIGRATIONS[44]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied_0045,
            vec!["0045_memory_canon_v2_consolidation".to_string()]
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM memories")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM memory_triage_issue_memories")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name IN ('confidence', 'active', 'surfaced_count', 'last_surfaced_at')"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name = 'name'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name IN ('job_id', 'node_seq') AND \"notnull\" = 1"
            )
            .await
            .unwrap(),
            2
        );
    }

    /// Apply migrations through 0024 (manager schema present), seed a manager
    /// plus a row referencing it in every manager column, then apply the
    /// FK-off migration 0025. Proves the runner's foreign-keys-off path handles
    /// real referencing data without violation, drops the manager surface
    /// physically, preserves every non-manager row, and leaves no foreign key
    /// pointing at a manager table.
    #[tokio::test]
    async fn migration_0025_removes_managers_with_referencing_data() {
        let temp = tempdir().unwrap();
        let path = temp.keep().join("cairn-remove-managers.db");
        let db = LocalDb::open(path).await.unwrap();

        // Everything before 0025 (manager tables + manager_id columns present).
        let pre = MigrationRunner::new(TURSO_MIGRATIONS[..24].to_vec())
            .run(&db)
            .await
            .unwrap();
        assert_eq!(pre.len(), 24);

        // Seed a manager and a referencing row in each manager column.
        db.execute_script(
            "
            INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO managers(id,project_id,name,branch,created_at,updated_at) VALUES('m','p','M','b',1,1);
            INSERT INTO issues(id,project_id,number,title,description,manager_id,created_at,updated_at) VALUES('i','p',1,'Issue title','Index me','m',1,1);
            INSERT INTO jobs(id,project_id,node_name,status,manager_id,created_at,updated_at) VALUES('j','p','N','running','m',1,1);
            INSERT INTO turns(id,session_id,sequence,manager_id,created_at,updated_at) VALUES('t','sess',1,'m',1,1);
            INSERT INTO messages(id,channel_type,channel_id,sender_name,content,recipient_manager_id,created_at) VALUES('msg','direct','i','system','hello','m',1);
            INSERT INTO merge_requests(id,job_id,project_id,issue_id,manager_id,title,source_branch,target_branch,opened_at,updated_at) VALUES('mr','j','p','i','m','PR','src','dst',1,1);
            ",
        )
        .await
        .unwrap();

        // Apply 0025 alone (the FK-off rebuild migration).
        let applied = MigrationRunner::new(vec![TURSO_MIGRATIONS[24]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(applied, vec!["0025_remove_managers".to_string()]);

        // Manager tables are physically gone.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '%manager%'"
            )
            .await
            .unwrap(),
            0
        );
        // No scratch rebuild tables left behind.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%_new' OR name LIKE '%_old'"
            )
            .await
            .unwrap(),
            0
        );
        // Every referencing row survived the rebuild.
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM issues WHERE id = 'i'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM jobs WHERE id = 'j'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM turns WHERE id = 't'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM messages WHERE id = 'msg'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM merge_requests WHERE id = 'mr'")
                .await
                .unwrap(),
            1
        );

        // No foreign key anywhere still targets a manager table, and the rebuilt
        // tables keep their non-manager foreign keys (e.g. jobs -> projects).
        let (manager_fks, jobs_to_projects) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut tables = Vec::new();
                    let mut rows = conn
                        .query("SELECT name FROM sqlite_master WHERE type = 'table'", ())
                        .await?;
                    while let Some(row) = rows.next().await? {
                        tables.push(row.text(0)?);
                    }
                    drop(rows);

                    let mut manager_fks = 0i64;
                    let mut jobs_to_projects = 0i64;
                    for table in &tables {
                        let q = format!("PRAGMA foreign_key_list('{table}')");
                        let mut rows = conn.query(&q, ()).await?;
                        while let Some(row) = rows.next().await? {
                            // columns: id, seq, table, from, to, ...
                            let target = row.text(2)?;
                            if target.starts_with("manager") {
                                manager_fks += 1;
                            }
                            if table == "jobs" && target == "projects" {
                                jobs_to_projects += 1;
                            }
                        }
                    }
                    Ok((manager_fks, jobs_to_projects))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            manager_fks, 0,
            "no FK should still point at a manager table"
        );
        assert!(
            jobs_to_projects >= 1,
            "jobs must retain its non-manager FK to projects"
        );

        // Search triggers survived the rebuild: inserting a fresh issue enqueues
        // a search_outbox row.
        let before = query_i64(&db, "SELECT COUNT(*) FROM search_outbox")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO issues(id,project_id,number,title,description,created_at,updated_at) VALUES('i2','p',2,'Another','Index me too',2,2)",
            (),
        )
        .await
        .unwrap();
        let after = query_i64(&db, "SELECT COUNT(*) FROM search_outbox")
            .await
            .unwrap();
        assert_eq!(after, before + 1, "issues search trigger must still fire");
    }

    #[tokio::test]
    async fn migrates_issue_backed_threads_to_first_class_ownership() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("thread-cutover.turso.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version != "0157")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','default','Cairn','CAIRN','/tmp/cairn',1,1);
            INSERT INTO issues(id,project_id,number,title,status,kind,created_at,updated_at)
              VALUES('thread','p',3404,'General','active','thread',2,3);
            INSERT INTO issues(id,project_id,number,title,status,kind,created_at,updated_at)
              VALUES('fake','p',3443,'Settings','closed','issue',2,3);
            INSERT INTO issues(id,project_id,number,title,parent_issue_id,kind,created_at,updated_at)
              VALUES('child','p',4000,'Child','thread','issue',2,3);
            INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
              VALUES('exec','build','thread','p','running',4,1);
            INSERT INTO jobs(id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at)
              VALUES('job','exec','thread','p','builder','running',4,4);
            INSERT INTO runs(id,issue_id,project_id,job_id,status,created_at,updated_at)
              VALUES('run','thread','p','job','running',4,4);
            INSERT INTO messages(id,channel_type,channel_id,sender_name,content,created_at)
              VALUES('message','issue','CAIRN/3404','user','hello',5);
            INSERT INTO comments(id,issue_id,content,source,created_at)
              VALUES('comment','thread','note','user',5);
            INSERT INTO labels(id,workspace_id,name,color,created_at,updated_at)
              VALUES('label','default','thread','#000000',5,5);
            INSERT INTO issue_labels(issue_id,label_id,created_at) VALUES('thread','label',5);
            INSERT INTO issue_workspaces(issue_id,execution_id,surface,layout_json,schema_version,updated_at,revision)
              VALUES('thread','exec','desktop','{}',1,5,1);
            ",
        )
        .await
        .unwrap();

        MigrationRunner::new(vec![shared_tail_migrate_issue_threads!()])
            .run(&db)
            .await
            .unwrap();

        assert_eq!(
            query_text(&db, "SELECT name FROM threads WHERE id='thread'")
                .await
                .unwrap(),
            "general"
        );
        assert_eq!(
            query_text(&db, "SELECT status FROM threads WHERE id='fake'")
                .await
                .unwrap(),
            "closed"
        );
        assert_eq!(
            query_text(&db, "SELECT thread_id FROM jobs WHERE id='job'")
                .await
                .unwrap(),
            "thread"
        );
        assert_eq!(
            query_text(&db, "SELECT channel_id FROM messages WHERE id='message'")
                .await
                .unwrap(),
            "thread"
        );
        assert_eq!(query_i64(&db, "SELECT COUNT(*) FROM jobs WHERE id='job' AND issue_id IS NULL AND execution_id IS NULL").await.unwrap(), 1);
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM runs WHERE id='run' AND issue_id IS NULL"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_text(&db, "SELECT channel_type FROM messages WHERE id='message'")
                .await
                .unwrap(),
            "thread"
        );
        assert_eq!(
            query_text(&db, "SELECT thread_id FROM comments WHERE id='comment'")
                .await
                .unwrap(),
            "thread"
        );
        assert_eq!(
            query_text(&db, "SELECT parent_thread_id FROM issues WHERE id='child'")
                .await
                .unwrap(),
            "thread"
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM executions WHERE id='exec'")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM issues WHERE id IN ('thread','fake')"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM issue_labels WHERE issue_id='thread'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('issues') WHERE name='kind'"
            )
            .await
            .unwrap(),
            0
        );
    }

    /// The backfill is what makes an already-spawned thread task openable: the
    /// thread pane can only see jobs whose `thread_id` matches, so a child that
    /// was never stamped stays orphaned forever without it. A grandchild proves
    /// the walk is recursive rather than one generation deep, and the issue job
    /// proves ownership is inherited, not invented — a child of a job that
    /// belongs to no thread must keep belonging to no thread.
    #[tokio::test]
    async fn backfills_thread_ownership_onto_children_of_thread_jobs() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("thread-child-backfill.turso.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version != "0167")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','default','Cairn','CAIRN','/tmp/cairn',1,1);
            INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
              VALUES('t','p','general','active','none',1,1);
            INSERT INTO jobs(id, thread_id, project_id, status, uri_segment, node_name, created_at, updated_at)
              VALUES('session','t','p','idle','thread','Thread',1,1);
            INSERT INTO jobs(id, parent_job_id, project_id, status, uri_segment, node_name, created_at, updated_at)
              VALUES('task','session','p','complete','survey','Survey',2,2);
            INSERT INTO jobs(id, parent_job_id, project_id, status, uri_segment, node_name, created_at, updated_at)
              VALUES('subtask','task','p','complete','probe','Probe',3,3);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
              VALUES('i','p',1,'Work','active',1,1);
            INSERT INTO jobs(id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
              VALUES('builder','i','p','running','builder','Builder',1,1);
            INSERT INTO jobs(id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
              VALUES('issue-task','builder','i','p','complete','explore','Explore',2,2);
            ",
        )
        .await
        .unwrap();

        MigrationRunner::new(vec![shared_tail_inherit_thread_id_for_child_jobs!()])
            .run(&db)
            .await
            .unwrap();

        assert_eq!(
            query_text(&db, "SELECT thread_id FROM jobs WHERE id='task'")
                .await
                .unwrap(),
            "t",
            "a task spawned by a thread's session belongs to that thread"
        );
        assert_eq!(
            query_text(&db, "SELECT thread_id FROM jobs WHERE id='subtask'")
                .await
                .unwrap(),
            "t",
            "ownership carries down every generation, not just the first"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM jobs WHERE thread_id IS NOT NULL AND id IN ('builder','issue-task')"
            )
            .await
            .unwrap(),
            0,
            "an issue execution's jobs belong to no thread"
        );
    }

    // ── Team lineage (TEAM_MIGRATIONS) ──────────────────────────────────

    /// Reads `sqlite_master` rows of one object kind into a name→DDL map.
    async fn schema_objects(
        db: &LocalDb,
        kind: &'static str,
    ) -> std::collections::BTreeMap<String, String> {
        db.read(|conn| {
            Box::pin(async move {
                let mut map = std::collections::BTreeMap::new();
                let mut rows = conn
                    .query(
                        "SELECT name, sql FROM sqlite_master WHERE type = ?1 AND sql IS NOT NULL",
                        (kind,),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    map.insert(row.text(0)?, row.text(1)?);
                }
                Ok(map)
            })
        })
        .await
        .unwrap()
    }

    /// Canonicalizes DDL for cross-lineage comparison. Turso's `sqlite_master`
    /// re-rendering is not idempotent for the trailing `FOREIGN KEY (...)
    /// REFERENCES x(id)` form (it inserts a space before `(id)`) and collapses
    /// trigger-body newlines, so byte-equality requires normalizing whitespace
    /// and the space-before-paren. Both differences are purely cosmetic.
    fn norm(sql: &str) -> String {
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace(" (", "(")
    }

    async fn migrated_team_db() -> (tempfile::TempDir, LocalDb) {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("team.turso.db"))
            .await
            .unwrap();
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        (temp, db)
    }

    #[tokio::test]
    async fn team_migrations_apply_in_order() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("team.turso.db"))
            .await
            .unwrap();
        let applied = MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied,
            vec![
                "0001_team_initial_schema".to_string(),
                "0002_labels_read_completeness".to_string(),
                // CAIRN-2629: team-only device_presence table (head migration).
                "0003_device_presence".to_string(),
                "0004_remote_intents".to_string(),
                // Non-secret fleet advertisements are team-visible; enrollment
                // credentials and grant consumption remain private.
                "0005_executor_registry".to_string(),
                "0006_elastic_executor_inventory".to_string(),
                // Shared-tail migrations land in the team lineage after the team
                // head, preserving one shared SQL source for project-scoped tables.
                "0084_archival_pack_hash".to_string(),
                // CAIRN-2270: the token_rollup hour re-grain is a shared-table
                // change, so it lands in the team lineage too.
                "0087_token_rollup_hourly".to_string(),
                "0089_check_result_cache".to_string(),
                "0090_check_result_cache_input_hash".to_string(),
                "0092_tool_invocation_durations".to_string(),
                "0093_check_result_cache_job_id".to_string(),
                "0095_relink_merge_request_jobs".to_string(),
                "0096_jobs_child_base".to_string(),
                "0097_jobs_owns_ephemeral_worktree".to_string(),
                "0098_call_output_contract_and_run_tags".to_string(),
                "0101_workflow_progress".to_string(),
                "0102_check_result_cache_failure_kind".to_string(),
                // CAIRN-2629: executions.runner_device_id is a shared column.
                "0103_executions_runner_device_id".to_string(),
                "0104_clear_invalid_job_worktree_paths".to_string(),
                "0105_add_turn_end_reason".to_string(),
                "0106_index_hot_gui_status_queries".to_string(),
                "0109_check_result_cache_provenance".to_string(),
                "0112_pack_catalog".to_string(),
                "0113_bind_agent_terminals_to_lifetime_leases".to_string(),
                "0114_add_jj_reconcile_intents".to_string(),
                "0115_add_agent_waits".to_string(),
                "0116_add_jj_reconcile_quarantines".to_string(),
                "0118_virtual_reconcile_coordinates".to_string(),
                "0121_check_result_cache_recency_index".to_string(),
                "0122_session_account".to_string(),
                "0123_job_repls".to_string(),
                "0124_repl_exchanges".to_string(),
                "0125_rebind_terminals_to_residencies".to_string(),
                "0126_agent_waits_concurrent_calls".to_string(),
                "0127_image_refs".to_string(),
                "0129_mcp_continuation_prompts".to_string(),
                "0130_retire_snapshot_child_wakes".to_string(),
                "0131_unique_event_sequence".to_string(),
                "0132_check_result_cache_infra_suppression".to_string(),
                "0134_check_result_observations".to_string(),
                "0135_check_definition_provenance".to_string(),
                "0136_conflict_resolution_sessions".to_string(),
                "0138_issue_kind".to_string(),
                "0145_pr_resolution_attribution".to_string(),
                "0146_add_check_observation_public_handle".to_string(),
                "0154_repair_check_observation_public_handle".to_string(),
                "0155_verdict_reuse_facts".to_string(),
                "0150_rebase_replay_status".to_string(),
                "0156_threads_entity".to_string(),
                "0157_migrate_issue_threads".to_string(),
                "0160_thread_title_retires".to_string(),
                "0163_turns_created_at_index".to_string(),
                "0165_check_result_cache_ran_at_millis".to_string(),
                "0167_inherit_thread_id_for_child_jobs".to_string(),
            ]
        );
        // The team lineage is rooted at `teams`, not the private `workspaces`.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='teams'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='workspaces'"
            )
            .await
            .unwrap(),
            0
        );
        // Re-running is idempotent (tracked in cairn_schema_migrations).
        let again = MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        assert!(again.is_empty(), "team migrations must be idempotent");
    }

    /// The anti-drift guarantee: every shared table, index, and trigger in the
    /// team lineage is byte-identical (after `norm`) to the private lineage,
    /// except the four intentional re-rootings, whose expected team DDL is
    /// DERIVED from the private DDL by exactly the documented transforms. If a
    /// future shared-table change lands in one lineage but not the other, this
    /// fails. (`teams` is the team-only root and has no private counterpart.)
    #[tokio::test]
    async fn team_schema_matches_private() {
        let priv_temp = tempdir().unwrap();
        let priv_db = LocalDb::open(priv_temp.path().join("private.turso.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&priv_db)
            .await
            .unwrap();
        let (_team_temp, team_db) = migrated_team_db().await;

        let priv_tables = schema_objects(&priv_db, "table").await;
        let team_tables = schema_objects(&team_db, "table").await;

        let rerooted = [
            "projects",
            "action_configs",
            "skill_configs",
            "issue_labels",
        ];
        for table in rerooted {
            let p = norm(&priv_tables[table]);
            let expected = match table {
                "projects" => p
                    .replace("workspace_id", "team_id")
                    .replace("REFERENCES workspaces(id)", "REFERENCES teams(id)"),
                // action_configs is db-backed config CRUD'd by ONE shared query
                // layer (action_configs::queries) that names workspace_id
                // unconditionally. So the team projection drops ONLY the
                // workspaces FK (workspaces is private-scoped) and KEEPS the
                // nullable workspace_id column + CHECK; a team row is always
                // project-anchored (workspace_id NULL), enforced by the CHECK.
                "action_configs" => p.replace(" REFERENCES workspaces(id) ON DELETE CASCADE", ""),
                // skill_configs has no db-backed CRUD path against a replica, so
                // its team projection fully drops the workspace arm.
                "skill_configs" => p
                    .replace(
                        "workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE, ",
                        "",
                    )
                    .replace(
                        "project_id TEXT REFERENCES projects(id)",
                        "project_id TEXT NOT NULL REFERENCES projects(id)",
                    )
                    .replace(", CHECK((workspace_id IS NULL) !=(project_id IS NULL))", ""),
                "issue_labels" => p.replace(
                    "label_id TEXT NOT NULL REFERENCES labels(id) ON DELETE CASCADE",
                    "label_id TEXT NOT NULL",
                ),
                _ => unreachable!(),
            };
            assert_eq!(
                norm(&team_tables[table]),
                expected,
                "re-rooted table `{table}` drifted from its private counterpart"
            );
        }

        // These fleet/remote-delivery tables are team-only: they have no private
        // counterpart, so they are skipped here exactly as `teams` is and added
        // explicitly to the expected team projection below.
        for (name, sql) in &team_tables {
            if matches!(
                name.as_str(),
                "teams" | "device_presence" | "remote_intents" | "executor_registry"
            ) || rerooted.contains(&name.as_str())
            {
                continue;
            }
            let p = priv_tables.get(name).unwrap_or_else(|| {
                panic!("team table `{name}` is missing from the private lineage")
            });
            assert_eq!(
                norm(sql),
                norm(p),
                "shared table `{name}` drifted between the team and private lineages"
            );
        }

        for kind in ["index", "trigger", "view"] {
            let priv_objs = schema_objects(&priv_db, kind).await;
            let team_objs = schema_objects(&team_db, kind).await;
            for (name, sql) in &team_objs {
                if matches!(
                    name.as_str(),
                    "idx_remote_intents_pending" | "idx_remote_intents_execution"
                ) {
                    continue;
                }
                let p = priv_objs.get(name).unwrap_or_else(|| {
                    panic!("team {kind} `{name}` is missing from the private lineage")
                });
                assert_eq!(
                    norm(sql),
                    norm(p),
                    "{kind} `{name}` drifted between lineages"
                );
            }
        }

        // ── The team schema is the PROJECTION of TABLE_SCOPES (CAIRN-2210) ─────
        //
        // The hand-curated CAIRN-2186 allowlist is gone. Scope is declared once,
        // in `TABLE_SCOPES`; the team lineage is exactly the projection of the
        // tables that classify into it. These assertions prove that projection,
        // which subsumes the old reverse-completeness guard (a private table the
        // team lineage lacks now surfaces as a projection mismatch).

        // 1. Exhaustiveness + no duplicate / stale entries. Every table the
        //    private lineage creates is classified exactly once, and every
        //    classified name is a real private table.
        // Infrastructure tables exist in EVERY database regardless of scope: the
        // Turso MVCC bookkeeping table and the migration ledger itself. `teams`
        // is special too — it exists in BOTH lineages with divergent schema (the
        // private routing registry from 0082 vs the team-only FK root), so it is
        // excluded from classification and handled explicitly, exactly as the
        // DDL loops above skip it.
        const SCHEMA_INFRA: &[&str] = &["__turso_internal_mvcc_meta", "cairn_schema_migrations"];
        let is_classifiable = |name: &str| !SCHEMA_INFRA.contains(&name) && name != "teams";

        let mut scope_map: std::collections::BTreeMap<&'static str, TableScope> =
            std::collections::BTreeMap::new();
        for (name, scope) in TABLE_SCOPES {
            assert!(
                scope_map.insert(name, *scope).is_none(),
                "TABLE_SCOPES has a duplicate entry for `{name}`"
            );
        }
        let mut unclassified: Vec<&str> = priv_tables
            .keys()
            .map(String::as_str)
            .filter(|name| is_classifiable(name) && !scope_map.contains_key(name))
            .collect();
        unclassified.sort_unstable();
        assert!(
            unclassified.is_empty(),
            "private table(s) missing a TABLE_SCOPES classification (scope must be \
             declared once per table): {unclassified:?}"
        );
        let mut stale: Vec<&str> = scope_map
            .keys()
            .copied()
            .filter(|name| !priv_tables.contains_key(*name))
            .collect();
        stale.sort_unstable();
        assert!(
            stale.is_empty(),
            "TABLE_SCOPES classifies table(s) the private lineage does not create \
             (stale entries): {stale:?}"
        );

        // 2. Projection. The team lineage's table set is EXACTLY the tables that
        //    classify into it — every ProjectScoped table, every SharedContent
        //    table located in the team lineage — plus the team-only `teams` root.
        let mut expected_team: std::collections::BTreeSet<&str> = scope_map
            .iter()
            .filter(|(_, scope)| scope.lives_in_team())
            .map(|(name, _)| *name)
            .collect();
        expected_team.insert("teams"); // present in both; classified specially
        expected_team.insert("device_presence"); // team-only (CAIRN-2629)
        expected_team.insert("remote_intents"); // team-only remote delivery inbox
        expected_team.insert("executor_registry"); // team-only non-secret fleet advertisements
        let actual_team: std::collections::BTreeSet<&str> = team_tables
            .keys()
            .map(String::as_str)
            .filter(|name| !SCHEMA_INFRA.contains(name))
            .collect();
        assert_eq!(
            expected_team, actual_team,
            "the team schema is not the projection of TABLE_SCOPES (left = expected \
             from the declarations, right = the actual team lineage). A table the \
             team lineage lacks but TABLE_SCOPES places in-team is the schema-\
             completeness gap the old allowlist guarded; an extra table is an \
             unclassified team-only table."
        );

        // 3. The complement falls out of the projection: every private table NOT
        //    in the team lineage is exactly a Private table or a SharedContent
        //    table located in private — no hand-curated list to keep in sync.
        let mut private_only_actual: Vec<&str> = priv_tables
            .keys()
            .map(String::as_str)
            .filter(|name| is_classifiable(name) && !team_tables.contains_key(*name))
            .collect();
        private_only_actual.sort_unstable();
        let mut private_only_expected: Vec<&str> = scope_map
            .iter()
            .filter(|(_, scope)| !scope.lives_in_team())
            .map(|(name, _)| *name)
            .collect();
        private_only_expected.sort_unstable();
        assert_eq!(
            private_only_actual, private_only_expected,
            "private-only tables diverge from their TABLE_SCOPES classification"
        );

        // 4. DeferredShared validity: every deferred-sharing exception names a
        //    real tracking issue and a concrete target scope, so it stays an
        //    owned, documented decision rather than an anonymous allowlist line.
        for (name, scope) in TABLE_SCOPES {
            if let TableScope::Private(PrivateReason::DeferredShared { issue, target }) = scope {
                assert!(
                    issue.starts_with("CAIRN-"),
                    "DeferredShared table `{name}` must name a CAIRN issue, got {issue:?}"
                );
                // `target` is a closed enum; its presence is the contract.
                let _ = target;
            }
        }
    }
}
