//! The two Turso migration lineages and everything derived from them.
//!
//! A migration's identity is written exactly once, as the name of its SQL file.
//! `migration!` and `team_migration!` below turn that one filename stem into the
//! registered version, the registered name, and the `include_str!` path, so the
//! three can never disagree.
//!
//! Everything composed in this file is frozen history, up to and including
//! migration [`CUTOVER`](super::migration_files::CUTOVER). Every later migration
//! is a file in `turso_migrations/` and nothing else: `build.rs` derives the tail
//! each lineage appends from the directory and the file's `-- lineage:` header
//! (see `storage/migration_files.rs`). Nobody edits the composers, the per-
//! migration macros, or the lineage pins below again; a new migration is
//! authored as `NEXT_name.sql` and numbered by the landing.

// The derived tail: `with_private_tail!` and `with_team_tail!`, which close each
// lineage's array, and the stems they append (`DERIVED_PRIVATE_TAIL`,
// `DERIVED_TEAM_TAIL`).
include!(concat!(env!("OUT_DIR"), "/migration_tail.rs"));

/// Registers a private-lineage migration (`turso_migrations/`) from its SQL
/// filename stem.
///
/// The stem — `0205_wake_schedules` — is the whole identity: the version, the
/// name recorded in `cairn_schema_migrations`, and the embedded SQL all come
/// from it. A stem that is not `NNNN_name`, or that names no file, fails the
/// build rather than registering a migration nobody wrote.
///
/// The `rebuild_fk_off:` form registers a table-rebuilding migration and states
/// what the rebuild must preserve; see `Migration::rebuild_fk_off`.
macro_rules! migration {
    ($stem:literal) => {
        Migration::new(
            version_from_stem($stem),
            name_from_stem($stem),
            include_str!(concat!("../../../../turso_migrations/", $stem, ".sql")),
        )
    };
    ($stem:literal, rebuild_fk_off: $verify:expr $(,)?) => {
        Migration::rebuild_fk_off(
            version_from_stem($stem),
            name_from_stem($stem),
            include_str!(concat!("../../../../turso_migrations/", $stem, ".sql")),
            $verify,
        )
    };
}

/// cairn/4898: empties the duration and contention windows learned before the
/// recorder stopped counting failed and empty-selection runs. Both tables are
/// machine-local learning, so this is private like they are.
macro_rules! private_purge_poisoned_duration_profiles {
    () => {
        migration!("0242_purge_poisoned_duration_profiles")
    };
}

/// cairn/5066: how each recent completion of a command ended, per machine, so
/// placement can price a machine by its own same-signature failures. Machine-
/// local observation like the duration profiles, so private like they are.
macro_rules! private_command_outcome_history {
    () => {
        migration!("0265_command_outcome_history")
    };
}

/// cairn/5099: the cairn-inference facet's profiles (then called recipes),
/// deployments and pools, and
/// the bginf link's last read (dropped since, when bginf retired). The orch's
/// own inventory of this installation's machines, so private.
macro_rules! private_inference_records {
    () => {
        migration!("0268_inference_records")
    };
}

/// cairn/4910: the canonical local record of ingress endpoint events, which
/// the fact log's `ingress` rows point at; this install's mirror of the
/// endpoints it minted; the relay skip counter. Replaces the writer-less
/// `webhook_events`. Private: endpoints belong to this install's relay channel.
macro_rules! private_ingress_events {
    () => {
        migration!("0270_ingress_events")
    };
}

/// cairn/5120: which subscription plan each provider account was on, and when,
/// so effective cost can price a past month after an account is removed.
/// Private like the credential history it complements.
macro_rules! private_subscription_periods {
    () => {
        migration!("0273_subscription_periods")
    };
}

/// Machines that could not prepare a project's cells because a setup command
/// was not found there. Describes this installation's own fleet only.
macro_rules! private_executor_preparation_refusals {
    () => {
        migration!("0246_executor_preparation_refusals")
    };
}

/// cairn/4817: the provider sign-in lifecycle journal. Private like the
/// authorization journal it sits beside: an install's credential history is
/// its own.
macro_rules! private_account_credential_events {
    () => {
        migration!("0249_account_credential_events")
    };
}

/// cairn/4978: a thread compaction trigger for a session whose first launch
/// never confirmed. Rebuilds the private-only `thread_compactions` table.
macro_rules! private_thread_compaction_unconfirmed_start_trigger {
    () => {
        migration!("0251_thread_compaction_unconfirmed_start_trigger")
    };
}

/// cairn/4975: the measured size of each project's shared ref store and the
/// operator's per-store prune choice. A ref store is a file on this machine, so
/// both are private.
macro_rules! private_ref_store_maintenance {
    () => {
        migration!("0253_ref_store_maintenance")
    };
}

/// cairn/5194: the runner's own lifecycle facts (runtime stalls, executor
/// generations attaching and detaching, link resets, listener restages) as
/// rows. Facts about this runner process and this machine's links, so private.
macro_rules! private_runner_events {
    () => {
        migration!("0274_runner_events")
    };
}

/// cairn/5085: the shape of each store's Git object database beside its refs,
/// and the `compact` rows of its geometric compaction. Rebuilds the
/// private-only `ref_store_samples` to widen its `kind` CHECK.
macro_rules! private_object_store_maintenance {
    () => {
        migration!(
            "0269_object_store_maintenance",
            rebuild_fk_off: &[RebuildCheck::Conserved("ref_store_samples")]
        )
    };
}

/// Immutable ordinary-session launch inputs waiting in runner-local admission.
macro_rules! private_prepared_ordinary_launches {
    () => {
        migration!("0241_prepared_ordinary_launches")
    };
}

/// cairn/5205: whether a prepared ordinary launch serves an operator or
/// background work, the fact its admission priority is derived from.
macro_rules! private_prepared_ordinary_launch_origin {
    () => {
        migration!("0278_prepared_ordinary_launch_origin")
    };
}

/// cairn/5286: rebuilds the execution-queue journal so its decline-reason CHECK
/// admits `build_volume_absent`. Private for the same reason as the table.
macro_rules! private_execution_queue_build_volume_absent {
    () => {
        migration!("0280_execution_queue_build_volume_absent")
    };
}

/// cairn/4482: purpose items granted to a machine and not yet confirmed dead,
/// so a runner restart cannot forget work a remote machine is still running.
/// Describes this installation's own fleet only.
macro_rules! private_purpose_debts {
    () => {
        migration!("0282_purpose_debts")
    };
}

/// cairn/5379: the embedding model is a setting, so the space stored vectors
/// share is recorded (`embedding_state`), and session positions move to a
/// private table keyed by it. Private like every embedding store.
macro_rules! private_embedding_space {
    () => {
        migration!("0285_embedding_space")
    };
}

/// cairn/5416: an out-of-memory kill of a profiled command records the
/// victim's resident size as a demand floor on that machine context. Private
/// with the rest of `command_resource_profiles`.
macro_rules! private_command_resource_memory_kills {
    () => {
        migration!("0287_command_resource_memory_kills")
    };
}

/// Runner-local recovery journal for the exact remote authority generation held
/// by a team ordinary process and any terminal action still owed to Postgres.
macro_rules! private_team_ordinary_admission_authority {
    () => {
        migration!("0240_team_ordinary_admission_authority")
    };
}

/// Pinning is optional decoration for runner-local channel state cards. Its
/// attempt and error state must not share the card delivery retry budget.
macro_rules! private_state_card_pin_attempts {
    () => {
        migration!("0231_state_card_pin_attempts")
    };
}

/// Recovery floors for contended counters are runner-local jj state. A store
/// path names this installation's store, so these rows never enter team sync.
macro_rules! private_contended_identifier_obligations {
    () => {
        migration!("0228_contended_identifier_obligations")
    };
}

/// CAIRN-4665: canonical execution lineage. `executions.lifecycle` /
/// `fence_generation`, `runs.execution_fence_generation`, and the
/// `execution_supersessions` audit all describe project-scoped executions that
/// a team owns, so this reaches BOTH lineages.
macro_rules! shared_tail_execution_canonical_lineage {
    () => {
        migration!("0230_execution_canonical_lineage")
    };
}

/// CAIRN-4767: a monotonic generation for an issue's resolution, so a resolved
/// attention notice is keyed to the resolution rather than to a whole-second
/// clock. `issues` is shared project state carried by both lineages, so the
/// column is written once and composed into both.
macro_rules! shared_tail_issue_resolution_generation {
    () => {
        migration!("0232_issue_resolution_generation")
    };
}

/// A condition evaluation records whether it routed or blocked, so a condition
/// that routes nowhere leaves an honest row instead of nothing.
/// `condition_evaluations` is project-scoped shared state carried by both
/// lineages, so the column is written once and composed into both.
macro_rules! shared_tail_condition_evaluation_outcome {
    () => {
        migration!("0239_condition_evaluation_outcome")
    };
}

/// cairn/4977: a terminal settled because its process was proven gone records
/// why, since its exit code cannot. `job_terminals` is carried by both lineages,
/// so the column is written once and composed into both.
macro_rules! shared_tail_terminal_exit_reason {
    () => {
        migration!("0252_terminal_exit_reason")
    };
}

// Holds describe unfinished project work, not a runner-local timer.
macro_rules! shared_tail_provider_limit_holds {
    () => {
        migration!("0255_provider_limit_holds")
    };
}

/// cairn/4959: `runs.account_id` records the account a run launched on, and a
/// hold may withhold automatic resume or wait on another account's reset. Both
/// tables are shared project state, so the columns reach both lineages.
macro_rules! shared_tail_provider_limit_failover {
    () => {
        migration!("0260_provider_limit_failover")
    };
}

/// cairn/4936: an answered permission whose job could not be continued records
/// why on the request row, so the refusal is visible where the answer is.
/// `permission_requests` is project-scoped shared state carried by both
/// lineages, so the column is written once and composed into both.
macro_rules! shared_tail_permission_resume_error {
    () => {
        migration!("0250_permission_resume_error")
    };
}

/// cairn/4735: `after:` gates on an execution. `execution_gates` describes
/// ProjectScoped executions and must be readable wherever they live, and the
/// two release columns land on `executions` itself, so the whole change is
/// shared and composes into both lineages.
macro_rules! shared_tail_execution_after_gates {
    () => {
        migration!("0256_execution_after_gates")
    };
}

/// cairn/4735: retire `depends_on` into `after:` gates. Each dependency edge on
/// an issue with an execution becomes a satisfied gate on its newest execution;
/// the rest are dropped with the table. `issue_dependencies` exists in both
/// lineages, so the conversion and the drop are shared.
macro_rules! shared_tail_retire_issue_dependencies {
    () => {
        migration!("0257_retire_issue_dependencies")
    };
}

/// cairn/5175: relabel check observations that took the recording
/// coordinator's platform instead of the claiming machine's, and keep an audit
/// row per correction. Observations are shared project state, so both lineages
/// carry the correction and its record.
macro_rules! shared_tail_check_observation_platform_relabels {
    () => {
        migration!("0272_check_observation_platform_relabels")
    };
}

/// Publication history follows its action run in either database lineage.
macro_rules! shared_tail_pr_publish_attempts {
    () => {
        migration!("0266_pr_publish_attempts")
    };
}

/// cairn/5119: the base-advance sibling lookup seeks its jobs and its open-PR
/// probe instead of scanning, on both tables either lineage carries.
macro_rules! shared_tail_sibling_job_lookup_indexes {
    () => {
        migration!("0267_index_sibling_job_lookup")
    };
}

/// cairn/5126: the artifact version each job was launched with, so a later
/// revision reaches the job as a diff. A consumer job lives wherever its
/// execution does, so the record is carried by both lineages.
macro_rules! shared_tail_job_artifact_inputs {
    () => {
        migration!("0275_job_artifact_inputs")
    };
}

/// cairn/5141: the fast tier's deferred per-test outcome, added beside the
/// CHECK-constrained `check_test_results` rather than by rebuilding it, plus the
/// observation's tier and deferred tally. Additive on tables both lineages carry.
macro_rules! shared_tail_check_deferred_tests {
    () => {
        migration!("0276_check_deferred_tests")
    };
}

/// cairn/5206: the gate census seeks an execution's gates and its supersession
/// record by execution id instead of scanning, on tables both lineages carry.
macro_rules! shared_tail_execution_gates_execution_index {
    () => {
        migration!("0277_index_execution_gates_execution")
    };
}

/// cairn/5304: drop the `check_test_outcomes` view, which Turso could answer
/// only by materializing every test row; readers filter the union per arm.
macro_rules! shared_tail_drop_check_test_outcomes_view {
    () => {
        migration!("0279_drop_check_test_outcomes_view")
    };
}

/// cairn/5337: the operator's durable on/off choice for a job's native provider
/// goal. `jobs` is carried by both lineages, so the column is written once and
/// composed into both.
macro_rules! shared_tail_job_goal_intent {
    () => {
        migration!("0281_job_goal_intent")
    };
}

/// cairn/5224, cairn/5236: the compile fact, one row per attempt to compile a
/// tree. Beside the check results it is read from, so in both lineages.
macro_rules! shared_tail_compile_facts {
    () => {
        migration!("0283_compile_facts")
    };
}

/// cairn/5282: a review wave's plan kept on its attempt row, so a node whose
/// branch has retired still answers `/checks` from the head its lanes were
/// recorded for. `turn_end_check_attempts` is carried by both lineages.
macro_rules! shared_tail_turn_end_check_attempt_plan {
    () => {
        migration!("0284_turn_end_check_attempt_plan")
    };
}

/// cairn/5433: a tenet's reasoning lives in `rationale`, beside the
/// one-sentence statement the operator answers. `tenets` is carried by both
/// lineages.
macro_rules! shared_tail_tenet_rationale {
    () => {
        migration!("0286_tenet_rationale")
    };
}

/// cairn/5396: per-run minutes with model output, the measure behind
/// analytics' agent time. Maintained by a trigger on `events`, which both
/// lineages carry.
macro_rules! shared_tail_run_activity_minutes {
    () => {
        migration!("0288_run_activity_minutes")
    };
}

/// cairn/4987: the base and incoming side a conflict session's committed
/// resolution was resolved against, and the tip and destination of an
/// outstanding take-committed-tip request. `jj_reconcile_items` is carried by
/// both lineages, so the columns are written once and composed into both.
macro_rules! shared_tail_reconcile_resolution_basis {
    () => {
        migration!("0258_reconcile_resolution_basis")
    };
}

/// cairn/4986: routed batch deltas that reached this machine's project store
/// but not their branch, held for republication at the next commit barrier.
/// Private: each row names store paths on this machine.
macro_rules! private_held_publications {
    () => {
        migration!("0259_held_publications")
    };
}

/// cairn/5028: tenets, the human checkpoint for threads, plus the per-project
/// numbering counter and operator-only release policy columns on `projects`.
/// Tenets are ProjectScoped and `projects` is shared, so the whole change lands
/// in both lineages.
macro_rules! shared_tail_tenets {
    () => {
        migration!("0261_tenets")
    };
}

/// cairn/5030: the per-thread ledger of the boot-time import that turned each
/// thread's rulings into migrated tenets. It shares a database with `tenets`
/// so a thread's ledger row commits atomically with the tenets it describes,
/// which puts it in both lineages.
macro_rules! shared_tail_tenet_ruling_imports {
    () => {
        migration!("0262_tenet_ruling_imports")
    };
}

/// cairn/5032: tenet evidence. Concluded work (an issue resolution, a thread
/// era) becomes an evaluation event, and the judged verdicts become evidence
/// rows on the tenets they bear on. Evidence references `tenets`, so both
/// tables are ProjectScoped and land in both lineages.
macro_rules! shared_tail_tenet_evidence {
    () => {
        migration!("0263_tenet_evidence")
    };
}

/// cairn/5064: tenet evidence is judged through `decide`, and each evidence
/// row records how its probability was measured. It alters a ProjectScoped
/// table, so it lands in both lineages.
macro_rules! shared_tail_tenet_evidence_measured_by {
    () => {
        migration!("0264_tenet_evidence_measured_by")
    };
}

/// cairn/5113: a tenet is a yes/no statement. The verdict CHECK changes, so
/// `tenets` is rebuilt; `tenet_evidence` references it, hence foreign keys off.
macro_rules! shared_tail_tenet_statements {
    () => {
        migration!(
            "0271_tenet_statements",
            rebuild_fk_off: &[
                RebuildCheck::Conserved("tenets"),
                RebuildCheck::Conserved("tenet_evidence"),
            ]
        )
    };
}

/// How each execution bound to a machine. Runner-transient state keyed by a
/// runner incarnation, like the workflow journal, so it never syncs to a team
/// replica.
macro_rules! private_execution_queue {
    () => {
        migration!("0235_execution_queue")
    };
}

/// CAIRN-4829: the account a response was served on, journaled with it.
/// `response_invocations` is private, so this is too.
macro_rules! private_response_invocation_account {
    () => {
        migration!("0236_response_invocation_account")
    };
}

/// Rebuilds the execution-queue journal so its decline-reason CHECK admits
/// `project_provisioning`. Private for the same reason as the table.
macro_rules! private_execution_queue_project_provisioning {
    () => {
        migration!("0237_execution_queue_project_provisioning")
    };
}

/// Rebuilds `channel_outbound` so its kind CHECK admits agent-authored
/// `message` rows, and grants the agent-text binding class to existing Discord
/// structural bindings. Both tables are runner-local channel state.
macro_rules! private_channel_agent_messages {
    () => {
        migration!("0238_channel_agent_messages")
    };
}

/// CAIRN-4885: the durable fact log, the runner's route-dispatch queue and the
/// index behind `cairn://p/{project}/events`. Private like `route_firings`,
/// which it extends with the `fact_log_id` join column. Team replication of
/// the log is a deliberate follow-on.
macro_rules! private_fact_log {
    () => {
        migration!("0243_fact_log")
    };
}

/// The route Playground samples the fact log now, so the private
/// `route_fact_samples` table 0169 created is dropped.
macro_rules! private_drop_route_fact_samples {
    () => {
        migration!("0244_drop_route_fact_samples")
    };
}

/// CAIRN-4911: stamp each provider attempt on the runner-local outbound queue,
/// so channel status can tell a current failure from one already sent past.
macro_rules! private_channel_outbound_last_attempt {
    () => {
        migration!("0245_channel_outbound_last_attempt")
    };
}

/// CAIRN-4912: pending and delivered coalesced thread wakes, one per
/// (thread job, source) settle window. Private like the fact log it batches.
macro_rules! private_fact_log_wakes {
    () => {
        migration!("0247_fact_log_wakes")
    };
}

/// cairn/4913: a Posts watch becomes the `{fact: post_created}` fact clause.
/// `wake_subscriptions` is shared schema, so both lineages rewrite their rows;
/// it is an in-place UPDATE, never a rebuild, because the team replica's sync
/// triggers ride on that table.
macro_rules! shared_tail_post_feed_watch_as_fact_clause {
    () => {
        migration!("0248_post_feed_watch_as_fact_clause")
    };
}

/// cairn/4955: a question an operator Compact withdrew carries when and why.
/// `prompts` is shared schema, so both lineages gain the columns additively.
macro_rules! shared_tail_prompt_withdrawal {
    () => {
        migration!("0254_prompt_withdrawal")
    };
}

macro_rules! shared_tail_merge_attempt_journal {
    () => {
        migration!("0220_merge_attempt_journal")
    };
}

macro_rules! shared_tail_human_base_merge_policy {
    () => {
        migration!("0219_human_base_merge_policy")
    };
}

/// CAIRN-4526: numbered branch entities are shared project state, composed with
/// their owning threads and backing jobs.
macro_rules! shared_tail_thread_branches {
    () => {
        migration!("0221_thread_branches")
    };
}

/// CAIRN-4647: durable composer attachments, the one record table images and
/// files share. Project-scoped shared state for the same reason `image_refs` is
/// -- a teammate opening a synced issue must resolve the same addresses -- so it
/// reaches both lineages.
macro_rules! shared_tail_attachments {
    () => {
        migration!("0227_attachments")
    };
}

/// CAIRN-4710: a monotonic per-write revision on `event_vibes`, because a vibe
/// upsert replaces a color in place and so moves neither `COUNT(*)` nor
/// second-resolution `MAX(created_at)`. Both lineages carry `event_vibes` and
/// the derived `session_skyline_cache` whose watermark column this renames, so
/// the change is shared.
macro_rules! shared_tail_event_vibe_revision {
    () => {
        migration!("0229_event_vibe_revision")
    };
}

/// Supersession bookkeeping for an executor credential is runner-local, like
/// the `executor_enrollments` rows it extends: an enrollment binds one machine
/// to this installation and never enters team sync.
macro_rules! private_executor_enrollment_supersession {
    () => {
        migration!("0234_executor_enrollment_supersession")
    };
}

/// Per-call ownership for write replay completion is runner-local, like the
/// replay ledger it extends.
macro_rules! private_write_replay_ownership {
    () => {
        migration!("0222_write_replay_ownership")
    };
}

/// `jobs` loses the fresh-session flag in place via ALTER TABLE DROP COLUMN, so
/// every row is carried and the sync triggers stay intact. Shared because the
/// column exists in both lineages: the private head added it at 0059 and the team
/// head carries it in its 0001 snapshot of `jobs`.
macro_rules! shared_tail_retire_needs_fresh_session {
    () => {
        migration!(
            "0223_retire_needs_fresh_session",
            rebuild_fk_off: &[RebuildCheck::Conserved("jobs")]
        )
    };
}

/// CAIRN-4613: closes the predecessors rotation left reading `open`. Shared
/// because `sessions` is a shared table, so the stale rows exist in both
/// lineages.
macro_rules! shared_tail_close_replaced_sessions {
    () => {
        migration!("0224_close_replaced_sessions")
    };
}

macro_rules! private_thread_automations {
    () => {
        migration!("0217_thread_automations")
    };
}

/// Durable evidence that an app mention reached the relay but named no local project.
macro_rules! private_repair_codex_watchdog_lifecycle_kind {
    () => {
        migration!("0215_repair_codex_watchdog_lifecycle_kind")
    };
}

macro_rules! private_github_relay_unmapped_mentions {
    () => {
        migration!("0213_github_relay_unmapped_mentions")
    };
}

/// Edge-triggered SQL predicates owned by a durable local thread home.
macro_rules! private_wake_conditions {
    () => {
        migration!("0214_wake_conditions")
    };
}

macro_rules! private_channel_state_card {
    () => {
        migration!("0216_channel_state_card")
    };
}

/// CAIRN-4477: retry state for the runner-local outbound channel queue.
macro_rules! private_channel_outbound_retry {
    () => {
        migration!("0211_channel_outbound_retry")
    };
}

/// CAIRN-4398: serve the per-job newest-check-result projection from one
/// check-partitioned recency index. `check_result_cache` is shared project state,
/// so both database lineages compose this migration.
macro_rules! shared_tail_check_result_cache_job_check_recency_index {
    () => {
        migration!("0209_check_result_cache_job_check_recency_index")
    };
}

/// Registers a team-lineage migration (`turso_migrations_team/`) from its SQL
/// filename stem. Identity is derived exactly as in `migration!`.
macro_rules! team_migration {
    ($stem:literal) => {
        Migration::new(
            version_from_stem($stem),
            name_from_stem($stem),
            include_str!(concat!("../../../../turso_migrations_team/", $stem, ".sql")),
        )
    };
}

macro_rules! private_codex_watchdog_ledger {
    () => {
        migration!("0188_codex_watchdog_ledger")
    };
}

/// Reconstructable launch payloads for ephemeral calls waiting in local runtime admission.
macro_rules! private_prepared_runtime_calls {
    () => {
        migration!("0204_prepared_runtime_calls")
    };
}

/// Anchored recurrence rules owned by a durable local thread home.
macro_rules! private_wake_schedules {
    () => {
        migration!("0205_wake_schedules")
    };
}

/// Persisted era summaries for thread seed composition (CAIRN-4302).
macro_rules! private_thread_eras {
    () => {
        migration!("0207_thread_eras")
    };
}

/// CAIRN-4364: parks a channel domain action whose refusal only an operator can
/// lift, so the cadence sweep stops retrying an answer that cannot change.
macro_rules! private_channel_ask_action_park {
    () => {
        migration!("0208_channel_ask_action_park")
    };
}

/// CAIRN-4225: the per-base-commit code map (source inventory + import graph)
/// the workspace map surface reads. Private only -- it is derived wholly from a
/// commit every replica already has, so replicating the projection would buy
/// nothing and cost a payload per merge.
macro_rules! private_codemap_cache {
    () => {
        migration!("0202_codemap_cache")
    };
}

/// Runner-local admission requests and fairness cursor. A private runner is the
/// only authority for these rows; team admission is coordinated by Postgres.
macro_rules! private_runtime_admission {
    () => {
        migration!("0203_runtime_admission")
    };
}

macro_rules! shared_tail_liveness_join_indexes {
    () => {
        migration!("0195_index_liveness_joins")
    };
}

/// CAIRN-4182: `attention_pushes` gains a retired state, so a push whose
/// referent resolved stops being re-resolved forever. The table is
/// ProjectScoped, so the change is written once and composed into both lineages.
macro_rules! shared_tail_attention_push_retirement {
    () => {
        migration!("0196_attention_push_retirement")
    };
}

/// CAIRN-4182: the one-time retirement backfill's keyset cursor. Runner-transient
/// per-install progress, like `analytics_rollup_backfill_state`, so it is absent
/// from the team lineage.
macro_rules! private_attention_push_retirement_backfill {
    () => {
        migration!("0197_attention_push_retirement_backfill")
    };
}

macro_rules! private_posts {
    () => {
        migration!("0194_posts")
    };
}

macro_rules! private_canonical_project_routes {
    () => {
        migration!("0193_canonical_project_routes")
    };
}

macro_rules! shared_tail_canonical_project_keys {
    () => {
        migration!("0191_canonical_project_keys")
    };
}

macro_rules! private_canonical_discord_surface_project_keys {
    () => {
        migration!("0192_canonical_discord_surface_project_keys")
    };
}

macro_rules! private_canonicalize_channel_permission_answers {
    () => {
        migration!("0190_canonicalize_channel_permission_answers")
    };
}

/// CAIRN-3759: `projects` gains `next_thread_number`, the durable allocator for
/// server-assigned thread addresses, mirroring `next_issue_number`. `projects`
/// is carried by both lineages, so the change is written once and composed into
/// both.
macro_rules! shared_tail_project_thread_number {
    () => {
        migration!("0199_project_thread_number")
    };
}

/// CAIRN-4226: the per-job file-touch stream a live workspace map renders.
/// ProjectScoped like `file_changes` and `tool_invocations` beside it — which
/// files a project's agents touched is collaboration data, not one install's
/// telemetry — so the change is written once and composed into both lineages.
macro_rules! shared_tail_job_file_activity {
    () => {
        migration!("0201_job_file_activity")
    };
}

macro_rules! shared_tail_job_presence {
    () => {
        migration!("0210_job_presence")
    };
}

/// Partial index for the sidebar thread unread scan (CAIRN-4331).
macro_rules! shared_tail_thread_unread_scan_index {
    () => {
        migration!("0206_index_thread_unread_scan")
    };
}

/// CAIRN-4195: the local operator's per-thread transcript read cursor. Private
/// only — a read position is one person's on one install, and replicating it
/// would let a teammate's reading clear this operator's unread badge.
macro_rules! private_thread_read_positions {
    () => {
        migration!("0200_thread_read_positions")
    };
}

macro_rules! shared_tail_thread_status_rollup_index {
    () => {
        migration!("0198_index_thread_status_rollup")
    };
}

macro_rules! private_github_relay_key_rotation_time {
    () => {
        migration!("0189_github_relay_key_rotation_time")
    };
}

macro_rules! private_repair_prompts_column_order_parity {
    () => {
        migration!(
            "0186_repair_prompts_column_order_parity",
            rebuild_fk_off: &[RebuildCheck::Conserved("prompts")]
        )
    };
}

macro_rules! private_ask_resolution_provenance {
    () => {
        migration!("0184_ask_resolution_provenance")
    };
}

macro_rules! private_repair_attention_pushes_index_parity {
    () => {
        migration!("0185_repair_attention_pushes_index_parity")
    };
}

macro_rules! shared_tail_issue_authorship {
    () => {
        migration!("0187_issue_authorship")
    };
}

macro_rules! private_channel_ask_resolution {
    () => {
        migration!("0182_channel_ask_resolution")
    };
}

macro_rules! private_channel_conversation_binding {
    () => {
        migration!("0181_channel_conversation_binding")
    };
}

macro_rules! private_discord_surface {
    () => {
        migration!("0180_discord_surface")
    };
}

macro_rules! private_channel_inbound_rejection_reason {
    () => {
        migration!("0176_channel_inbound_rejection_reason")
    };
}

macro_rules! private_github_relay_pending_key {
    () => {
        migration!("0179_github_relay_pending_key")
    };
}

macro_rules! private_github_relay_delivery_health {
    () => {
        migration!("0178_github_relay_delivery_health")
    };
}

use super::migration::{name_from_stem, version_from_stem, RebuildCheck};
use super::Migration;

macro_rules! shared_tail_tool_analytics_hourly {
    () => {
        migration!("0177_tool_analytics_hourly")
    };
}

macro_rules! private_canonicalize_uri_identity_columns {
    () => {
        migration!("0175_canonicalize_uri_identity_columns")
    };
}

macro_rules! shared_tail_lowercase_project_keys {
    () => {
        migration!("0173_lowercase_project_keys")
    };
}

macro_rules! private_lowercase_project_routes {
    () => {
        migration!("0174_lowercase_project_routes")
    };
}

/// CAIRN-3897: make the two logical families sharing check_result_cache explicit.
macro_rules! shared_tail_check_result_cache_row_family {
    () => {
        migration!("0171_check_result_cache_row_family")
    };
}

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
        migration!("0084_archival_pack_hash")
    };
}

macro_rules! private_build_change_ride_alongs {
    () => {
        migration!("0170_build_change_ride_alongs")
    };
}

macro_rules! private_route_fact_samples {
    () => {
        migration!("0169_route_fact_samples")
    };
}

/// CAIRN-3810: recency ordering for the semantic lane's turn sweep. `turns` is
/// shared project state, so the index is composed once into both lineages.
macro_rules! shared_tail_turns_created_at_index {
    () => {
        migration!("0163_turns_created_at_index")
    };
}

/// Private-only: an authority grant is this operator's decision about this
/// install and must never replicate to a teammate's machine. See the SQL
/// file's header for why that is an authorization property, not a storage one.
macro_rules! private_authority_grants {
    () => {
        migration!("0166_authority_grants")
    };
}

/// CAIRN-3828: disclosure incidents, their affected-record inventory, and the
/// quarantine that withholds an affected record from serving. Private-only: the
/// inventory maps exactly which records carry a credential, and syncing that map
/// would send a disclosure hint further than the disclosure.
macro_rules! private_authority_attribution {
    () => {
        migration!("0183_authority_attribution")
    };
}

macro_rules! private_disclosure_remediation {
    () => {
        migration!("0172_disclosure_remediation")
    };
}

/// CAIRN-3861: thread ownership was never stamped on a delegated child, so a
/// task a thread spawned could not be listed or opened from the thread pane.
/// Carries the inheritance job creation now performs back over existing rows.
macro_rules! shared_tail_inherit_thread_id_for_child_jobs {
    () => {
        migration!("0167_inherit_thread_id_for_child_jobs")
    };
}

/// CAIRN-3888: durable current arm attempt for turn-end check settlement.
macro_rules! shared_tail_turn_end_check_attempts {
    () => {
        migration!("0168_turn_end_check_attempts")
    };
}

macro_rules! shared_tail_thread_title_retires {
    () => {
        migration!("0160_thread_title_retires")
    };
}

macro_rules! shared_tail_migrate_issue_threads {
    () => {
        migration!("0157_migrate_issue_threads")
    };
}

macro_rules! shared_tail_threads_entity {
    () => {
        migration!("0156_threads_entity")
    };
}

/// CAIRN-3823: one unit for `check_result_cache.ran_at`. The observation
/// projection writes milliseconds; the pre-observation writer wrote seconds.
macro_rules! shared_tail_check_result_cache_ran_at_millis {
    () => {
        migration!("0165_check_result_cache_ran_at_millis")
    };
}

macro_rules! shared_tail_verdict_reuse_facts {
    () => {
        migration!("0155_verdict_reuse_facts")
    };
}

macro_rules! shared_tail_repair_check_observation_public_handle {
    () => {
        migration!("0154_repair_check_observation_public_handle")
    };
}

macro_rules! private_route_firing_content {
    () => {
        migration!("0159_route_firing_content")
    };
}

macro_rules! private_channel_outbound_route_kind {
    () => {
        migration!("0153_channel_outbound_route_kind")
    };
}

macro_rules! shared_tail_rebase_replay_status {
    () => {
        migration!("0150_rebase_replay_status")
    };
}

macro_rules! private_command_contention_profiles {
    () => {
        migration!("0149_command_contention_profiles")
    };
}

macro_rules! private_channel_thread_suppression {
    () => {
        migration!("0148_channel_thread_suppression")
    };
}

macro_rules! shared_tail_check_observation_public_handle {
    () => {
        migration!("0146_add_check_observation_public_handle")
    };
}

macro_rules! shared_tail_pr_resolution_attribution {
    () => {
        migration!("0145_pr_resolution_attribution")
    };
}

macro_rules! private_channel_thread_state {
    () => {
        migration!("0144_channel_thread_state")
    };
}

macro_rules! shared_tail_virtual_reconcile_coordinates {
    () => {
        // `jobs` loses two columns in place via ALTER TABLE DROP COLUMN;
        // `jj_reconcile_items` is copied whole out of its `_legacy` rename. Both
        // carry every row.
        migration!(
            "0118_virtual_reconcile_coordinates",
            rebuild_fk_off: &[
                RebuildCheck::Conserved("jobs"),
                RebuildCheck::Conserved("jj_reconcile_items"),
            ]
        )
    };
}

macro_rules! shared_tail_jj_reconcile_quarantines {
    () => {
        migration!("0116_add_jj_reconcile_quarantines")
    };
}

macro_rules! shared_tail_agent_waits {
    () => {
        migration!("0115_add_agent_waits")
    };
}

/// Canonical, synced pack metadata and durable references. This is deliberately
/// shared between lineages: the team replica is the authority consumed by the
/// API-owned mark-and-sweep, while local projects retain the same data model.
macro_rules! shared_tail_pack_catalog {
    () => {
        migration!("0112_pack_catalog")
    };
}

/// Terminal rows persist the lifetime lease fence and process generation used by
/// the executor-hosted PTY transport.
macro_rules! shared_tail_terminal_lifetime_lease {
    () => {
        migration!("0113_bind_agent_terminals_to_lifetime_leases")
    };
}

macro_rules! shared_tail_jj_reconcile_intents {
    () => {
        migration!("0114_add_jj_reconcile_intents")
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
        migration!("0087_token_rollup_hourly")
    };
}

/// CAIRN-2251: sync-on-write check result cache, keyed by (project, sealed tree,
/// check name). A project-scoped SHARED table (present in both lineages), so it is
/// written once here and appended last in each lineage. Numbered 0089 to follow
/// main's analytics migrations (0086-0088) after this branch rebased onto #2037.
macro_rules! shared_tail_check_result_cache {
    () => {
        migration!("0089_check_result_cache")
    };
}

/// CAIRN-2281: re-key check_result_cache by each check's impact-scoped input hash
/// (the content of just the files matching its globs) instead of the whole sealed
/// tree, so a commit touching none of a check's inputs reuses its cached verdict.
/// A project-scoped SHARED table change, so it is written once here and appended
/// after 0089 in each lineage.
macro_rules! shared_tail_check_result_input_hash {
    () => {
        migration!("0090_check_result_cache_input_hash")
    };
}

/// CAIRN-2348: add result timestamps to the tool-invocation rollup so duration
/// analytics can derive tool-call wall time from assistant/tool_result event
/// pairs. `tool_invocations` and its watermark table are project-scoped shared
/// tables, so this schema change is appended to both lineages.
macro_rules! shared_tail_tool_invocation_durations {
    () => {
        migration!("0092_tool_invocation_durations")
    };
}

/// CAIRN-2368: add a durable per-job listing index and cached/fresh stamp to
/// check_result_cache. The table is project-scoped shared state, so this append-only
/// migration lands in both lineages; the cache key remains input-hash based.
macro_rules! shared_tail_check_result_job_id {
    () => {
        migration!("0093_check_result_cache_job_id")
    };
}

/// CAIRN-2386: relink first-class PR-node merge request rows from action-run or
/// otherwise dangling ids back to the producing builder job, when that job is an
/// unambiguous match for the PR source branch.
macro_rules! shared_tail_relink_merge_request_jobs {
    () => {
        migration!("0095_relink_merge_request_jobs")
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
        migration!("0096_jobs_child_base")
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
        migration!("0097_jobs_owns_ephemeral_worktree")
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
        migration!("0098_call_output_contract_and_run_tags")
    };
}

/// CAIRN-2499: durable phase/log progress timeline for a workflow node, the
/// reader the workflow monitoring panel renders. A project-scoped SHARED table
/// (a new CREATE TABLE), so it is written once here and appended after 0098 in
/// both lineages -- it lives alongside the jobs/runs the monitor joins it with,
/// and its progress syncs so teammates see the panel.
macro_rules! shared_tail_workflow_progress {
    () => {
        migration!("0101_workflow_progress")
    };
}

/// CAIRN-2623: classify a FAILING check's terminal outcome (timeout vs spawn
/// failure vs signal kill) so a slow-but-healthy suite killed at its budget
/// renders AS a timeout, not an opaque `exit -1`. A project-scoped SHARED table
/// ADD COLUMN, written once here and appended after 0101 in both lineages.
macro_rules! shared_tail_check_result_failure_kind {
    () => {
        migration!("0102_check_result_cache_failure_kind")
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
        migration!("0103_executions_runner_device_id")
    };
}

/// CAIRN-2580: repair invalid empty/root worktree assignments left on historical
/// child jobs. `jobs` is shared, so both private and team replicas apply the same
/// data cleanup and represent these worktree-less records canonically as NULL.
macro_rules! shared_tail_clear_invalid_job_worktree_paths {
    () => {
        migration!("0104_clear_invalid_job_worktree_paths")
    };
}

macro_rules! shared_tail_add_turn_end_reason {
    () => {
        migration!("0105_add_turn_end_reason")
    };
}

macro_rules! shared_tail_index_hot_gui_status_queries {
    () => {
        migration!("0106_index_hot_gui_status_queries")
    };
}
/// CAIRN-2804: retain executor attribution and the runner-local toolchain claim
/// alongside each cached verdict. The cache is shared project state, so this
/// additive migration is composed once into both lineages.
macro_rules! shared_tail_check_result_cache_provenance {
    () => {
        migration!("0109_check_result_cache_provenance")
    };
}

/// CAIRN-3108: index the `(check_name, ran_at, tree_hash)` recency ranking that
/// selects each check's latest verdict. The cache is shared project state, so
/// this index-only migration is composed once into both lineages.
macro_rules! shared_tail_check_result_cache_recency_index {
    () => {
        migration!("0121_check_result_cache_recency_index")
    };
}

/// CAIRN-3167: pin a durable session to its selected provider account. Sessions
/// are shared project state, so this additive migration reaches both lineages.
macro_rules! shared_tail_session_account {
    () => {
        migration!("0122_session_account")
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
        migration!("0123_job_repls")
    };
}

/// CAIRN-3152: the durable REPL transcript, replacing the in-memory 200-entry
/// exchange ring. Project-scoped shared state hanging off `job_repls`, so it is
/// composed into both lineages immediately after it.
macro_rules! shared_tail_repl_exchanges {
    () => {
        migration!("0124_repl_exchanges")
    };
}

/// CAIRN-3112: a terminal is a process inside an execution environment, so its
/// durable fence becomes (holder, incarnation, cell epoch). `job_terminals` is
/// carried by the synced team lineage, so this reaches both.
macro_rules! shared_tail_rebind_terminals_to_residencies {
    () => {
        migration!("0125_rebind_terminals_to_residencies")
    };
}

/// CAIRN-3232: re-key the active-suspension bound from the job to the call, so
/// a turn can park every one of its concurrent long-running calls instead of
/// only the first. `agent_waits` is project-scoped shared state, so this
/// index-only migration is composed once here and reaches both lineages.
macro_rules! shared_tail_agent_waits_concurrent_calls {
    () => {
        migration!("0126_agent_waits_concurrent_calls")
    };
}

/// CAIRN-3242: the reference rows that give a stored image a human address. The
/// blob's sha256 stays its storage identity in the content store; these rows map
/// a scoped ordinal onto it. Project-scoped shared state — a teammate opening a
/// synced issue must resolve the same image URIs — so it reaches both lineages.
macro_rules! shared_tail_image_refs {
    () => {
        migration!("0127_image_refs")
    };
}

/// CAIRN-3264: the write-replay ledger, which makes a second delivery of one
/// `write` call a no-op instead of a second application of its patch. Host-local
/// delivery state, so it is a private-lineage migration rather than a shared
/// tail — a teammate has no in-flight socket of ours to deduplicate.
macro_rules! private_write_replay_ledger {
    () => {
        migration!("0128_write_replay_ledger")
    };
}

/// CAIRN-3265: ordinary prompt rows used for external MCP input carry an
/// explicit durable-wait owner and one-time consumption marker. Both referenced
/// tables are project-scoped shared state, so both lineages receive the column.
macro_rules! shared_tail_mcp_continuation_prompts {
    () => {
        migration!("0129_mcp_continuation_prompts")
    };
}

/// CAIRN-3293: delete the snapshotted child-attention subscriptions now that the
/// recipient is derived from the parent edge. The table is shared schema, so both
/// lineages converge on having none; it is a no-op wherever none were written.
macro_rules! shared_tail_retire_snapshot_child_wakes {
    () => {
        migration!("0130_retire_snapshot_child_wakes")
    };
}

/// CAIRN-3290: `events(run_id, sequence)` becomes UNIQUE. Repairs the duplicate
/// slots that hand-rolled sequence counters wrote, then constrains the column so
/// a regression fails loudly instead of silently corrupting transcript order.
/// `events` is project-scoped shared state, so both lineages receive it.
macro_rules! shared_tail_unique_event_sequence {
    () => {
        migration!("0131_unique_event_sequence")
    };
}

/// CAIRN-3245: bound the re-execution of an infrastructure-failing check. The
/// consecutive-failure counter and its one-shot escalation stamp live on the
/// cache row whose key is already the suppressed triple, so this is additive to
/// shared project state and composed once into both lineages.
macro_rules! shared_tail_check_result_cache_infra_suppression {
    () => {
        migration!("0132_check_result_cache_infra_suppression")
    };
}

macro_rules! shared_tail_check_result_observations {
    () => {
        migration!(
            "0134_check_result_observations",
            rebuild_fk_off: &[RebuildCheck::Conserved("check_result_cache")]
        )
    };
}

macro_rules! shared_tail_check_definition_provenance {
    () => {
        migration!("0135_check_definition_provenance")
    };
}

/// The conflict diagnostic captured before a rolled-back rebase, plus the
/// incoming change's normalized file inventory. Shared because the reconcile
/// tables it extends are shared; a team replica carries the same data model.
macro_rules! shared_tail_conflict_resolution_sessions {
    () => {
        migration!("0136_conflict_resolution_sessions")
    };
}

/// The discriminator that says what a row in `issues` is: an ordinary issue or a
/// thread. `issues` is a project-scoped table both lineages carry, so the column
/// is written once and composed into both.
macro_rules! shared_tail_issue_kind {
    () => {
        migration!("0138_issue_kind")
    };
}

/// External channel delivery state is tied to this runner's provider process and
/// personal messaging account. It must not replicate into a team database.
macro_rules! private_channel_persistence {
    () => {
        migration!("0137_channel_persistence")
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
        migration!("0143_command_duration_profiles")
    };
}

macro_rules! private_route_firings {
    () => {
        migration!("0152_route_firings")
    };
}

macro_rules! private_response_invocations {
    () => {
        migration!("0151_response_invocations")
    };
}

macro_rules! private_quarantine_saturation_memory_profiles {
    () => {
        migration!("0147_quarantine_saturation_memory_profiles")
    };
}

macro_rules! private_thread_compaction {
    () => {
        migration!("0139_thread_compaction")
    };
}

/// The abandoned-intent state the live-tap channel needs. Channel delivery state
/// never replicates into a team database, so this rebuild stays private.
macro_rules! private_channel_outbound_expired {
    () => {
        migration!("0142_channel_outbound_expired")
    };
}

macro_rules! team_lineage {
    ($($head:expr),* $(,)?) => {
        with_team_tail![
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
            shared_tail_turn_end_check_attempts!(),
            shared_tail_check_result_cache_row_family!(),
            shared_tail_lowercase_project_keys!(),
            shared_tail_tool_analytics_hourly!(),
            shared_tail_issue_authorship!(),
            shared_tail_canonical_project_keys!(),
            shared_tail_liveness_join_indexes!(),
            shared_tail_attention_push_retirement!(),
            shared_tail_thread_status_rollup_index!(),
            shared_tail_project_thread_number!(),
            shared_tail_job_file_activity!(),
            shared_tail_job_presence!(),
            shared_tail_thread_unread_scan_index!(),
            shared_tail_human_base_merge_policy!(),
            shared_tail_merge_attempt_journal!(),
            shared_tail_thread_branches!(),
            shared_tail_retire_needs_fresh_session!(),
            shared_tail_close_replaced_sessions!(),
            shared_tail_attachments!(),
            shared_tail_event_vibe_revision!(),
            shared_tail_execution_canonical_lineage!(),
            shared_tail_issue_resolution_generation!(),
            shared_tail_condition_evaluation_outcome!(),
            shared_tail_post_feed_watch_as_fact_clause!(),
            shared_tail_permission_resume_error!(),
            shared_tail_terminal_exit_reason!(),
            shared_tail_prompt_withdrawal!(),
            shared_tail_provider_limit_holds!(),
            shared_tail_execution_after_gates!(),
            shared_tail_retire_issue_dependencies!(),
            shared_tail_reconcile_resolution_basis!(),
            shared_tail_provider_limit_failover!(),
            shared_tail_tenets!(),
            shared_tail_tenet_ruling_imports!(),
            shared_tail_tenet_evidence!(),
            shared_tail_tenet_evidence_measured_by!(),
            shared_tail_pr_publish_attempts!(),
            shared_tail_sibling_job_lookup_indexes!(),
            shared_tail_tenet_statements!(),
            shared_tail_check_observation_platform_relabels!(),
            shared_tail_job_artifact_inputs!(),
            shared_tail_check_deferred_tests!(),
            shared_tail_execution_gates_execution_index!(),
            shared_tail_drop_check_test_outcomes_view!(),
            shared_tail_job_goal_intent!(),
            shared_tail_compile_facts!(),
            shared_tail_turn_end_check_attempt_plan!(),
            shared_tail_tenet_rationale!(),
            shared_tail_run_activity_minutes!(),
            // ── TEAM_TAIL ───────────────────────────────────────────────────
            // Intentionally empty for now. CAIRN-2277's team-side removal of
            // `projects.server_id` lives in the team snapshot instead of a
            // post-snapshot rebuild: destructive create-copy-drop-rename rebuilds
            // on synced replica tables break Turso sync triggers. The unfenced
            // `turso_sync_roundtrip` lane failed 8/15 tests when this was modeled
            // as `0003_drop_projects_server_id`, so do not re-add that tail entry.
            //
            // ── DERIVED TAIL ────────────────────────────────────────────────
            // Nothing is composed by hand past the cutover. `with_team_tail!`
            // appends every post-cutover file headed `-- lineage: shared` or
            // `-- lineage: team`, in number order.
        ]
    };
}

macro_rules! private_lineage {
    ($($head:expr),* $(,)?) => {
        with_private_tail![
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
            migration!("0085_project_routes_local_path"),
            // CAIRN-2252: one-time marker for the historical analytics-rollup
            // backfill. Per-install runner-transient state (mirrors
            // archival_backfill_state); never synced to a team replica.
            migration!("0086_analytics_rollup_backfill_state"),
            // CAIRN-2270: reopen the one-time historical analytics-rollup backfill
            // so it re-folds every run into the hour-grain token_rollup the shared
            // 0087 migration recreated empty. Private-only: it resets
            // analytics_rollup_backfill_state (migration 0086), which exists only
            // in this lineage.
            migration!("0088_reopen_analytics_backfill"),
            shared_tail_check_result_cache!(),
            shared_tail_check_result_input_hash!(),
            // CAIRN-2277: drop the dead `projects.server_id` column, its FK to the
            // local `servers` table, and then that now-orphaned `servers` table. A
            // rebuild_fk_off because many tables FK-reference `projects(id)` and
            // `projects` itself referenced `servers`. Private-only: the team
            // snapshot schema never carries the local `servers` column/table.
            migration!(
                "0091_drop_projects_server_id_and_servers",
                // `servers` is dropped outright, not rebuilt; `projects` copies
                // every row minus the `server_id` column.
                rebuild_fk_off: &[RebuildCheck::Conserved("projects")]
            ),
            shared_tail_tool_invocation_durations!(),
            shared_tail_check_result_job_id!(),
            // CAIRN-2385: one-time repair marker for historical
            // tool_invocations.result_ts rows. Private-only because the marker
            // table is runner-transient state, not team-replicated data.
            migration!("0094_tool_invocation_result_backfill_state"),
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
            migration!("0099_workflow_journal"),
            // CAIRN-2498: workflow restart durability. workflow_run records the
            // durable spawn params for startup re-dispatch; workflow_call links
            // an in-flight ephemeral call back to its journal key. Both are
            // per-machine runner-transient replay state (never synced), so they
            // live ONLY in the private lineage's tail -- absent from
            // TEAM_MIGRATIONS, like workflow_journal above.
            migration!("0100_workflow_restart_durability"),
            shared_tail_add_turn_end_reason!(),
            shared_tail_index_hot_gui_status_queries!(),
            // Token usage rows and analytics backfill state are local/private.
            // Re-normalize OpenAI-style history and force a complete rollup fold.
            migration!("0107_normalize_openai_token_usage_components"),
            // Executor enrollment grants and credentials establish trust with
            // this runner device. They are host-local identity/credential state,
            // never collaboration data synced into a team replica.
            migration!("0108_executor_enrollment"),
            shared_tail_check_result_cache_provenance!(),
            migration!("0110_executor_enrollment_expiry"),
            // Machine-local resource learning contains executor observations and
            // must never replicate into a team database.
            migration!("0111_command_resource_profiles"),
            shared_tail_pack_catalog!(),
            shared_tail_terminal_lifetime_lease!(),
            shared_tail_jj_reconcile_intents!(),
            shared_tail_agent_waits!(),
            shared_tail_jj_reconcile_quarantines!(),
            // Workflow restart records are runner-local state, so their executor
            // anchor migration belongs to the private lineage with workflow_run.
            migration!("0117_workflow_executor_anchor"),
            shared_tail_virtual_reconcile_coordinates!(),
            migration!("0119_virtual_workflow_coordinates"),
            // CAIRN-3103: cadence marker for the whole-database integrity sweep,
            // which replaced the per-migration integrity_check. Per-install
            // runner-transient state (mirrors archival_backfill_state); never
            // synced to a team replica.
            migration!("0120_integrity_sweep_state"),
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
            migration!("0133_executor_enrollment_names"),
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
            migration!("0158_executor_desktop_automation"),
            shared_tail_thread_title_retires!(),
            // Must follow `private_thread_compaction!()`: they alter that table.
            migration!("0140_thread_compaction_seed_bytes"),
            migration!("0141_thread_compaction_capacity_trigger"),
            // Data-only clear of a derived cache. The table is shared with the
            // team lineage, but nothing here is schema, and a team replica's
            // stale rows self-heal: an unparseable row is a cache miss that
            // rebuilds in the compact shape.
            migration!("0161_clear_skyline_cache_for_compact_bars"),
            // CAIRN-3810: per-turn semantic vectors backing the search lane.
            // Private-only for the same reason `resource_embeddings` is.
            migration!("0162_turn_embeddings"),
            shared_tail_turns_created_at_index!(),
            // CAIRN-3810: separates the sweep's consent boundary from its
            // reconciliation floor. Private-only, like `turn_embeddings`.
            migration!("0164_turn_embedding_state"),
            shared_tail_check_result_cache_ran_at_millis!(),
            private_authority_grants!(),
            shared_tail_inherit_thread_id_for_child_jobs!(),
            shared_tail_turn_end_check_attempts!(),
            private_route_fact_samples!(),
            private_build_change_ride_alongs!(),
            shared_tail_check_result_cache_row_family!(),
            private_disclosure_remediation!(),
            shared_tail_lowercase_project_keys!(),
            private_lowercase_project_routes!(),
            private_canonicalize_uri_identity_columns!(),
            private_channel_inbound_rejection_reason!(),
            shared_tail_tool_analytics_hourly!(),
            private_github_relay_delivery_health!(),
            private_github_relay_pending_key!(),
            private_discord_surface!(),
            private_channel_conversation_binding!(),
            private_channel_ask_resolution!(),
            private_authority_attribution!(),
            private_ask_resolution_provenance!(),
            private_repair_attention_pushes_index_parity!(),
            private_repair_prompts_column_order_parity!(),
            shared_tail_issue_authorship!(),
            private_codex_watchdog_ledger!(),
            private_github_relay_key_rotation_time!(),
            private_canonicalize_channel_permission_answers!(),
            shared_tail_canonical_project_keys!(),
            private_canonical_discord_surface_project_keys!(),
            private_canonical_project_routes!(),
            private_posts!(),
            shared_tail_liveness_join_indexes!(),
            shared_tail_attention_push_retirement!(),
            private_attention_push_retirement_backfill!(),
            shared_tail_thread_status_rollup_index!(),
            shared_tail_project_thread_number!(),
            private_thread_read_positions!(),
            shared_tail_job_file_activity!(),
            shared_tail_job_presence!(),
            private_codemap_cache!(),
            private_runtime_admission!(),
            private_prepared_runtime_calls!(),
            private_wake_schedules!(),
            shared_tail_thread_unread_scan_index!(),
            private_thread_eras!(),
            private_channel_ask_action_park!(),
            shared_tail_check_result_cache_job_check_recency_index!(),
            private_channel_outbound_retry!(),
            migration!("0212_project_home_indexes"),
            private_github_relay_unmapped_mentions!(),
            private_wake_conditions!(),
            private_repair_codex_watchdog_lifecycle_kind!(),
            private_channel_state_card!(),
            private_thread_automations!(),
            migration!("0218_kind_scoped_device_enrollment"),
            shared_tail_human_base_merge_policy!(),
            shared_tail_merge_attempt_journal!(),
            shared_tail_thread_branches!(),
            private_write_replay_ownership!(),
            shared_tail_retire_needs_fresh_session!(),
            shared_tail_close_replaced_sessions!(),
            migration!("0225_home_prose_index"),
            migration!("0226_executor_repairs"),
            shared_tail_attachments!(),
            private_contended_identifier_obligations!(),
            shared_tail_event_vibe_revision!(),
            shared_tail_execution_canonical_lineage!(),
            private_state_card_pin_attempts!(),
            shared_tail_issue_resolution_generation!(),
            migration!("0233_attention_push_key_lookup"),
            private_executor_enrollment_supersession!(),
            private_execution_queue!(),
            private_response_invocation_account!(),
            private_execution_queue_project_provisioning!(),
            private_channel_agent_messages!(),
            shared_tail_condition_evaluation_outcome!(),
            private_prepared_ordinary_launches!(),
            private_team_ordinary_admission_authority!(),
            private_purge_poisoned_duration_profiles!(),
            private_fact_log!(),
            private_drop_route_fact_samples!(),
            private_channel_outbound_last_attempt!(),
            private_executor_preparation_refusals!(),
            private_fact_log_wakes!(),
            shared_tail_post_feed_watch_as_fact_clause!(),
            private_account_credential_events!(),
            shared_tail_permission_resume_error!(),
            private_thread_compaction_unconfirmed_start_trigger!(),
            shared_tail_terminal_exit_reason!(),
            private_ref_store_maintenance!(),
            shared_tail_prompt_withdrawal!(),
            shared_tail_provider_limit_holds!(),
            shared_tail_execution_after_gates!(),
            shared_tail_retire_issue_dependencies!(),
            shared_tail_reconcile_resolution_basis!(),
            private_held_publications!(),
            shared_tail_provider_limit_failover!(),
            shared_tail_tenets!(),
            shared_tail_tenet_ruling_imports!(),
            shared_tail_tenet_evidence!(),
            shared_tail_tenet_evidence_measured_by!(),
            private_command_outcome_history!(),
            shared_tail_pr_publish_attempts!(),
            shared_tail_sibling_job_lookup_indexes!(),
            private_inference_records!(),
            private_object_store_maintenance!(),
            private_ingress_events!(),
            shared_tail_tenet_statements!(),
            shared_tail_check_observation_platform_relabels!(),
            private_subscription_periods!(),
            private_runner_events!(),
            shared_tail_job_artifact_inputs!(),
            shared_tail_check_deferred_tests!(),
            private_prepared_ordinary_launch_origin!(),
            shared_tail_execution_gates_execution_index!(),
            shared_tail_drop_check_test_outcomes_view!(),
            private_execution_queue_build_volume_absent!(),
            shared_tail_job_goal_intent!(),
            private_purpose_debts!(),
            shared_tail_compile_facts!(),
            shared_tail_turn_end_check_attempt_plan!(),
            private_embedding_space!(),
            shared_tail_tenet_rationale!(),
            private_command_resource_memory_kills!(),
            shared_tail_run_activity_minutes!(),
            // ── DERIVED TAIL ────────────────────────────────────────────────
            // Nothing is composed by hand past the cutover. `with_private_tail!`
            // appends every post-cutover file headed `-- lineage: shared` or
            // `-- lineage: private`, in number order.
        ]
    };
}

pub const TURSO_MIGRATIONS: &[Migration] = private_lineage![
    migration!("0001_initial_schema"),
    migration!("0002_search_outbox"),
    migration!("0003_seed_default_workspace"),
    migration!("0004_add_issue_dependencies"),
    migration!("0005_change_preview_events"),
    migration!("0006_uri_segments"),
    migration!("0007_add_uri_segment_to_prompts"),
    migration!("0008_add_job_id_to_prompts"),
    migration!("0009_cohere_embeddings"),
    migration!("0010_anon_device"),
    migration!("0011_session_current_pos"),
    migration!("0012_resource_surfacings"),
    migration!("0013_memory_redux"),
    migration!("0014_add_tool_use_id_to_prompts"),
    migration!("0015_add_artifact_confirmed"),
    migration!("0016_remove_ready_status"),
    migration!("0017_messages_delivered_at"),
    migration!("0018_pr_node_port_fires"),
    migration!("0019_merge_request_owner"),
    migration!("0020_add_uri_segment_to_action_runs"),
    migration!("0021_vibe_axes"),
    migration!("0022_add_segments_to_permission_requests"),
    migration!("0023_add_labels"),
    migration!("0024_add_parent_issue"),
    migration!(
        "0025_remove_managers",
        // Five unfiltered rebuilds dropping `manager_id`. The four manager tables
        // are dropped, not rebuilt.
        rebuild_fk_off: &[
            RebuildCheck::Conserved("issues"),
            RebuildCheck::Conserved("jobs"),
            RebuildCheck::Conserved("turns"),
            RebuildCheck::Conserved("messages"),
            RebuildCheck::Conserved("merge_requests"),
        ]
    ),
    migration!("0026_child_side_channel_notices"),
    migration!("0027_session_channel_cursor"),
    migration!("0028_issue_parent_job"),
    migration!("0029_queued_messages"),
    migration!("0030_checkpoint_runs"),
    migration!(
        "0031_drop_dead_chats_table",
        // Only the FK reference to `chats` goes away; `chat_id` survives as a
        // vestigial column and every row is copied.
        rebuild_fk_off: &[
            RebuildCheck::Conserved("runs"),
            RebuildCheck::Conserved("sessions"),
        ]
    ),
    migration!("0032_drop_workspaces_timezone_column"),
    migration!("0033_annotations"),
    migration!("0034_annotation_message_links"),
    migration!("0035_annotation_uri_seq"),
    migration!("0036_wake_subscriptions"),
    migration!("0037_unify_side_channel_notices"),
    migration!(
        "0038_drop_annotation_tables",
        // Three bare `DROP TABLE IF EXISTS`; nothing is rebuilt, so there is
        // nothing to verify.
        rebuild_fk_off: &[]
    ),
    migration!("0039_memory_intake_ledger"),
    migration!("0040_add_is_workspace_to_projects"),
    migration!("0041_memory_triage_batches_and_drop_when_to_use"),
    // Column values transformed in place by CASE expressions; no WHERE filter.
    migration!(
        "0042_memory_scope_node_id_and_status_lattice",
        rebuild_fk_off: &[RebuildCheck::Conserved("memories")]
    ),
    migration!("0043_memory_triage_decision"),
    migration!("0044_jobs_memory_review_state"),
    migration!(
        "0045_memory_canon_v2_consolidation",
        // Deliberately lossy: pre-v2 rows are not copied at all (the new table is
        // created empty) and `memory_triage_issue_memories` is emptied first.
        rebuild_fk_off: &[RebuildCheck::Reshaped("memories")]
    ),
    migration!(
        "0046_memory_review_sent_state",
        // `memory_review_state` value remapped; every row copied.
        rebuild_fk_off: &[RebuildCheck::Conserved("jobs")]
    ),
    migration!("0047_add_message_urgency"),
    migration!("0048_add_event_thinking_tokens"),
    migration!("0049_event_vibes_session_id"),
    migration!("0050_session_skyline_cache_vibe_watermark"),
    migration!("0051_clear_skyline_cache_for_content_bar_decomposition"),
    migration!("0052_clear_skyline_cache_for_visual_height_sizing"),
    migration!("0053_job_pack_anchor"),
    migration!("0054_archival_storage"),
    migration!("0055_archival_backfill_state"),
    migration!("0056_archival_blobs"),
    migration!("0057_event_read_tokens"),
    migration!("0058_terminal_exit_wakes"),
    migration!("0059_jobs_needs_fresh_session"),
    migration!("0060_attention_items"),
    migration!("0061_attention_escalate_at"),
    migration!("0062_attention_fingerprint"),
    migration!("0063_comment_seq"),
    migration!("0064_clear_skyline_cache_for_system_event_filter"),
    migration!("0065_merge_request_is_local"),
    migration!("0066_config_disables"),
    migration!("0067_tool_invocations"),
    migration!("0068_job_browsers"),
    migration!("0069_add_event_cost_usd"),
    migration!("0070_attention_pushes"),
    migration!("0071_attention_push_fingerprint"),
    migration!("0072_merge_request_head_sha"),
    migration!("0073_attention_read_cursors"),
    migration!("0074_drop_attention_ledger"),
    migration!("0075_drop_messages_delivered_at"),
    migration!("0076_terminal_output_wakes"),
    migration!("0077_event_content_change_id"),
    migration!("0078_browser_last_active_at"),
    migration!("0079_index_runs_session_id_created_at"),
    migration!("0080_token_rollup"),
    migration!("0081_drop_runs_backend"),
    migration!("0082_team_routing"),
    // PRIVATE-ONLY (CAIRN-2188): the local read-through cache for content-store
    // objects (team-run packs/blobs fetched by hash). Fetched bytes must never
    // land on the synced team replica, so this is a private head entry, not a
    // SHARED_TAIL change. Classified `Private(PrivateReason::RebuildableCache)` in
    // `TABLE_SCOPES`; the `team_schema_matches_private` projection test proves it
    // stays out of the team lineage.
    migration!("0083_cas_cache"),
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
    team_migration!("0001_team_initial_schema"),
    // Catch-up: the team head (0001) omitted `labels` (team-scoped label
    // management is deferred), but the routed issue-content paths JOIN it for
    // read-resolution. This adds the EMPTY table so that JOIN resolves uniformly
    // across both lineages instead of failing `no such table: labels`
    // (CAIRN-2186). Team-only by design: the private lineage already has `labels`
    // from 0023, so this is not a SHARED_TAIL change.
    team_migration!("0002_labels_read_completeness"),
    // CAIRN-2629: per-machine device presence for team runner selection. Team-only
    // (no private counterpart): the runner picker, clone validation, and
    // "waiting for <device>" UX all read it. `CREATE TABLE IF NOT EXISTS` makes it
    // self-sufficient against the migration-vs-sync race; the idempotent
    // `ensure_device_presence_table` repair at team-open backstops it.
    team_migration!("0003_device_presence"),
    // Team-only durable inbox for remote clients. Canonical effects remain in
    // shared tables; only the synced delivery envelope is team-specific.
    team_migration!("0004_remote_intents"),
    // Team-only non-secret executor advertisements. Enrollment credentials and
    // revocation state remain private; only fleet availability is replicated.
    team_migration!("0005_executor_registry"),
    // CAIRN-2870: inventory is emergent executor health, not advertised capacity.
    team_migration!("0006_elastic_executor_inventory"),
    team_migration!("0007_ask_resolution_provenance"),
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
    /// Bytes this install holds that nothing can rebuild or refetch (local
    /// attachment bytes), kept private by the issue that owns their home.
    AuthoritativeContent { owner: &'static str },
    /// This install's own configuration of the machines it runs (the
    /// inference inventory): written by its operator, durable, and rebuilt
    /// from nowhere, but about this install's hardware, so never synced.
    HostConfiguration,
    /// Names objects in this machine's file archive, and is what makes them
    /// reachable. Never cleared: losing it strands the archive's contents.
    HostArchive,
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
    // Provider sign-in history, beside the authorization journal: who started a
    // sign-in on this install and how it ended.
    (
        "account_credential_events",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // Which subscription plan each provider account held, and when: the
    // install's own billing history, priced by effective cost.
    (
        "subscription_periods",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // Execution offers belong to one runner incarnation, never a team replica.
    (
        "execution_queue",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // Which of this install's runner incarnations have had their queue entries
    // closed: an index over the runner-local journal above.
    (
        "execution_queue_closed_incarnations",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // A project's ref store is a file on this machine: its measured size, what
    // each maintenance prune removed, and the operator's prune choice here.
    (
        "ref_store_samples",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "ref_store_maintenance",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // The daily census of this install's own database file: what its tables
    // hold and how fast they grow. Meaningless for any other database.
    (
        "storage_census",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // The runner's own lifecycle facts: this process's stalls and this
    // machine's executor links, meaningless on any other installation.
    (
        "runner_events",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // What a page's open frames read and reported on this install, and which
    // viewer had it open: an audit trail of this install's own viewers, pruned
    // to the newest rows per page.
    (
        "page_calls",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // What this runner's service processes did (lifecycle, stderr, calls),
    // sanitized on write and pruned to the newest rows per service.
    (
        "service_log",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // The cadence of this runner's scheduled service methods: anchors and
    // last outcomes for the processes it supervises, rebuilt from the
    // declarations on any runner that serves them.
    (
        "service_schedules",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // A node scratch's store is a directory on this runner and its replicas
    // are this runner's executors.
    (
        "node_scratches",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "node_scratch_replicas",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // ── ProjectScoped: the durable shared collaboration surface ──────────────
    ("action_configs", TableScope::ProjectScoped),
    ("action_runs", TableScope::ProjectScoped),
    ("agent_waits", TableScope::ProjectScoped),
    // A run batch that outlived its call; read beside `agent_waits` by the
    // replay mutation boundary, so it lives where they do.
    ("run_batch_detached", TableScope::ProjectScoped),
    // A result a restarted runner adopted for a batch it did not start; read
    // beside `run_batch_detached` and `agent_waits`, so it lives where they do.
    ("run_batch_adopted_outcomes", TableScope::ProjectScoped),
    ("artifact_content", TableScope::ProjectScoped),
    ("artifacts", TableScope::ProjectScoped),
    // Per-install progress of the one-time retirement backfill (CAIRN-4182):
    // how far THIS runner has classified its own historical queue. A team
    // replica has no use for another machine's cursor.
    (
        "attention_push_retirement_backfill",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    ("attention_pushes", TableScope::ProjectScoped),
    ("attention_read_cursors", TableScope::ProjectScoped),
    // cairn/6391: one receipt per (fact, stable recipient). A push receipt
    // lives beside its push, so it shares the push's scope; a fact-log receipt
    // lives in the private database beside its coalesced wake.
    ("delivery_receipts", TableScope::ProjectScoped),
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
    (
        "requests",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_requesters",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_waiting",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_cohorts",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_cohort_members",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_attempts",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_attempt_members",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_facts",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_outbox",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "request_report_receipts",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // cairn/5712: who may spend this install's models through the served API,
    // and the journal of what each such client spent. Same reasoning as the
    // grants: a teammate's install never issued these keys.
    (
        "served_api_clients",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "model_requests",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // cairn/6032: the usage-window time series of this install's provider
    // accounts. The accounts and their credentials are this install's.
    (
        "usage_window_readings",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    ("main_incident_sources", TableScope::ProjectScoped),
    ("main_incident_budget", TableScope::ProjectScoped),
    ("main_incidents", TableScope::ProjectScoped),
    ("main_incident_evidence", TableScope::ProjectScoped),
    ("check_result_cache", TableScope::ProjectScoped),
    // CAIRN-4665: the durable record of which execution stopped owning an
    // issue's attempt, and the work item that interrupts its surviving agents.
    // It describes ProjectScoped executions and must be readable wherever they
    // live, so it is ProjectScoped too.
    ("execution_supersessions", TableScope::ProjectScoped),
    ("check_result_commit_aliases", TableScope::ProjectScoped),
    ("check_result_observations", TableScope::ProjectScoped),
    ("manual_check_handles", TableScope::ProjectScoped),
    ("check_observation_relabels", TableScope::ProjectScoped),
    ("check_test_results", TableScope::ProjectScoped),
    ("check_deferred_tests", TableScope::ProjectScoped),
    ("check_audit_findings", TableScope::ProjectScoped),
    ("compile_facts", TableScope::ProjectScoped),
    ("checkpoint_command_cache", TableScope::ProjectScoped),
    ("checkpoint_runs", TableScope::ProjectScoped),
    ("pr_publish_attempts", TableScope::ProjectScoped),
    ("job_artifact_inputs", TableScope::ProjectScoped),
    // A code map is a projection of a commit, not a fact about the work. Any
    // replica holding that commit rebuilds it locally in one walk, so shipping
    // the payload would be pure transfer for nothing.
    (
        "codemap_cache",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    ("comments", TableScope::ProjectScoped),
    ("condition_evaluations", TableScope::ProjectScoped),
    // A disclosure response is this host's own record, and its inventory is a
    // map of exactly which records carry a credential. Replicating that map
    // would publish the locations of the very thing the subsystem exists to
    // contain — a disclosure hint travelling further than the disclosure. A
    // replica remediates from its own scan. See 0169's header.
    (
        "disclosure_actions",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "disclosure_affected_records",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "disclosure_incidents",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "discord_surface",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    ("doc_references", TableScope::ProjectScoped),
    ("event_read_tokens", TableScope::ProjectScoped),
    ("event_vibes", TableScope::ProjectScoped),
    ("events", TableScope::ProjectScoped),
    ("execution_trigger_sources", TableScope::ProjectScoped),
    // cairn/4735: the `after:` gates holding an execution. They are read
    // wherever the execution they hold is read -- the status projection derives
    // `held` from their presence -- so they share its scope.
    ("execution_gates", TableScope::ProjectScoped),
    // cairn/5943: when each queue lane last started work. Written beside the
    // release it records, in the database that owns the released execution.
    ("queue_lane_turns", TableScope::ProjectScoped),
    ("executions", TableScope::ProjectScoped),
    // cairn/5814: how each execution create resolved its recipe, the option
    // sets those resolutions chose among, and live edits to a step's
    // allotment. Read wherever the execution is, so they share its scope.
    ("recipe_resolutions", TableScope::ProjectScoped),
    ("recipe_option_sets", TableScope::ProjectScoped),
    ("execution_agent_edits", TableScope::ProjectScoped),
    // cairn/5935: conditions on a pending ask (a deferred gated artifact). Read
    // and written wherever the artifact they defer is, so they share its scope.
    ("ask_conditions", TableScope::ProjectScoped),
    // cairn/5920: a node's or thread's placement default. Read and written
    // wherever its job or thread is, so it shares their scope.
    ("placement_defaults", TableScope::ProjectScoped),
    // cairn/5597: an execution's delegated packets, moved out of its snapshot.
    // Read and written wherever the execution is, so they share its scope.
    ("delegated_packets", TableScope::ProjectScoped),
    ("attachments", TableScope::ProjectScoped),
    ("file_changes", TableScope::ProjectScoped),
    // Superseded by `attachments` and no longer read or written. It stays in the
    // schema because destructive changes to synced replica tables break Turso
    // sync triggers; see 0225_attachments.sql.
    ("image_refs", TableScope::ProjectScoped),
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
    // cairn/5387: branch preparation's acceptance journal, beside the reconcile
    // tables it supersedes; rows describe this store's installs.
    ("branch_advance_acceptances", TableScope::ProjectScoped),
    // cairn/6173: this runner's durable fences on its own store's bookmarks.
    ("jj_branch_recoveries", TableScope::ProjectScoped),
    // cairn/6080: the operator-set merge catch-up mode of one store bookmark,
    // and the audit trail of its every set, clear, use and review.
    ("branch_catch_up_modes", TableScope::ProjectScoped),
    ("branch_catch_up_events", TableScope::ProjectScoped),
    // cairn/5222: the identifiers this installation's store bound at landing.
    // A landing runs against this machine's store, so the record of what it
    // allocated is this installation's own.
    (
        "landing_allocations",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    ("job_file_activity", TableScope::ProjectScoped),
    ("job_presence", TableScope::ProjectScoped),
    ("jobs", TableScope::ProjectScoped),
    ("labels", TableScope::ProjectScoped),
    ("memories", TableScope::ProjectScoped),
    ("memory_triage_issue_memories", TableScope::ProjectScoped),
    ("merge_requests", TableScope::ProjectScoped),
    ("merge_attempt_events", TableScope::ProjectScoped),
    ("human_base_merge_policy_events", TableScope::ProjectScoped),
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
    ("provider_limit_holds", TableScope::ProjectScoped),
    // Which records this install withholds from serving, and why. Private for
    // the same reason as the incident tables above.
    (
        "quarantined_records",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    ("queued_messages", TableScope::ProjectScoped),
    ("repl_exchanges", TableScope::ProjectScoped),
    ("resource_surfacings", TableScope::ProjectScoped),
    ("run_activity_minutes", TableScope::ProjectScoped),
    ("runs", TableScope::ProjectScoped),
    ("search_outbox", TableScope::ProjectScoped),
    ("session_skyline_cache", TableScope::ProjectScoped),
    ("sessions", TableScope::ProjectScoped),
    ("skill_configs", TableScope::ProjectScoped),
    ("camps", TableScope::ProjectScoped),
    ("camp_events", TableScope::ProjectScoped),
    ("suppressed_wakes", TableScope::ProjectScoped),
    // Where THIS operator has read up to in each thread's transcript. The thread
    // is shared; the reading of it is not. Replicating this would let a
    // teammate's attention clear the unread badge on someone else's sidebar,
    // which is the opposite of what the marker means.
    (
        "thread_read_positions",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // What THIS operator closed in their notification center. The facts behind
    // the entries are shared; the decision to stop looking at one is not.
    // Replicating it would let a teammate's dismissal hide an entry from someone
    // who never saw it.
    (
        "operator_notification_dismissals",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    ("tenet_evidence", TableScope::ProjectScoped),
    ("tenet_evidence_events", TableScope::ProjectScoped),
    ("tenet_ruling_imports", TableScope::ProjectScoped),
    ("tenets", TableScope::ProjectScoped),
    ("threads", TableScope::ProjectScoped),
    ("todos", TableScope::ProjectScoped),
    ("token_rollup", TableScope::ProjectScoped),
    ("token_rollup_runs", TableScope::ProjectScoped),
    ("tool_analytics_hourly", TableScope::ProjectScoped),
    ("tool_invocation_runs", TableScope::ProjectScoped),
    ("tool_invocations", TableScope::ProjectScoped),
    ("turn_end_check_attempts", TableScope::ProjectScoped),
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
    // The largest single process and the out-of-memory kills learned per
    // suite and machine context, for memory fit. Machine-local and rebuildable
    // for the same reason as the profiles beside it.
    (
        "suite_memory_evidence",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // Which resident out-of-memory kills the suite evidence above already
    // counts; machine-local and rebuildable with it.
    (
        "resident_kills_learned",
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
    // How recent completions of a command ended on each machine (0264). Same
    // category again: what happened HERE, rebuildable by running the work.
    (
        "command_outcome_history",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // The inference records (0268): how this install's inference machines run
    // their models, written by its operator and rebuilt from nowhere.
    (
        "inference_profiles",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    (
        "inference_deployments",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    (
        "inference_machines",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    // Placement's preferences for the pools it detects here (cairn/5728):
    // each pool's warm model and idle override, and the standing pins.
    (
        "model_pool_preferences",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    (
        "model_pins",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    // The models those profiles serve, by real name (cairn/5730).
    (
        "inference_models",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    // What verifying a profile on this install's pools measured (cairn/5730).
    (
        "inference_verifications",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // The profile generations the operator started on this install's pools
    // (cairn/5885).
    (
        "inference_generations",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // Which inventory profiles a generation wrote and whether a verification
    // pass promoted that exact revision (cairn/6093). It follows the profiles
    // it guards, so it is host configuration rather than runner state.
    (
        "inference_generated_profiles",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    // Which of this runner's runs a generation holds to its scope (cairn/6093):
    // authority this install's own launches recorded, which no replica supplies.
    (
        "inference_generation_runs",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // What a generation regenerating a profile in place captured of it at
    // launch (cairn/6093): authority this install's own launch recorded about
    // its own inventory.
    (
        "inference_generation_replacements",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // What this runner has commanded its inference machines to do (cairn/5660):
    // the phase of each deployment it acted on, and the loads it measured.
    (
        "inference_deployment_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "inference_deployment_loads",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // Placement's queue of model requests: what this orch was asked of its
    // own inference machines.
    (
        "inference_requests",
        TableScope::Private(PrivateReason::RunnerTransient),
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
    // An operator's declaration about one attached machine. It describes this
    // installation's own fleet and means nothing on another.
    (
        "executor_repairs",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // What one attached machine failed to prepare. Like a repair, a fact about
    // this installation's own fleet.
    (
        "executor_preparation_refusals",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // A capability a project's work was seen to need on one of this
    // installation's machines. Its evidence names those machines.
    (
        "project_learned_requirements",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // The operator's trust in a checkout's library definitions on this
    // install, and the authority they confirmed. One operator's decision about
    // one machine's checkout, so never synced.
    (
        "project_library_trust",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    (
        "project_library_confirmations",
        TableScope::Private(PrivateReason::HostConfiguration),
    ),
    // Work this runner granted to one of its own machines and has not yet seen
    // end. Names executor identities that mean nothing on another install.
    (
        "purpose_debts",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // A routed batch's commit awaiting republication onto its branch. Names
    // store paths on this machine, so it means nothing on another.
    (
        "held_publications",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "github_app",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "github_installations",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // The install's relay channel (cairn/5129): its secret, keypair and cursor.
    (
        "relay_channel",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    // Ingress endpoint events and this install's endpoint mirror (0267). The
    // endpoints belong to this install's relay channel, and the events were
    // sealed to its key, like the GitHub App credentials beside them.
    (
        "ingress_events",
        TableScope::Private(PrivateReason::IdentityCredential),
    ),
    (
        "ingress_endpoints",
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
    // The running app's build identity and its pending per-recipient rebuild
    // notices describe this operator installation, never shared project state.
    (
        "app_boot_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "archival_backfill_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // Where each archival maintenance pass over this install's database
    // resumes (the sweep watermark, the archive offload's cursors), and the
    // archive root this database's stored bytes live in. That `archive` row is
    // what stops boot from standing an empty archive in for a missing one, so
    // the table is not clearable runner state.
    (
        "archival_progress",
        TableScope::Private(PrivateReason::HostArchive),
    ),
    // The catalog of history packs in this machine's archive, and the runs and
    // sessions each covers. The store cannot be enumerated, so these rows are
    // the only list.
    (
        "history_tier",
        TableScope::Private(PrivateReason::HostArchive),
    ),
    (
        "history_tier_runs",
        TableScope::Private(PrivateReason::HostArchive),
    ),
    (
        "history_tier_sessions",
        TableScope::Private(PrivateReason::HostArchive),
    ),
    (
        "delegated_packet_backfill_state",
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
    // A main-cadence lane pass this runner's own restart cancelled, held only
    // until the next boot's re-arm cites it.
    (
        "main_cadence_interruptions",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // This runner's claims on remote check attempts it was granted, and the
    // consumers waiting on each. A claim names this install's executor and
    // request ids and is only adoptable by the runner that granted it, so it
    // never syncs; the waiters cascade from their claim.
    (
        "check_lane_adoptions",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "check_lane_adoption_waiters",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // This install's rebuild history and idempotent journal receipts.
    (
        "rebuilds",
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
    (
        "runtime_admission_requests",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "runtime_admission_cursors",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "prepared_runtime_calls",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // Ordinary-session launches waiting in runner-local admission, and the
    // remote authority a team launch holds while it waits. Both cascade from
    // `runtime_admission_requests` and describe this runner's queue only.
    (
        "prepared_ordinary_launches",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "team_ordinary_admission_authority",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "wake_schedules",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-4107",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
    // The condition half of the same wake surface as `wake_schedules` above:
    // identical `home_kind`/`home_id` addressing, written and evaluated by the
    // same runner. It shares that table's deferral for the same reason and must
    // move with it, not separately.
    (
        "wake_conditions",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-4107",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
    // A thread's service packages are thread content, so they lean shared; but
    // a service runs only on this runner's machine until resident processes
    // reach the callback relay (cairn/5562), so a team copy would be source no
    // teammate's runner can serve.
    (
        "thread_services",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-5524",
            target: ScopeTarget::ProjectScoped,
        }),
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
    // The fact log is this runner's dispatch queue and the index its event
    // reads cite (0238). Team replication is a follow-on.
    (
        "fact_log",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "fact_log_wakes",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // The ambient route's label set is swept from this runner's fact log and
    // firing journal, and sits beside them for the same reason.
    (
        "ambient_routing_feedback",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "ambient_routing_feedback_cursor",
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
    // Provider message handles and delivery/pin retries belong to the runner
    // driving the channel, just like its outbound queue.
    (
        "channel_state_card",
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
        "channel_conversation_binding",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "channel_ask_resolution",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "channel_ask_action",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // A terminal child makes parent turns compactable; the mark sits in this
    // queue until an expiry or ratio trigger applies it. Local to the runner
    // that owns the thread's agent session.
    (
        "thread_compaction_marks",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // A Codex watchdog lease is ownership held by one runner boot, and the
    // lifecycle rows are that runner's evidence of what it armed and expired.
    // Both are keyed by the boot that wrote them, so they mean nothing on
    // another host.
    (
        "codex_watchdog_leases",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    (
        "codex_watchdog_lifecycle",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // On the private database `cas_cache` is the authoritative home of local
    // attachment bytes, not a cache: nothing refetches them. (A team store's
    // read-through copies land here too, and those are refetchable.)
    (
        "cas_cache",
        TableScope::Private(PrivateReason::AuthoritativeContent {
            owner: "cairn/4648",
        }),
    ),
    // ── Private: rebuildable / refetchable caches ────────────────────────────
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
        "thread_eras",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    (
        "thread_era_children",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    (
        "ci_logs_cache",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // ── Private: deferred-shared (lean is shared, move tracked by an owner) ───
    (
        "posts",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-4106",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
    (
        "post_comments",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-4106",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
    (
        "feed_cursors",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-4106",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
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
    // embedding_state: this host's sweep progress, alongside the
    // archival-backfill progress the same reason already covers. It describes
    // when THIS install began participating and which space its own vectors
    // are in, so it is meaningless elsewhere.
    (
        "embedding_state",
        TableScope::Private(PrivateReason::RunnerTransient),
    ),
    // session_positions: live semantic positions folded from this install's
    // embeddings; shares resource_embeddings' deferral.
    (
        "session_positions",
        TableScope::Private(PrivateReason::DeferredShared {
            issue: "CAIRN-2210",
            target: ScopeTarget::ProjectScoped,
        }),
    ),
    // vibe_axes: colour axes learned from this install's own history in one
    // model's space; a workspace without them relearns them from its own.
    (
        "vibe_axes",
        TableScope::Private(PrivateReason::RebuildableCache),
    ),
    // vibe_axis_samples: the resumable work of a recompute in progress.
    (
        "vibe_axis_samples",
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
        table: "turn_end_check_attempts",
        id_columns: &["id", "job_id"],
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
        table: "run_batch_detached",
        id_columns: &["job_id", "run_id"],
    },
    RekeyTableManifest {
        table: "run_batch_adopted_outcomes",
        id_columns: &["job_id"],
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
        table: "delivery_receipts",
        id_columns: &[
            "id",
            "recipient_id",
            "project_id",
            "message_id",
            "delivered_job_id",
            "push_id",
        ],
    },
    RekeyTableManifest {
        table: "main_incident_sources",
        id_columns: &["project_id", "observation_id"],
    },
    RekeyTableManifest {
        table: "main_incident_budget",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "main_incidents",
        id_columns: &["project_id", "issue_id"],
    },
    RekeyTableManifest {
        table: "main_incident_evidence",
        id_columns: &["project_id", "issue_id", "observation_id", "comment_id"],
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
        table: "manual_check_handles",
        id_columns: &["project_id", "observation_id"],
    },
    RekeyTableManifest {
        table: "check_test_results",
        id_columns: &["observation_id"],
    },
    RekeyTableManifest {
        table: "check_observation_relabels",
        id_columns: &["observation_id", "project_id"],
    },
    RekeyTableManifest {
        table: "check_deferred_tests",
        id_columns: &["observation_id"],
    },
    RekeyTableManifest {
        table: "check_audit_findings",
        id_columns: &["observation_id"],
    },
    RekeyTableManifest {
        table: "compile_facts",
        id_columns: &["id", "project_id", "merge_request_id", "observation_id"],
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
        table: "pr_publish_attempts",
        id_columns: &["id", "action_run_id"],
    },
    RekeyTableManifest {
        table: "job_artifact_inputs",
        id_columns: &["job_id", "producer_job_id"],
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
        table: "execution_supersessions",
        id_columns: &[
            "id",
            "issue_id",
            "superseded_execution_id",
            "successor_execution_id",
        ],
    },
    RekeyTableManifest {
        table: "execution_trigger_sources",
        id_columns: &["id", "source_job_id", "triggered_execution_id"],
    },
    RekeyTableManifest {
        table: "execution_gates",
        id_columns: &["id", "execution_id"],
    },
    // `lane` embeds a thread or issue id behind a prefix and stays bare: a
    // moved project's lanes read as never served, which costs one round.
    RekeyTableManifest {
        table: "queue_lane_turns",
        id_columns: &["execution_id"],
    },
    RekeyTableManifest {
        table: "ask_conditions",
        id_columns: &["id", "ask_id", "job_id", "issue_id"],
    },
    RekeyTableManifest {
        table: "placement_defaults",
        id_columns: &["id", "job_id", "thread_id"],
    },
    RekeyTableManifest {
        table: "executions",
        id_columns: &["id", "issue_id", "project_id"],
    },
    RekeyTableManifest {
        table: "recipe_resolutions",
        id_columns: &[
            "id",
            "project_id",
            "issue_id",
            "execution_id",
            "supersedes_execution_id",
        ],
    },
    RekeyTableManifest {
        table: "recipe_option_sets",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "execution_agent_edits",
        id_columns: &["id", "execution_id"],
    },
    RekeyTableManifest {
        table: "delegated_packets",
        id_columns: &[
            "execution_id",
            "id",
            "parent_job_id",
            "parent_turn_id",
            "result_artifact_job_id",
        ],
    },
    RekeyTableManifest {
        table: "file_changes",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "attachments",
        id_columns: &["project_id", "thread_id"],
    },
    RekeyTableManifest {
        table: "image_refs",
        id_columns: &["project_id"],
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
            "camp_id",
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
        table: "branch_advance_acceptances",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "jj_branch_recoveries",
        id_columns: &["project_id"],
    },
    RekeyTableManifest {
        table: "branch_catch_up_modes",
        id_columns: &["project_id", "job_id"],
    },
    RekeyTableManifest {
        table: "branch_catch_up_events",
        id_columns: &["project_id"],
    },
    // job_file_activity.id is the composite `{event_id}:{tool_use_id}:{ordinal}`
    // (a non-uuid, so rekey_to_team leaves it as-is); job_id is the routable
    // column that gets re-prefixed on a local->team promotion.
    RekeyTableManifest {
        table: "job_file_activity",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "job_presence",
        id_columns: &["job_id", "project_id"],
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
            "turn_end_check_attempt_id",
            "resume_session_id",
            "turn_end_check_attempt_id",
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
        table: "merge_attempt_events",
        id_columns: &[
            "event_id",
            "attempt_id",
            "project_id",
            "merge_request_id",
            "job_id",
        ],
    },
    RekeyTableManifest {
        table: "human_base_merge_policy_events",
        id_columns: &["event_id", "project_id"],
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
        table: "provider_limit_holds",
        id_columns: &["id", "job_id", "run_id", "turn_id"],
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
        table: "tenet_evidence",
        id_columns: &[
            "id",
            "project_id",
            "tenet_id",
            "event_id",
            "dismissed_by_thread_id",
        ],
    },
    RekeyTableManifest {
        table: "tenet_evidence_events",
        id_columns: &["id", "project_id"],
    },
    RekeyTableManifest {
        table: "tenet_ruling_imports",
        id_columns: &["thread_id", "project_id"],
    },
    RekeyTableManifest {
        table: "tenets",
        id_columns: &["id", "project_id", "proposing_thread_id"],
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
        table: "run_activity_minutes",
        id_columns: &["run_id"],
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
        table: "tool_analytics_hourly",
        id_columns: &["id", "project_id", "run_id", "session_id"],
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
        table: "turn_end_check_attempts",
        id_columns: &["id", "job_id"],
    },
    RekeyTableManifest {
        table: "turns",
        id_columns: &["id", "run_id", "job_id", "predecessor_id"],
    },
    RekeyTableManifest {
        table: "wake_subscriptions",
        id_columns: &["id", "job_id", "camp_id", "subject_key"],
    },
    RekeyTableManifest {
        table: "camps",
        id_columns: &["id", "project_id", "lead_thread_id"],
    },
    RekeyTableManifest {
        table: "camp_events",
        id_columns: &["camp_id", "subject_key"],
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

    /// The name half of every `version_name` entry `MigrationRunner::run` reports.
    ///
    /// The two lineage pins below list names rather than full stems on purpose.
    /// What they exist to catch is a silent change in apply ORDER or MEMBERSHIP,
    /// and neither is derivable from the registry: order here is authored, not
    /// ascending. The private tail interleaves shared-tail migrations with
    /// private-only ones in the order they were written, so several pairs run out
    /// of numeric order by design, and no computation over the registry can tell a
    /// correct order from one somebody quietly changed. Versions are the opposite
    /// — wholly derivable, and already checked twice over: `migration!` splits
    /// each one out of its filename at compile time, and `check:migrations` proves
    /// the files are contiguously numbered and each registered. Restating them
    /// here would buy nothing and would mean that renumbering a migration, routine
    /// while the version is a globally contended counter, had to be retyped in
    /// every entry below.
    fn applied_names(applied: &[String]) -> Vec<&str> {
        applied
            .iter()
            .map(|entry| {
                entry
                    .split_once('_')
                    .expect("the runner reports applied migrations as version_name")
                    .1
            })
            .collect()
    }

    /// Every migration is registered exactly once in each lineage it belongs to.
    ///
    /// Nothing else checks this. `MigrationRunner::run` skips a version it has
    /// already recorded, so a migration composed into one lineage twice applies
    /// once and the lineage pins stay byte-identical to a correct registry;
    /// `check:migrations` collects registrations into a set, where the duplicate
    /// collapses too. A lineage is assembled from per-migration macros whose
    /// invocation says nothing about where else it is invoked, so composing one
    /// twice is an easy edit to make and, until this test, an invisible one.
    ///
    /// Names are held to the same rule as versions because the lineage pins
    /// identify migrations by name; two migrations sharing one would make those
    /// pins ambiguous about which had moved.
    #[test]
    fn each_migration_is_registered_once_per_lineage() {
        for (lineage, registry) in [("private", TURSO_MIGRATIONS), ("team", TEAM_MIGRATIONS)] {
            for (field, values) in [
                (
                    "version",
                    registry.iter().map(|m| m.version).collect::<Vec<_>>(),
                ),
                (
                    "name",
                    registry.iter().map(|m| m.name()).collect::<Vec<_>>(),
                ),
            ] {
                let mut sorted = values;
                sorted.sort_unstable();
                let mut duplicates = sorted
                    .windows(2)
                    .filter(|pair| pair[0] == pair[1])
                    .map(|pair| pair[0])
                    .collect::<Vec<_>>();
                duplicates.dedup();

                assert!(
                    duplicates.is_empty(),
                    "the {lineage} lineage registers {field} more than once: {duplicates:?}"
                );
            }
        }
    }

    /// The post-cutover stems one lineage takes, read from the directory at test
    /// time through the same grammar `build.rs` used, so the compiled tail is
    /// checked against the files rather than against itself.
    fn directory_tail(team: bool) -> Vec<String> {
        let directory =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../turso_migrations");
        let mut files: Vec<(String, String)> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    std::fs::read_to_string(entry.path()).unwrap_or_default(),
                )
            })
            .collect();
        files.sort();
        crate::storage::migration_files::derive_tail(&files)
            .unwrap()
            .into_iter()
            .filter(|migration| {
                if team {
                    migration.headers.lineage.reaches_team()
                } else {
                    migration.headers.lineage.reaches_private()
                }
            })
            .map(|migration| migration.stem)
            .collect()
    }

    /// Holds an applied lineage to its frozen pin, then its tail to the
    /// directory.
    ///
    /// The pin is history: every migration up to the cutover, in the order the
    /// lineage composes them. Nothing is added to it again. Every migration after
    /// it is a file whose header names its lineage, appended in number order
    /// (provisional `NEXT` stems last); the tail is asserted against the
    /// directory rather than written down here, which is what lets two branches
    /// add migrations without touching one shared line.
    #[track_caller]
    fn assert_lineage_pin(lineage: &str, applied: &[String], pinned: &[&str]) {
        let names = applied_names(applied);
        assert!(
            names.len() >= pinned.len(),
            "the {lineage} lineage applies {} migrations but its frozen history pins {}",
            names.len(),
            pinned.len()
        );
        assert_eq!(
            &names[..pinned.len()],
            pinned,
            "the {lineage} lineage's frozen history changed. Migrations up to {:04} are composed \
             by hand and pinned here, and neither is edited again; a new migration is a \
             `NEXT_name.sql` file with a `-- lineage:` header",
            crate::storage::migration_files::CUTOVER
        );
        // The runner applied the registry in order (`migrated_db` holds it to
        // that), so the tail's stems are the registry's past the pin.
        let registry = if lineage == "team" {
            TEAM_MIGRATIONS
        } else {
            TURSO_MIGRATIONS
        };
        assert_eq!(applied.len(), registry.len());
        let tail: Vec<String> = registry[pinned.len()..]
            .iter()
            .map(|migration| {
                if crate::storage::migration::is_provisional_version(migration.version()) {
                    migration.version().to_string()
                } else {
                    format!("{}_{}", migration.version(), migration.name())
                }
            })
            .collect();
        assert_eq!(
            tail,
            directory_tail(lineage == "team"),
            "the {lineage} lineage's derived tail is not the post-cutover files of \
             turso_migrations/ in number order"
        );
    }

    /// The compiled tail lists exactly what the directory holds, so a build that
    /// missed a new file (a stale `build.rs` run) fails here.
    #[test]
    fn the_compiled_tail_is_the_directory() {
        assert_eq!(DERIVED_PRIVATE_TAIL, directory_tail(false).as_slice());
        assert_eq!(DERIVED_TEAM_TAIL, directory_tail(true).as_slice());
        let registered: Vec<&str> = TURSO_MIGRATIONS
            .iter()
            .rev()
            .take(DERIVED_PRIVATE_TAIL.len())
            .map(|migration| migration.name())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let expected: Vec<&str> = DERIVED_PRIVATE_TAIL
            .iter()
            .map(|stem| stem.split_once('_').map_or(*stem, |(_, name)| name))
            .collect();
        assert_eq!(registered, expected);
    }

    /// The full private lineage applied to a fresh database.
    ///
    /// Checks only that the runner applied every registered migration once, in
    /// registry order. Which migrations those are, and in what order, is
    /// `private_migrations_apply_in_order`'s pin, so adding a migration fails
    /// that one test rather than every test that merely needs the real schema.
    async fn migrated_db() -> DbResult<LocalDb> {
        let temp = tempdir()?;
        let path = temp.keep().join("cairn-real-turso-schema.db");
        let db = LocalDb::open(path).await?;
        let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await?;
        let registered: Vec<String> = TURSO_MIGRATIONS
            .iter()
            .map(|migration| format!("{}_{}", migration.version(), migration.name()))
            .collect();
        assert_eq!(applied, registered);
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('projects') WHERE name='human_base_merge_policy'"
            )
            .await?,
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='merge_attempt_events'"
            )
            .await?,
            1
        );
        Ok(db)
    }

    #[tokio::test]
    async fn private_migrations_apply_in_order() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("private.turso.db"))
            .await
            .unwrap();
        let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        assert_lineage_pin(
            "private",
            &applied,
            &[
                "initial_schema",
                "search_outbox",
                "seed_default_workspace",
                "add_issue_dependencies",
                "change_preview_events",
                "uri_segments",
                "add_uri_segment_to_prompts",
                "add_job_id_to_prompts",
                "cohere_embeddings",
                "anon_device",
                "session_current_pos",
                "resource_surfacings",
                "memory_redux",
                "add_tool_use_id_to_prompts",
                "add_artifact_confirmed",
                "remove_ready_status",
                "messages_delivered_at",
                "pr_node_port_fires",
                "merge_request_owner",
                "add_uri_segment_to_action_runs",
                "vibe_axes",
                "add_segments_to_permission_requests",
                "add_labels",
                "add_parent_issue",
                "remove_managers",
                "child_side_channel_notices",
                "session_channel_cursor",
                "issue_parent_job",
                "queued_messages",
                "checkpoint_runs",
                "drop_dead_chats_table",
                "drop_workspaces_timezone_column",
                "annotations",
                "annotation_message_links",
                "annotation_uri_seq",
                "wake_subscriptions",
                "unify_side_channel_notices",
                "drop_annotation_tables",
                "memory_intake_ledger",
                "add_is_workspace_to_projects",
                "memory_triage_batches_and_drop_when_to_use",
                "memory_scope_node_id_and_status_lattice",
                "memory_triage_decision",
                "jobs_memory_review_state",
                "memory_canon_v2_consolidation",
                "memory_review_sent_state",
                "add_message_urgency",
                "add_event_thinking_tokens",
                "event_vibes_session_id",
                "session_skyline_cache_vibe_watermark",
                "clear_skyline_cache_for_content_bar_decomposition",
                "clear_skyline_cache_for_visual_height_sizing",
                "job_pack_anchor",
                "archival_storage",
                "archival_backfill_state",
                "archival_blobs",
                "event_read_tokens",
                "terminal_exit_wakes",
                "jobs_needs_fresh_session",
                "attention_items",
                "attention_escalate_at",
                "attention_fingerprint",
                "comment_seq",
                "clear_skyline_cache_for_system_event_filter",
                "merge_request_is_local",
                "config_disables",
                "tool_invocations",
                "job_browsers",
                "add_event_cost_usd",
                "attention_pushes",
                "attention_push_fingerprint",
                "merge_request_head_sha",
                "attention_read_cursors",
                "drop_attention_ledger",
                "drop_messages_delivered_at",
                "terminal_output_wakes",
                "event_content_change_id",
                "browser_last_active_at",
                "index_runs_session_id_created_at",
                "token_rollup",
                "drop_runs_backend",
                "team_routing",
                "cas_cache",
                "archival_pack_hash",
                // CAIRN-2270: the shared 0087 re-grain lives in the SHARED_TAIL,
                // which the macro emits BEFORE the private tail (0085/0086), so it
                // applies here between 0084 and 0085 despite its higher number.
                "token_rollup_hourly",
                "project_routes_local_path",
                "analytics_rollup_backfill_state",
                "reopen_analytics_backfill",
                "check_result_cache",
                "check_result_cache_input_hash",
                "drop_projects_server_id_and_servers",
                "tool_invocation_durations",
                "check_result_cache_job_id",
                "tool_invocation_result_backfill_state",
                "relink_merge_request_jobs",
                "jobs_child_base",
                "jobs_owns_ephemeral_worktree",
                "call_output_contract_and_run_tags",
                "workflow_progress",
                "check_result_cache_failure_kind",
                "executions_runner_device_id",
                "clear_invalid_job_worktree_paths",
                "workflow_journal",
                "workflow_restart_durability",
                "add_turn_end_reason",
                "index_hot_gui_status_queries",
                "normalize_openai_token_usage_components",
                "executor_enrollment",
                "check_result_cache_provenance",
                "executor_enrollment_expiry",
                "command_resource_profiles",
                "pack_catalog",
                "bind_agent_terminals_to_lifetime_leases",
                "add_jj_reconcile_intents",
                "add_agent_waits",
                "add_jj_reconcile_quarantines",
                "workflow_executor_anchor",
                "virtual_reconcile_coordinates",
                "virtual_workflow_coordinates",
                "integrity_sweep_state",
                "check_result_cache_recency_index",
                "session_account",
                "job_repls",
                "repl_exchanges",
                "rebind_terminals_to_residencies",
                "agent_waits_concurrent_calls",
                "image_refs",
                "write_replay_ledger",
                "mcp_continuation_prompts",
                "retire_snapshot_child_wakes",
                "unique_event_sequence",
                "check_result_cache_infra_suppression",
                "executor_enrollment_names",
                "check_result_observations",
                "check_definition_provenance",
                "conflict_resolution_sessions",
                "thread_compaction",
                "command_duration_profiles",
                "quarantine_saturation_memory_profiles",
                "channel_persistence",
                "channel_outbound_expired",
                "channel_thread_state",
                "channel_thread_suppression",
                "issue_kind",
                "pr_resolution_attribution",
                "add_check_observation_public_handle",
                "repair_check_observation_public_handle",
                "verdict_reuse_facts",
                "command_contention_profiles",
                "rebase_replay_status",
                "response_invocations",
                "route_firings",
                "channel_outbound_route_kind",
                "route_firing_content",
                "threads_entity",
                "migrate_issue_threads",
                "executor_desktop_automation",
                "thread_title_retires",
                "thread_compaction_seed_bytes",
                "thread_compaction_capacity_trigger",
                "clear_skyline_cache_for_compact_bars",
                "turn_embeddings",
                "turns_created_at_index",
                "turn_embedding_state",
                "check_result_cache_ran_at_millis",
                "authority_grants",
                "inherit_thread_id_for_child_jobs",
                "turn_end_check_attempts",
                "route_fact_samples",
                "build_change_ride_alongs",
                "check_result_cache_row_family",
                "disclosure_remediation",
                "lowercase_project_keys",
                "lowercase_project_routes",
                "canonicalize_uri_identity_columns",
                "channel_inbound_rejection_reason",
                "tool_analytics_hourly",
                "github_relay_delivery_health",
                "github_relay_pending_key",
                "discord_surface",
                "channel_conversation_binding",
                "channel_ask_resolution",
                "authority_attribution",
                "ask_resolution_provenance",
                "repair_attention_pushes_index_parity",
                "repair_prompts_column_order_parity",
                "issue_authorship",
                "codex_watchdog_ledger",
                "github_relay_key_rotation_time",
                "canonicalize_channel_permission_answers",
                "canonical_project_keys",
                "canonical_discord_surface_project_keys",
                "canonical_project_routes",
                "posts",
                "index_liveness_joins",
                "attention_push_retirement",
                "attention_push_retirement_backfill",
                "index_thread_status_rollup",
                "project_thread_number",
                "thread_read_positions",
                "job_file_activity",
                "job_presence",
                "codemap_cache",
                "runtime_admission",
                "prepared_runtime_calls",
                "wake_schedules",
                "index_thread_unread_scan",
                "thread_eras",
                "channel_ask_action_park",
                "check_result_cache_job_check_recency_index",
                "channel_outbound_retry",
                "project_home_indexes",
                "github_relay_unmapped_mentions",
                "wake_conditions",
                "repair_codex_watchdog_lifecycle_kind",
                "channel_state_card",
                "thread_automations",
                "kind_scoped_device_enrollment",
                "human_base_merge_policy",
                "merge_attempt_journal",
                "thread_branches",
                "write_replay_ownership",
                "retire_needs_fresh_session",
                "close_replaced_sessions",
                "home_prose_index",
                "executor_repairs",
                "attachments",
                "contended_identifier_obligations",
                "event_vibe_revision",
                "execution_canonical_lineage",
                "state_card_pin_attempts",
                "issue_resolution_generation",
                "attention_push_key_lookup",
                "executor_enrollment_supersession",
                "execution_queue",
                "response_invocation_account",
                "execution_queue_project_provisioning",
                "channel_agent_messages",
                "condition_evaluation_outcome",
                "prepared_ordinary_launches",
                "team_ordinary_admission_authority",
                "purge_poisoned_duration_profiles",
                "fact_log",
                "drop_route_fact_samples",
                "channel_outbound_last_attempt",
                "executor_preparation_refusals",
                "fact_log_wakes",
                "post_feed_watch_as_fact_clause",
                "account_credential_events",
                "permission_resume_error",
                "thread_compaction_unconfirmed_start_trigger",
                "terminal_exit_reason",
                "ref_store_maintenance",
                "prompt_withdrawal",
                "provider_limit_holds",
                "execution_after_gates",
                "retire_issue_dependencies",
                "reconcile_resolution_basis",
                "held_publications",
                "provider_limit_failover",
                "tenets",
                "tenet_ruling_imports",
                "tenet_evidence",
                "tenet_evidence_measured_by",
                "command_outcome_history",
                "pr_publish_attempts",
                "index_sibling_job_lookup",
                "inference_records",
                "object_store_maintenance",
                "ingress_events",
                "tenet_statements",
                "check_observation_platform_relabels",
                "subscription_periods",
                "runner_events",
                "job_artifact_inputs",
                "check_deferred_tests",
                "prepared_ordinary_launch_origin",
                "index_execution_gates_execution",
                "drop_check_test_outcomes_view",
                "execution_queue_build_volume_absent",
                "job_goal_intent",
                "purpose_debts",
                "compile_facts",
                "turn_end_check_attempt_plan",
                "embedding_space",
                "tenet_rationale",
                "command_resource_memory_kills",
                "run_activity_minutes",
            ],
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
        db.execute_script(format!(
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

    /// The vibe writer derives each new revision from the newest existing one,
    /// and the embed worker persists a batch one row at a time — so this
    /// subquery runs once per vibe. If it walks the table, a batch of M vibes
    /// against N accumulated ones costs O(M*N), which would trade a read-side
    /// win for a write-side regression (CAIRN-4710 review).
    ///
    /// The form is `ORDER BY revision DESC LIMIT 1` rather than
    /// `MAX(revision)` because that is what this engine answers by seeking the
    /// index edge; `MAX(indexed_col)` walks the index instead. Both return the
    /// same number, so only a plan assertion can witness the difference.
    #[tokio::test]
    async fn event_vibe_revision_is_assigned_from_the_index_edge() {
        const NEWEST_REVISION: &str =
            "SELECT revision FROM event_vibes ORDER BY revision DESC LIMIT 1";

        let db = migrated_db().await.unwrap();
        let plan = explain_plan(&db, NEWEST_REVISION).await;

        assert!(
            plan.iter()
                .any(|step| step.contains("idx_event_vibes_revision")),
            "the revision edge must be reached through its index, got {plan:?}"
        );
        // Deliberately NOT asserting the absence of "SCAN": this engine labels
        // an ordered covering-index read SCAN even though `LIMIT 1` stops it at
        // the first entry. That the cost really is constant is measured by
        // `assigning_a_vibe_revision_costs_the_same_at_any_table_size`; what
        // this test pins is that the index supplies the order, which is what
        // lets the limit stop early at all.
        assert!(
            !plan.iter().any(|step| step.contains("SORTER")),
            "the index order must satisfy ORDER BY without a sort, got {plan:?}"
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

    /// Rotation wrote only `replaced_by_id` to the session it replaced, so live
    /// installs carry predecessors that still read `open` with no terminal
    /// reason — two of the five sessions under CAIRN-4602's builder, whose rows
    /// this seeds verbatim. Closing a replaced row is safe by construction and
    /// closing anything else is not, so the backfill's edge is exactly
    /// `replaced_by_id IS NOT NULL`.
    #[tokio::test]
    async fn backfill_closes_only_the_sessions_something_replaced() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("close-replaced-sessions.turso.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.version != "0224")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','default','Cairn','cairn','/tmp/cairn',1,1);
            INSERT INTO jobs(id, project_id, status, created_at, updated_at)
              VALUES('j','p','failed',1,1);
            -- `replaced_by_id` and `jobs.current_session_id` are enforced
            -- references, so the lineage seeds newest-first and the job adopts
            -- its head afterwards.
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES('s5','j','codex','open',5,41,41);
            INSERT INTO sessions(id, job_id, backend, status, replaced_by_id,
                                 sequence, created_at, updated_at)
              VALUES('s4','j','codex','open','s5',4,31,40);
            INSERT INTO sessions(id, job_id, backend, status, terminal_reason, replaced_by_id,
                                 sequence, created_at, closed_at, updated_at)
              VALUES('s3','j','codex','closed','provider_silence_already_terminal_reconciled',
                     's4',3,1,30,30);
            UPDATE jobs SET current_session_id='s5' WHERE id='j';
            ",
        )
        .await
        .unwrap();

        MigrationRunner::new(vec![shared_tail_close_replaced_sessions!()])
            .run(&db)
            .await
            .unwrap();

        assert_eq!(
            query_text(
                &db,
                "SELECT status || ' ' || terminal_reason FROM sessions WHERE id='s4'"
            )
            .await
            .unwrap(),
            "closed rotated",
            "a session something replaced can never take another turn"
        );
        assert_eq!(
            query_i64(&db, "SELECT closed_at FROM sessions WHERE id='s4'")
                .await
                .unwrap(),
            40,
            "a backfilled close dates from the replacement, not from the migration"
        );
        assert_eq!(
            query_text(&db, "SELECT terminal_reason FROM sessions WHERE id='s3'")
                .await
                .unwrap(),
            "provider_silence_already_terminal_reconciled",
            "a row that already recorded why it died keeps that reason"
        );
        assert_eq!(
            query_text(&db, "SELECT status FROM sessions WHERE id='s5'")
                .await
                .unwrap(),
            "open",
            "the job's current session is not replaced, so the backfill leaves it alone — \
             a terminal job is not evidence its conversation is unusable"
        );
    }

    #[tokio::test]
    async fn pin_backfill_separates_pin_outcomes_from_delivery_failures() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("state-card-pin-attempts.turso.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.name() != "state_card_pin_attempts")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before).run(&db).await.unwrap();
        // `channel_state_card` is runner-local and references nothing, so the
        // rows stand alone. All four share one conversation, because that is the
        // scope in which a later successful pin is read as repairing an earlier
        // failure. 1_000_000 is the table's newest `updated_at`, the clock the
        // one-day revival window is measured against; card 1 is the trap, pinned
        // long before card 2's failure but edited long after it.
        db.execute_script(
            "
            INSERT INTO channel_state_card(provider, conversation, subject_uri, target_uri,
                project_key, job_id, pinned, status, attempts, next_attempt_at, last_error,
                created_at, updated_at)
            VALUES
              ('discord','c','cairn://p/cairn/1/1/a','cairn://p/cairn/1','cairn','j1',
               1,'settled',0,0,NULL,100,1000000),
              ('discord','c','cairn://p/cairn/2/1/a','cairn://p/cairn/2','cairn','j2',
               0,'failed',8,123,'could not pin Discord message: Missing Permissions',500,999000),
              ('discord','c','cairn://p/cairn/3/1/a','cairn://p/cairn/3','cairn','j3',
               0,'failed',8,123,'could not pin Discord message: Missing Permissions',200,800000),
              ('discord','c','cairn://p/cairn/4/1/a','cairn://p/cairn/4','cairn','j4',
               0,'failed',8,123,'Discord edit failed: 500',300,950000);
            ",
        )
        .await
        .unwrap();

        MigrationRunner::new(vec![private_state_card_pin_attempts!()])
            .run(&db)
            .await
            .unwrap();

        /// One card's whole pin-and-delivery outcome as `status
        /// pin_attempted_at pin_error attempts last_error`, absent values
        /// rendered as `-1`/`-`.
        macro_rules! outcome {
            ($subject:literal) => {
                concat!(
                    "SELECT status || ' ' || COALESCE(pin_attempted_at, -1) || ' '",
                    " || COALESCE(pin_error, '-') || ' ' || attempts || ' '",
                    " || COALESCE(last_error, '-') FROM channel_state_card",
                    " WHERE subject_uri = 'cairn://p/cairn/",
                    $subject,
                    "/1/a'"
                )
            };
        }

        assert_eq!(
            query_text(&db, outcome!("1")).await.unwrap(),
            "settled 100 - 0 -",
            "a card that is already pinned has spent its one attempt, so the first \
             sweep after upgrade must not pin it again — and it is dated by its \
             creation, not by its last edit at 1000000, which would forge a pin \
             success newer than card 2's failure and hide a live permission gap"
        );
        assert_eq!(
            query_text(&db, outcome!("2")).await.unwrap(),
            "live 500 could not pin Discord message: Missing Permissions 0 -",
            "a card the pin killed within the last day is revived to keep editing, \
             carrying the pin error in its own slot"
        );
        assert_eq!(
            query_text(&db, outcome!("3")).await.unwrap(),
            "failed 200 could not pin Discord message: Missing Permissions 8 \
             could not pin Discord message: Missing Permissions",
            "an older pin-killed card records the attempt but stays failed: its agent \
             is long finished and reviving it would re-edit a historical thread"
        );
        assert_eq!(
            query_text(&db, outcome!("4")).await.unwrap(),
            "failed -1 - 8 Discord edit failed: 500",
            "a genuine delivery failure never attempted a pin, so it keeps its \
             retry state and gains no pin record"
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
    /// text depends on how an object reached its shape, and the two lineages
    /// build some tables by different routes (one `CREATE`, or a `CREATE` plus
    /// `ALTER TABLE ADD COLUMN`). Re-rendering inserts a space before `(id)` in
    /// `REFERENCES x(id)` and collapses trigger-body newlines; whether `ADD
    /// COLUMN` re-renders the statement (spacing every list comma) or splices the
    /// column into the authored text varies by Turso revision. Byte-equality
    /// therefore normalizes whitespace, the space before a paren and the space
    /// after a comma. All of these differences are purely cosmetic.
    fn norm(sql: &str) -> String {
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace(" (", "(")
            .replace(", ", ",")
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
        assert_lineage_pin(
            "team",
            &applied,
            &[
                "team_initial_schema",
                "labels_read_completeness",
                // CAIRN-2629: team-only device_presence table (head migration).
                "device_presence",
                "remote_intents",
                // Non-secret fleet advertisements are team-visible; enrollment
                // credentials and grant consumption remain private.
                "executor_registry",
                "elastic_executor_inventory",
                "ask_resolution_provenance",
                // Shared-tail migrations land in the team lineage after the team
                // head, preserving one shared SQL source for project-scoped tables.
                "archival_pack_hash",
                // CAIRN-2270: the token_rollup hour re-grain is a shared-table
                // change, so it lands in the team lineage too.
                "token_rollup_hourly",
                "check_result_cache",
                "check_result_cache_input_hash",
                "tool_invocation_durations",
                "check_result_cache_job_id",
                "relink_merge_request_jobs",
                "jobs_child_base",
                "jobs_owns_ephemeral_worktree",
                "call_output_contract_and_run_tags",
                "workflow_progress",
                "check_result_cache_failure_kind",
                // CAIRN-2629: executions.runner_device_id is a shared column.
                "executions_runner_device_id",
                "clear_invalid_job_worktree_paths",
                "add_turn_end_reason",
                "index_hot_gui_status_queries",
                "check_result_cache_provenance",
                "pack_catalog",
                "bind_agent_terminals_to_lifetime_leases",
                "add_jj_reconcile_intents",
                "add_agent_waits",
                "add_jj_reconcile_quarantines",
                "virtual_reconcile_coordinates",
                "check_result_cache_recency_index",
                "session_account",
                "job_repls",
                "repl_exchanges",
                "rebind_terminals_to_residencies",
                "agent_waits_concurrent_calls",
                "image_refs",
                "mcp_continuation_prompts",
                "retire_snapshot_child_wakes",
                "unique_event_sequence",
                "check_result_cache_infra_suppression",
                "check_result_observations",
                "check_definition_provenance",
                "conflict_resolution_sessions",
                "issue_kind",
                "pr_resolution_attribution",
                "add_check_observation_public_handle",
                "repair_check_observation_public_handle",
                "verdict_reuse_facts",
                "rebase_replay_status",
                "threads_entity",
                "migrate_issue_threads",
                "thread_title_retires",
                "turns_created_at_index",
                "check_result_cache_ran_at_millis",
                "inherit_thread_id_for_child_jobs",
                "turn_end_check_attempts",
                "check_result_cache_row_family",
                "lowercase_project_keys",
                "tool_analytics_hourly",
                "issue_authorship",
                "canonical_project_keys",
                "index_liveness_joins",
                "attention_push_retirement",
                "index_thread_status_rollup",
                "project_thread_number",
                // CAIRN-4226: `job_file_activity` is ProjectScoped, so the shared
                // tail composes it into this lineage as well as the private one.
                "job_file_activity",
                "job_presence",
                // CAIRN-4331: the sidebar unread-scan partial index covers a
                // shared table, so it lands in both lineages.
                "index_thread_unread_scan",
                "human_base_merge_policy",
                "merge_attempt_journal",
                "thread_branches",
                // `jobs` loses the fresh-session flag in both lineages
                // (CAIRN-4618).
                "retire_needs_fresh_session",
                // CAIRN-4613: `sessions` is a shared table, so the backfill that
                // closes rotated-out rows lands in both lineages.
                "close_replaced_sessions",
                // CAIRN-4647: durable attachments are addressed identically from
                // a private database and a team replica, so the one record table
                // reaches both lineages.
                "attachments",
                // CAIRN-4710: `event_vibes.revision` and the skyline cache column
                // it renames both live in either lineage, so the change is shared.
                "event_vibe_revision",
                // CAIRN-4665: canonical execution lineage. `executions` and
                // `runs` are shared tables and the supersession audit describes
                // them, so all three land in both lineages.
                "execution_canonical_lineage",
                // CAIRN-4767: `issues` is shared project state, so the
                // resolution generation reaches both lineages.
                "issue_resolution_generation",
                // CAIRN-4841: `condition_evaluations` is shared project state,
                // so the routed/blocked outcome reaches both lineages.
                "condition_evaluation_outcome",
                // cairn/4913: Posts watches become a fact clause in both.
                "post_feed_watch_as_fact_clause",
                // cairn/4936: `permission_requests` is shared project state, so
                // the resume refusal reaches both lineages.
                "permission_resume_error",
                "terminal_exit_reason",
                // cairn/4955: questions can be withdrawn in both.
                "prompt_withdrawal",
                "provider_limit_holds",
                // cairn/4735: `after:` gates hang off ProjectScoped executions
                // and the release columns land on `executions` itself, so the
                // gate substrate reaches both lineages.
                "execution_after_gates",
                // cairn/4735: `issue_dependencies` is shared project state, so
                // its retirement into `after:` gates reaches both lineages.
                "retire_issue_dependencies",
                // cairn/4987: a conflict session's resolution basis, in both.
                "reconcile_resolution_basis",
                "provider_limit_failover",
                // cairn/5028: tenets are ProjectScoped and their counter and
                // policy columns land on the shared `projects` row.
                "tenets",
                "tenet_ruling_imports",
                "tenet_evidence",
                // cairn/5064: evidence rows record how they were measured.
                "tenet_evidence_measured_by",
                "pr_publish_attempts",
                "index_sibling_job_lookup",
                // cairn/5113: tenets become yes/no statements.
                "tenet_statements",
                "check_observation_platform_relabels",
                "job_artifact_inputs",
                // cairn/5141: the fast tier's deferred per-test outcome.
                "check_deferred_tests",
                "index_execution_gates_execution",
                "drop_check_test_outcomes_view",
                // cairn/5337: the operator's goal toggle on the shared `jobs` row.
                "job_goal_intent",
                "compile_facts",
                // cairn/5282: a review wave's plan kept on its attempt row.
                "turn_end_check_attempt_plan",
                // cairn/5433: a tenet's rationale beside its statement.
                "tenet_rationale",
                "run_activity_minutes",
            ],
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('projects') WHERE name='human_base_merge_policy'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='merge_attempt_events'"
            )
            .await
            .unwrap(),
            1
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
        for (table, sql) in [
            (
                "authority_grants",
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='authority_grants'",
            ),
            (
                "authorization_events",
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='authorization_events'",
            ),
        ] {
            assert_eq!(
                query_i64(&db, sql).await.unwrap(),
                0,
                "private authority journal table {table} must not enter the team lineage",
            );
        }
        // Re-running is idempotent (tracked in cairn_schema_migrations).
        let again = MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        assert!(again.is_empty(), "team migrations must be idempotent");
    }

    /// A team database that never received 0221 applies it and then the drop, and
    /// comes out with no ledger, the dormant column, and its threads intact. The
    /// shipped lineage recorded later migrations without 0221, so a version cutoff
    /// would not reproduce this.
    #[tokio::test]
    async fn team_database_that_missed_the_branch_ledger_applies_it_and_its_retirement() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("team-branches.db"))
            .await
            .unwrap();
        let previous = TEAM_MIGRATIONS
            .iter()
            .copied()
            .filter(|migration| {
                !matches!(
                    migration.name(),
                    "thread_branches" | "retire_thread_branches"
                )
            })
            .collect::<Vec<_>>();
        MigrationRunner::new(previous).run(&db).await.unwrap();
        db.execute_script(
            "INSERT INTO teams(id, name, created_at, updated_at) VALUES ('team', 'Team', 1, 1);
             INSERT INTO projects(id, team_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'team', 'Project', 'project', '/tmp/project', 1, 1);
             INSERT INTO threads(id, project_id, name, created_at, updated_at)
             VALUES ('t', 'p', 'keep-this-thread', 1, 1);",
        )
        .await
        .unwrap();

        let runner = MigrationRunner::new(TEAM_MIGRATIONS.to_vec());
        let applied = runner.run(&db).await.unwrap();
        assert_eq!(applied.len(), 2, "{applied:?}");
        assert_eq!(applied[0], "0221_thread_branches");
        assert!(
            applied[1].ends_with("retire_thread_branches"),
            "{applied:?}"
        );

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE tbl_name = 'thread_branches'"
            )
            .await
            .unwrap(),
            0,
            "neither the ledger nor its index survives"
        );
        assert_eq!(
            query_i64(&db, "SELECT next_branch_number FROM threads WHERE id = 't'")
                .await
                .unwrap(),
            1,
            "the column stays, dormant, because threads is synced"
        );
        assert_eq!(
            query_text(&db, "SELECT name FROM threads WHERE id = 't'")
                .await
                .unwrap(),
            "keep-this-thread"
        );
        assert!(runner.run(&db).await.unwrap().is_empty());
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
        let priv_db = LocalDb::open_migrated_for_test(priv_temp.path().join("private.turso.db"))
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
                        "workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE,",
                        "",
                    )
                    .replace(
                        "project_id TEXT REFERENCES projects(id)",
                        "project_id TEXT NOT NULL REFERENCES projects(id)",
                    )
                    .replace(",CHECK((workspace_id IS NULL) !=(project_id IS NULL))", ""),
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
                // `projects` is re-rooted (above), so its owner-key index is
                // the private one under the same transform.
                if name == "idx_projects_team_id" {
                    assert_eq!(
                        norm(sql),
                        norm(&priv_objs["idx_projects_workspace_id"])
                            .replace("workspace_id", "team_id"),
                        "re-rooted index `{name}` drifted from its private counterpart"
                    );
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
        const SCHEMA_INFRA: &[&str] = &[
            "__turso_internal_mvcc_meta",
            "cairn_schema_migrations",
            "sqlite_sequence",
        ];
        let is_schema_infra =
            |name: &str| SCHEMA_INFRA.contains(&name) || name.starts_with("__turso_internal_seq_");
        let is_classifiable = |name: &str| !is_schema_infra(name) && name != "teams";

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
            .filter(|name| !is_schema_infra(name))
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

    /// A foreign key as the census reads it: the child's referencing columns in
    /// key order, the parent table, and the action a parent delete runs.
    struct ChildKey {
        columns: Vec<String>,
        parent: String,
        on_delete: String,
    }

    /// Every foreign key on `db` whose child columns lead no full index, as
    /// `child(columns) -> parent (ON DELETE action)`, sorted.
    ///
    /// A parent delete runs each child key's action (for NO ACTION and RESTRICT,
    /// its constraint check) as a lookup on the child columns. With an index
    /// leading with them the lookup seeks; without one it walks the whole child
    /// table once per deleted parent row, and a cascade multiplies that by the
    /// rows it deletes (cairn/5956). A key is covered by an index with
    /// `partial = 0` whose leading columns are the key's columns in any order (a
    /// `sqlite_autoindex_*` counts, which is how a PRIMARY KEY or UNIQUE
    /// constraint covers), or by the table's INTEGER PRIMARY KEY rowid alias. A
    /// partial index never covers: the lookup's `col = ?` does not imply its
    /// predicate to Turso.
    ///
    /// One exemption, by rule rather than by list: a NO ACTION or RESTRICT key
    /// that references its own table. The pinned engine checks that constraint
    /// by scanning the table whatever indexes exist (cairn/5623), so an index
    /// there costs every write and buys nothing.
    /// `storage::retention::tests::the_engine_still_scans_for_self_referencing_children`
    /// pins that behaviour; when it fails after an engine bump, this exemption
    /// goes too. A self-referencing CASCADE or SET NULL key is not exempt: its
    /// action is an ordinary child lookup and seeks through an index.
    async fn uncovered_foreign_keys(db: &LocalDb) -> Vec<String> {
        let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let tables = db
            .query_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' \
                 AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' \
                 ORDER BY name",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        let mut uncovered = Vec::new();
        for table in tables {
            let mut fk_rows = db
                .query_all(
                    format!("PRAGMA foreign_key_list({})", quote(&table)),
                    (),
                    |row| {
                        Ok((
                            row.i64(0)?,
                            row.i64(1)?,
                            row.text(2)?,
                            row.text(3)?,
                            row.text(6)?,
                        ))
                    },
                )
                .await
                .unwrap();
            if fk_rows.is_empty() {
                continue;
            }
            fk_rows.sort_by_key(|(id, seq, ..)| (*id, *seq));
            let mut keys: std::collections::BTreeMap<i64, ChildKey> =
                std::collections::BTreeMap::new();
            for (id, _seq, parent, from, on_delete) in fk_rows {
                keys.entry(id)
                    .or_insert_with(|| ChildKey {
                        columns: Vec::new(),
                        parent,
                        on_delete: on_delete.to_ascii_uppercase(),
                    })
                    .columns
                    .push(from);
            }

            // The ordered column names of every full index; an expression
            // column reads as `None` and matches no key column.
            let indexes = db
                .query_all(format!("PRAGMA index_list({})", quote(&table)), (), |row| {
                    Ok((row.text(1)?, row.i64(4)?))
                })
                .await
                .unwrap();
            let mut leads: Vec<Vec<Option<String>>> = Vec::new();
            for (index, partial) in indexes {
                if partial != 0 {
                    continue;
                }
                let mut columns = db
                    .query_all(format!("PRAGMA index_info({})", quote(&index)), (), |row| {
                        Ok((row.i64(0)?, row.opt_text(2)?))
                    })
                    .await
                    .unwrap();
                columns.sort_by_key(|(seqno, _)| *seqno);
                leads.push(columns.into_iter().map(|(_, name)| name).collect());
            }
            let primary_key = db
                .query_all(format!("PRAGMA table_info({})", quote(&table)), (), |row| {
                    Ok((row.text(1)?, row.text(2)?, row.i64(5)?))
                })
                .await
                .unwrap()
                .into_iter()
                .filter(|(_, _, pk)| *pk > 0)
                .collect::<Vec<_>>();
            let rowid_alias = match primary_key.as_slice() {
                [(name, kind, _)] if kind.eq_ignore_ascii_case("INTEGER") => Some(name.clone()),
                _ => None,
            };

            for key in keys.into_values() {
                if key.parent == table && matches!(key.on_delete.as_str(), "NO ACTION" | "RESTRICT")
                {
                    continue;
                }
                let wanted: std::collections::BTreeSet<&str> =
                    key.columns.iter().map(String::as_str).collect();
                let width = key.columns.len();
                let by_rowid =
                    width == 1 && rowid_alias.as_deref() == Some(key.columns[0].as_str());
                let by_index = leads.iter().any(|lead| {
                    lead.len() >= width
                        && lead[..width]
                            .iter()
                            .map(Option::as_deref)
                            .collect::<Option<std::collections::BTreeSet<&str>>>()
                            .is_some_and(|leading| leading == wanted)
                });
                if !by_rowid && !by_index {
                    uncovered.push(format!(
                        "{table}({}) -> {} (ON DELETE {})",
                        key.columns.join(", "),
                        key.parent,
                        key.on_delete
                    ));
                }
            }
        }
        uncovered.sort();
        uncovered
    }

    /// Foreign keys whose index a named issue owes, as
    /// `(lineage, child table, child column, issue)`. Only an index held back on
    /// measured boot cost belongs here, until the table it would be built over
    /// is small enough to build at boot. An entry whose key is covered fails the
    /// census as stale, so the list only shrinks.
    const PENDING_FOREIGN_KEY_INDEXES: &[(&str, &str, &str, &str)] = &[];

    /// `docs/database.md` ("Deleting Rows With Dependents"): every foreign-key
    /// child column needs a full index, in both lineages. See
    /// `uncovered_foreign_keys` for what covers a key and the one exemption,
    /// and `PENDING_FOREIGN_KEY_INDEXES` for the indexes an issue owes.
    #[tokio::test]
    async fn every_foreign_key_child_column_leads_an_index() {
        let private = migrated_db().await.unwrap();
        let (_team_temp, team) = migrated_team_db().await;
        let mut uncovered = Vec::new();
        let mut pending_seen = std::collections::BTreeSet::new();
        for (lineage, db) in [("private", &private), ("team", &team)] {
            for key in uncovered_foreign_keys(db).await {
                let pending = PENDING_FOREIGN_KEY_INDEXES.iter().position(
                    |(pending_lineage, table, column, _)| {
                        *pending_lineage == lineage
                            && key.starts_with(&format!("{table}({column}) ->"))
                    },
                );
                match pending {
                    Some(index) => {
                        pending_seen.insert(index);
                    }
                    None => uncovered.push(format!("{lineage}: {key}")),
                }
            }
        }
        let stale: Vec<String> = PENDING_FOREIGN_KEY_INDEXES
            .iter()
            .enumerate()
            .filter(|(index, _)| !pending_seen.contains(index))
            .map(|(_, (lineage, table, column, issue))| {
                format!("{lineage}: {table}({column}) ({issue})")
            })
            .collect();
        assert!(
            stale.is_empty(),
            "these pending foreign-key indexes now exist; remove them from \
             PENDING_FOREIGN_KEY_INDEXES: {stale:?}"
        );
        assert!(
            uncovered.is_empty(),
            "{} foreign key(s) lead no full index, so each parent delete walks the \
             whole child table. Add a full index (no WHERE) leading with the child \
             column(s) in a migration, shared when both lineages carry the table:\n  {}",
            uncovered.len(),
            uncovered.join("\n  ")
        );
    }

    /// Rows written before act bindings were salted lose the unsalted digest
    /// and keep everything else: the card still reads as an authority prompt,
    /// and the grant keeps its exact_act constraint, so it still parses and
    /// still covers nothing. A salted binding is left alone.
    #[tokio::test]
    async fn unsalted_act_digests_are_scrubbed_from_cards_and_grants() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("scrub-act-digests.turso.db"))
            .await
            .unwrap();
        let name = "scrub_unsalted_act_digests";
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.name() != name)
            .copied()
            .collect::<Vec<_>>();
        let scrub = TURSO_MIGRATIONS
            .iter()
            .find(|migration| migration.name() == name)
            .copied()
            .expect("the scrub is in the private lineage");
        MigrationRunner::new(before).run(&db).await.unwrap();
        let unsalted = r#"{"algorithm":"sha256","encoding_version":1,"digest":"UNSALTED"}"#;
        let salted = r#"{"algorithm":"sha256","encoding_version":2,"salt":"SALT","digest":"KEEP"}"#;
        let card = format!(
            r#"{{"kind":"authority_request","verb":"write","authority":{{"summary":"s","facts":{{"act":{unsalted}}}}}}}"#
        );
        let crossing = r#"{"kind":"external_host_write","authority":{"facts":{"act":"UNSALTED-but-not-a-card"}}}"#;
        let modes = r#"{"constraint":"mutation_modes","modes":["update"]}"#;
        let mcp = r#"{"constraint":"mcp_config","fingerprint":{"algorithm":"sha256","encoding_version":1,"digest":"cfg"}}"#;
        let set = |constraints: &[&str]| {
            format!(
                r#"{{"version":3,"constraints":[{}]}}"#,
                constraints.join(",")
            )
        };
        let act = |binding: &str| format!(r#"{{"constraint":"exact_act","digest":{binding}}}"#);
        let grants = [
            ("settings", set(&[modes, &act(unsalted)])),
            ("mcp", set(&[modes, mcp, &act(unsalted)])),
            ("salted", set(&[modes, &act(salted)])),
        ];
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('project', 'default', 'Project', 'project', '/tmp/project', 1, 1);
             INSERT INTO jobs(id, project_id, status, created_at, updated_at)
               VALUES ('job', 'project', 'running', 1, 1);
             INSERT INTO runs(id, job_id, created_at, updated_at) VALUES ('run', 'job', 1, 1);",
        )
        .await
        .unwrap();
        let mut script = String::new();
        for (id, tool_use, input) in [("card", "t1", card.as_str()), ("crossing", "t2", crossing)] {
            script.push_str(&format!(
                "INSERT INTO permission_requests(id, run_id, tool_use_id, tool_name, tool_input, created_at)
                   VALUES ('{id}', 'run', '{tool_use}', 'write', '{input}', 1);\n"
            ));
        }
        for (id, constraints) in &grants {
            script.push_str(&format!(
                "INSERT INTO authority_grants(id, scope_key, place_kind, action, scope_json,
                   constraints_json, principal_json, audience_json, workspace_id, lifetime_kind,
                   lifetime_anchor, provenance_json, created_at)
                   VALUES ('{id}', 'k', 'settings', 'write', '{{}}', '{constraints}', '{{}}', '{{}}',
                     'default', 'once', 'perm', '{{}}', 1);\n"
            ));
        }
        db.execute_script(&script).await.unwrap();

        MigrationRunner::new(vec![scrub]).run(&db).await.unwrap();

        let card = query_text(
            &db,
            "SELECT tool_input FROM permission_requests WHERE id = 'card'",
        )
        .await
        .unwrap();
        assert!(!card.contains("UNSALTED"), "{card}");
        assert!(
            card.contains("authority_request") && card.contains("\"summary\""),
            "{card}"
        );
        let crossing = query_text(
            &db,
            "SELECT tool_input FROM permission_requests WHERE id = 'crossing'",
        )
        .await
        .unwrap();
        assert!(crossing.contains("UNSALTED-but-not-a-card"), "{crossing}");
        for (id, sql) in [
            (
                "settings",
                "SELECT constraints_json FROM authority_grants WHERE id = 'settings'",
            ),
            (
                "mcp",
                "SELECT constraints_json FROM authority_grants WHERE id = 'mcp'",
            ),
        ] {
            let stored = query_text(&db, sql).await.unwrap();
            assert!(!stored.contains("UNSALTED"), "{id}: {stored}");
            assert!(stored.contains("exact_act"), "{id}: {stored}");
        }
        let kept = query_text(
            &db,
            "SELECT constraints_json FROM authority_grants WHERE id = 'salted'",
        )
        .await
        .unwrap();
        assert!(kept.contains("KEEP") && kept.contains("SALT"), "{kept}");
    }

    /// The resource-embedding sweep's artifact walk reads only
    /// `idx_artifacts_embedding_scan`, on both lineages and on a database
    /// upgraded to it. The statement is byte-for-byte
    /// `embeddings::resources::ARTIFACT_VERSIONS_SQL`. A plan line saying
    /// `COVERING INDEX` is not enough on its own: the bytecode must open no
    /// cursor on `artifacts` and never seek from an index entry into the table.
    #[tokio::test]
    async fn the_artifact_embedding_walk_reads_only_its_covering_index() {
        const ARTIFACT_VERSIONS: &str =
            "SELECT id, job_id, COALESCE(output_name, artifact_type), version, created_at, rowid
       FROM artifacts INDEXED BY idx_artifacts_embedding_scan
      WHERE job_id IS NOT NULL";
        const INDEX: &str = "idx_artifacts_embedding_scan";
        const MIGRATION: &str = "index_artifacts_embedding_scan";

        async fn assert_index_only(db: &LocalDb, lineage: &str) {
            assert_eq!(
                explain_plan(db, ARTIFACT_VERSIONS).await,
                [format!("SCAN artifacts USING COVERING INDEX {INDEX}")],
                "{lineage}"
            );
            let program = db
                .query_all(format!("EXPLAIN {ARTIFACT_VERSIONS}"), (), |row| {
                    Ok((row.text(1)?, row.text(7)?))
                })
                .await
                .unwrap();
            let opens: Vec<&str> = program
                .iter()
                .filter(|(opcode, _)| opcode == "OpenRead")
                .map(|(_, comment)| comment.as_str())
                .collect();
            assert!(
                opens.len() == 1 && opens[0].starts_with(&format!("index={INDEX}")),
                "{lineage}: only the covering index is opened: {program:#?}"
            );
            assert!(
                !program.iter().any(|(opcode, _)| matches!(
                    opcode.as_str(),
                    "DeferredSeek" | "SeekRowid" | "NotExists" | "SorterOpen"
                )),
                "{lineage}: no table seek and no sort: {program:#?}"
            );
            // And it runs: the index answers the statement it was named for.
            db.query_all(ARTIFACT_VERSIONS, (), |row| row.text(0))
                .await
                .unwrap();
        }

        let private = migrated_db().await.unwrap();
        assert_index_only(&private, "private").await;
        let (_team_temp, team) = migrated_team_db().await;
        assert_index_only(&team, "team").await;

        for (lineage, migrations) in [
            ("private", TURSO_MIGRATIONS.to_vec()),
            ("team", TEAM_MIGRATIONS.to_vec()),
        ] {
            let temp = tempdir().unwrap();
            let db = LocalDb::open(temp.path().join("artifact-embedding-scan.turso.db"))
                .await
                .unwrap();
            let before = migrations
                .iter()
                .take_while(|migration| migration.name() != MIGRATION)
                .copied()
                .collect::<Vec<_>>();
            assert!(
                before.len() < migrations.len(),
                "{lineage} carries {MIGRATION}"
            );
            MigrationRunner::new(before).run(&db).await.unwrap();
            assert_eq!(index_sql(&db, INDEX).await, None, "{lineage}");
            MigrationRunner::new(migrations).run(&db).await.unwrap();
            assert_index_only(&db, &format!("{lineage} upgraded")).await;
        }
    }

    /// Deleting an event runs the `message_streams.final_event_id` foreign-key
    /// action (`ON DELETE SET NULL`), which Turso compiles into a subprogram.
    /// `EXPLAIN` lists every subprogram's opcodes after the delete's own, so the
    /// action's access path is part of the delete's program: it must open
    /// `idx_message_streams_final_event` and never `Rewind` (scan) the streams
    /// table (cairn/5584). Turso keeps no table statistics, so the program is
    /// the same however many streams exist; no fixture size or timing is needed
    /// to see a scan.
    #[tokio::test]
    async fn deleting_events_with_final_streams_does_not_scan_streams() {
        async fn delete_program(db: &LocalDb) -> Vec<(String, String)> {
            db.query_all("EXPLAIN DELETE FROM events WHERE id = 'e'", (), |row| {
                Ok((row.text(1)?, row.text(7)?))
            })
            .await
            .unwrap()
        }
        fn scans_streams(program: &[(String, String)]) -> bool {
            program
                .iter()
                .any(|(opcode, comment)| opcode == "Rewind" && comment.contains("message_streams"))
        }

        let db = migrated_db().await.unwrap();
        let program = delete_program(&db).await;
        assert!(
            program.iter().any(|(opcode, comment)| opcode == "OpenRead"
                && comment.contains("index=idx_message_streams_final_event")),
            "the final_event_id SET NULL action must seek its index: {program:#?}"
        );
        assert!(
            !scans_streams(&program),
            "deleting an event must not scan message_streams: {program:#?}"
        );

        // The action runs: enforcement is on, and it clears only the deleted
        // events' streams.
        db.execute_script(
            "INSERT INTO runs(id, status, created_at, updated_at) VALUES ('r', 'running', 1, 1);
             INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('e0', 'r', 's', 0, 1, 'assistant', '{}', 1),
                      ('e1', 'r', 's', 1, 1, 'assistant', '{}', 1),
                      ('e2', 'r', 's', 2, 1, 'assistant', '{}', 1);
             INSERT INTO message_streams(id, run_id, backend, sequence, status, final_event_id,
                                         created_at, updated_at)
               VALUES ('m0', 'r', 'b', 0, 'finalized', 'e0', 1, 1),
                      ('m1', 'r', 'b', 1, 'finalized', 'e1', 1, 1),
                      ('m2', 'r', 'b', 2, 'finalized', 'e2', 1, 1);
             DELETE FROM events WHERE id IN ('e1', 'e2');",
        )
        .await
        .unwrap();
        let nulled = db
            .query_one(
                "SELECT COUNT(*) FROM message_streams WHERE final_event_id IS NULL",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        let kept = db
            .query_one(
                "SELECT COUNT(*) FROM message_streams WHERE final_event_id IS NOT NULL",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            (nulled, kept),
            (2, 1),
            "deleted events' streams survive with final_event_id cleared; the rest are untouched"
        );

        // Without the index the same delete scans, so the assertions above can
        // tell the difference.
        db.execute("DROP INDEX idx_message_streams_final_event", ())
            .await
            .unwrap();
        let unindexed = delete_program(&db).await;
        assert!(
            scans_streams(&unindexed),
            "without its index the action must scan message_streams: {unindexed:#?}"
        );
    }

    /// The stored `CREATE INDEX` text for `name`, or `None` if it does not exist.
    async fn index_sql(db: &LocalDb, name: &str) -> Option<String> {
        let name = name.to_string();
        db.read(move |conn| {
            let name = name.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?1",
                        (name,),
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                })
            })
        })
        .await
        .unwrap()
    }
}
