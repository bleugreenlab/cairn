//! The two lists the plan audit is held to: the tables whose rows carry a blob
//! large enough that walking them is a defect, and every trigger either lineage
//! creates.
//!
//! Both are audited against `sqlite_master` the way `TABLE_RETENTION` is
//! (`plan_audit::tests`): an entry naming nothing fails, and so does a trigger
//! nobody declared. Adding a trigger is therefore a visible decision, made
//! here, beside the reason its body stays keyed to the row that fired it.

/// A table whose rows overflow, and the columns that make them do so.
///
/// Column positions are read from the schema, never declared: `ALTER TABLE ADD
/// COLUMN` appends after the blob, which is how `executions.released_at` and
/// `lifecycle` came to sit after `snapshot` (cairn/5894).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WideRowTable {
    pub table: &'static str,
    /// The large columns. The first of them, by position, is where a row read
    /// starts walking the overflow chain.
    pub blob_columns: &'static [&'static str],
    pub reason: &'static str,
}

const fn wide(
    table: &'static str,
    blob_columns: &'static [&'static str],
    reason: &'static str,
) -> WideRowTable {
    WideRowTable {
        table,
        blob_columns,
        reason,
    }
}

/// Every table a statement must not walk. `wide_row_list_matches_the_clone`
/// checks it against a production clone's payload census.
pub const WIDE_ROW_TABLES: &[WideRowTable] = &[
    wide(
        "executions",
        &["snapshot"],
        "the execution snapshot, ~158 KB per row on the 11-02 clone (cairn/5894)",
    ),
    wide(
        "events",
        &["data", "data_blob"],
        "every event body; the largest table in the database",
    ),
    wide(
        "agent_waits",
        &["condition_json", "resolution_json"],
        "durable-suspend state, read row by row by the 11-02 scheduler (cairn/5866)",
    ),
    wide(
        "check_result_cache",
        &["target_results_json"],
        "cached per-target check verdicts",
    ),
    wide(
        "session_skyline_cache",
        &["bars_json"],
        "a session's rendered skyline bars",
    ),
    wide("artifacts", &["data"], "artifact bodies"),
    wide(
        "artifact_content",
        &["data"],
        "content-addressed artifact bodies",
    ),
    wide(
        "message_stream_chunks",
        &["data"],
        "streamed message chunk bodies",
    ),
    wide("delegated_packets", &["spec"], "delegation packet specs"),
    wide(
        "execution_history",
        &["pack", "pack_idx"],
        "an execution's packed event history and its index",
    ),
    wide("archival_blobs", &["content"], "archived blob bodies"),
    wide("cas_cache", &["content"], "content-addressed cache bodies"),
    wide(
        "prepared_ordinary_launches",
        &["prompt"],
        "a prepared launch's prompt, walked by the reaper on 11-02 (cairn/5864)",
    ),
    wide(
        "prepared_runtime_calls",
        &["prepared_json"],
        "a prepared runtime call, walked by the reaper on 11-02 (cairn/5864)",
    ),
    wide(
        "runtime_admission_requests",
        &["claim_json"],
        "an admission request's resource claim (cairn/5864)",
    ),
    wide(
        "pr_resolution_attributions",
        &["lane_snapshot"],
        "the check-lane snapshot an attribution was made against (cairn/5585 census)",
    ),
    wide(
        "codemap_cache",
        &["codemap_json"],
        "rendered code maps (cairn/5585 census)",
    ),
    wide(
        "effect_outbox",
        &["payload_json"],
        "effect payloads; building an index over them took 39 s on the 10-30 clone",
    ),
];

/// The registry entry for `table`, if it is wide.
pub fn wide_row_table(table: &str) -> Option<&'static WideRowTable> {
    WIDE_ROW_TABLES.iter().find(|entry| entry.table == table)
}

/// A trigger either lineage creates, and why its body is keyed to the row that
/// fired it. The trigger sweep (`every_trigger_body_seeks`) is what holds the
/// reason true.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditedTrigger {
    pub trigger: &'static str,
    pub table: &'static str,
    pub reason: &'static str,
}

const fn trigger(
    trigger: &'static str,
    table: &'static str,
    reason: &'static str,
) -> AuditedTrigger {
    AuditedTrigger {
        trigger,
        table,
        reason,
    }
}

const SEARCH_OUTBOX: &str = "queues one search_outbox row for the written row; reads no table";
const IMMUTABLE: &str = "refuses the write with RAISE; reads no table";
const ROW_CHECK: &str = "checks the written row's own columns with RAISE; reads no table";
const DISJOINT: &str =
    "probes the sibling table by its (observation_id, test_id) key, which a unique index serves";
const TOOL_ANALYTICS: &str = "upserts the invocation's hour bucket by its id and reads runs and jobs by primary key; the zero-count delete is scoped to that bucket (cairn/5907)";

/// Every trigger in either lineage, private and team alike.
pub const AUDITED_TRIGGERS: &[AuditedTrigger] = &[
    trigger("main_incident_sources_immutable", "main_incident_sources", IMMUTABLE),
    trigger("main_incident_source_retention_guard", "check_result_observations",
        "probes pending sources by idx_main_incident_sources_observation_pending; ignores deletion until ack"),
    trigger("main_incident_source_enqueue", "check_result_observations",
        "enqueues one prospective main source by its (project_id,event_key) primary key; reads no table"),
    trigger("main_incidents_immutable", "main_incidents", IMMUTABLE),
    trigger("publications_settled_immutable", "publications", IMMUTABLE),
    trigger("publications_identity_immutable", "publications", IMMUTABLE),
    trigger("publication_legs_terminal_immutable", "publication_legs", IMMUTABLE),
    trigger("publication_legs_mode_immutable", "publication_legs", IMMUTABLE),
    trigger("publication_legs_shadow_never_uploads_insert", "publication_legs", ROW_CHECK),
    trigger("publication_legs_shadow_never_uploads_update", "publication_legs", ROW_CHECK),
    trigger(
        "main_incident_evidence_immutable",
        "main_incident_evidence",
        IMMUTABLE,
    ),
    trigger(
        "main_incident_evidence_filed_immutable",
        "main_incident_evidence",
        IMMUTABLE,
    ),
    trigger("authorization_events_no_update", "authorization_events", IMMUTABLE),
    trigger("authorization_events_no_delete", "authorization_events", IMMUTABLE),
    trigger("request_attempt_members_no_update", "request_attempt_members", IMMUTABLE),
    trigger("request_attempt_members_no_delete", "request_attempt_members", IMMUTABLE),
    trigger("request_attempt_inputs_immutable", "request_attempts", ROW_CHECK),
    trigger("request_attempts_no_delete", "request_attempts", IMMUTABLE),
    trigger("request_accepted_facts_no_update", "request_facts", "checks acceptance history by indexed cohort_id before refusing mutation of an accepted facts version"),
    trigger("request_accepted_facts_no_delete", "request_facts", "checks acceptance history by indexed cohort_id before refusing deletion of an accepted facts version"),
    trigger("request_receipts_no_update", "request_report_receipts", IMMUTABLE),
    trigger("request_receipts_no_delete", "request_report_receipts", IMMUTABLE),
    trigger("request_decline_immutable", "requests", ROW_CHECK),
    trigger("request_identity_no_delete", "requests", IMMUTABLE),
    trigger("search_artifacts_insert", "artifacts", SEARCH_OUTBOX),
    trigger("search_artifacts_update", "artifacts", SEARCH_OUTBOX),
    trigger("search_artifacts_delete", "artifacts", SEARCH_OUTBOX),
    trigger("search_events_insert", "events", SEARCH_OUTBOX),
    trigger("search_events_update", "events", SEARCH_OUTBOX),
    trigger("search_events_delete", "events", SEARCH_OUTBOX),
    trigger("search_issues_insert", "issues", SEARCH_OUTBOX),
    trigger("search_issues_update", "issues", SEARCH_OUTBOX),
    trigger("search_issues_delete", "issues", SEARCH_OUTBOX),
    trigger("search_comments_insert", "comments", SEARCH_OUTBOX),
    trigger("search_comments_update", "comments", SEARCH_OUTBOX),
    trigger("search_comments_delete", "comments", SEARCH_OUTBOX),
    trigger("search_messages_insert", "messages", SEARCH_OUTBOX),
    trigger("search_messages_update", "messages", SEARCH_OUTBOX),
    trigger("search_messages_delete", "messages", SEARCH_OUTBOX),
    trigger("search_posts_insert", "posts", SEARCH_OUTBOX),
    trigger("search_posts_delete", "posts", SEARCH_OUTBOX),
    trigger(
        "search_post_comments_insert",
        "post_comments",
        SEARCH_OUTBOX,
    ),
    trigger(
        "search_post_comments_delete",
        "post_comments",
        SEARCH_OUTBOX,
    ),
    trigger(
        "check_test_results_immutable",
        "check_test_results",
        IMMUTABLE,
    ),
    trigger(
        "check_result_commit_aliases_immutable",
        "check_result_commit_aliases",
        IMMUTABLE,
    ),
    trigger(
        "check_result_observations_immutable",
        "check_result_observations",
        IMMUTABLE,
    ),
    trigger(
        "check_deferred_tests_immutable",
        "check_deferred_tests",
        IMMUTABLE,
    ),
    trigger(
        "check_audit_findings_immutable",
        "check_audit_findings",
        IMMUTABLE,
    ),
    trigger("execution_queue_immutable", "execution_queue", IMMUTABLE),
    trigger(
        "merge_attempt_events_immutable",
        "merge_attempt_events",
        IMMUTABLE,
    ),
    trigger(
        "human_base_merge_policy_events_immutable",
        "human_base_merge_policy_events",
        IMMUTABLE,
    ),
    trigger(
        "account_credential_events_no_update",
        "account_credential_events",
        IMMUTABLE,
    ),
    trigger(
        "account_credential_events_no_delete",
        "account_credential_events",
        IMMUTABLE,
    ),
    trigger(
        "validate_channel_ask_resolution_provenance_insert",
        "channel_ask_resolution",
        ROW_CHECK,
    ),
    trigger(
        "validate_channel_ask_resolution_provenance_update",
        "channel_ask_resolution",
        ROW_CHECK,
    ),
    trigger(
        "validate_permission_provenance_update",
        "permission_requests",
        ROW_CHECK,
    ),
    trigger("validate_prompt_provenance_update", "prompts", ROW_CHECK),
    trigger("validate_issue_authorship_insert", "issues", ROW_CHECK),
    trigger("validate_issue_authorship_update", "issues", ROW_CHECK),
    trigger("preserve_issue_authorship_update", "issues", ROW_CHECK),
    trigger(
        "validate_attachment_authorship_insert",
        "attachments",
        ROW_CHECK,
    ),
    trigger(
        "validate_attachment_authorship_update",
        "attachments",
        ROW_CHECK,
    ),
    trigger(
        "preserve_attachment_authorship_update",
        "attachments",
        ROW_CHECK,
    ),
    trigger(
        "check_deferred_tests_disjoint",
        "check_deferred_tests",
        DISJOINT,
    ),
    trigger(
        "check_test_results_not_deferred",
        "check_test_results",
        DISJOINT,
    ),
    trigger(
        "tool_analytics_after_insert",
        "tool_invocations",
        TOOL_ANALYTICS,
    ),
    trigger(
        "tool_analytics_before_update",
        "tool_invocations",
        TOOL_ANALYTICS,
    ),
    trigger(
        "tool_analytics_after_update",
        "tool_invocations",
        TOOL_ANALYTICS,
    ),
    trigger(
        "run_activity_after_event_insert",
        "events",
        "inserts the event's (run_id, minute) pair, ignoring a duplicate; reads no table",
    ),
];
