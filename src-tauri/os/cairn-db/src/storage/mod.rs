mod blocking;
mod context_tokens;
mod error;
mod executor_desktop_automation;
mod fact_log;
mod feed;
mod local_db;
mod migration;
mod migrations;
mod posts;
mod response_invocations;
mod route_firings;
mod row;
mod runtime_admission;
mod search_index;
#[cfg(any(test, feature = "test-utils"))]
mod test_template;

pub mod authority;
pub mod content_store;
pub mod events;
pub mod pack_catalog;
pub mod quarantine;
pub mod render;

/// Normalized team identifier. Defined here (rather than reaching up into
/// cairn-core's `db` module) so `content_store` and `local_db` can name it
/// within cairn-db; cairn-core re-exports this exact alias at `crate::db::TeamId`.
pub type TeamId = String;

pub use blocking::{panic_message, run_db_blocking};
pub use context_tokens::get_latest_context_token_event;
pub use error::{DbError, DbResult};
pub use executor_desktop_automation::{
    delete_executor_desktop_automation, get_executor_desktop_automation,
    upsert_executor_desktop_automation, ExecutorDesktopAutomation,
};
pub use fact_log::{
    claim_due_wake as claim_due_fact_wake, claim_for_index, claim_next_queued,
    coalesce_wake as coalesce_fact_wake, count_facts, finish as finish_fact,
    finish_wake as finish_fact_wake, get_fact, get_wakes as get_fact_wakes, insert_fact,
    latest_for_source, latest_per_source, list_facts, prune as prune_facts,
    prune_wakes as prune_fact_wakes, recover_on_start as recover_fact_log, route_state,
    set_index_outcome, FactFilter, FactRow, FactScope, FactWake, LogDedupe, NewFact, PruneSelector,
    Recovery, WakeMatch, WakeWindow,
};
pub use feed::{FeedAck, FeedHome, FeedHomeKind, FeedPage, FEED_PAGE_DEFAULT, FEED_PAGE_MAX};
pub use local_db::{
    db_set_paths, db_set_size, install_crypto_provider, move_db_set, CheckpointReport, DrainReport,
    LocalDb, RetryConfig, TrackedConnection,
};
pub use migration::{
    repair_index_entry_drift, run_integrity_sweep, IntegritySweepOutcome, Migration,
    MigrationRunner,
};
pub use migrations::{
    Lineage, PrivateReason, RekeyTableManifest, ScopeTarget, TableScope, PROJECT_REKEY_MANIFEST,
    TABLE_SCOPES, TEAM_MIGRATIONS, TURSO_MIGRATIONS,
};
pub use posts::PostScope;
pub use response_invocations::{
    count_response_invocations, get_response_invocation, insert_response_invocation,
    list_response_invocations, NewResponseInvocation, ResponseInvocationRecord,
};
pub use route_firings::{
    count_route_firings, firing_snapshot, get_route_firing, has_recent_fact, insert_route_firing,
    list_firings_for_fact, list_route_firings, NewRouteFiring, RouteFiringRecord, SNAPSHOT_CHARS,
};
pub use row::{
    next_i64, next_opt_text, next_text, query_opt_i64_conn, query_opt_text_conn, query_text_conn,
    FromDbRow, RowExt,
};
pub use runtime_admission::{
    admission_status, advance_team_mirror_phase, cancel_queued_admission,
    clear_team_ordinary_terminal, enqueue_admission_request, enqueue_prepared_ordinary_launch,
    finish_admission_by_launch_key, get_admission_request, get_admission_request_by_launch_key,
    get_prepared_ordinary_launch, get_prepared_runtime_call, heartbeat_admission, lane_occupancy,
    lease_next_admission, lease_next_admission_with_capacity, lease_next_admission_within,
    list_pending_team_ordinary_terminals, list_queued_ordinary_launches,
    list_recoverable_team_ordinary_launches, list_unsettled_call_admissions,
    ordinary_admission_projections, pending_launch_run_ids, persist_prepared_ordinary_launch,
    persist_prepared_runtime_call, persist_team_ordinary_lease, queue_team_ordinary_terminal,
    reap_expired_admissions, requeue_orphaned_admissions, retire_team_mirror, transition_admission,
    withdraw_queued_call_admission, withdraw_queued_ordinary_admission, AdmissionRequest,
    AdmissionStatus, HostMemoryEnvelope, LaneBudget, LaneOccupancy, LaneShortfall, LaunchOrigin,
    LeaseDecision, NewAdmissionRequest, OrdinaryAdmissionProjection, PreparedOrdinaryLaunch,
    PreparedRuntimeCall, PreparedRuntimeCapability, PreparedRuntimeClass, PreparedRuntimeHostKind,
    PreparedRuntimeHostRequirement, PreparedSessionStart, PreparedUserIdentity,
    QueuedOrdinaryLaunch, RecoverableTeamOrdinaryLaunch, RuntimeAdmissionReason,
    RuntimeAdmissionState, RuntimeHostIdentity, RuntimeResourceCapacity, RuntimeResourceClaim,
    SpawnWindow, TeamOrdinaryAuthority, UnsettledCallAdmission, WithdrawnAdmission,
    CALL_ADMISSION_WORKSPACE, DEFAULT_URGENT_BURST,
};
pub use search_index::{SearchIndex, SearchIndexHit};

#[cfg(any(test, feature = "test-utils"))]
pub use content_store::InMemoryContentStore;
pub use content_store::{ContentStore, ContentStoreFactory, TeamReplicaContext};
#[cfg(any(test, feature = "test-utils"))]
pub use events::event_fixture;
pub use events::{
    build_execution_pack, compress, count_commits_ahead, decompress, list_range_commits,
    reconstruct_events, reconstruct_events_reporting, render_range_diff, render_range_file_diffs,
    NodeDiffFile, ObjectStore, RangeCommit, Reconstruction, ResolvePathError, CODEC_NONE,
    CODEC_ZSTD_V1,
};

/// A freshly migrated, temp-backed test database. Exposed under `test-utils`
/// (not just `cfg(test)`) so cairn-core's cross-crate test modules keep
/// resolving `crate::storage::migrated_test_db`.
#[cfg(any(test, feature = "test-utils"))]
pub async fn migrated_test_db(name: &str) -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.keep().join(name);
    LocalDb::open_migrated_for_test(path).await.unwrap()
}

pub mod outbox;
