mod blocking;
mod error;
mod local_db;
mod migration;
mod migrations;
mod row;
mod search_index;
mod team_sync;

pub(crate) mod content_store;
pub(crate) mod events;

pub(crate) use blocking::run_db_blocking;
pub use error::{DbError, DbResult};
pub use local_db::{
    db_set_paths, db_set_size, install_crypto_provider, move_db_set, LocalDb, RetryConfig,
};
pub use migration::{repair_index_entry_drift, Migration, MigrationRunner};
pub use migrations::{
    Lineage, PrivateReason, RekeyTableManifest, ScopeTarget, TableScope, PROJECT_REKEY_MANIFEST,
    TABLE_SCOPES, TEAM_MIGRATIONS, TURSO_MIGRATIONS,
};
pub use row::{
    next_i64, next_opt_text, next_text, query_opt_i64_conn, query_opt_text_conn, query_text_conn,
    FromDbRow, RowExt,
};
pub use search_index::{SearchIndex, SearchIndexHit};
pub use team_sync::{run_pull_task, run_push_task, RouteReconcile, SyncCadence};

#[cfg(any(test, feature = "test-utils"))]
pub use content_store::InMemoryContentStore;
pub use content_store::{
    BrokeredContentStore, BrokeredContentStoreFactory, ContentStore, ContentStoreFactory,
    TeamReplicaContext,
};
#[cfg(any(test, feature = "test-utils"))]
pub use events::event_fixture;
pub use events::{
    build_execution_pack, compress, count_commits_ahead, decompress, reconstruct_events,
    render_range_diff, render_range_file_diffs, NodeDiffFile, ObjectStore, ResolvePathError,
    CODEC_NONE, CODEC_ZSTD_V1,
};

#[cfg(test)]
pub(crate) async fn migrated_test_db(name: &str) -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.keep().join(name);
    let db = LocalDb::open(path).await.unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db
}
