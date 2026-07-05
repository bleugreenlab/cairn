mod blocking;
mod error;
mod local_db;
mod migration;
mod migrations;
mod row;
mod search_index;

pub mod content_store;
pub mod events;
pub mod render;

/// Normalized team identifier. Defined here (rather than reaching up into
/// cairn-core's `db` module) so `content_store` and `local_db` can name it
/// within cairn-db; cairn-core re-exports this exact alias at `crate::db::TeamId`.
pub type TeamId = String;

pub use blocking::run_db_blocking;
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

#[cfg(any(test, feature = "test-utils"))]
pub use content_store::InMemoryContentStore;
pub use content_store::{ContentStore, ContentStoreFactory, TeamReplicaContext};
#[cfg(any(test, feature = "test-utils"))]
pub use events::event_fixture;
pub use events::{
    build_execution_pack, compress, count_commits_ahead, decompress, reconstruct_events,
    render_range_diff, render_range_file_diffs, NodeDiffFile, ObjectStore, ResolvePathError,
    CODEC_NONE, CODEC_ZSTD_V1,
};

/// A freshly migrated, temp-backed test database. Exposed under `test-utils`
/// (not just `cfg(test)`) so cairn-core's cross-crate test modules keep
/// resolving `crate::storage::migrated_test_db`.
#[cfg(any(test, feature = "test-utils"))]
pub async fn migrated_test_db(name: &str) -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.keep().join(name);
    let db = LocalDb::open(path).await.unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db
}
