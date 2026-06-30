mod blocking;
mod error;
mod local_db;
mod migration;
mod migrations;
mod row;
mod search_index;
mod team_sync;

pub(crate) use blocking::run_db_blocking;
pub use error::{DbError, DbResult};
pub use local_db::{
    db_set_paths, db_set_size, install_crypto_provider, move_db_set, LocalDb, RetryConfig,
};
pub use migration::{Migration, MigrationRunner};
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
