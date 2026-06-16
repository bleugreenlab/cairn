mod blocking;
mod error;
mod local_db;
mod migration;
mod migrations;
mod row;
mod search_index;

pub(crate) use blocking::run_db_blocking;
pub use error::{DbError, DbResult};
pub use local_db::{LocalDb, RetryConfig};
pub use migration::{Migration, MigrationRunner};
pub use migrations::TURSO_MIGRATIONS;
pub use row::{
    next_i64, next_opt_text, next_text, query_opt_i64_conn, query_opt_text_conn, query_text_conn,
    FromDbRow, RowExt,
};
pub use search_index::{SearchIndex, SearchIndexHit};

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
