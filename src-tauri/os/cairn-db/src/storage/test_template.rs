//! A migrated test database materialized from a per-run template.
//!
//! Replaying the whole migration chain into a fresh file is the dominant cost of
//! a database-backed test: on a 32-core Linux executor, open took ~0.16 s and the
//! 267-migration replay ~5.2 s of a ~5.4 s fixture, while copying an already
//! migrated, checkpointed image and opening it took ~0.02 s (CAIRN-5142). nextest
//! runs every test in its own process, so the image has to live on disk to be
//! shared at all: the first fixture a run needs replays the chain once into
//! `temp_dir()`, and every later fixture — in any process — copies that image.
//!
//! The image lives under the process temp dir, which the Rust test runner's seal
//! points at per-run scratch it deletes afterwards, so nothing crosses runs
//! there. Under bare `cargo test` it persists in the user's temp dir, which is
//! harmless because the directory is keyed by a fingerprint of the exact chain
//! that built it: a chain that differs by one byte builds its own image.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::OnceCell;

use super::local_db::{db_set_paths, LocalDb};
use super::migration::{chain_fingerprint, MigrationRunner};
use super::migrations::TURSO_MIGRATIONS;
use super::{DbError, DbResult};

const TEMPLATE_FILE: &str = "template.db";

/// The published template for this process's migration chain, built or found
/// once per process.
static TEMPLATE: OnceCell<PathBuf> = OnceCell::const_new();

impl LocalDb {
    /// Open the database at `path` with the full private migration chain applied:
    /// the same end state as `LocalDb::open(path)` followed by
    /// `MigrationRunner::new(TURSO_MIGRATIONS.to_vec()).run(&db)`.
    ///
    /// When nothing exists at `path` yet, the migrated state is copied from the
    /// run's template rather than replayed. When a database is already there,
    /// this opens it and runs the chain over it exactly as that pair of calls
    /// would, since a copy would clobber what the caller put there.
    pub async fn open_migrated_for_test(path: impl AsRef<Path>) -> DbResult<Self> {
        let path = path.as_ref();
        if db_set_paths(path).iter().any(|member| member.exists()) {
            let db = Self::open(path).await?;
            MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
                .run(&db)
                .await?;
            return Ok(db);
        }
        let template = TEMPLATE.get_or_try_init(published_template).await?;
        for (from, to) in db_set_paths(template).iter().zip(db_set_paths(path)) {
            if from.exists() {
                std::fs::copy(from, &to).map_err(|error| {
                    DbError::internal(format!(
                        "copy test template {} to {}: {error}",
                        from.display(),
                        to.display()
                    ))
                })?;
            }
        }
        Self::open(path).await
    }
}

/// Find this chain's template under the temp dir, or build and publish it.
///
/// Publication is an atomic directory rename, so a reader sees either no
/// template or a complete one. Processes that start together may each build
/// one; the first rename wins and the others discard theirs.
async fn published_template() -> DbResult<PathBuf> {
    let root = std::env::temp_dir().join("cairn-test-db-templates");
    let published = root.join(chain_fingerprint(TURSO_MIGRATIONS));
    let template = published.join(TEMPLATE_FILE);
    if template.exists() {
        return Ok(template);
    }

    let io = |what: &str, error: std::io::Error| {
        DbError::internal(format!("test template {what}: {error}"))
    };
    std::fs::create_dir_all(&root).map_err(|e| io("root", e))?;
    let staging = tempfile::Builder::new()
        .prefix("building-")
        .tempdir_in(&root)
        .map_err(|e| io("staging dir", e))?;
    {
        let db = LocalDb::open(staging.path().join(TEMPLATE_FILE)).await?;
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await?;
        // Fold the logical log into the main file so every copy opens without
        // replaying it. A pass that loses still leaves a valid three-file set,
        // which the copy carries whole.
        db.checkpoint(
            3,
            Duration::from_millis(10),
            crate::storage::DrainBudget::fixed(Duration::from_secs(1)),
        )
        .await?;
    }

    let staged = staging.keep();
    if std::fs::rename(&staged, &published).is_err() {
        let _ = std::fs::remove_dir_all(&staged);
        if !template.exists() {
            return Err(DbError::internal(format!(
                "test template could not be published at {}",
                published.display()
            )));
        }
    }
    Ok(template)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RowExt;

    async fn schema_and_ledger(db: &LocalDb) -> (Vec<String>, Vec<String>) {
        let schema = db
            .query_all(
                "SELECT type || ' ' || name || ' ' || coalesce(sql, '') FROM sqlite_schema ORDER BY type, name",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        let ledger = db
            .query_all(
                "SELECT version || '_' || name FROM cairn_schema_migrations ORDER BY version",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        (schema, ledger)
    }

    #[tokio::test]
    async fn a_template_copy_matches_a_replayed_chain() {
        let dir = tempfile::tempdir().unwrap();
        let replayed = LocalDb::open_migrated_for_test(dir.path().join("replayed.db"))
            .await
            .unwrap();
        let copied = LocalDb::open_migrated_for_test(dir.path().join("copied.db"))
            .await
            .unwrap();

        assert_eq!(
            schema_and_ledger(&copied).await,
            schema_and_ledger(&replayed).await
        );
        let reapplied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&copied)
            .await
            .unwrap();
        assert!(reapplied.is_empty(), "{reapplied:?}");
    }

    #[tokio::test]
    async fn copies_are_independent_databases() {
        let dir = tempfile::tempdir().unwrap();
        let first = LocalDb::open_migrated_for_test(dir.path().join("first.db"))
            .await
            .unwrap();
        let second = LocalDb::open_migrated_for_test(dir.path().join("second.db"))
            .await
            .unwrap();
        first
            .execute(
                "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1)",
                (),
            )
            .await
            .unwrap();
        let in_second: Vec<String> = second
            .query_all("SELECT id FROM workspaces WHERE id = 'w'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        assert!(in_second.is_empty(), "{in_second:?}");
    }

    #[tokio::test]
    async fn an_existing_database_is_migrated_in_place_not_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.db");
        {
            let db = LocalDb::open(&path).await.unwrap();
            db.execute_batch("CREATE TABLE keepsake (id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
        }
        let db = LocalDb::open_migrated_for_test(&path).await.unwrap();
        let tables: Vec<String> = db
            .query_all(
                "SELECT name FROM sqlite_schema WHERE name IN ('keepsake', 'cairn_schema_migrations') ORDER BY name",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(tables, ["cairn_schema_migrations", "keepsake"]);
    }
}
