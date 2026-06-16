use super::{DbError, DbResult, LocalDb, RowExt};

#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub version: &'static str,
    pub name: &'static str,
    pub sql: &'static str,
    /// When true, run this migration with foreign-key enforcement disabled.
    ///
    /// libsql has no usable `ALTER TABLE ... DROP COLUMN` and enforces
    /// `PRAGMA foreign_keys = ON` on every connection. Dropping an FK-child
    /// column, or rebuilding a table that other tables reference, therefore
    /// requires a full table rebuild with enforcement off. `PRAGMA foreign_keys`
    /// is a no-op inside an open transaction, so the runner toggles it *before*
    /// `BEGIN` on a dedicated connection.
    pub fk_off: bool,
}

impl Migration {
    /// A standard migration, applied inside the normal exclusive transaction
    /// with foreign keys enforced.
    pub const fn new(version: &'static str, name: &'static str, sql: &'static str) -> Self {
        Self {
            version,
            name,
            sql,
            fk_off: false,
        }
    }

    /// A migration that rebuilds FK-referenced tables and must run with foreign
    /// keys disabled. See the [`Migration::fk_off`] field docs.
    pub const fn rebuild_fk_off(
        version: &'static str,
        name: &'static str,
        sql: &'static str,
    ) -> Self {
        Self {
            version,
            name,
            sql,
            fk_off: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationRunner {
    migrations: Vec<Migration>,
}

impl MigrationRunner {
    pub fn new(migrations: impl Into<Vec<Migration>>) -> Self {
        Self {
            migrations: migrations.into(),
        }
    }

    pub async fn run(&self, db: &LocalDb) -> DbResult<Vec<String>> {
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS cairn_schema_migrations (
                version TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                applied_at INTEGER NOT NULL
            )",
        )
        .await?;

        let mut applied = Vec::new();
        for migration in &self.migrations {
            if self.is_applied(db, migration.version).await? {
                continue;
            }

            if migration.fk_off {
                self.run_fk_off(db, migration).await?;
            } else {
                self.run_standard(db, migration).await?;
            }
            applied.push(format!("{}_{}", migration.version, migration.name));
        }

        Ok(applied)
    }

    /// Apply a migration inside the normal exclusive transaction.
    async fn run_standard(&self, db: &LocalDb, migration: &Migration) -> DbResult<()> {
        let version = migration.version.to_string();
        let name = migration.name.to_string();
        let sql = migration.sql;
        db.exclusive(|conn| {
            let version = version.clone();
            let name = name.clone();
            Box::pin(async move {
                conn.execute_batch(sql).await?;
                conn.execute(
                    "INSERT INTO cairn_schema_migrations(version, name, applied_at)
                     VALUES (?1, ?2, unixepoch())",
                    (version.as_str(), name.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| {
            DbError::Migration(format!(
                "{}_{} failed: {}",
                migration.version, migration.name, error
            ))
        })
    }

    /// Apply a migration with foreign-key enforcement disabled.
    ///
    /// Used for table rebuilds that drop FK-child columns or recreate tables
    /// that other tables reference. The connection toggles
    /// `PRAGMA foreign_keys = OFF` *before* opening the transaction (the pragma
    /// is ignored once a transaction is open), performs the rebuild inside an
    /// explicit `BEGIN`/`COMMIT`, runs a structural `PRAGMA integrity_check`,
    /// records the version, and restores enforcement. libsql does not implement
    /// `PRAGMA foreign_key_check`, so referential integrity is guaranteed by the
    /// migration's construction (a full rebuild leaving no surviving references
    /// to dropped tables); integrity_check only guards against a structurally
    /// botched rebuild. The version record is committed atomically with the
    /// rebuild, so a crash leaves the migration entirely unapplied and a re-run
    /// is a clean retry.
    async fn run_fk_off(&self, db: &LocalDb, migration: &Migration) -> DbResult<()> {
        let label = format!("{}_{}", migration.version, migration.name);
        let conn = db.connect().await?;
        // Must precede BEGIN: connect() set foreign_keys = ON, and the pragma is
        // a no-op inside an open transaction.
        conn.execute("PRAGMA foreign_keys = OFF", ())
            .await
            .map_err(|e| {
                DbError::Migration(format!("{label} failed to disable foreign keys: {e}"))
            })?;

        let outcome: DbResult<()> = async {
            conn.execute("BEGIN", ()).await?;
            conn.execute_batch(migration.sql).await?;

            let mut rows = conn.query("PRAGMA integrity_check", ()).await?;
            let status = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row("integrity_check returned no rows".to_string()))?
                .text(0)?;
            drop(rows);
            if status != "ok" {
                return Err(DbError::Migration(format!(
                    "{label} integrity_check failed: {status}"
                )));
            }

            conn.execute(
                "INSERT INTO cairn_schema_migrations(version, name, applied_at)
                 VALUES (?1, ?2, unixepoch())",
                (migration.version, migration.name),
            )
            .await?;
            conn.execute("COMMIT", ()).await?;
            Ok(())
        }
        .await;

        match outcome {
            Ok(()) => {
                conn.execute("PRAGMA foreign_keys = ON", ())
                    .await
                    .map_err(|e| {
                        DbError::Migration(format!("{label} failed to re-enable foreign keys: {e}"))
                    })?;
                Ok(())
            }
            Err(error) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                let _ = conn.execute("PRAGMA foreign_keys = ON", ()).await;
                Err(match error {
                    DbError::Migration(_) => error,
                    other => DbError::Migration(format!("{label} failed: {other}")),
                })
            }
        }
    }

    async fn is_applied(&self, db: &LocalDb, version: &str) -> DbResult<bool> {
        let version = version.to_string();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM cairn_schema_migrations WHERE version = ?1",
                        (version.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("missing migration count row".to_string()))?;
                Ok(row.i64(0)? > 0)
            })
        })
        .await
    }
}
