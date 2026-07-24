use super::{DbError, DbResult, LocalDb, RowExt};
use turso::Connection;

/// Outcome of reading every `PRAGMA integrity_check` row.
#[derive(Debug, PartialEq, Eq)]
enum IntegrityStatus {
    Ok,
    /// Every failure was `wrong # of entries in index <name>` and is recoverable
    /// by rebuilding the named indexes from their recorded DDL.
    IndexDrift(Vec<String>),
    /// At least one failure is something other than benign index-entry drift.
    Corrupt(Vec<String>),
}

const INDEX_DRIFT_PREFIX: &str = "wrong # of entries in index ";

/// Classify the raw `integrity_check` rows. `ok` only means a single `ok` row.
fn classify_integrity(rows: Vec<String>) -> IntegrityStatus {
    if rows.len() == 1 && rows[0] == "ok" {
        return IntegrityStatus::Ok;
    }

    let mut drifted = Vec::new();
    for msg in &rows {
        match msg.strip_prefix(INDEX_DRIFT_PREFIX) {
            Some(name) if !name.is_empty() => drifted.push(name.to_string()),
            _ => return IntegrityStatus::Corrupt(rows),
        }
    }

    IntegrityStatus::IndexDrift(drifted)
}

async fn read_integrity_check(conn: &Connection) -> DbResult<IntegrityStatus> {
    let mut rows = conn.query("PRAGMA integrity_check", ()).await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(row.text(0)?);
    }

    if out.is_empty() {
        return Err(DbError::Row("integrity_check returned no rows".to_string()));
    }

    Ok(classify_integrity(out))
}

/// Rebuild each named index from its recorded schema DDL.
///
/// Auto-created UNIQUE/PRIMARY KEY indexes have no stored `CREATE INDEX` SQL, so
/// they cannot safely be dropped and recreated this way. Treat them as
/// unrecoverable instead of silently skipping a named integrity failure.
async fn rebuild_indexes(conn: &Connection, names: &[String]) -> DbResult<()> {
    for name in names {
        let mut rows = conn
            .query(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                (name.as_str(),),
            )
            .await?;
        let ddl = match rows.next().await? {
            Some(row) => row.opt_text(0)?,
            None => None,
        };
        drop(rows);

        let Some(ddl) = ddl else {
            return Err(DbError::Migration(format!(
                "cannot rebuild index {name}: no recorded CREATE INDEX DDL (auto-index or missing)"
            )));
        };

        conn.execute(
            &format!("DROP INDEX IF EXISTS \"{}\"", name.replace('"', "\"\"")),
            (),
        )
        .await?;
        conn.execute_batch(&ddl).await?;
    }

    Ok(())
}

/// Best-effort, one-shot index-drift repair for a freshly imported database.
///
/// Returns the names of any rebuilt indexes. Errors only when a genuinely
/// unrecoverable problem is found or a rebuild fails; import callers can log and
/// continue so the normal migration path surfaces real corruption.
pub async fn repair_index_entry_drift(db: &LocalDb) -> DbResult<Vec<String>> {
    let conn = db.connect().await?;
    match read_integrity_check(&conn).await? {
        IntegrityStatus::Ok => Ok(Vec::new()),
        IntegrityStatus::IndexDrift(names) => {
            rebuild_indexes(&conn, &names).await?;
            match read_integrity_check(&conn).await? {
                IntegrityStatus::Ok => Ok(names),
                other => Err(DbError::Migration(format!(
                    "integrity_check still failing after index rebuild: {other:?}"
                ))),
            }
        }
        IntegrityStatus::Corrupt(msgs) => Err(DbError::Migration(format!(
            "integrity_check found non-index-drift corruption: {}",
            msgs.join("; ")
        ))),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub(crate) version: &'static str,
    name: &'static str,
    sql: &'static str,
    /// When true, run this migration with foreign-key enforcement disabled.
    ///
    /// libsql has no usable `ALTER TABLE ... DROP COLUMN` and enforces
    /// `PRAGMA foreign_keys = ON` on every connection. Dropping an FK-child
    /// column, or rebuilding a table that other tables reference, therefore
    /// requires a full table rebuild with enforcement off. `PRAGMA foreign_keys`
    /// is a no-op inside an open transaction, so the runner toggles it *before*
    /// `BEGIN` on a dedicated connection.
    fk_off: bool,
}

impl Migration {
    /// A standard migration, applied inside the normal exclusive transaction
    /// with foreign keys enforced.
    pub(crate) const fn new(version: &'static str, name: &'static str, sql: &'static str) -> Self {
        Self {
            version,
            name,
            sql,
            fk_off: false,
        }
    }

    /// A migration that rebuilds FK-referenced tables and must run with foreign
    /// keys disabled. See the [`Migration::fk_off`] field docs.
    pub(crate) const fn rebuild_fk_off(
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

            match read_integrity_check(&conn).await? {
                IntegrityStatus::Ok => {}
                IntegrityStatus::IndexDrift(names) => {
                    log::warn!(
                        "{label}: integrity_check reported index-entry drift on {names:?}; rebuilding those indexes and re-verifying"
                    );
                    rebuild_indexes(&conn, &names).await?;
                    match read_integrity_check(&conn).await? {
                        IntegrityStatus::Ok => {
                            log::info!(
                                "{label}: index rebuild cleared integrity_check ({names:?})"
                            );
                        }
                        other => {
                            return Err(DbError::Migration(format!(
                                "{label} integrity_check still failing after index rebuild: {other:?}"
                            )));
                        }
                    }
                }
                IntegrityStatus::Corrupt(msgs) => {
                    return Err(DbError::Migration(format!(
                        "{label} integrity_check failed: {}",
                        msgs.join("; ")
                    )));
                }
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

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::TURSO_MIGRATIONS;

    async fn test_db(name: &str) -> LocalDb {
        let temp = tempdir().unwrap();
        let path = temp.keep().join(name);
        LocalDb::open(path).await.unwrap()
    }

    async fn index_sql(conn: &Connection, name: &str) -> DbResult<Option<String>> {
        let mut rows = conn
            .query(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                (name,),
            )
            .await?;
        match rows.next().await? {
            Some(row) => row.opt_text(0),
            None => Ok(None),
        }
    }

    #[test]
    fn classify_integrity_accepts_single_ok_row() {
        assert_eq!(
            classify_integrity(vec!["ok".to_string()]),
            IntegrityStatus::Ok
        );
    }

    #[test]
    fn classify_integrity_collects_index_drift_rows() {
        assert_eq!(
            classify_integrity(vec![
                "wrong # of entries in index idx_a".to_string(),
                "wrong # of entries in index idx_b".to_string(),
            ]),
            IntegrityStatus::IndexDrift(vec!["idx_a".to_string(), "idx_b".to_string()])
        );
    }

    #[test]
    fn classify_integrity_rejects_mixed_index_and_non_index_failures() {
        let rows = vec![
            "wrong # of entries in index idx_a".to_string(),
            "row 5 missing from index idx_b".to_string(),
        ];
        assert_eq!(
            classify_integrity(rows.clone()),
            IntegrityStatus::Corrupt(rows)
        );
    }

    #[test]
    fn classify_integrity_rejects_structural_corruption() {
        let rows = vec!["*** in database main *** Page 42: btree corruption".to_string()];
        assert_eq!(
            classify_integrity(rows.clone()),
            IntegrityStatus::Corrupt(rows)
        );
    }

    #[tokio::test]
    async fn rebuild_indexes_round_trips_recorded_ddl_inside_transaction() {
        let db = test_db("rebuild-index-round-trip.db").await;
        let conn = db.connect().await.unwrap();
        conn.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, updated_at INTEGER NOT NULL);
             CREATE INDEX idx_items_updated_at ON items(updated_at);",
        )
        .await
        .unwrap();

        let before = index_sql(&conn, "idx_items_updated_at")
            .await
            .unwrap()
            .unwrap();

        conn.execute("BEGIN", ()).await.unwrap();
        rebuild_indexes(&conn, &["idx_items_updated_at".to_string()])
            .await
            .unwrap();
        let after = index_sql(&conn, "idx_items_updated_at")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after, before);
        assert_eq!(
            read_integrity_check(&conn).await.unwrap(),
            IntegrityStatus::Ok
        );
        conn.execute("COMMIT", ()).await.unwrap();
    }

    #[tokio::test]
    async fn rebuild_indexes_errors_without_recorded_ddl() {
        let db = test_db("rebuild-index-missing-ddl.db").await;
        let conn = db.connect().await.unwrap();

        let err = rebuild_indexes(&conn, &["idx_does_not_exist".to_string()])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            DbError::Migration(message)
                if message == "cannot rebuild index idx_does_not_exist: no recorded CREATE INDEX DDL (auto-index or missing)"
        ));
    }

    #[tokio::test]
    async fn repair_index_entry_drift_is_noop_on_clean_migrated_db() {
        let db = test_db("repair-index-clean-migrated.db").await;
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let rebuilt = repair_index_entry_drift(&db).await.unwrap();
        assert!(rebuilt.is_empty());
    }
}
