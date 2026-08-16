use std::collections::HashSet;

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

/// Cadence for the whole-database integrity sweep. Verifying every index of a
/// multi-gigabyte database costs on the order of a minute of CPU, far too much to
/// pay on every boot for drift that accumulates over weeks.
const INTEGRITY_SWEEP_INTERVAL_SECS: i64 = 7 * 24 * 60 * 60;

/// What a whole-database integrity sweep found.
#[derive(Debug, PartialEq, Eq)]
pub enum IntegritySweepOutcome {
    /// The previous sweep is still inside the cadence window; nothing was read.
    Skipped,
    Clean,
    /// Index-entry drift was found, and rebuilding those indexes cleared it.
    Repaired(Vec<String>),
    /// Index-entry drift survived the rebuild, or a named index had no recorded
    /// DDL to rebuild from (an auto-index).
    Unrepaired(Vec<String>),
    /// Something other than index-entry drift. Reported, never repaired.
    Corrupt(Vec<String>),
}

/// Verify the whole database and repair recoverable index-entry drift.
///
/// This is maintenance, not a gate: every outcome is a log line plus a recorded
/// sweep timestamp, and nothing here can fail a boot. Migrations verify their own
/// blast radius (see [`RebuildCheck`]), so whole-database verification has no
/// reason to sit on the startup path, where its cost scales with the database
/// rather than with the work being done.
pub async fn run_integrity_sweep(db: &LocalDb) -> DbResult<IntegritySweepOutcome> {
    if !integrity_sweep_due(db).await? {
        return Ok(IntegritySweepOutcome::Skipped);
    }

    let conn = db.connect().await?;
    let outcome = match read_integrity_check(&conn).await? {
        IntegrityStatus::Ok => {
            log::info!("integrity sweep: database verified clean");
            IntegritySweepOutcome::Clean
        }
        IntegrityStatus::IndexDrift(names) => {
            log::warn!("integrity sweep: index-entry drift on {names:?}; rebuilding those indexes");
            match rebuild_indexes(&conn, &names).await {
                Ok(()) => match read_integrity_check(&conn).await? {
                    IntegrityStatus::Ok => {
                        log::info!("integrity sweep: index rebuild cleared drift on {names:?}");
                        IntegritySweepOutcome::Repaired(names)
                    }
                    other => {
                        log::error!(
                            "integrity sweep: integrity_check still failing after index rebuild: {other:?}"
                        );
                        IntegritySweepOutcome::Unrepaired(names)
                    }
                },
                Err(error) => {
                    log::error!("integrity sweep: cannot rebuild {names:?}: {error}");
                    IntegritySweepOutcome::Unrepaired(names)
                }
            }
        }
        IntegrityStatus::Corrupt(msgs) => {
            log::error!(
                "integrity sweep: non-index-drift corruption found: {}",
                msgs.join("; ")
            );
            IntegritySweepOutcome::Corrupt(msgs)
        }
    };

    // Recorded for every outcome, including the bad ones: a database that reports
    // corruption should not re-pay a whole-database verification on every boot.
    record_integrity_sweep(db).await?;
    Ok(outcome)
}

async fn integrity_sweep_due(db: &LocalDb) -> DbResult<bool> {
    db.query_one(
        "SELECT last_swept_at IS NULL OR last_swept_at <= unixepoch() - ?1
         FROM integrity_sweep_state WHERE id = 1",
        (INTEGRITY_SWEEP_INTERVAL_SECS,),
        |row| Ok(row.i64(0)? != 0),
    )
    .await
}

async fn record_integrity_sweep(db: &LocalDb) -> DbResult<()> {
    db.execute(
        "UPDATE integrity_sweep_state SET last_swept_at = unixepoch() WHERE id = 1",
        (),
    )
    .await
    .map(|_| ())
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

/// What [`MigrationRunner::run_fk_off`] verifies before committing a table-rebuild
/// migration.
///
/// The rebuild's own `INSERT INTO <new> SELECT ... FROM <old>` already enforces
/// the new table's NOT NULL, CHECK, and PRIMARY KEY constraints on every row it
/// writes, and `CREATE INDEX` builds each index from those same rows. The
/// residual risk is rows lost or duplicated in transit — a `SELECT` that silently
/// filters, a join that fans out, a truncated copy — which a count comparison
/// catches at a cost proportional to the tables the migration touched rather than
/// to the whole database.
///
/// Declared per migration rather than sniffed from the SQL: the rebuilt table
/// names are only mostly inferable (most rebuilds use a `_new` suffix, 0118
/// inverts that with `_legacy`, and 0038 rebuilds nothing at all), so a regex
/// would silently verify nothing whenever an author deviated.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RebuildCheck {
    /// Every source row is carried across. The post-rebuild count must equal the
    /// count taken inside the transaction before the migration SQL ran.
    Conserved(&'static str),
    /// The migration intentionally drops or filters rows, so no count relation
    /// holds. The table is still scanned, so a structurally broken btree fails the
    /// migration instead of committing.
    Reshaped(&'static str),
}

impl RebuildCheck {
    const fn table(self) -> &'static str {
        match self {
            Self::Conserved(table) | Self::Reshaped(table) => table,
        }
    }
}

async fn count_rows(conn: &Connection, table: &str) -> DbResult<i64> {
    let sql = format!("SELECT COUNT(*) FROM \"{}\"", table.replace('"', "\"\""));
    let mut rows = conn.query(&sql, ()).await?;
    rows.next()
        .await?
        .ok_or_else(|| DbError::Row(format!("no count row for {table}")))?
        .i64(0)
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
    /// Tables whose rebuild this migration wants verified before it commits. Only
    /// meaningful for `fk_off` migrations; see [`RebuildCheck`].
    verify: &'static [RebuildCheck],
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
            verify: &[],
        }
    }

    /// A migration that rebuilds FK-referenced tables and must run with foreign
    /// keys disabled. See the [`Migration::fk_off`] field docs.
    ///
    /// `verify` states what the rebuild is expected to preserve; it is a required
    /// parameter so every call site declares its intent rather than inheriting a
    /// default that quietly verifies nothing. See [`RebuildCheck`].
    pub(crate) const fn rebuild_fk_off(
        version: &'static str,
        name: &'static str,
        sql: &'static str,
        verify: &'static [RebuildCheck],
    ) -> Self {
        Self {
            version,
            name,
            sql,
            fk_off: true,
            verify,
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

        // Read the ledger once, up front, rather than re-asking it per migration.
        // The chain is well over a hundred entries long and every one of those
        // lookups put the same single-row question to the same table.
        let mut recorded = self.applied_versions(db).await?;

        let mut applied = Vec::new();
        for migration in &self.migrations {
            if recorded.contains(migration.version) {
                continue;
            }

            if migration.fk_off {
                self.run_fk_off(db, migration).await?;
            } else {
                self.run_standard(db, migration).await?;
            }
            // Kept current as the chain applies, so a version listed twice is
            // still applied exactly once — the behaviour the per-migration
            // re-query gave for free.
            recorded.insert(migration.version.to_string());
            applied.push(format!("{}_{}", migration.version, migration.name));
        }

        Ok(applied)
    }

    /// Apply a migration inside the normal exclusive transaction.
    async fn run_standard(&self, db: &LocalDb, migration: &Migration) -> DbResult<()> {
        let version = migration.version.to_string();
        let name = migration.name.to_string();
        let sql = migration.sql;
        let db_path = db.path().display().to_string();
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
            if migration.version == "0191"
                && error.to_string().contains("UNIQUE constraint failed")
            {
                return DbError::Migration(format!(
                    "Migration 0191 refused for database {db_path}: two or more projects' keys differ only by letter case, and project keys are canonically lowercase. Cairn will not merge or silently skip them. List them: SELECT id, key, name, repo_path FROM projects WHERE lower(key) IN (SELECT lower(key) FROM projects GROUP BY lower(key) HAVING COUNT(*) > 1); Resolve by giving one project a distinct key, updating both rows together: UPDATE projects SET key = '<new-key>' WHERE id = '<id>'; UPDATE project_routes SET project_key = '<new-key>' WHERE project_key = '<old-key>'; then restart. Renaming a key does not rewrite prose or links that already reference the old one."
                ));
            }
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
    /// explicit `BEGIN`/`COMMIT`, verifies the migration's declared
    /// [`RebuildCheck`]s, records the version, and restores enforcement. libsql
    /// does not implement `PRAGMA foreign_key_check`, so referential integrity is
    /// guaranteed by the migration's construction (a full rebuild leaving no
    /// surviving references to dropped tables).
    ///
    /// The verification is proportional to the migration. A rebuild's own
    /// `INSERT INTO <new> SELECT ... FROM <old>` enforces the new table's NOT
    /// NULL, CHECK, and PRIMARY KEY constraints on every row it writes, and each
    /// `CREATE INDEX` builds from those same freshly written rows, so constraint
    /// soundness and index construction are already proven inside this
    /// transaction. What remains is rows lost or duplicated in transit, which the
    /// count comparison catches.
    ///
    /// Deliberately no whole-database `PRAGMA integrity_check` or `quick_check`
    /// here. Both scale with the whole database rather than the migration's blast
    /// radius, and both report pre-existing index-entry drift from unrelated
    /// tables — which attributed that drift to whatever rebuild happened to run
    /// next, and, on an auto-index with no recorded DDL to rebuild from, would
    /// have failed the migration and bricked startup (CAIRN-3103).
    /// Whole-database verification lives in [`run_integrity_sweep`], which
    /// reports rather than gates. The version record is committed atomically with
    /// the rebuild, so a crash leaves the migration entirely unapplied and a
    /// re-run is a clean retry.
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

            // Snapshot inside the transaction, before the rebuild, so the
            // comparison is against the exact rows the migration carries across.
            let mut before = Vec::with_capacity(migration.verify.len());
            for check in migration.verify {
                before.push(count_rows(&conn, check.table()).await?);
            }

            conn.execute_batch(migration.sql).await?;

            for (check, before) in migration.verify.iter().zip(before) {
                let table = check.table();
                let after = count_rows(&conn, table).await?;
                match check {
                    RebuildCheck::Conserved(_) if after != before => {
                        return Err(DbError::Migration(format!(
                            "{label} rebuild of {table} changed row count: {before} before, {after} after"
                        )));
                    }
                    _ => log::info!("{label}: {table} rebuilt, {before} -> {after} rows"),
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

    /// Every migration version the ledger already records.
    async fn applied_versions(&self, db: &LocalDb) -> DbResult<HashSet<String>> {
        Ok(db
            .query_all("SELECT version FROM cairn_schema_migrations", (), |row| {
                row.text(0)
            })
            .await?
            .into_iter()
            .collect())
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

    /// Seed a three-row `items` table for the rebuild-verification tests.
    async fn seeded_items_db(name: &str) -> LocalDb {
        let db = test_db(name).await;
        db.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
             INSERT INTO items (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        )
        .await
        .unwrap();
        db
    }

    /// A full create-copy-drop-rename rebuild of `items` carrying every row.
    const CONSERVING_REBUILD: &str = "
        CREATE TABLE items_new (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
        INSERT INTO items_new (id, label) SELECT id, label FROM items;
        DROP TABLE items;
        ALTER TABLE items_new RENAME TO items;";

    /// The same rebuild with a filter that silently drops a row — the exact
    /// failure mode the count comparison exists to catch.
    const LOSSY_REBUILD: &str = "
        CREATE TABLE items_new (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
        INSERT INTO items_new (id, label) SELECT id, label FROM items WHERE id < 3;
        DROP TABLE items;
        ALTER TABLE items_new RENAME TO items;";

    async fn item_count(db: &LocalDb) -> i64 {
        let conn = db.connect().await.unwrap();
        count_rows(&conn, "items").await.unwrap()
    }

    async fn recorded_versions(db: &LocalDb) -> Vec<String> {
        db.query_all(
            "SELECT version FROM cairn_schema_migrations ORDER BY version",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn conserving_rebuild_commits() {
        let db = seeded_items_db("rebuild-conserving.db").await;
        let migration = Migration::rebuild_fk_off(
            "9001",
            "conserving",
            CONSERVING_REBUILD,
            &[RebuildCheck::Conserved("items")],
        );

        let applied = MigrationRunner::new(vec![migration])
            .run(&db)
            .await
            .unwrap();

        assert_eq!(applied, vec!["9001_conserving".to_string()]);
        assert_eq!(recorded_versions(&db).await, vec!["9001".to_string()]);
        assert_eq!(item_count(&db).await, 3);
    }

    #[tokio::test]
    async fn lossy_rebuild_rolls_back() {
        let db = seeded_items_db("rebuild-lossy.db").await;
        let migration = Migration::rebuild_fk_off(
            "9002",
            "lossy",
            LOSSY_REBUILD,
            &[RebuildCheck::Conserved("items")],
        );

        let error = MigrationRunner::new(vec![migration])
            .run(&db)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("rebuild of items changed row count: 3 before, 2 after"),
            "unexpected error: {error}"
        );
        // The rollback is the point: the migration is unrecorded and the original
        // rows survive, so a re-run is a clean retry.
        assert!(recorded_versions(&db).await.is_empty());
        assert_eq!(item_count(&db).await, 3);
    }

    #[tokio::test]
    async fn reshaped_rebuild_commits_despite_dropped_rows() {
        let db = seeded_items_db("rebuild-reshaped.db").await;
        let migration = Migration::rebuild_fk_off(
            "9003",
            "reshaped",
            LOSSY_REBUILD,
            &[RebuildCheck::Reshaped("items")],
        );

        MigrationRunner::new(vec![migration])
            .run(&db)
            .await
            .unwrap();

        assert_eq!(recorded_versions(&db).await, vec!["9003".to_string()]);
        assert_eq!(item_count(&db).await, 2);
    }

    #[tokio::test]
    async fn declared_table_that_does_not_exist_fails_loudly() {
        let db = seeded_items_db("rebuild-missing-table.db").await;
        let migration = Migration::rebuild_fk_off(
            "9004",
            "missing_table",
            CONSERVING_REBUILD,
            &[RebuildCheck::Conserved("not_a_table")],
        );

        let error = MigrationRunner::new(vec![migration])
            .run(&db)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("not_a_table"),
            "a declaration naming a table that does not exist must name it in the \
             failure rather than silently verifying nothing: {error}"
        );
        assert!(recorded_versions(&db).await.is_empty());
    }

    /// The direct regression test for CAIRN-3103. Real index drift cannot be
    /// manufactured in a unit test, but the absence of a whole-database pragma is
    /// exactly what makes unrelated drift unable to fail an unrelated migration,
    /// and that is checkable.
    #[test]
    fn rebuild_path_runs_no_whole_database_pragma() {
        const SOURCE: &str = include_str!("migration.rs");
        let start = SOURCE.find("    async fn run_fk_off").unwrap();
        let end = start
            + SOURCE[start..]
                .find("    async fn applied_versions")
                .unwrap();
        let body = &SOURCE[start..end];

        assert!(
            !body.contains("integrity_check") && !body.contains("quick_check"),
            "run_fk_off must not run a whole-database pragma: its cost scales with \
             the database rather than the migration, and it reports unrelated \
             pre-existing drift as this migration's failure"
        );
    }

    #[tokio::test]
    async fn integrity_sweep_runs_on_cadence() {
        let db = test_db("integrity-sweep-cadence.db").await;
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        // Never swept: the first call runs and records its timestamp.
        assert_eq!(
            run_integrity_sweep(&db).await.unwrap(),
            IntegritySweepOutcome::Clean
        );
        // Inside the cadence window the sweep is a fast no-op, so most boots pay
        // nothing for it.
        assert_eq!(
            run_integrity_sweep(&db).await.unwrap(),
            IntegritySweepOutcome::Skipped
        );

        db.execute(
            "UPDATE integrity_sweep_state SET last_swept_at = unixepoch() - ?1 WHERE id = 1",
            (INTEGRITY_SWEEP_INTERVAL_SECS + 1,),
        )
        .await
        .unwrap();
        assert_eq!(
            run_integrity_sweep(&db).await.unwrap(),
            IntegritySweepOutcome::Clean
        );
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
