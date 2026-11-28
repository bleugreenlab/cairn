use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tempfile::{tempdir, TempDir};

use super::*;

/// Tables shaped for the copy's edge cases, created in an order that makes
/// foreign keys matter: the child is created, and therefore copied, before
/// the parent it references.
const SHAPES: &str = "
CREATE TABLE reclaim_child (id INTEGER PRIMARY KEY, parent_id TEXT REFERENCES reclaim_parent(id), note TEXT);
CREATE TABLE reclaim_parent (id TEXT PRIMARY KEY, big BLOB);
CREATE INDEX reclaim_child_parent ON reclaim_child(parent_id);
CREATE TABLE reclaim_auto (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL);
CREATE TABLE reclaim_log (body TEXT);
CREATE TRIGGER reclaim_auto_log AFTER INSERT ON reclaim_auto BEGIN INSERT INTO reclaim_log(body) VALUES (NEW.body); END;
CREATE VIEW reclaim_view AS SELECT id, body FROM reclaim_auto;
CREATE TABLE reclaim_gaps (body BLOB, n REAL);
CREATE UNIQUE INDEX reclaim_gaps_n ON reclaim_gaps(n) WHERE n IS NOT NULL;
";

/// A value larger than [`small_pages`]'s page byte bound.
const BIG_ROW_BYTES: usize = 10_000;

fn small_pages() -> ReclaimLimits {
    ReclaimLimits {
        cap_bytes: u64::MAX,
        fold_bytes: DEFAULT_FOLD_BYTES,
        page_rows: 3,
        page_bytes: 4_096,
        stop_after_bytes: None,
    }
}

fn unbounded() -> Option<u64> {
    Some(1)
}

/// A migrated database seeded across real and edge-case tables, closed again
/// so the copy is the only thing that has it open.
async fn seeded_source() -> (TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    let path = temp.path().join("source.db");
    let db = LocalDb::open_migrated_for_test(&path).await.unwrap();
    db.execute_batch(SHAPES).await.unwrap();
    for i in 0..50 {
        db.execute(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES (?1, ?2, 1, 1)",
            (format!("w{i}"), format!("name-{i}")),
        )
        .await
        .unwrap();
    }
    db.execute_script(format!(
        "INSERT INTO reclaim_parent VALUES ('p1', randomblob({BIG_ROW_BYTES})), ('p2', X'01');
         INSERT INTO reclaim_child(id, parent_id, note) VALUES (1, 'p1', 'a'), (2, 'p2', NULL), (5, 'p1', 'e');
         INSERT INTO reclaim_gaps(rowid, body, n) VALUES (5, X'0102', 1.5), (7, NULL, NULL), (1000, 'text', 2.0), (-3, X'', NULL);"
    ))
    .await
    .unwrap();
    for i in 1..=10 {
        db.execute(
            "INSERT INTO reclaim_auto(body) VALUES (?1)",
            (format!("row-{i}"),),
        )
        .await
        .unwrap();
    }
    // The counter stays at 10 with the largest rowid at 8.
    db.execute("DELETE FROM reclaim_auto WHERE id > 8", ())
        .await
        .unwrap();
    drop(db);
    (temp, path)
}

fn set_absent(path: &Path) -> bool {
    db_set_paths(path).iter().all(|member| !member.exists())
}

#[tokio::test]
async fn the_copy_folds_its_log_whenever_it_passes_the_bound() {
    let temp = tempdir().unwrap();
    let source = temp.path().join("source.db");
    let db = LocalDb::open(&source).await.unwrap();
    db.execute_batch("CREATE TABLE filler (body TEXT)")
        .await
        .unwrap();
    for batch in 0..20 {
        let values: Vec<String> = (0..100)
            .map(|i| format!("('{}')", format!("{batch}-{i}-").repeat(20)))
            .collect();
        db.execute_script(format!("INSERT INTO filler VALUES {}", values.join(", ")))
            .await
            .unwrap();
    }
    drop(db);

    let dest = temp.path().join("copy.db");
    let fold_bytes = 32 * 1024;
    let limits = ReclaimLimits {
        fold_bytes,
        page_rows: 50,
        ..small_pages()
    };
    let report = copy_with_reader(&source, &dest, limits, &unbounded, |_| {})
        .await
        .unwrap();
    assert_eq!(report.rows, 2_000);
    // Every fold but the last was triggered by the bound.
    let triggered = &report.folds[..report.folds.len() - 1];
    assert!(triggered.len() >= 3, "{:?}", report.folds);
    for fold in triggered {
        assert!(fold.log_bytes_before >= fold_bytes, "{fold:?}");
        assert!(
            fold.log_bytes_before < fold_bytes * 4,
            "the log ran far past the bound before folding: {fold:?}"
        );
    }
    assert!(report.folds.last().unwrap().log_bytes_after < fold_bytes);
}

#[tokio::test]
async fn an_unreadable_memory_reading_fails_closed() {
    let (temp, source) = seeded_source().await;
    let dest = temp.path().join("blind.db");

    let never = || None;
    let error = copy_with_reader(&source, &dest, small_pages(), &never, |_| {})
        .await
        .unwrap_err();
    assert!(
        matches!(error, ReclaimError::MemoryUnreadable(1)),
        "{error}"
    );
    assert!(set_absent(&dest));

    let readings = AtomicU64::new(0);
    let goes_dark = || (readings.fetch_add(1, Ordering::Relaxed) < 2).then_some(1_000);
    let error = copy_with_reader(&source, &dest, small_pages(), &goes_dark, |_| {})
        .await
        .unwrap_err();
    assert!(
        matches!(error, ReclaimError::MemoryUnreadable(3)),
        "{error}"
    );
    assert!(set_absent(&dest), "the destination was left behind");
}

#[tokio::test]
async fn the_copy_refuses_an_occupied_or_misnamed_destination() {
    let (temp, source) = seeded_source().await;

    // A sidecar alone is enough to refuse: it would be adopted as the copy's.
    let occupied = temp.path().join("occupied.db");
    std::fs::write(temp.path().join("occupied.db-log"), b"x").unwrap();
    let error = copy_with_reader(&source, &occupied, small_pages(), &unbounded, |_| {})
        .await
        .unwrap_err();
    assert!(matches!(error, ReclaimError::Refused(_)), "{error}");
    assert!(!occupied.exists());
    assert_eq!(
        std::fs::read(temp.path().join("occupied.db-log")).unwrap(),
        b"x",
        "a refused copy touched what was there"
    );

    let misnamed = temp.path().join("copy.db.reclaimed");
    let error = copy_with_reader(&source, &misnamed, small_pages(), &unbounded, |_| {})
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must end in .db"), "{error}");
    assert!(set_absent(&misnamed));
}

#[test]
fn schema_sql_compares_modulo_whitespace_outside_quotes() {
    assert_eq!(
        normalized_sql("CREATE TRIGGER t AFTER INSERT ON a BEGIN\nSELECT 1;\n  END"),
        normalized_sql("CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; END"),
    );
    assert_eq!(
        normalized_sql(
            "CREATE TABLE a (s TEXT CHECK(s IN ('x')), FOREIGN KEY (s) REFERENCES b(id))"
        ),
        normalized_sql(
            "CREATE TABLE a (s TEXT CHECK (s IN ('x')), FOREIGN KEY (s) REFERENCES b (id))"
        ),
    );
    assert_ne!(
        normalized_sql("SELECT RAISE (ABORT, 'two  spaces')"),
        normalized_sql("SELECT RAISE (ABORT, 'two spaces')"),
    );
    assert_ne!(
        normalized_sql("CREATE TABLE \"a  b\" (x)"),
        normalized_sql("CREATE TABLE \"a b\" (x)"),
    );
    assert_ne!(
        normalized_sql("CREATE INDEX i ON t (a)"),
        normalized_sql("CREATE INDEX i ON t (b)"),
    );
}

#[tokio::test]
async fn verification_catches_a_changed_value_and_a_missing_row() {
    let (temp, source) = seeded_source().await;
    let dest = temp.path().join("copy.db");
    copy_with_reader(&source, &dest, small_pages(), &unbounded, |_| {})
        .await
        .unwrap();

    let copy = LocalDb::open(&dest).await.unwrap();
    copy.execute("UPDATE reclaim_gaps SET body = X'0103' WHERE rowid = 5", ())
        .await
        .unwrap();
    copy.execute("DELETE FROM workspaces WHERE id = 'w7'", ())
        .await
        .unwrap();
    drop(copy);

    let verification = verify(&source, &dest).await.unwrap();
    let mismatched: BTreeMap<&str, &TableDigest> = verification
        .tables
        .iter()
        .filter(|table| !table.matches())
        .map(|table| (table.table.as_str(), table))
        .collect();
    assert_eq!(
        mismatched.keys().copied().collect::<Vec<_>>(),
        vec!["reclaim_gaps", "workspaces"]
    );
    let changed = mismatched["reclaim_gaps"];
    assert_eq!(changed.source_rows, changed.dest_rows);
    assert_ne!(changed.source_sha256, changed.dest_sha256);
    let short = mismatched["workspaces"];
    assert_eq!(short.source_rows, short.dest_rows + 1);
    assert!(!verification.passed());
}

/// A SQLite varint, for values below 2^56 (eight bytes or fewer).
fn varint(mut value: u64) -> Vec<u8> {
    assert!(value < 1 << 56);
    let mut groups = vec![(value & 0x7f) as u8];
    value >>= 7;
    while value > 0 {
        groups.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    groups.reverse();
    groups
}

/// A `sqlite_master` record: type, name, tbl_name, rootpage, sql.
fn master_record(kind: &str, name: &str, table: &str, rootpage: i64, sql: &str) -> Vec<u8> {
    let text_type = |text: &str| varint(13 + 2 * text.len() as u64);
    let mut types = Vec::new();
    let mut body = Vec::new();
    for text in [kind, name, table] {
        types.extend(text_type(text));
        body.extend(text.as_bytes());
    }
    if rootpage == 0 {
        types.push(8);
    } else {
        types.push(6);
        body.extend(rootpage.to_be_bytes());
    }
    types.extend(text_type(sql));
    body.extend(sql.as_bytes());
    let mut header_len = types.len() + 1;
    while varint(header_len as u64).len() + types.len() != header_len {
        header_len = varint(header_len as u64).len() + types.len();
    }
    let mut record = varint(header_len as u64);
    record.extend(types);
    record.extend(body);
    record
}

/// Add a row to the `sqlite_master` of the database at `path`, folded and
/// closed, by writing it into the file.
///
/// No statement can: the pinned engine refuses DML on the schema table, has
/// no `writable_schema`, and every SQL sequence tried for cairn/7751 (a stale
/// second connection, two concurrent transactions, a drop and re-create in one
/// transaction) leaves one row per name. A small schema is page 1 alone, a
/// table leaf, so the row is one more cell there, at a rowid above the rest.
fn append_master_row(path: &Path, rowid: i64, row: (&str, &str, &str, i64, &str)) {
    let (kind, name, table, rootpage, sql) = row;
    let mut file = std::fs::read(path).unwrap();
    let page_size = match u16::from_be_bytes([file[16], file[17]]) {
        1 => 65_536,
        size => size as usize,
    };
    let usable = page_size - file[20] as usize;
    assert_eq!(file[100], 0x0d, "sqlite_master is not a single leaf page");
    let cells = u16::from_be_bytes([file[103], file[104]]) as usize;
    let content = match u16::from_be_bytes([file[105], file[106]]) {
        0 => 65_536,
        start => start as usize,
    };
    let payload = master_record(kind, name, table, rootpage, sql);
    assert!(payload.len() <= usable - 35, "the row would overflow");
    let mut cell = varint(payload.len() as u64);
    cell.extend(varint(rowid as u64));
    cell.extend(payload);
    let start = content - cell.len();
    let pointer = 108 + 2 * cells;
    assert!(start >= pointer + 2, "page 1 has no room for the row");
    file[start..content].copy_from_slice(&cell);
    file[pointer..pointer + 2].copy_from_slice(&(start as u16).to_be_bytes());
    file[103..105].copy_from_slice(&((cells + 1) as u16).to_be_bytes());
    file[105..107].copy_from_slice(&(start as u16).to_be_bytes());
    std::fs::write(path, file).unwrap();
}

type MasterRow = (i64, String, String, String, i64, Option<String>);

async fn master_rows(db: &LocalDb) -> Vec<MasterRow> {
    db.query_all(
        "SELECT rowid, type, name, tbl_name, rootpage, sql FROM sqlite_master ORDER BY rowid",
        (),
        |row| {
            Ok((
                row.i64(0)?,
                row.text(1)?,
                row.text(2)?,
                row.text(3)?,
                row.i64(4)?,
                row.opt_text(5)?,
            ))
        },
    )
    .await
    .unwrap()
}

const SMALL_SCHEMA: &str = "
CREATE TABLE dup_target (id INTEGER PRIMARY KEY, body TEXT);
CREATE TABLE dup_log (body TEXT);
CREATE TRIGGER dup_target_log AFTER INSERT ON dup_target BEGIN INSERT INTO dup_log(body) VALUES (NEW.body); END;
";

/// A small database (its schema on page 1) with rows in it, folded and
/// closed, and its `sqlite_master` rows. Its migration ledger records every
/// migration as applied, as a real database's does, so verification's
/// migration check has nothing to apply; the migrated schema itself would not
/// fit on page 1.
async fn small_source(temp: &TempDir) -> (PathBuf, Vec<MasterRow>) {
    let path = temp.path().join("small.db");
    let db = LocalDb::open(&path).await.unwrap();
    db.execute_batch(SMALL_SCHEMA).await.unwrap();
    let ledger: Vec<String> = TURSO_MIGRATIONS
        .iter()
        .map(|migration| format!("('{}', '{}', 1)", migration.version, migration.name))
        .collect();
    db.execute_script(format!(
        "CREATE TABLE {MIGRATION_LEDGER} (version TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, \
         applied_at INTEGER NOT NULL);
         INSERT INTO {MIGRATION_LEDGER}(version, name, applied_at) VALUES {};",
        ledger.join(", ")
    ))
    .await
    .unwrap();
    for i in 0..20 {
        db.execute(
            "INSERT INTO dup_target(body) VALUES (?1)",
            (format!("b{i}"),),
        )
        .await
        .unwrap();
    }
    let rows = master_rows(&db).await;
    let folded = fold(&db, 0).await.unwrap();
    assert_eq!(folded.log_bytes_after, 0);
    drop(db);
    (path, rows)
}

#[tokio::test]
async fn a_trigger_listed_twice_reclaims_and_verifies_naming_the_duplicate() {
    let temp = tempdir().unwrap();
    let (source, rows) = small_source(&temp).await;
    let (old_rowid, _, name, table, rootpage, sql) =
        rows.iter().find(|row| row.1 == "trigger").cloned().unwrap();
    let new_rowid = rows.iter().map(|row| row.0).max().unwrap() + 1;
    let sql = sql.unwrap();
    append_master_row(
        &source,
        new_rowid,
        ("trigger", &name, &table, rootpage, &sql),
    );
    let db = LocalDb::open(&source).await.unwrap();
    let listed = master_rows(&db).await;
    assert_eq!(
        listed.iter().filter(|row| row.2 == name).count(),
        2,
        "the seeding did not duplicate the trigger: {listed:?}"
    );
    drop(db);

    let dest = temp.path().join("copy.db");
    let report = copy_with_reader(&source, &dest, small_pages(), &unbounded, |_| {})
        .await
        .unwrap();
    let expected = vec![SchemaDuplicate {
        kind: "trigger".to_string(),
        name: name.clone(),
        kept_rowid: new_rowid,
        dropped_rowids: vec![old_rowid],
        texts_differ: false,
    }];
    assert_eq!(report.schema_duplicates, expected);
    assert_eq!(report.schema_rows_dropped, 1);
    assert_eq!(report.rows, 40 + TURSO_MIGRATIONS.len() as u64);

    let verification = verify(&source, &dest).await.unwrap();
    assert!(verification.passed(), "{:?}", verification.failures());
    assert_eq!(verification.source_schema_duplicates, expected);
    let copy = LocalDb::open(&dest).await.unwrap();
    let triggers: Vec<String> = master_rows(&copy)
        .await
        .into_iter()
        .filter(|row| row.1 == "trigger")
        .map(|row| row.2)
        .collect();
    assert_eq!(triggers, vec![name]);
}

#[tokio::test]
async fn an_unruled_table_is_named_without_failing_the_copy() {
    let (temp, source) = seeded_source().await;
    let dest = temp.path().join("copy.db");
    let report = copy_with_reader(&source, &dest, small_pages(), &unbounded, |_| {})
        .await
        .unwrap();
    assert!(report.schema_duplicates.is_empty());

    let verification = verify(&source, &dest).await.unwrap();
    assert!(verification.passed(), "{:?}", verification.failures());
    assert!(
        verification
            .unruled_tables
            .contains(&"reclaim_child".to_string()),
        "{:?}",
        verification.unruled_tables
    );
}

#[tokio::test]
async fn an_unreplayable_schema_fails_before_any_row_is_copied() {
    let temp = tempdir().unwrap();
    let (source, rows) = small_source(&temp).await;
    // A trigger outliving its table opens, but `CREATE TRIGGER` refuses it.
    let rowid = rows.iter().map(|row| row.0).max().unwrap() + 1;
    append_master_row(
        &source,
        rowid,
        (
            "trigger",
            "orphan_trigger",
            "gone_table",
            0,
            "CREATE TRIGGER orphan_trigger AFTER INSERT ON gone_table BEGIN SELECT 1; END",
        ),
    );

    let dest = temp.path().join("copy.db");
    let mut progress = 0;
    let error = copy_with_reader(&source, &dest, small_pages(), &unbounded, |_| progress += 1)
        .await
        .unwrap_err();
    let ReclaimError::Schema(problems) = &error else {
        panic!("{error}");
    };
    assert!(
        problems
            .iter()
            .any(|problem| problem.starts_with("trigger orphan_trigger:")),
        "{problems:?}"
    );
    assert_eq!(progress, 0, "rows were copied before the schema failed");
    assert!(set_absent(&dest));

    let rehearsal = rehearse_with_reader(&source, &dest, small_pages(), &unbounded).await;
    assert!(matches!(rehearsal, Err(ReclaimError::Schema(_))));
    assert!(set_absent(&dest));
    let leftovers: Vec<_> = std::fs::read_dir(temp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().starts_with(".reclaim-schema-check"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[tokio::test]
async fn a_rehearsal_checks_everything_but_the_rows_and_leaves_nothing() {
    let (temp, source) = seeded_source().await;
    let dest = temp.path().join("rehearsal.db");
    let rehearsal = rehearse_with_reader(&source, &dest, small_pages(), &unbounded)
        .await
        .unwrap();
    let verification = &rehearsal.verification;
    assert!(verification.passed(), "{:?}", verification.failures());
    assert!(rehearsal.copy.rehearsal && verification.rehearsal);
    assert_eq!(
        rehearsal
            .copy
            .tables
            .iter()
            .map(|table| table.table.as_str())
            .collect::<Vec<_>>(),
        vec![MIGRATION_LEDGER]
    );
    assert!(rehearsal.copy.tables[0].rows > 0);
    // The counter above the largest rowid is carried with no rows behind it.
    assert_eq!(
        rehearsal.copy.sequences_raised,
        vec![("reclaim_auto".to_string(), 10)]
    );
    assert!(verification.sequence_differences.is_empty());
    assert!(verification.migrations_applied.is_empty());
    assert_eq!(verification.foreign_key_check, ForeignKeyCheck::NotRun);
    assert!(verification
        .unruled_tables
        .contains(&"reclaim_child".to_string()));
    assert!(set_absent(&dest), "the rehearsal left its copy behind");
}
