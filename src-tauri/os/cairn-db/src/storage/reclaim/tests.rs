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
