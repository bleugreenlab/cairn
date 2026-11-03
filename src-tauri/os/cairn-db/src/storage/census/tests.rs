use tempfile::{tempdir, TempDir};

use super::*;

const NOW: i64 = 1_800_000_000;

async fn private_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(temp.path().join("census.db"))
        .await
        .unwrap();
    (temp, db)
}

async fn bare_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("bare.db")).await.unwrap();
    (temp, db)
}

/// `count` rows of `width`-byte text, ids `start, start + step, ...`.
async fn fill(db: &LocalDb, table: &str, start: i64, step: i64, count: i64, width: usize) {
    let value = "x".repeat(width);
    let mut id = start;
    let mut left = count;
    while left > 0 {
        let batch = left.min(400);
        let rows: Vec<String> = (0..batch)
            .map(|i| format!("({}, '{value}')", id + i * step))
            .collect();
        db.execute_script(format!(
            "INSERT INTO {table}(id, body) VALUES {}",
            rows.join(", ")
        ))
        .await
        .unwrap();
        id += batch * step;
        left -= batch;
    }
}

/// Exact rows and payload, by the same definition the census estimates.
async fn exact_of(db: &LocalDb, table: &str) -> (i64, i64) {
    let payload = payload_expr(&columns(db, table).await.unwrap());
    db.query_opt(
        format!("SELECT count(*), coalesce(sum({payload}), 0) FROM {table}"),
        (),
        |row| Ok((row.i64(0)?, row.i64(1)?)),
    )
    .await
    .unwrap()
    .unwrap()
}

fn within(estimate: i64, actual: i64, tolerance: f64) -> bool {
    ((estimate - actual) as f64).abs() <= tolerance * actual as f64
}

#[test]
fn payload_counts_text_and_blob_bytes_numbers_and_a_header() {
    let columns = [Column::new("id", "INTEGER"), Column::new("body", "TEXT")];
    assert_eq!(
        payload_expr(&columns),
        "6 + (CASE WHEN \"id\" IS NULL THEN 0 ELSE 4 END) + coalesce(length(CAST(\"body\" AS BLOB)), 0)"
    );
}

#[test]
fn strata_scale_each_window_by_its_density_and_width() {
    let stratum = |start: i64, density: Option<f64>, rows: i64, bytes: i64| Stratum {
        start,
        end: start + 100,
        rows: (0..rows).map(|i| (start + i, bytes)).collect(),
        density,
    };
    // Dense, half-full, empty, and one whose window could not measure a
    // density, which takes the mean of the others (0.5).
    let (rows, bytes, newest) = estimate(&[
        stratum(0, Some(1.0), 10, 10),
        stratum(100, Some(0.5), 10, 30),
        stratum(200, Some(0.0), 0, 0),
        stratum(300, None, 1, 20),
    ]);
    assert_eq!(rows, 100.0 + 50.0 + 0.0 + 50.0);
    assert_eq!(bytes, 100.0 * 10.0 + 50.0 * 30.0 + 50.0 * 20.0);
    assert_eq!(newest, Some(20.0));
}

#[test]
fn an_interval_read_whole_counts_its_rows_and_a_cut_one_ends_on_its_last() {
    let rows = |ids: &[i64]| ids.iter().map(|id| (*id, 1)).collect::<Vec<_>>();
    // Three rows in [100, 400), under the limit: the interval was read whole.
    assert_eq!(
        interval_density(100, 400, 8, &rows(&[150, 250, 350])),
        3.0 / 300.0
    );
    // The limit cut the read at rowid 103: four rows over [100, 103].
    assert_eq!(
        interval_density(100, 400, 4, &rows(&[100, 101, 102, 103])),
        1.0
    );
    // From a starting row, density is gaps: 3 gaps over 30 rowids.
    assert_eq!(gap_density(&rows(&[10, 20, 30, 40])), Some(0.1));
    assert_eq!(gap_density(&rows(&[10])), None);
}

#[tokio::test]
async fn a_small_table_is_read_whole_and_reported_exactly() {
    let (_temp, db) = bare_db().await;
    db.execute_batch("CREATE TABLE small(id INTEGER PRIMARY KEY, body TEXT); CREATE TABLE vacant(id INTEGER PRIMARY KEY, body TEXT);")
        .await
        .unwrap();
    fill(&db, "small", 1, 3, 40, 50).await;
    let mut clock = Clock::new();
    let measured = measure_table(&db, "small", 7, &mut clock).await.unwrap();
    assert!(measured.exact);
    assert_eq!(
        (measured.rows, measured.bytes),
        exact_of(&db, "small").await
    );
    assert_eq!((measured.low, measured.high), (Some(1), Some(118)));

    let vacant = measure_table(&db, "vacant", 7, &mut clock).await.unwrap();
    assert!(vacant.exact);
    assert_eq!((vacant.rows, vacant.bytes, vacant.high), (0, 0, None));
}

#[tokio::test]
async fn a_large_table_is_estimated_from_windows_without_reading_it() {
    let (_temp, db) = bare_db().await;
    db.execute_batch(
        "CREATE TABLE dense(id INTEGER PRIMARY KEY, body TEXT);
         CREATE TABLE sparse(id INTEGER PRIMARY KEY, body TEXT);
         CREATE TABLE evicted(id INTEGER PRIMARY KEY, body TEXT);",
    )
    .await
    .unwrap();
    db.execute_batch("CREATE TABLE scattered(id INTEGER PRIMARY KEY, body TEXT);")
        .await
        .unwrap();
    fill(&db, "dense", 1, 1, 6_000, 200).await;
    fill(&db, "sparse", 1, 7, 6_000, 200).await;
    // An upserted table: a fresh rowid per update leaves a few rows spread
    // over a huge rowid range.
    fill(&db, "scattered", 1, 25_000, 3_000, 200).await;
    // A journal whose oldest two thirds an evictor deleted.
    fill(&db, "evicted", 1, 1, 6_000, 200).await;
    db.execute("DELETE FROM evicted WHERE id <= 4000", ())
        .await
        .unwrap();

    for table in ["dense", "sparse", "evicted", "scattered"] {
        let mut clock = Clock::new();
        let measured = measure_table(&db, table, 11, &mut clock).await.unwrap();
        let (rows, bytes) = exact_of(&db, table).await;
        assert!(!measured.exact, "{table}");
        assert!(
            within(measured.rows, rows, 0.02),
            "{table}: {} rows estimated, {rows} actual",
            measured.rows
        );
        assert!(
            within(measured.bytes, bytes, 0.02),
            "{table}: {} bytes estimated, {bytes} actual",
            measured.bytes
        );
        assert!(
            // The survey's window, the newest row, one window per stratum.
            measured.sampled_rows as usize <= (STRATA as usize + 1) * MAX_WINDOW_ROWS + 1,
            "{table}: the sample is bounded whatever the table's size, sampled {}",
            measured.sampled_rows
        );
        assert!(
            clock.reads <= STRATA as u64 + 2,
            "{table}: {} reads",
            clock.reads
        );
    }
}

#[tokio::test]
async fn wide_rows_get_short_windows() {
    let (_temp, db) = bare_db().await;
    db.execute_batch("CREATE TABLE wide(id INTEGER PRIMARY KEY, body TEXT);")
        .await
        .unwrap();
    fill(&db, "wide", 1, 1, 400, 200_000).await;
    let mut clock = Clock::new();
    let measured = measure_table(&db, "wide", 3, &mut clock).await.unwrap();
    assert!(!measured.exact);
    // A row wider than a window is read alone, and the table's byte budget
    // buys the minimum strata: one row to learn the width, the newest row,
    // and a one-row window per stratum.
    assert_eq!(
        (measured.sampled_rows, measured.strata),
        (2 + MIN_STRATA as u32, MIN_STRATA as u32)
    );
    let (rows, bytes) = exact_of(&db, "wide").await;
    assert!(within(measured.rows, rows, 0.02));
    assert!(within(measured.bytes, bytes, 0.02));
}

/// The oldest rows of a table can be far narrower than its newest; the
/// windows are sized by both, so none reads a run of wide rows.
#[tokio::test]
async fn windows_are_sized_by_the_newest_row_as_well_as_the_oldest() {
    let (_temp, db) = bare_db().await;
    db.execute_batch("CREATE TABLE widening(id INTEGER PRIMARY KEY, body TEXT);")
        .await
        .unwrap();
    fill(&db, "widening", 1, 1, 200, 100).await;
    fill(&db, "widening", 201, 1, 200, 100_000).await;
    let mut clock = Clock::new();
    let measured = measure_table(&db, "widening", 9, &mut clock).await.unwrap();
    assert!(!measured.exact);
    // 64 narrow rows from the start, the newest row, then one-row windows.
    assert!(
        measured.sampled_rows as i64 <= 64 + 1 + STRATA,
        "sampled {}",
        measured.sampled_rows
    );
}

fn sampled(rows: i64, bytes: i64, low: i64, high: i64, sampled: u32) -> TableCensus {
    TableCensus {
        table: "t".to_string(),
        low_water: Some(low),
        high_water: Some(high),
        rows,
        bytes,
        sampled_rows: sampled,
        ..TableCensus::default()
    }
}

/// A sampled table carries decayed means of its density and row width
/// forward, applied to today's span; an exact table carries nothing.
#[test]
fn sampled_estimates_pool_with_the_previous_census() {
    let mut first = sampled(500, 50_000, 1, 1_000, 40);
    pool(&mut first, None);
    assert_eq!(
        (first.rows, first.bytes, first.pool_weight),
        (500, 50_000, 40.0)
    );
    assert_eq!((first.day_rows, first.day_bytes), (Some(500), Some(50_000)));

    // The next day the span moved up by 1,000 rowids (an eviction) and the
    // windows read denser, wider rows.
    let mut second = sampled(1_000, 300_000, 1_001, 2_000, 40);
    pool(&mut second, Some(&first));
    let carried = POOL_DECAY * 40.0;
    let density = (40.0 * 1.0 + carried * 0.5) / (40.0 + carried);
    let row_bytes = (40.0 * 300.0 + carried * 100.0) / (40.0 + carried);
    assert_eq!(second.rows, (density * 1_000.0).round() as i64);
    assert_eq!(
        second.bytes,
        (second.rows as f64 * row_bytes).round() as i64
    );
    assert_eq!(second.pool_weight, 40.0 + carried);
    assert_eq!(second.day_rows, Some(1_000));

    let mut exact = sampled(10, 100, 1, 10, 10);
    exact.exact = true;
    pool(&mut exact, Some(&second));
    assert_eq!(
        (exact.rows, exact.pool_weight, exact.day_rows),
        (10, 0.0, None)
    );
}

#[test]
fn the_history_constant_pools_by_event_rows() {
    let first = HistoryCalibration {
        event_rows: 1_000,
        bytes_per_event_row: 2_000.0,
        pool_weight: 1_000.0,
        ..HistoryCalibration::default()
    };
    let mut second = HistoryCalibration {
        event_rows: 250,
        bytes_per_event_row: 3_000.0,
        ..HistoryCalibration::default()
    };
    second.pool(Some(&first));
    let carried = POOL_DECAY * 1_000.0;
    assert_eq!(second.day_bytes_per_event_row, 3_000.0);
    assert_eq!(
        second.bytes_per_event_row,
        (250.0 * 3_000.0 + carried * 2_000.0) / (250.0 + carried)
    );
    assert_eq!(second.pool_weight, 250.0 + carried);
}

#[tokio::test]
async fn a_declared_rowid_column_leaves_the_table_unsized() {
    let (_temp, db) = bare_db().await;
    db.execute_batch(
        "CREATE TABLE shadowed(rowid TEXT, value TEXT); INSERT INTO shadowed VALUES ('r', 'v');",
    )
    .await
    .unwrap();
    let measured = measure_table(&db, "shadowed", 1, &mut Clock::new())
        .await
        .unwrap();
    assert!(measured.rowid_hidden);
}

/// `(addr, opcode, p2)` of each instruction in `sql`'s compiled program.
async fn program(db: &LocalDb, sql: &str) -> Vec<(i64, String, i64)> {
    db.query_all(format!("EXPLAIN {sql}"), (), |row| {
        Ok((row.i64(0)?, row.text(1)?, row.i64(3)?))
    })
    .await
    .unwrap()
}

/// Whether a program loops over rows with no early exit: a `Next`/`Prev`
/// back-edge whose body has no `Goto` past it.
fn iterates_rows(program: &[(i64, String, i64)]) -> bool {
    program.iter().any(|(edge, op, start)| {
        (op == "Next" || op == "Prev")
            && !program
                .iter()
                .any(|(at, inner, to)| inner == "Goto" && (*start..*edge).contains(at) && to > edge)
    })
}

async fn query_plan(db: &LocalDb, sql: &str) -> String {
    db.query_all(format!("EXPLAIN QUERY PLAN {sql}"), (), |row| row.text(3))
        .await
        .unwrap()
        .join("\n")
}

/// The bounds are seeks to either end of the table, and a window is a seek to
/// its first rowid: no census statement walks a table.
#[tokio::test]
async fn bounds_and_windows_seek_rather_than_scan() {
    let (_temp, db) = bare_db().await;
    db.execute_batch("CREATE TABLE big(id INTEGER PRIMARY KEY, body TEXT);")
        .await
        .unwrap();
    let quoted = quote_identifier("big");
    for sql in [min_rowid_sql(&quoted), max_rowid_sql(&quoted)] {
        let program = program(&db, &sql).await;
        assert!(!iterates_rows(&program), "{sql}: {program:?}");
    }
    let window = window_sql(&quoted, "1");
    let plan = query_plan(&db, &window).await;
    assert!(plan.contains("SEARCH"), "{window}: {plan}");
    assert!(!plan.contains("SORT"), "{window}: {plan}");
    let program = program(&db, &window).await;
    assert!(
        program.iter().any(|(_, op, _)| op.starts_with("Seek")),
        "{program:?}"
    );
}

/// The calibration pages a run's events through the unique `(run_id,
/// sequence)` index: a two-column seek, so a page never walks the run's
/// earlier rows.
#[tokio::test]
async fn history_event_pages_seek_the_run_sequence_index() {
    let (_temp, db) = private_db().await;
    let sql = history_events_sql("1");
    let plan = query_plan(&db, &sql).await;
    assert!(plan.contains("idx_events_run_sequence"), "{plan}");
    assert!(!plan.contains("SORT"), "{plan}");
}

async fn seed_project(db: &LocalDb) {
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
           VALUES ('proj','ws','P','p','/nonexistent','main',1,1);",
    )
    .await
    .unwrap();
}

/// An issue created at `created`, resolved at `completed`, with one job, one
/// run, one session and `events` events of `event_bytes` text each, plus one
/// tool invocation.
async fn seed_issue(
    db: &LocalDb,
    n: i64,
    created: i64,
    completed: i64,
    events: i64,
    event_bytes: usize,
) {
    db.execute_script(format!(
        "INSERT INTO issues(id, project_id, number, title, status, completed_at, created_at, updated_at)
           VALUES ('i{n}','proj',{n},'T','merged',{completed},{created},{created});
         INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('x{n}','r','complete',{created});
         INSERT INTO jobs(id, execution_id, project_id, issue_id, status, created_at, updated_at)
           VALUES ('j{n}','x{n}','proj','i{n}','complete',{created},{created});
         INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
           VALUES ('r{n}','j{n}','proj','exited',{created},{created});"
    ))
    .await
    .unwrap();
    // Valid JSON: the home-prose index evaluates `json_type(data)` on insert.
    let data = format!(r#"{{"content":"{}"}}"#, "d".repeat(event_bytes));
    for sequence in 0..events {
        db.execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES (?1, ?2, ?3, ?3, 'assistant', ?4, ?3)",
            (
                format!("e{n}-{sequence}"),
                format!("r{n}"),
                sequence,
                data.clone(),
            ),
        )
        .await
        .unwrap();
    }
}

/// The calibration samples only issues resolved 30-60 days ago, and its
/// constant is their history bytes over their event rows.
#[tokio::test]
async fn history_calibrates_on_issues_resolved_in_the_band() {
    let (_temp, db) = private_db().await;
    seed_project(&db).await;
    let day = DAY_S;
    // In creation order, as issue rowids are. Created before the lookback,
    // though resolved in the band: never probed.
    seed_issue(&db, 1, NOW - 200 * day, NOW - 45 * day, 5, 9_000).await;
    seed_issue(&db, 2, NOW - 100 * day, NOW - 80 * day, 5, 9_000).await;
    // In the band.
    seed_issue(&db, 3, NOW - 70 * day, NOW - 45 * day, 1_100, 300).await;
    seed_issue(&db, 4, NOW - 50 * day, NOW - 35 * day, 20, 300).await;
    // Resolved too recently.
    seed_issue(&db, 5, NOW - 40 * day, NOW - 10 * day, 5, 9_000).await;
    // Created after the band's newest edge: past the probed range.
    seed_issue(&db, 6, NOW - 20 * day, NOW - day, 5, 9_000).await;

    let mut clock = Clock::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    let history = calibrate_history(&db, NOW, 5, &mut clock, deadline)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.band_issues, 2);
    assert_eq!(history.sampled_issues, 2);
    assert_eq!(history.event_rows, 1_120);
    let events_payload = payload_expr(&columns(&db, "events").await.unwrap());
    let event_bytes: i64 = db
        .query_opt(
            format!("SELECT sum({events_payload}) FROM events WHERE run_id IN ('r3', 'r4')"),
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.history_bytes, event_bytes);
    assert_eq!(history.bytes_per_event_row, event_bytes as f64 / 1_120.0);
}

/// Deleted issues leave rowid gaps; the search for the lookback's first issue
/// must still end, on the first issue created at or after the cutoff.
#[tokio::test]
async fn the_lookback_search_ends_across_rowid_gaps() {
    let (_temp, db) = private_db().await;
    seed_project(&db).await;
    for n in 1..=40 {
        seed_issue(&db, n, n * 1_000, n * 1_000, 0, 0).await;
    }
    db.execute_script(
        "DELETE FROM runs; DELETE FROM jobs; DELETE FROM executions;
         DELETE FROM issues WHERE number BETWEEN 5 AND 9 OR number BETWEEN 21 AND 33 OR number = 40;",
    )
    .await
    .unwrap();
    let rowid_of = |number: i64| {
        let db = &db;
        async move {
            db.query_opt(
                "SELECT rowid FROM issues WHERE number = ?1",
                (number,),
                |row| row.i64(0),
            )
            .await
            .unwrap()
            .unwrap()
        }
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let (low, high) = rowid_bounds(&db, "issues").await.unwrap();
    let (low, end) = (low.unwrap(), high.unwrap() + 1);
    for (cutoff, first) in [
        (0, Some(1)),
        (4_500, Some(10)),
        (20_001, Some(34)),
        (39_500, None),
    ] {
        let mut clock = Clock::new();
        let found = first_issue_created_after(&db, low, end, cutoff, &mut clock, deadline)
            .await
            .unwrap();
        let expected = match first {
            Some(number) => Some(rowid_of(number).await),
            None => None,
        };
        assert_eq!(found, expected, "cutoff {cutoff}");
        assert!(clock.reads < 16, "cutoff {cutoff}: {} reads", clock.reads);
    }
}

fn table(name: &str, rows: i64, bytes: i64, high: i64) -> TableCensus {
    TableCensus {
        table: name.to_string(),
        rule: rule_summary(name).0,
        high_water: Some(high),
        rows,
        bytes,
        ..TableCensus::default()
    }
}

fn census_at(taken_at: i64, tables: Vec<TableCensus>) -> CensusReport {
    CensusReport {
        taken_at,
        payload_bytes: tables.iter().map(|t| t.bytes).sum(),
        tables,
        tokens_per_day: Some(2e9),
        ..CensusReport::default()
    }
}

/// A growing table projects forward at its row trend times its row size; an
/// evicted journal inserts as fast but keeps a flat trend, so it projects
/// flat.
#[test]
fn the_projection_follows_kept_rows_not_inserted_ones() {
    let census = |day: i64| {
        census_at(
            NOW + day * DAY_S,
            vec![
                // 1,000 rows a day kept, 100 B each.
                table(
                    "events",
                    10_000 + day * 1_000,
                    (10_000 + day * 1_000) * 100,
                    10_000 + day * 1_000,
                ),
                // 50,000 rows a day inserted and evicted.
                table("effect_outbox", 50_000, 5_000_000, 50_000 + day * 50_000),
            ],
        )
    };
    let previous = vec![census(1), census(0)];
    assert_eq!(
        project(&census(1), &[census(0)]),
        None,
        "two censuses are not enough"
    );

    let projection = project(&census(2), &previous).unwrap();
    assert_eq!(projection.censuses, 3);
    assert_eq!(projection.span_days, 2.0);
    let events = projection
        .fastest
        .iter()
        .find(|t| t.table == "events")
        .unwrap();
    assert!(
        (events.bytes_per_day - 100_000.0).abs() < 1e-6,
        "{events:?}"
    );
    assert_eq!(events.projected_bytes, 1_200_000 + 9_000_000);
    let outbox = projection
        .fastest
        .iter()
        .find(|t| t.table == "effect_outbox")
        .unwrap();
    assert_eq!(outbox.bytes_per_day, 0.0);
    assert_eq!(outbox.projected_bytes, 5_000_000);
    assert_eq!(
        projection.by_rule["tiered"].bytes_per_day,
        events.bytes_per_day
    );
    assert_eq!(projection.by_rule["evict"].bytes_per_day, 0.0);
    // 100 KB a day against two billion tokens a day.
    assert!((projection.mb_per_billion_tokens.unwrap() - 0.05).abs() < 1e-9);
}

#[test]
fn censuses_older_than_the_lookback_do_not_enter_the_projection() {
    let census = |day: i64| census_at(NOW + day * DAY_S, vec![table("events", 1, 1, 1)]);
    assert_eq!(project(&census(0), &[census(-40), census(-50)]), None);
}

#[tokio::test]
async fn a_census_is_recorded_read_back_and_pruned_to_the_newest() {
    let (temp, db) = private_db().await;
    seed_project(&db).await;
    seed_issue(&db, 1, NOW - 70 * DAY_S, NOW - 45 * DAY_S, 30, 500).await;
    assert_eq!(latest_table_rows(&db).await.unwrap(), None);
    assert_eq!(latest_taken_at(&db).await.unwrap(), None);

    let archive = temp.path().join("archive");
    std::fs::create_dir_all(archive.join("objects/ab")).unwrap();
    std::fs::write(archive.join("objects/ab/one"), [0u8; 1_000]).unwrap();
    let first = take(&db, NOW, &archive).await.unwrap();
    assert!(first.unmeasured.is_empty(), "{:?}", first.unmeasured);
    assert_eq!(first.archive.bytes, 1_000);
    assert_eq!(first.archive.files, 1);
    let events = first.tables.iter().find(|t| t.table == "events").unwrap();
    assert_eq!((events.rows, events.exact), (30, true));
    assert_eq!(events.rule, "tiered with history (cairn/5586)");
    assert!(first.history.as_ref().is_some_and(|h| h.event_rows == 30));
    assert_eq!(first.projection, None);
    record(&db, &first).await.unwrap();

    // A day later, with more events: growth is measured against the first.
    for sequence in 30..60 {
        db.execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES (?1, 'r1', ?2, ?2, 'assistant', '{}', ?2)",
            (format!("late-{sequence}"), sequence),
        )
        .await
        .unwrap();
    }
    let second = take(&db, NOW + DAY_S, &archive).await.unwrap();
    let events = second.tables.iter().find(|t| t.table == "events").unwrap();
    assert_eq!(events.rowids_per_day, Some(30.0));
    assert_eq!(events.rows_per_day, Some(30.0));
    record(&db, &second).await.unwrap();
    let third = take(&db, NOW + 2 * DAY_S, &archive).await.unwrap();
    assert!(third.projection.is_some());
    record(&db, &third).await.unwrap();

    let counts = latest_table_rows(&db).await.unwrap().unwrap();
    assert_eq!(counts.taken_at, NOW + 2 * DAY_S);
    assert_eq!(counts.rows["events"], 60);
    assert_eq!(latest_taken_at(&db).await.unwrap(), Some(NOW + 2 * DAY_S));
    let (per_row, at) = latest_bytes_per_event_row(&db).await.unwrap().unwrap();
    assert_eq!(at, NOW + 2 * DAY_S);
    assert_eq!(Some(per_row), third.history.map(|h| h.bytes_per_event_row));

    // Only the newest KEPT_CENSUSES rows survive a record.
    let small = census_at(NOW + 3 * DAY_S, vec![table("events", 1, 1, 1)]);
    for _ in 0..KEPT_CENSUSES {
        record(&db, &small).await.unwrap();
    }
    let (kept, oldest): (i64, i64) = db
        .query_opt(
            "SELECT count(*), min(taken_at) FROM storage_census",
            (),
            |row| Ok((row.i64(0)?, row.i64(1)?)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!((kept, oldest), (KEPT_CENSUSES, NOW + 3 * DAY_S));
}

#[tokio::test]
async fn a_census_that_runs_out_of_time_names_what_it_did_not_reach() {
    let (temp, db) = private_db().await;
    let report = take_within(&db, NOW, temp.path(), Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.unmeasured.len(), report.tables.len());
    assert!(report.tables.iter().all(|t| t.error.is_some()));
    assert_eq!(report.payload_bytes, 0);
}

/// A deleting rule reports its tables' backlog over budget and the rowids
/// they vacated, so a starved rule reads as a flat line beside a growing
/// backlog.
#[test]
fn deleting_rules_report_their_backlog_and_their_deletions() {
    let mut chunks = table(
        "message_stream_chunks",
        11_000_000,
        1_400_000_000,
        11_500_000,
    );
    chunks.budget_mb = 256;
    chunks.vacated_per_day = Some(0.0);
    let mut streams = table("message_streams", 650_000, 160_000_000, 650_000);
    streams.budget_mb = 64;
    streams.vacated_per_day = Some(10.0);
    let rules = deleting_rules(&[chunks, streams, table("jobs", 1, 1, 1)]);
    assert_eq!(rules.len(), 1, "{rules:?}");
    let rule = &rules[0];
    assert_eq!(rule.rule, "evicted: terminal_streams");
    assert_eq!(rule.tables, ["message_stream_chunks", "message_streams"]);
    assert_eq!(rule.over_budget_bytes, 1_144_000_000 + 96_000_000);
    assert_eq!(rule.vacated_per_day, Some(10.0));
}

#[test]
fn tables_over_their_budget_are_summarised() {
    let mut over = table("events", 10, 9_000_000_000, 10);
    over.budget_mb = 8_000;
    over.over_budget = true;
    let report = census_at(NOW, vec![over, table("jobs", 1, 1, 1)]);
    let summary = report.summary(1);
    assert_eq!(summary["over_budget"][0]["table"], "events");
    assert_eq!(summary["largest"].as_array().unwrap().len(), 1);
}
