use super::*;
use crate::threads::db_store;

const FAR: &str = "2099-01-01T00:00:00Z";

fn operator() -> PrincipalRef {
    PrincipalRef::Machine {
        device_id: "device-1".into(),
    }
}

/// A project with three active threads and one closed one, each with its own
/// store under `dir`.
async fn fixture(dir: &std::path::Path, name: &str) -> LocalDb {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(format!(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES ('p-hl', 'default', 'Headlines', 'hl', '/tmp/hl', 1, 1);
         INSERT INTO threads (id, project_id, name, created_at, updated_at) VALUES
           ('{name}-a', 'p-hl', 'alpha', 1, 1),
           ('{name}-b', 'p-hl', 'bravo', 1, 1),
           ('{name}-c', 'p-hl', 'charlie', 1, 1),
           ('{name}-z', 'p-hl', 'zulu', 1, 1);
         UPDATE threads SET status = 'closed' WHERE id = '{name}-z';"
    ))
    .await
    .unwrap();
    for suffix in ["a", "b", "c", "z"] {
        let id = format!("{name}-{suffix}");
        db_store::use_test_path(&id, dir.join(format!("{id}.db")));
    }
    db
}

/// Insert `(ts, line, until)` rows into a thread's store, in order.
async fn lines(thread_id: &str, rows: &[(&str, &str, &str)]) {
    let store = db_store::open(thread_id).await.unwrap();
    for (ts, line, until) in rows {
        store
            .execute(
                "INSERT INTO headlines(ts, line, ref, until) VALUES (?1, ?2, 'cairn://p/hl/1', ?3)",
                params![ts.to_string(), line.to_string(), until.to_string()],
            )
            .await
            .unwrap();
    }
}

fn shape(merged: &MergedHeadlines) -> Vec<(String, i64, bool)> {
    merged
        .lines
        .iter()
        .map(|line| (line.thread.clone(), line.id, line.seen))
        .collect()
}

#[tokio::test]
async fn new_lines_lead_newest_first_then_seen_ones_and_expired_lines_are_gone() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path(), "hl-merge").await;
    lines(
        "hl-merge-a",
        &[
            ("2026-11-10T01:00:00Z", "Decided A1.", FAR),
            ("2026-11-10T04:00:00Z", "Assuming A2.", FAR),
            ("2026-11-10T06:00:00Z", "Stopped A3.", FAR),
        ],
    )
    .await;
    lines(
        "hl-merge-b",
        &[
            ("2026-11-10T02:00:00Z", "Decided B1.", FAR),
            // Expired: never shown, seen or not.
            (
                "2020-01-01T00:00:00Z",
                "Expired B2.",
                "2020-01-02T00:00:00Z",
            ),
            ("2026-11-10T05:00:00Z", "Decided B3.", FAR),
        ],
    )
    .await;
    lines(
        "hl-merge-c",
        &[("2026-11-10T03:00:00Z", "Decided C1.", FAR)],
    )
    .await;
    // A closed thread's lines are not in the merged view.
    lines("hl-merge-z", &[("2026-11-10T09:00:00Z", "Closed Z1.", FAR)]).await;

    // The operator has read alpha through #2 and bravo through #1; never charlie.
    mark_seen(
        &db,
        &operator(),
        "hl-merge-a",
        2,
        Some(SeenOn::Desktop),
        100,
    )
    .await
    .unwrap();
    mark_seen(&db, &operator(), "hl-merge-b", 1, Some(SeenOn::Ios), 100)
        .await
        .unwrap();

    let merged = merged(&db, "p-hl", &HeadlineFilter::default())
        .await
        .unwrap();
    assert!(merged.errors.is_empty(), "{:?}", merged.errors);
    assert_eq!(
        shape(&merged),
        [
            ("alpha".to_string(), 3, false),
            ("bravo".to_string(), 3, false),
            ("charlie".to_string(), 1, false),
            ("alpha".to_string(), 2, true),
            ("bravo".to_string(), 1, true),
            ("alpha".to_string(), 1, true),
        ]
    );
    assert_eq!((merged.total, merged.unseen), (6, 3));
    assert!(merged
        .lines
        .iter()
        .all(|line| !line.line.contains("Expired")));
    let first = &merged.lines[0];
    assert_eq!(
        (
            first.line.as_str(),
            first.link.as_str(),
            first.ts.as_str(),
            first.until.as_str()
        ),
        ("Stopped A3.", "cairn://p/hl/1", "2026-11-10T06:00:00Z", FAR)
    );

    let unseen = merged_with(&db, None, true, None).await;
    assert_eq!(
        shape(&unseen),
        [
            ("alpha".to_string(), 3, false),
            ("bravo".to_string(), 3, false),
            ("charlie".to_string(), 1, false),
        ]
    );
    let bravo = merged_with(&db, Some("bravo"), false, None).await;
    assert_eq!(
        shape(&bravo),
        [
            ("bravo".to_string(), 3, false),
            ("bravo".to_string(), 1, true)
        ]
    );
    let capped = merged_with(&db, None, false, Some(2)).await;
    assert_eq!((capped.lines.len(), capped.total, capped.unseen), (2, 6, 3));
}

async fn merged_with(
    db: &LocalDb,
    thread: Option<&str>,
    unseen_only: bool,
    limit: Option<usize>,
) -> MergedHeadlines {
    merged(
        db,
        "p-hl",
        &HeadlineFilter {
            thread: thread.map(str::to_string),
            unseen_only,
            limit,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn the_cursor_never_moves_backward_or_past_the_newest_line() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path(), "hl-cursor").await;
    let thread = "hl-cursor-a";
    assert_eq!(cursor(&db, thread).await.unwrap(), None);
    assert_eq!(
        read_through_line(None, 0),
        "Operator has never read your headlines"
    );

    // No store yet: nothing to have read, so nothing moves.
    let empty = mark_seen(&db, &operator(), thread, 4, None, 10)
        .await
        .unwrap();
    assert_eq!(
        empty,
        SeenOutcome {
            moved: false,
            cursor: None
        }
    );

    lines(
        thread,
        &[
            ("2026-11-10T01:00:00Z", "One.", FAR),
            ("2026-11-10T02:00:00Z", "Two.", FAR),
            ("2026-11-10T03:00:00Z", "Three.", FAR),
        ],
    )
    .await;
    let moved = mark_seen(&db, &operator(), thread, 2, Some(SeenOn::Desktop), 100)
        .await
        .unwrap();
    assert!(moved.moved);
    assert_eq!(
        moved.cursor,
        Some(HeadlineCursor {
            seen_through_id: 2,
            seen_at: 100,
            seen_on: Some("desktop".into())
        })
    );

    // Behind or at the cursor: unchanged, and its time and screen stand.
    for through in [1, 2] {
        let stale = mark_seen(&db, &operator(), thread, through, Some(SeenOn::Ios), 200)
            .await
            .unwrap();
        assert!(!stale.moved);
        assert_eq!(stale.cursor, moved.cursor);
    }

    // Past the newest line: clamped to it, so line #4 is new when it lands.
    let ahead = mark_seen(&db, &operator(), thread, 99, Some(SeenOn::Ios), 300)
        .await
        .unwrap();
    assert_eq!(ahead.cursor.as_ref().unwrap().seen_through_id, 3);
    lines(thread, &[("2026-11-10T04:00:00Z", "Four.", FAR)]).await;
    let fresh = merged_with(&db, Some("alpha"), true, None).await;
    assert_eq!(shape(&fresh), [("alpha".to_string(), 4, false)]);

    assert_eq!(
        read_through_line(ahead.cursor.as_ref(), 300 + 5 * 3600 + 59),
        "Operator has read through #3, 5h ago"
    );
    assert!(mark_seen(&db, &operator(), thread, 0, None, 400)
        .await
        .is_err());
}

#[tokio::test]
async fn only_the_operator_moves_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path(), "hl-agent").await;
    let thread = "hl-agent-a";
    lines(thread, &[("2026-11-10T01:00:00Z", "One.", FAR)]).await;
    let agent = PrincipalRef::Agent {
        node: "cairn://p/hl/alpha".into(),
        run_id: Some("run-1".into()),
    };
    assert_eq!(
        mark_seen(&db, &agent, thread, 1, None, 1)
            .await
            .unwrap_err(),
        OPERATOR_ONLY_REFUSAL
    );
    assert_eq!(cursor(&db, thread).await.unwrap(), None);
}
