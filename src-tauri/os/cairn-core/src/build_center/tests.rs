use super::*;
use cairn_db::storage::rebuilds::{ingest, record_launch_intent, LaunchIntent};
use serde_json::{json, Value};

async fn db() -> LocalDb {
    LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("ledger.db"))
        .await
        .unwrap()
}

fn event(rebuild: &str, id: &str, at: &str, body: Value) -> (String, String, Value) {
    let mut value = json!({"event": id, "rebuild": rebuild, "at": at});
    value
        .as_object_mut()
        .unwrap()
        .extend(body.as_object().unwrap().clone());
    (id.into(), value.to_string(), value)
}

fn phase(rebuild: &str, id: &str, at: &str, name: &str, state: &str) -> (String, String, Value) {
    event(
        rebuild,
        id,
        at,
        json!({"type": "phase", "name": name, "state": state, "outcome": "ok", "detail": "/private/path"}),
    )
}

#[tokio::test]
async fn the_view_keeps_phases_times_and_outcome_and_drops_the_actors_bookkeeping() {
    let db = db().await;
    ingest(
        &db,
        "r",
        vec![
            phase("r", "a", "2026-11-11T21:43:56Z", "fetch", "started"),
            phase("r", "b", "2026-11-11T21:44:10Z", "fetch", "ended"),
            event(
                "r",
                "c",
                "2026-11-11T21:44:11Z",
                json!({"type": "fact", "facts": {"version": "0.13.8", "commit": "b70efa604c9a355f9ddb122bb0bb8fb073328b00"}}),
            ),
            event(
                "r",
                "d",
                "2026-11-11T23:01:00Z",
                json!({"type": "outcome", "outcome": "done", "summary": "The new build is installed and verified."}),
            ),
        ],
        vec![],
        false,
    )
    .await
    .unwrap();
    let views = list(&db, 10).await.unwrap();
    assert_eq!(views.len(), 1);
    let view = &views[0];
    assert_eq!(view.status, "done");
    assert_eq!(view.version.as_deref(), Some("0.13.8"));
    assert_eq!(view.started_at.as_deref(), Some("2026-11-11T21:43:56Z"));
    assert_eq!(view.ended_at.as_deref(), Some("2026-11-11T23:01:00Z"));
    assert_eq!(
        view.phases,
        vec![RebuildPhaseView {
            name: "fetch".into(),
            started: Some("2026-11-11T21:43:56Z".into()),
            ended: Some("2026-11-11T21:44:10Z".into()),
            outcome: Some("ok".into()),
        }]
    );
    let wire = serde_json::to_string(view).unwrap();
    for absent in ["b70efa60", "/private/path", "events", "facts"] {
        assert!(!wire.contains(absent), "{absent}: {wire}");
    }
}

fn launch() -> LaunchIntent {
    LaunchIntent {
        request_ids: vec!["request-1".into()],
        attempt_id: "attempt-1".into(),
        authorization_event_id: "event-1".into(),
        kind: "rebuild".into(),
        target: None,
        commit: "b".repeat(40),
        facts_version: 1,
        recorded_at: 1_794_433_435,
        bootstrap_requested_at: None,
    }
}

#[tokio::test]
async fn a_launched_rebuild_starts_at_its_launch_until_a_phase_begins() {
    let db = db().await;
    record_launch_intent(&db, "req-1", launch()).await.unwrap();
    let view = list(&db, 10).await.unwrap().remove(0);
    assert_eq!(view.status, "acting");
    assert_eq!(view.request_id.as_deref(), Some("request-1"));
    assert_eq!(
        view.started_at.as_deref(),
        Some("2026-11-11T21:43:55+00:00")
    );
    assert!(view.ended_at.is_none());
}

#[test]
fn a_terminal_line_is_what_the_screen_keeps() {
    assert_eq!(
        terminal_line("\u{1b}[1m\u{1b}[32m   Compiling\u{1b}[0m cairn-core v0.13.7\r"),
        "   Compiling cairn-core v0.13.7"
    );
    assert_eq!(
        terminal_line("Building [=>   ] 1/9\rBuilding [====>] 9/9"),
        "Building [====>] 9/9"
    );
    assert_eq!(
        terminal_line("\u{1b}]8;;https://example.com\u{7}link\u{1b}]8;;\u{1b}\\ done\u{0}"),
        "link done"
    );
    assert_eq!(terminal_line("a\tb"), "a b");
}

#[tokio::test]
async fn the_tail_is_served_only_for_an_acting_rebuild() {
    let db = db().await;
    let root = tempfile::tempdir().unwrap();
    record_launch_intent(&db, "req-1", launch()).await.unwrap();
    // Acting, nothing logged yet.
    assert_eq!(
        build_log_tail(&db, root.path(), "req-1", 5).await.unwrap(),
        Some(vec![])
    );
    std::fs::create_dir_all(root.path().join("req-1")).unwrap();
    let log: String = (1..=30).map(|n| format!("line {n}\n\n")).collect();
    std::fs::write(root.path().join("req-1").join(BUILD_LOG), log).unwrap();
    assert_eq!(
        build_log_tail(&db, root.path(), "req-1", 3).await.unwrap(),
        Some(vec!["line 28".into(), "line 29".into(), "line 30".into()])
    );

    ingest(
        &db,
        "req-1",
        vec![event(
            "req-1",
            "done",
            "2026-11-11T23:01:00Z",
            json!({"type": "outcome", "outcome": "done", "summary": "done"}),
        )],
        vec![],
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        build_log_tail(&db, root.path(), "req-1", 3).await.unwrap(),
        None
    );
    assert_eq!(
        build_log_tail(&db, root.path(), "unknown", 3)
            .await
            .unwrap(),
        None
    );
    for unsafe_id in ["", "../req-1", "req-1/build.log", "."] {
        assert!(
            build_log_tail(&db, root.path(), unsafe_id, 3)
                .await
                .is_err(),
            "{unsafe_id}"
        );
    }
}

#[tokio::test]
async fn a_long_log_is_read_from_its_end_without_a_torn_first_line() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(BUILD_LOG);
    let filler = "x".repeat(1000);
    let mut log = String::new();
    for n in 0..200 {
        log.push_str(&format!("{n} {filler}\n"));
    }
    std::fs::write(&path, &log).unwrap();
    let tail = read_tail(&path, 50).unwrap();
    assert!(tail.len() < 70 && !tail.is_empty());
    assert!(
        tail.iter().all(|line| line.ends_with(&filler)),
        "no torn line"
    );
    assert!(tail.last().unwrap().starts_with("199 "));
}

#[cfg(unix)]
#[tokio::test]
async fn a_linked_log_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let elsewhere = dir.path().join("secret");
    std::fs::write(&elsewhere, "not the build\n").unwrap();
    let path = dir.path().join(BUILD_LOG);
    std::os::unix::fs::symlink(&elsewhere, &path).unwrap();
    assert!(read_tail(&path, 5).is_err());
}
