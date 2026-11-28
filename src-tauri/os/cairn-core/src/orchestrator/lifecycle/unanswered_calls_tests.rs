use super::unanswered_calls::{
    lost_calls_note, render_lost_calls, CALL_ANSWERS_SQL, HOST_RESTART_REASON, PROCESS_LOST_REASON,
    TURN_ASSISTANT_EVENTS_SQL, USER_STOP_REASON,
};
use crate::agent_process::stream::TranscriptEvent;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::Value;

/// Every assistant and tool_result row of a turn: what the note derived its
/// answer from before it read the answers alone.
const WHOLE_TURN_SQL: &str = "SELECT data FROM events
     WHERE +run_id = ?1 AND turn_id = ?2 AND +event_type IN ('assistant', 'tool_result')
     ORDER BY sequence ASC, rowid ASC";

fn binds(run: &str, turn: &str) -> Vec<Value> {
    vec![Value::Text(run.into()), Value::Text(turn.into())]
}

async fn transcript(db: &LocalDb, sql: &str, run: &str, turn: &str) -> Vec<TranscriptEvent> {
    db.query_all(sql, binds(run, turn), |row| row.text(0))
        .await
        .unwrap()
        .iter()
        .filter_map(|data| serde_json::from_str(data).ok())
        .collect()
}

/// A turn of `calls` run calls, each answered by its own 4 KB result except
/// the last two, which a settlement answered: one for a process loss and one
/// for a user stop.
async fn seed_long_turn(db: &LocalDb, run: &str, turn: &str, calls: usize) {
    db.execute_script(format!(
        "INSERT INTO runs (id, status, created_at, updated_at) VALUES ('{run}', 'exited', 1, 1);
         INSERT INTO turns (id, session_id, run_id, sequence, state, start_reason, created_at, updated_at)
          VALUES ('{turn}', 'sess-{turn}', '{run}', 1, 'interrupted', 'initial', 1, 1);
         WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i + 1 < {calls})
         INSERT INTO events (id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
         SELECT '{turn}-a-' || i, '{run}', '{turn}', 2 * i, 1, 'assistant',
                '{{\"eventType\":\"assistant\",\"isError\":false,\"toolUses\":[{{\"id\":\"{turn}-call-' || i ||
                '\",\"name\":\"mcp__cairn__run\",\"input\":{{\"commands\":[{{\"command\":\"echo ' || i || '\"}}]}}}}]}}', 1
         FROM n;
         WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i + 1 < {calls} - 2)
         INSERT INTO events (id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
         SELECT '{turn}-r-' || i, '{run}', '{turn}', 2 * i + 1, 1, 'tool_result',
                '{{\"eventType\":\"tool_result\",\"isError\":false,\"toolUseId\":\"{turn}-call-' || i ||
                '\",\"toolName\":\"mcp__cairn__run\",\"toolResult\":\"' || hex(randomblob(2048)) || '\"}}', 1
         FROM n;
         INSERT INTO events (id, run_id, turn_id, sequence, timestamp, event_type, data, created_at) VALUES
          ('{turn}-lost', '{run}', '{turn}', 2 * {calls} + 1, 1, 'tool_result',
           '{{\"eventType\":\"tool_result\",\"isError\":true,\"toolUseId\":\"{turn}-call-' || ({calls} - 2) ||
           '\",\"toolName\":\"mcp__cairn__run\",\"toolResult\":\"lost-marker\",\"raw\":{{\"synthetic\":true,\"reason\":\"{PROCESS_LOST_REASON}\"}}}}', 1),
          ('{turn}-stop', '{run}', '{turn}', 2 * {calls} + 2, 1, 'tool_result',
           '{{\"eventType\":\"tool_result\",\"isError\":true,\"toolUseId\":\"{turn}-call-' || ({calls} - 1) ||
           '\",\"toolName\":\"mcp__cairn__run\",\"toolResult\":\"stop-marker\",\"raw\":{{\"synthetic\":true,\"reason\":\"{USER_STOP_REASON}\"}}}}', 1);"
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn lost_calls_note_reads_the_turns_answers_not_its_history() {
    let temp = tempfile::tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(temp.path().join("lost-calls.db"))
        .await
        .unwrap();
    // Other work in the table, which the note once walked through its type
    // index for every turn it answered.
    seed_long_turn(&db, "run-other", "turn-other", 1000).await;
    for (turn, calls) in [("turn-long", 400), ("turn-longer", 800)] {
        let run = format!("run-{turn}");
        seed_long_turn(&db, &run, turn, calls).await;
        let whole = transcript(&db, WHOLE_TURN_SQL, &run, turn).await;
        assert_eq!(whole.len(), 2 * calls);
        for reasons in [
            &[PROCESS_LOST_REASON][..],
            &[PROCESS_LOST_REASON, USER_STOP_REASON],
            &[HOST_RESTART_REASON],
        ] {
            let note = lost_calls_note(&db, &run, turn, reasons).await.unwrap();
            assert_eq!(
                note,
                render_lost_calls(&whole, &whole, reasons),
                "{turn} {reasons:?}: the answers alone decide what the whole turn did"
            );
            assert_eq!(note.is_some(), reasons != [HOST_RESTART_REASON]);
        }
        let note = lost_calls_note(&db, &run, turn, &[PROCESS_LOST_REASON])
            .await
            .unwrap()
            .unwrap();
        assert!(note.contains(&format!("`run` on `echo {}`: lost-marker", calls - 2)));

        // The answers read fetches the two answers however long the turn is.
        assert_eq!(
            transcript(&db, CALL_ANSWERS_SQL, &run, turn).await.len(),
            2,
            "{turn}"
        );
        // A turn whose answers are for no asked reason reads nothing more.
        let before = db.read_statement_count();
        assert!(lost_calls_note(&db, &run, turn, &[HOST_RESTART_REASON])
            .await
            .unwrap()
            .is_none());
        assert_eq!(db.read_statement_count() - before, 1, "{turn}");
    }

    let answers = cairn_db::storage::query_plan(&db, CALL_ANSWERS_SQL, binds("r", "t")).await;
    assert!(
        answers.contains("idx_events_turn_call_answers")
            && !answers.contains("idx_events_type_run")
            && !answers.contains("SCAN")
            && !answers.contains("SORTER"),
        "the answers are a seek on the partial index: {answers}"
    );
    let calls =
        cairn_db::storage::query_plan(&db, TURN_ASSISTANT_EVENTS_SQL, binds("r", "t")).await;
    assert!(
        calls.contains("idx_events_turn_id")
            && !calls.contains("idx_events_type_run")
            && !calls.contains("SCAN"),
        "the calls are a seek on the turn: {calls}"
    );
}
