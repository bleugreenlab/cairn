//! Dump assistant + user events to JSONL for offline vibe-color research.
//!
//! Single pass over `events` ordered by (run_id, sequence). Buffers one run at
//! a time to compute, for each emitted event, the event-step distance to the
//! nearest errored `tool_result` ahead and behind it within the same run.
//!
//! Open a *copy* of the prod db — the running app holds an exclusive lock.
//!
//! Usage:
//!   cargo run --example vibe_corpus_dump --features internal-api -- \
//!       /path/to/copy.turso.db /path/to/out.jsonl

use std::fs::File;
use std::io::{BufWriter, Write};

use cairn_core::internal::embeddings::extract_embeddable_text;
use cairn_core::internal::storage::{LocalDb, RowExt};
use serde::Serialize;

#[derive(Serialize)]
struct OutRow<'a> {
    id: &'a str,
    run_id: &'a str,
    session_id: Option<&'a str>,
    sequence: i64,
    timestamp: i64,
    event_type: &'a str,
    /// content + "\n" + thinking, exactly what the vibe worker embeds.
    text: &'a str,
    content: Option<&'a str>,
    thinking: Option<&'a str>,
    is_subagent: bool,
    output_tokens: Option<i64>,
    /// Event-steps forward to the next errored tool_result in this run (None if none ahead).
    dist_next_error: Option<i64>,
    /// Event-steps back to the previous errored tool_result in this run (None if none behind).
    dist_prev_error: Option<i64>,
    /// 0.0 (run start) .. 1.0 (run end), by position among this run's events.
    run_progress: f64,
}

/// One buffered event in the current run.
struct RunEvent {
    id: String,
    session_id: Option<String>,
    sequence: i64,
    timestamp: i64,
    event_type: String,
    parent_tool_use_id: Option<String>,
    output_tokens: Option<i64>,
    is_error: bool,
    // Parsed embeddable fields (only populated for assistant/user).
    text: Option<String>,
    content: Option<String>,
    thinking: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let db_path = args
        .next()
        .expect("usage: vibe_corpus_dump <db> <out.jsonl>");
    let out_path = args
        .next()
        .expect("usage: vibe_corpus_dump <db> <out.jsonl>");

    let db = LocalDb::open(&db_path)
        .await
        .expect("open db (use a copy!)");
    let out = File::create(&out_path).expect("create out file");

    // All mutable state is owned by the read closure (which is FnOnce), then the
    // counts are returned — avoids borrowing outer locals across the boxed future.
    let (emitted, runs) = db
        .read(move |conn| {
            Box::pin(async move {
                let mut writer = BufWriter::new(out);
                let mut current_run: Option<String> = None;
                let mut buf: Vec<RunEvent> = Vec::new();
                let mut emitted = 0u64;
                let mut runs = 0u64;
                let writer = &mut writer;
                let current_run = &mut current_run;
                let buf = &mut buf;
                let emitted_ref = &mut emitted;
                let runs_ref = &mut runs;
                let mut rows = conn
                    .query(
                        "SELECT id, run_id, session_id, sequence, timestamp, event_type, \
                            data, parent_tool_use_id, output_tokens \
                     FROM events ORDER BY run_id, sequence",
                        (),
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    let id = row.text(0)?;
                    let run_id = row.text(1)?;
                    let session_id = row.opt_text(2)?;
                    let sequence = row.i64(3)?;
                    let timestamp = row.i64(4)?;
                    let event_type = row.text(5)?;
                    let data = row.text(6)?;
                    let parent_tool_use_id = row.opt_text(7)?;
                    let output_tokens = row.opt_i64(8)?;

                    if current_run.as_deref() != Some(run_id.as_str()) {
                        if let Some(prev) = current_run.as_deref() {
                            flush_run(writer, prev, buf, emitted_ref);
                            *runs_ref += 1;
                        }
                        buf.clear();
                        *current_run = Some(run_id.clone());
                    }

                    let is_error = if event_type == "tool_result" {
                        serde_json::from_str::<serde_json::Value>(&data)
                            .ok()
                            .and_then(|v| v.get("isError").and_then(|b| b.as_bool()))
                            .unwrap_or(false)
                    } else {
                        false
                    };

                    let (text, content, thinking) = if event_type == "assistant"
                        || event_type == "user"
                    {
                        let text = extract_embeddable_text(&data);
                        let parsed: Option<serde_json::Value> = serde_json::from_str(&data).ok();
                        let content = parsed
                            .as_ref()
                            .and_then(|v| v.get("content").and_then(|s| s.as_str()))
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                        let thinking = parsed
                            .as_ref()
                            .and_then(|v| v.get("thinking").and_then(|s| s.as_str()))
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                        (text, content, thinking)
                    } else {
                        (None, None, None)
                    };

                    buf.push(RunEvent {
                        id,
                        session_id,
                        sequence,
                        timestamp,
                        event_type,
                        parent_tool_use_id,
                        output_tokens,
                        is_error,
                        text,
                        content,
                        thinking,
                    });
                }

                if let Some(prev) = current_run.as_deref() {
                    flush_run(writer, prev, buf, emitted_ref);
                    *runs_ref += 1;
                }
                writer.flush().expect("flush");
                Ok((emitted, runs))
            })
        })
        .await
        .expect("stream events");

    eprintln!("wrote {emitted} rows across {runs} runs to {out_path}");
}

/// Compute error distances for one run and write the embeddable (assistant/user
/// with text) events as JSONL.
fn flush_run(writer: &mut impl Write, run_id: &str, buf: &[RunEvent], emitted: &mut u64) {
    let n = buf.len();
    if n == 0 {
        return;
    }
    // Positions of errored tool_results within this run.
    let error_pos: Vec<usize> = buf
        .iter()
        .enumerate()
        .filter(|(_, e)| e.is_error)
        .map(|(i, _)| i)
        .collect();

    for (i, e) in buf.iter().enumerate() {
        let Some(text) = e.text.as_deref() else {
            continue; // only emit events that carry embeddable prose
        };
        if text.is_empty() {
            continue;
        }

        let dist_next_error = error_pos
            .iter()
            .filter(|&&p| p > i)
            .map(|&p| (p - i) as i64)
            .min();
        let dist_prev_error = error_pos
            .iter()
            .filter(|&&p| p < i)
            .map(|&p| (i - p) as i64)
            .min();

        let row = OutRow {
            id: &e.id,
            run_id,
            session_id: e.session_id.as_deref(),
            sequence: e.sequence,
            timestamp: e.timestamp,
            event_type: &e.event_type,
            text,
            content: e.content.as_deref(),
            thinking: e.thinking.as_deref(),
            is_subagent: e.parent_tool_use_id.is_some(),
            output_tokens: e.output_tokens,
            dist_next_error,
            dist_prev_error,
            run_progress: if n > 1 {
                i as f64 / (n - 1) as f64
            } else {
                0.0
            },
        };
        serde_json::to_writer(&mut *writer, &row).expect("serialize row");
        writer.write_all(b"\n").expect("newline");
        *emitted += 1;
    }
}
