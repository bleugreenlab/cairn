//! What the sync-to-async database bridge costs a session's stdout reader.
//!
//! Each subcommand prints one JSON document on stdout. `scoped` runs with no
//! bridge runtime installed, which is the per-call thread and runtime path;
//! `handle` installs the runtime as a runner does.
//!
//!   cost <scoped|handle> <calls>
//!       `run_db_blocking` on a future that is ready at once, from one plain
//!       thread: the bridge's own cost per call, with no database in it
//!   sessions <db> <scoped|handle> <sessions> <seconds> <lines_per_second>
//!       one plain thread per session, as the Claude backend has, each writing
//!       transcript lines through `stream_store::insert_event` against one of
//!       the database's most recent runs; reports call latency and the process's
//!       thread count. A rate of 0 writes as fast as the database takes them
//!
//! `sessions` writes rows, so point it at a copy of a database, never the live
//! file. `cairn-db`'s `retention_clone` example documents how to take one.
//!
//! Usage:
//!   cargo run --release -p cairn-session --example bridge_load -- cost handle 100000

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_db::storage::{install_bridge_runtime, run_db_blocking, LocalDb, RowExt};
use cairn_session::agent_process::stream::{TranscriptEvent, TranscriptJson};
use cairn_session::transcripts::stream_store::{insert_event, EventInsert};
use serde_json::{json, Value};

/// Data directories a live Cairn keeps its database in, relative to `$HOME`.
const LIVE_DIRS: &[&str] = &[".cairn", "Library/Application Support/com.cairn.desktop"];
const THREAD_SAMPLE_INTERVAL: Duration = Duration::from_millis(20);

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let result = match args.as_slice() {
        ["cost", mode, calls] => cost(mode, calls),
        ["sessions", db, mode, sessions, seconds, rate] => {
            streaming_sessions(db, mode, sessions, seconds, rate)
        }
        _ => Err(
            "usage: bridge_load cost <mode> <calls> | sessions <db> <mode> <sessions> <seconds> \
             <lines_per_second>"
                .to_string(),
        ),
    };
    match result {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("bridge_load: {error}");
            std::process::exit(2);
        }
    }
}

fn runner_runtime(mode: &str) -> Result<tokio::runtime::Runtime, String> {
    if mode != "scoped" && mode != "handle" {
        return Err(format!("mode must be scoped or handle, not {mode}"));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    if mode == "handle" {
        install_bridge_runtime(runtime.handle().clone());
    }
    Ok(runtime)
}

fn cost(mode: &str, calls: &str) -> Result<Value, String> {
    let calls: u32 = calls.parse().map_err(|_| "calls must be a number")?;
    let _runtime = runner_runtime(mode)?;
    let started = Instant::now();
    for _ in 0..calls {
        run_db_blocking(|| async { Ok(()) })?;
    }
    let elapsed = started.elapsed();
    Ok(json!({
        "mode": mode,
        "calls": calls,
        "ns_per_call": elapsed.as_nanos() / u128::from(calls.max(1)),
        "load_average": load_average(),
    }))
}

fn streaming_sessions(
    path: &str,
    mode: &str,
    sessions: &str,
    seconds: &str,
    rate: &str,
) -> Result<Value, String> {
    refuse_a_live_database(path)?;
    let sessions: usize = sessions.parse().map_err(|_| "sessions must be a number")?;
    let seconds: u64 = seconds.parse().map_err(|_| "seconds must be a number")?;
    let rate: f64 = rate.parse().map_err(|_| "rate must be a number")?;

    let runtime = runner_runtime(mode)?;
    let db = Arc::new(
        runtime
            .block_on(LocalDb::open(path))
            .map_err(|e| format!("open {path}: {e}"))?,
    );
    let runs = runtime.block_on(recent_runs(&db, sessions))?;
    if runs.is_empty() {
        return Err(format!("{path} has no runs to write transcript lines to"));
    }

    let load_before = load_average();
    let threads_before = process_thread_count();
    let writing = Arc::new(AtomicBool::new(true));
    let thread_counts = Arc::new(Mutex::new(Vec::new()));
    let sampler = {
        let (writing, thread_counts) = (writing.clone(), thread_counts.clone());
        std::thread::spawn(move || {
            while writing.load(Ordering::Relaxed) {
                if let Some(count) = process_thread_count() {
                    thread_counts.lock().unwrap().push(count);
                }
                std::thread::sleep(THREAD_SAMPLE_INTERVAL);
            }
        })
    };

    let deadline = Instant::now() + Duration::from_secs(seconds);
    let readers: Vec<_> = (0..sessions)
        .map(|session| {
            let run_id = runs[session % runs.len()].clone();
            let db = db.clone();
            std::thread::spawn(move || read_session_output(&db, &run_id, deadline, rate))
        })
        .collect();

    let mut latencies = Vec::new();
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    for reader in readers {
        let (session_latencies, session_errors) = reader.join().map_err(|_| "reader panicked")?;
        latencies.extend(session_latencies);
        for (error, count) in session_errors {
            *errors.entry(error).or_default() += count;
        }
    }
    writing.store(false, Ordering::Relaxed);
    sampler.join().map_err(|_| "sampler panicked")?;

    latencies.sort_unstable();
    let mut counts = std::mem::take(&mut *thread_counts.lock().unwrap());
    counts.sort_unstable();
    let lines = latencies.len();
    Ok(json!({
        "mode": mode,
        "sessions": sessions,
        "seconds": seconds,
        "lines_per_second_per_session": rate,
        "lines": lines,
        "lines_per_second": lines as f64 / seconds.max(1) as f64,
        "errors": errors,
        "call_ms": {
            "p50": quantile_ms(&latencies, 0.50),
            "p99": quantile_ms(&latencies, 0.99),
            "max": quantile_ms(&latencies, 1.0),
        },
        "process_threads": {
            "before_sessions": threads_before,
            "p50": counts.get(counts.len() / 2),
            "peak": counts.last(),
        },
        "load_average": { "before": load_before, "after": load_average() },
    }))
}

/// One session's reader thread: a transcript line, a database write, repeat.
fn read_session_output(
    db: &Arc<LocalDb>,
    run_id: &str,
    deadline: Instant,
    rate: f64,
) -> (Vec<Duration>, BTreeMap<String, u64>) {
    let interval = (rate > 0.0).then(|| Duration::from_secs_f64(1.0 / rate));
    let mut latencies = Vec::new();
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    let mut next_line = Instant::now();
    while Instant::now() < deadline {
        if let Some(interval) = interval {
            std::thread::sleep(next_line.saturating_duration_since(Instant::now()));
            next_line += interval;
        }
        let called = Instant::now();
        match insert_event(db.clone(), transcript_line(run_id)) {
            Ok(_) => latencies.push(called.elapsed()),
            Err(error) => *errors.entry(error).or_default() += 1,
        }
    }
    (latencies, errors)
}

fn transcript_line(run_id: &str) -> EventInsert {
    let now = chrono::Utc::now().timestamp() as i32;
    let data = TranscriptEvent {
        event_type: "text".to_string(),
        session_id: None,
        parent_tool_use_id: None,
        content: Some("bridge_load transcript line".to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: None,
    }
    .observed()
    .to_event_json();
    EventInsert {
        id: uuid::Uuid::new_v4().to_string(),
        run_id: run_id.to_string(),
        session_id: None,
        timestamp: now,
        event_type: "text".to_string(),
        data,
        parent_tool_use_id: None,
        created_at: now,
        input_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        output_tokens: None,
        thinking_tokens: None,
        turn_id: None,
        cost_usd: None,
    }
}

async fn recent_runs(db: &LocalDb, limit: usize) -> Result<Vec<String>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM runs ORDER BY created_at DESC LIMIT ?1",
                    [limit as i64],
                )
                .await?;
            let mut runs = Vec::new();
            while let Some(row) = rows.next().await? {
                runs.push(row.text(0)?);
            }
            Ok(runs)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

fn quantile_ms(sorted: &[Duration], quantile: f64) -> Option<f64> {
    let last = sorted.len().checked_sub(1)?;
    let index = (last as f64 * quantile).round() as usize;
    Some(sorted[index].as_secs_f64() * 1000.0)
}

fn refuse_a_live_database(path: &str) -> Result<(), String> {
    if Path::new(path).extension().and_then(|e| e.to_str()) != Some("db") {
        return Err(format!("{path} does not end in .db"));
    }
    let dir = std::fs::canonicalize(path)
        .map_err(|e| format!("{path}: {e}"))?
        .parent()
        .map(Path::to_path_buf);
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    for live in LIVE_DIRS {
        if std::fs::canonicalize(home.join(live)).ok() == dir {
            return Err(format!("{path} is in a live Cairn data directory"));
        }
    }
    Ok(())
}

fn load_average() -> Option<f64> {
    let text = std::process::Command::new("sysctl")
        .args(["-n", "vm.loadavg"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
        .or_else(|| std::fs::read_to_string("/proc/loadavg").ok())?;
    text.split_whitespace()
        .find_map(|field| field.parse::<f64>().ok())
}

#[cfg(target_os = "macos")]
fn process_thread_count() -> Option<i32> {
    let mut task: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    // SAFETY: the kernel fills a caller-owned struct of the size it is told.
    let read = unsafe {
        libc::proc_pidinfo(
            std::process::id() as i32,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut task as *mut libc::proc_taskinfo).cast(),
            size,
        )
    };
    (read == size).then_some(task.pti_threadnum)
}

#[cfg(not(target_os = "macos"))]
fn process_thread_count() -> Option<i32> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))?
        .trim()
        .parse()
        .ok()
}
