//! Retention for the Codex home a host runs its sessions in.
//!
//! Codex never removes a thread: every thread a session, call or image request
//! ever opened keeps its rollout (`sessions/YYYY/MM/DD/rollout-*.jsonl`), its
//! row in the state database and, for paginated history, its projection rows
//! in `thread_history_1.sqlite`. This pass bounds the home by deleting threads
//! nothing will plausibly resume, through Codex's own `thread/delete` on a
//! short-lived app-server, so the files, both databases and the thread-name
//! index stay coherent with each other. Raw file removal would leave the
//! databases naming rollouts that no longer exist.
//!
//! A thread is deleted only when both clocks agree it is idle: the host's own
//! record of the session bound to it (`last_activity`) and the newest write to
//! any of its rollouts. Codex adds two guards of its own: it refuses a thread
//! another process holds the writer lock of (a live session), and one whose
//! rollout forked history still references. A thread resumed or forked after
//! deletion is answered as missing, which the session start rebuilds from the
//! host's transcript (see `server.rs`), so pruning degrades an old resume; it
//! never breaks one.
//!
//! Deleting reclaims rollout bytes at once. Rows removed from the SQLite
//! databases free pages inside the files, which later history reuses; the
//! files themselves shrink only under `PRAGMA incremental_vacuum`, which Codex
//! never runs.

use crate::wire::codex_app_server::errors::{is_missing_thread_error, json_rpc_error_message};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// How long a thread must be idle, by both clocks, before it is deleted.
/// Cairn's own runs bound it: of 1,824 native Codex resumes recorded through
/// 2026-11, every one after an idle gap came within 21 days but one (82 days),
/// and every fork within a day of its source but one (116 days). Past this
/// window a resume rebuilds from the transcript instead.
pub const IDLE_WINDOW: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Consecutive refused deletions after which a pass stops: one or two are
/// threads Codex protects; a run of them is a Codex that refuses everything.
const MAX_CONSECUTIVE_REFUSALS: usize = 10;

/// Refusal texts a report keeps, so a log line shows what Codex said.
const REFUSAL_SAMPLES: usize = 3;

/// One request's ceiling. `thread/delete` rescans every rollout's header, a
/// few seconds on a home of ten thousand rollouts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Codex's archive directory, beside `sessions/`.
const ARCHIVED_SESSIONS: &str = "archived_sessions";

/// The rollouts of one Codex thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutThread {
    /// The thread id the rollouts' session header names. A rollout's file
    /// name carries its own rollout id, which differs from the thread's once
    /// Codex replaces a paginated thread's rollout.
    pub thread_id: String,
    pub files: Vec<PathBuf>,
    pub bytes: u64,
    /// Unix seconds of the newest write to any of its rollouts.
    pub last_written: i64,
}

/// Every thread with a readable rollout under `codex_home`. A rollout whose
/// session header cannot be read is left out, since nothing says which
/// thread it belongs to. Compressed `.jsonl.zst` rollouts are not read:
/// Cairn's home never enables Codex's compression.
pub fn scan_threads(codex_home: &Path) -> std::io::Result<Vec<RolloutThread>> {
    let mut threads: HashMap<String, RolloutThread> = HashMap::new();
    let mut stack = vec![
        codex_home.join("sessions"),
        codex_home.join(ARCHIVED_SESSIONS),
    ];
    while let Some(directory) = stack.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() || !is_rollout_file(&path) {
                continue;
            }
            let Some(thread_id) = session_thread_id(&path) else {
                continue;
            };
            let metadata = entry.metadata()?;
            let written = metadata
                .modified()
                .ok()
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |since| since.as_secs() as i64);
            let thread = threads
                .entry(thread_id.clone())
                .or_insert_with(|| RolloutThread {
                    thread_id,
                    files: Vec::new(),
                    bytes: 0,
                    last_written: 0,
                });
            thread.files.push(path);
            thread.bytes += metadata.len();
            thread.last_written = thread.last_written.max(written);
        }
    }
    Ok(threads.into_values().collect())
}

fn is_rollout_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
}

/// The thread id in a rollout's first line, its `session_meta` record.
fn session_thread_id(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut first = String::new();
    BufReader::new(file).read_line(&mut first).ok()?;
    let record: Value = serde_json::from_str(first.trim()).ok()?;
    (record.get("type").and_then(Value::as_str) == Some("session_meta"))
        .then(|| record.pointer("/payload/id").and_then(Value::as_str))
        .flatten()
        .map(str::to_string)
}

/// Which threads a pass deletes, oldest first, and how many it keeps.
#[derive(Debug, Default)]
pub struct RetentionPlan {
    pub candidates: Vec<RolloutThread>,
    /// Kept because the host's session bound to the thread was active within
    /// the window.
    pub kept_active: usize,
    /// Kept because a rollout was written within the window, whatever the
    /// host's record says.
    pub kept_recent: usize,
    pub scanned_bytes: u64,
}

impl RetentionPlan {
    pub fn candidate_bytes(&self) -> u64 {
        self.candidates.iter().map(|thread| thread.bytes).sum()
    }
}

/// Choose the threads idle past `window` by both clocks. `last_activity` maps
/// a Codex thread id to the unix seconds the host last saw activity on the
/// session bound to it; a thread the host has no session for (a call's or an
/// image request's thread, or one a rebuilt session left behind) is judged by
/// its rollouts alone.
pub fn plan_retention(
    threads: Vec<RolloutThread>,
    last_activity: &HashMap<String, i64>,
    now: i64,
    window: Duration,
) -> RetentionPlan {
    let cutoff = now - window.as_secs() as i64;
    let mut plan = RetentionPlan::default();
    for thread in threads {
        plan.scanned_bytes += thread.bytes;
        if last_activity
            .get(&thread.thread_id)
            .is_some_and(|active| *active > cutoff)
        {
            plan.kept_active += 1;
        } else if thread.last_written > cutoff {
            plan.kept_recent += 1;
        } else {
            plan.candidates.push(thread);
        }
    }
    plan.candidates
        .sort_by(|a, b| (a.last_written, &a.thread_id).cmp(&(b.last_written, &b.thread_id)));
    plan
}

/// Why a deletion did not happen.
#[derive(Debug)]
pub enum DeleteFailure {
    /// Codex refused this thread: another process holds it, or forked
    /// history still references it. The pass moves on.
    Refused(String),
    /// The app-server cannot answer; the pass stops.
    Server(String),
}

/// Deletes one Codex thread with everything Codex keeps for it.
pub trait ThreadDeleter {
    fn delete(&mut self, thread_id: &str) -> Result<(), DeleteFailure>;
}

/// Why a pass ended before its last candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionStop {
    /// The time budget ran out; the rest waits for the next pass.
    Budget,
    /// Codex refused this many deletions in a row.
    Refusals(usize),
    /// The app-server could not be started or stopped answering.
    Server(String),
}

/// What one pass did, for its log line.
#[derive(Debug, Default)]
pub struct RetentionReport {
    pub scanned_threads: usize,
    pub scanned_bytes: u64,
    pub kept_active: usize,
    pub kept_recent: usize,
    pub candidates: usize,
    pub candidate_bytes: u64,
    pub deleted: usize,
    /// Candidates Codex already had no thread for.
    pub already_gone: usize,
    pub refused: usize,
    pub refusal_samples: Vec<String>,
    /// Rollout bytes no longer on disk after the pass.
    pub reclaimed_bytes: u64,
    /// Candidates the pass did not reach.
    pub remaining: usize,
    pub stopped: Option<RetentionStop>,
    pub elapsed: Duration,
}

impl RetentionReport {
    fn planned(plan: &RetentionPlan) -> Self {
        Self {
            scanned_threads: plan.candidates.len() + plan.kept_active + plan.kept_recent,
            scanned_bytes: plan.scanned_bytes,
            kept_active: plan.kept_active,
            kept_recent: plan.kept_recent,
            candidates: plan.candidates.len(),
            candidate_bytes: plan.candidate_bytes(),
            remaining: plan.candidates.len(),
            ..Self::default()
        }
    }
}

/// Delete `plan`'s candidates in order until they run out, `deadline` passes,
/// or Codex stops cooperating.
pub fn execute_retention(
    plan: &RetentionPlan,
    deleter: &mut dyn ThreadDeleter,
    deadline: Instant,
) -> RetentionReport {
    let started = Instant::now();
    let mut report = RetentionReport::planned(plan);
    let mut consecutive_refusals = 0;
    for thread in &plan.candidates {
        if Instant::now() >= deadline {
            report.stopped = Some(RetentionStop::Budget);
            break;
        }
        report.remaining -= 1;
        match deleter.delete(&thread.thread_id) {
            Ok(()) => {
                report.deleted += 1;
                consecutive_refusals = 0;
            }
            Err(DeleteFailure::Refused(message))
                if is_missing_thread_error(&message, &thread.thread_id) =>
            {
                report.already_gone += 1;
                consecutive_refusals = 0;
            }
            Err(DeleteFailure::Refused(message)) => {
                report.refused += 1;
                if report.refusal_samples.len() < REFUSAL_SAMPLES {
                    report
                        .refusal_samples
                        .push(format!("{}: {message}", thread.thread_id));
                }
                consecutive_refusals += 1;
                if consecutive_refusals >= MAX_CONSECUTIVE_REFUSALS {
                    report.stopped = Some(RetentionStop::Refusals(consecutive_refusals));
                    break;
                }
                continue;
            }
            Err(DeleteFailure::Server(message)) => {
                report.remaining += 1;
                report.stopped = Some(RetentionStop::Server(message));
                break;
            }
        }
        if thread.files.iter().all(|file| !file.exists()) {
            report.reclaimed_bytes += thread.bytes;
        }
    }
    report.elapsed = started.elapsed();
    report
}

/// One retention pass over `codex_home` with the Codex CLI at `codex_path`,
/// deleting for at most `budget`.
pub fn prune_codex_home(
    codex_path: &str,
    codex_home: &Path,
    last_activity: &HashMap<String, i64>,
    now: i64,
    budget: Duration,
) -> RetentionReport {
    let deadline = Instant::now() + budget;
    let started = Instant::now();
    let threads = match scan_threads(codex_home) {
        Ok(threads) => threads,
        Err(error) => {
            return RetentionReport {
                stopped: Some(RetentionStop::Server(format!(
                    "could not scan {}: {error}",
                    codex_home.display()
                ))),
                ..RetentionReport::default()
            }
        }
    };
    let plan = plan_retention(threads, last_activity, now, IDLE_WINDOW);
    if plan.candidates.is_empty() {
        let mut report = RetentionReport::planned(&plan);
        report.elapsed = started.elapsed();
        return report;
    }
    // Codex 0.167's `thread/delete` resolves the archive directory before it
    // deletes anything and fails every request while the directory is absent,
    // and Cairn's home never archives.
    if let Err(error) = std::fs::create_dir_all(codex_home.join(ARCHIVED_SESSIONS)) {
        let mut report = RetentionReport::planned(&plan);
        report.stopped = Some(RetentionStop::Server(format!(
            "could not create the Codex archive directory: {error}"
        )));
        return report;
    }
    let mut report = match AppServerDeleter::start(codex_path, codex_home) {
        Ok(mut deleter) => execute_retention(&plan, &mut deleter, deadline),
        Err(error) => {
            let mut report = RetentionReport::planned(&plan);
            report.stopped = Some(RetentionStop::Server(error));
            report
        }
    };
    report.elapsed = started.elapsed();
    report
}

/// A Codex app-server started only to delete threads. It signs in to nothing
/// and opens no thread; it shares the home with every other Codex process the
/// host runs, whose writer locks keep their live threads out of its reach.
pub struct AppServerDeleter {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    lines: mpsc::Receiver<String>,
    next_id: i64,
}

impl AppServerDeleter {
    pub fn start(codex_path: &str, codex_home: &Path) -> Result<Self, String> {
        let mut child = super::models::without_launch_credentials(Command::new(codex_path))
            .arg("app-server")
            .env("CODEX_HOME", codex_home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("could not start the Codex app-server: {error}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or("the Codex app-server has no stdin")?;
        let stdout = child
            .stdout
            .take()
            .ok_or("the Codex app-server has no stdout")?;
        let (send, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if send.send(line).is_err() {
                    return;
                }
            }
        });
        let mut deleter = Self {
            child,
            stdin,
            lines,
            next_id: 0,
        };
        deleter
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "cairn",
                        "title": "Cairn",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": {"experimentalApi": true}
                }),
            )
            .map_err(|failure| match failure {
                DeleteFailure::Refused(message) | DeleteFailure::Server(message) => {
                    format!("the Codex app-server refused initialize: {message}")
                }
            })?;
        deleter
            .send(&json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}))
            .map_err(|failure| match failure {
                DeleteFailure::Refused(message) | DeleteFailure::Server(message) => message,
            })?;
        Ok(deleter)
    }

    fn send(&mut self, message: &Value) -> Result<(), DeleteFailure> {
        writeln!(self.stdin, "{message}")
            .and_then(|()| self.stdin.flush())
            .map_err(|error| {
                DeleteFailure::Server(format!("the Codex app-server stopped reading: {error}"))
            })
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value, DeleteFailure> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))?;
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        loop {
            let wait = deadline.saturating_duration_since(Instant::now());
            let line = match self.lines.recv_timeout(wait) {
                Ok(line) => line,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(DeleteFailure::Server(format!(
                        "the Codex app-server did not answer {method} within {}s",
                        REQUEST_TIMEOUT.as_secs()
                    )))
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(DeleteFailure::Server(format!(
                        "the Codex app-server exited before answering {method}"
                    )))
                }
            };
            let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            if message.get("id").and_then(Value::as_i64) != Some(id) {
                continue;
            }
            return match message.get("error") {
                Some(error) => Err(DeleteFailure::Refused(json_rpc_error_message(
                    &error.to_string(),
                ))),
                None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
            };
        }
    }
}

impl ThreadDeleter for AppServerDeleter {
    fn delete(&mut self, thread_id: &str) -> Result<(), DeleteFailure> {
        self.request("thread/delete", json!({"threadId": thread_id}))
            .map(|_| ())
    }
}

impl Drop for AppServerDeleter {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Unix seconds now, for callers that pass `now` through.
pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 24 * 60 * 60;
    const NOW: i64 = 1_800_000_000;

    /// A rollout for `thread_id` under `home`, named by `rollout_id`, last
    /// written `age_days` before [`NOW`].
    fn rollout(home: &Path, rollout_id: &str, thread_id: &str, age_days: i64) -> PathBuf {
        let directory = home.join("sessions/2026/03/25");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join(format!("rollout-2026-03-25T17-55-27-{rollout_id}.jsonl"));
        let header = json!({"type": "session_meta", "payload": {"id": thread_id}});
        std::fs::write(&path, format!("{header}\n{{\"type\":\"response_item\"}}\n")).unwrap();
        let written = UNIX_EPOCH + Duration::from_secs((NOW - age_days * DAY) as u64);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(written)
            .unwrap();
        path
    }

    fn ids(plan: &RetentionPlan) -> Vec<&str> {
        plan.candidates
            .iter()
            .map(|thread| thread.thread_id.as_str())
            .collect()
    }

    /// Deletes the way Codex does, by removing every rollout of the thread,
    /// and answers as Codex does for a thread it no longer has.
    struct FileDeleter<'a> {
        home: &'a Path,
        refuse: Vec<&'static str>,
        asked: Vec<String>,
    }

    impl ThreadDeleter for FileDeleter<'_> {
        fn delete(&mut self, thread_id: &str) -> Result<(), DeleteFailure> {
            self.asked.push(thread_id.to_string());
            if self.refuse.contains(&thread_id) {
                return Err(DeleteFailure::Refused(format!(
                    "thread {thread_id} already has an active writer"
                )));
            }
            let files: Vec<_> = scan_threads(self.home)
                .unwrap()
                .into_iter()
                .filter(|thread| thread.thread_id == thread_id)
                .flat_map(|thread| thread.files)
                .collect();
            if files.is_empty() {
                return Err(DeleteFailure::Refused(format!(
                    "no rollout found for thread id {thread_id}"
                )));
            }
            for file in files {
                std::fs::remove_file(file).unwrap();
            }
            Ok(())
        }
    }

    fn far_future() -> Instant {
        Instant::now() + Duration::from_secs(3600)
    }

    #[test]
    fn a_scan_groups_rollouts_by_the_thread_their_header_names() {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), "thread-a", "thread-a", 40);
        // A paginated thread's replacement rollout carries its own rollout id
        // in its name but the thread's id in its header.
        rollout(home.path(), "rollout-a2", "thread-a", 35);
        rollout(home.path(), "thread-b", "thread-b", 40);
        std::fs::write(
            home.path()
                .join("sessions/2026/03/25/rollout-2026-03-25T00-00-00-unreadable.jsonl"),
            "{not json}\n",
        )
        .unwrap();
        std::fs::write(home.path().join("sessions/2026/03/25/notes.txt"), "x").unwrap();

        let mut threads = scan_threads(home.path()).unwrap();
        threads.sort_by(|a, b| a.thread_id.cmp(&b.thread_id));
        assert_eq!(threads.len(), 2);
        assert_eq!(threads[0].thread_id, "thread-a");
        assert_eq!(threads[0].files.len(), 2);
        assert_eq!(threads[0].last_written, NOW - 35 * DAY);
        assert_eq!(threads[1].thread_id, "thread-b");
        assert!(threads.iter().all(|thread| thread.bytes > 0));
    }

    #[test]
    fn a_live_or_recently_bound_thread_is_kept_whatever_its_rollout_age() {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), "thread-bound", "thread-bound", 90);
        rollout(home.path(), "thread-closed", "thread-closed", 90);
        let activity = HashMap::from([
            // An open session that ran yesterday on a thread whose rollout
            // Codex last wrote months ago.
            ("thread-bound".to_string(), NOW - DAY),
            ("thread-closed".to_string(), NOW - 60 * DAY),
        ]);
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &activity,
            NOW,
            IDLE_WINDOW,
        );
        assert_eq!(ids(&plan), ["thread-closed"]);
        assert_eq!(plan.kept_active, 1);
    }

    #[test]
    fn a_recent_rollout_write_keeps_a_thread_the_host_thinks_idle() {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), "thread-written", "thread-written", 2);
        let activity = HashMap::from([("thread-written".to_string(), NOW - 60 * DAY)]);
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &activity,
            NOW,
            IDLE_WINDOW,
        );
        assert!(plan.candidates.is_empty());
        assert_eq!(plan.kept_recent, 1);
    }

    #[test]
    fn a_thread_no_session_names_is_judged_by_its_rollouts_alone() {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), "call-old", "call-old", 31);
        rollout(home.path(), "call-new", "call-new", 29);
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &HashMap::new(),
            NOW,
            IDLE_WINDOW,
        );
        assert_eq!(ids(&plan), ["call-old"]);
        assert_eq!(plan.kept_recent, 1);
    }

    #[test]
    fn candidates_are_deleted_oldest_first() {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), "thread-60", "thread-60", 60);
        rollout(home.path(), "thread-90", "thread-90", 90);
        rollout(home.path(), "thread-45", "thread-45", 45);
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &HashMap::new(),
            NOW,
            IDLE_WINDOW,
        );
        assert_eq!(ids(&plan), ["thread-90", "thread-60", "thread-45"]);
    }

    #[test]
    fn a_pass_removes_an_old_terminal_thread_keeps_a_bound_one_and_is_idempotent() {
        let home = tempfile::tempdir().unwrap();
        let old = rollout(home.path(), "thread-old", "thread-old", 60);
        let replacement = rollout(home.path(), "rollout-old-2", "thread-old", 50);
        let bound = rollout(home.path(), "thread-bound", "thread-bound", 60);
        let activity = HashMap::from([
            ("thread-old".to_string(), NOW - 45 * DAY),
            ("thread-bound".to_string(), NOW - DAY),
        ]);
        let expected_bytes =
            std::fs::metadata(&old).unwrap().len() + std::fs::metadata(&replacement).unwrap().len();

        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &activity,
            NOW,
            IDLE_WINDOW,
        );
        let mut deleter = FileDeleter {
            home: home.path(),
            refuse: Vec::new(),
            asked: Vec::new(),
        };
        let report = execute_retention(&plan, &mut deleter, far_future());
        assert_eq!(deleter.asked, ["thread-old"]);
        assert_eq!(
            (report.deleted, report.refused, report.remaining),
            (1, 0, 0)
        );
        assert_eq!(report.reclaimed_bytes, expected_bytes);
        assert_eq!(report.kept_active, 1);
        assert!(!old.exists() && !replacement.exists());
        assert!(bound.exists());

        // A second pass finds nothing left to do.
        let again = plan_retention(
            scan_threads(home.path()).unwrap(),
            &activity,
            NOW,
            IDLE_WINDOW,
        );
        assert!(again.candidates.is_empty());
        let report = execute_retention(&again, &mut deleter, far_future());
        assert_eq!((report.deleted, report.reclaimed_bytes), (0, 0));
        assert!(bound.exists());
    }

    #[test]
    fn a_thread_codex_already_lacks_counts_as_gone_not_refused() {
        let home = tempfile::tempdir().unwrap();
        rollout(home.path(), "thread-old", "thread-old", 60);
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &HashMap::new(),
            NOW,
            IDLE_WINDOW,
        );
        let mut deleter = FileDeleter {
            home: home.path(),
            refuse: Vec::new(),
            asked: Vec::new(),
        };
        execute_retention(&plan, &mut deleter, far_future());
        // Planned before another pass removed it.
        let report = execute_retention(&plan, &mut deleter, far_future());
        assert_eq!((report.already_gone, report.refused), (1, 0));
    }

    #[test]
    fn a_refused_thread_survives_and_the_pass_moves_on() {
        let home = tempfile::tempdir().unwrap();
        let held = rollout(home.path(), "thread-held", "thread-held", 90);
        let old = rollout(home.path(), "thread-old", "thread-old", 60);
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &HashMap::new(),
            NOW,
            IDLE_WINDOW,
        );
        let mut deleter = FileDeleter {
            home: home.path(),
            refuse: vec!["thread-held"],
            asked: Vec::new(),
        };
        let report = execute_retention(&plan, &mut deleter, far_future());
        assert_eq!((report.deleted, report.refused), (1, 1));
        assert!(report.refusal_samples[0].contains("active writer"));
        assert!(held.exists());
        assert!(!old.exists());
    }

    #[test]
    fn a_run_of_refusals_stops_the_pass() {
        let home = tempfile::tempdir().unwrap();
        let mut refuse = Vec::new();
        for index in 0..12 {
            let id: &'static str = Box::leak(format!("thread-{index:02}").into_boxed_str());
            rollout(home.path(), id, id, 60 + index);
            refuse.push(id);
        }
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &HashMap::new(),
            NOW,
            IDLE_WINDOW,
        );
        let mut deleter = FileDeleter {
            home: home.path(),
            refuse,
            asked: Vec::new(),
        };
        let report = execute_retention(&plan, &mut deleter, far_future());
        assert_eq!(
            report.stopped,
            Some(RetentionStop::Refusals(MAX_CONSECUTIVE_REFUSALS))
        );
        assert_eq!(deleter.asked.len(), MAX_CONSECUTIVE_REFUSALS);
        assert_eq!(report.remaining, 12 - MAX_CONSECUTIVE_REFUSALS);
    }

    #[test]
    fn a_spent_budget_deletes_nothing_and_leaves_the_rest_for_the_next_pass() {
        let home = tempfile::tempdir().unwrap();
        let old = rollout(home.path(), "thread-old", "thread-old", 60);
        let plan = plan_retention(
            scan_threads(home.path()).unwrap(),
            &HashMap::new(),
            NOW,
            IDLE_WINDOW,
        );
        let mut deleter = FileDeleter {
            home: home.path(),
            refuse: Vec::new(),
            asked: Vec::new(),
        };
        let report = execute_retention(&plan, &mut deleter, Instant::now());
        assert_eq!(report.stopped, Some(RetentionStop::Budget));
        assert_eq!((report.deleted, report.remaining), (0, 1));
        assert!(deleter.asked.is_empty());
        assert!(old.exists());
    }

    #[cfg(unix)]
    fn scripted_codex(dir: &Path) -> String {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("codex");
        std::fs::write(
            &script,
            format!("#!/bin/sh\n{SCRIPTED_DELETING_APP_SERVER}"),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        script.to_string_lossy().into_owned()
    }

    /// Answers `thread/delete` the way Codex 0.167 does: removes the thread's
    /// rollouts, refuses while `archived_sessions/` is missing, and reports a
    /// thread it has no rollout for. Request ids are echoed from the request.
    #[cfg(unix)]
    const SCRIPTED_DELETING_APP_SERVER: &str = r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":%s,"result":{}}\n' "$id" ;;
    *'"method":"thread/delete"'*)
      thread=$(printf '%s' "$line" | sed -n 's/.*"threadId":"\([^"]*\)".*/\1/p')
      if [ ! -d "$CODEX_HOME/archived_sessions" ]; then
        printf '{"id":%s,"error":{"code":-32603,"message":"failed to resolve archived sessions directory"}}\n' "$id"
      elif ls "$CODEX_HOME"/sessions/*/*/*/*"$thread".jsonl >/dev/null 2>&1; then
        rm -f "$CODEX_HOME"/sessions/*/*/*/*"$thread".jsonl
        printf '{"id":%s,"result":{}}\n' "$id"
      else
        printf '{"id":%s,"error":{"code":-32600,"message":"no rollout found for thread id %s"}}\n' "$id" "$thread"
      fi ;;
  esac
done
"#;

    /// The whole pass against a process that speaks the app-server protocol:
    /// the archive directory Codex needs is created, the old thread goes, the
    /// bound one stays, and a second pass has nothing to do.
    #[cfg(unix)]
    #[test]
    fn a_pass_drives_the_app_server_and_keeps_what_is_bound() {
        let bin = tempfile::tempdir().unwrap();
        let codex = scripted_codex(bin.path());
        let home = tempfile::tempdir().unwrap();
        let now = unix_now();
        let age = |days: i64| (NOW - now) / DAY + days;
        let old = rollout(home.path(), "thread-old", "thread-old", age(40));
        let bound = rollout(home.path(), "thread-bound", "thread-bound", age(40));
        let activity = HashMap::from([("thread-bound".to_string(), now - DAY)]);

        let report = prune_codex_home(&codex, home.path(), &activity, now, Duration::from_secs(60));
        assert_eq!(report.stopped, None, "{report:?}");
        assert_eq!((report.deleted, report.kept_active), (1, 1));
        assert!(report.reclaimed_bytes > 0);
        assert!(home.path().join(ARCHIVED_SESSIONS).is_dir());
        assert!(!old.exists());
        assert!(bound.exists());

        let again = prune_codex_home(&codex, home.path(), &activity, now, Duration::from_secs(60));
        assert_eq!((again.candidates, again.deleted), (0, 0));
    }

    #[cfg(unix)]
    #[test]
    fn a_codex_that_cannot_start_stops_the_pass_and_deletes_nothing() {
        let home = tempfile::tempdir().unwrap();
        let old = rollout(home.path(), "thread-old", "thread-old", 10_000);
        let report = prune_codex_home(
            "/nonexistent/codex",
            home.path(),
            &HashMap::new(),
            unix_now(),
            Duration::from_secs(60),
        );
        assert!(matches!(report.stopped, Some(RetentionStop::Server(_))));
        assert_eq!((report.candidates, report.remaining), (1, 1));
        assert!(old.exists());
    }
}
