//! A runner process's record of why it deliberately killed itself, left behind
//! for its successor to find and report.
//!
//! A deliberate self-abort is an exceptional fact about the fleet, and it is the
//! one fact the process that knows it cannot report: the process is gone a
//! microsecond later, and the service manager relaunches a successor whose
//! `/api/health` answers "healthy" with no memory of what it replaced. Left
//! unrecorded the abort evaporates entirely, and reconstructing it means an
//! operator reading `.ips` crash reports by hand.
//!
//! So the dying process writes a small marker into its data directory as its
//! last act, and the successor adopts it at boot: reads it, deletes it, and
//! holds it for the rest of its life as the answer to "why am I young?".
//!
//! The marker is deliberately NOT the crash report. The OS writes its report
//! seconds after the abort — 18s in the incident this was built from — so the
//! successor cannot name that path at boot. [`crash_report_for`] resolves it at
//! read time instead, when an operator is actually asking.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// The marker a dying runner leaves in its data directory.
pub const MARKER_FILENAME: &str = "abnormal-exit.json";

/// Where macOS writes the crash report that corresponds to a marker. Absent on
/// other platforms, which is a named gap rather than an error.
const MACOS_CRASH_REPORT_DIR: &str = "Library/Logs/DiagnosticReports";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AbnormalExit {
    /// When the exiting process decided to die, in Unix milliseconds.
    pub at_unix_ms: u64,
    /// The pid that died, so a marker can be matched against a crash report and
    /// against the log lines that precede it.
    pub pid: u32,
    /// One complete sentence naming what happened and the evidence behind it.
    /// Written for an operator reading it cold, not for a log grep.
    pub reason: String,
}

impl AbnormalExit {
    pub fn new(pid: u32, reason: String) -> Self {
        Self {
            at_unix_ms: unix_time_ms(),
            pid,
            reason,
        }
    }

    /// How long ago this exit happened, relative to a caller-supplied instant, so
    /// every age in one rendering derives from the same reading of the clock.
    pub fn elapsed_ms(&self, now_unix_ms: u64) -> u64 {
        now_unix_ms.saturating_sub(self.at_unix_ms)
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

pub fn marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(MARKER_FILENAME)
}

/// Record an imminent deliberate exit. Best-effort by construction: this runs on
/// the last few microseconds of a process that has already decided to die, so a
/// failure to write must never become a second failure mode. The abort proceeds
/// either way; the worst case is the silence this module exists to remove.
pub fn record(data_dir: &Path, exit: &AbnormalExit) {
    let path = marker_path(data_dir);
    let Ok(encoded) = serde_json::to_vec_pretty(exit) else {
        return;
    };
    if let Err(error) = std::fs::write(&path, encoded) {
        tracing::warn!(
            "Could not record abnormal exit marker at {}: {error}",
            path.display()
        );
    }
}

/// Read and clear a predecessor's marker.
///
/// Deleting on read is what keeps the fact tied to ONE restart: a marker left in
/// place would make every subsequent boot report an abort that has long since
/// been superseded, and a warning that is always on carries no information.
///
/// Separate from [`adopt_predecessor`] so the file handling stays a pure,
/// testable function of a directory. Publication is the part that can happen
/// only once per process; reading a marker is not.
pub fn take_marker(data_dir: &Path) -> Option<AbnormalExit> {
    let path = marker_path(data_dir);
    let adopted = match std::fs::read(&path) {
        Ok(bytes) => match serde_json::from_slice::<AbnormalExit>(&bytes) {
            Ok(exit) => Some(exit),
            Err(error) => {
                tracing::warn!(
                    "Ignoring unreadable abnormal exit marker at {}: {error}",
                    path.display()
                );
                None
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            tracing::warn!(
                "Could not read abnormal exit marker at {}: {error}",
                path.display()
            );
            None
        }
    };
    // Clear whatever was there, including a marker that failed to parse: an
    // unreadable marker that survives is a permanent warning nobody can act on.
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }
    adopted
}

/// Adopt a predecessor's marker at boot, publishing it for the life of this
/// process.
///
/// Idempotent: only the first call in a process can set the record, matching the
/// build-identity pattern this mirrors -- a process-wide fact established once
/// at boot and read from anywhere afterwards.
pub fn adopt_predecessor(data_dir: &Path) -> Option<&'static AbnormalExit> {
    let _ = PREDECESSOR.set(take_marker(data_dir));
    predecessor()
}

static PREDECESSOR: OnceLock<Option<AbnormalExit>> = OnceLock::new();

/// The predecessor this process replaced, if it died deliberately and said so.
/// `None` covers both "the predecessor exited cleanly" and "nothing has adopted
/// yet", which are the same thing from a reader's perspective: no abort to
/// report.
pub fn predecessor() -> Option<&'static AbnormalExit> {
    PREDECESSOR.get().and_then(|exit| exit.as_ref())
}

/// The OS crash report matching an abnormal exit, resolved at read time.
///
/// Resolved lazily rather than stored, because the report does not exist yet
/// when the marker is adopted: macOS wrote the report for the incident behind
/// this module 18 seconds after the abort, long after the successor had booted.
/// By the time anyone reads the fleet, it is there.
///
/// A report must name the pid that died. Timing alone cannot attribute one:
/// several runner reports can land in the same interval -- a dev instance, or a
/// successor that crashed on its own way up -- and the first to appear would
/// otherwise be presented as the explanation for THIS abort. A confidently wrong
/// path is worse than a named gap, because it sends an operator to read the
/// wrong process's threads.
pub fn crash_report_for(exit: &AbnormalExit) -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    crash_report_in(&PathBuf::from(home).join(MACOS_CRASH_REPORT_DIR), exit)
}

/// How far after an abort a report may appear and still be attributed to it.
/// Pids are reused, so identity alone does not bound the search: this process
/// can outlive its predecessor's restart by days.
const CRASH_REPORT_WINDOW_MS: u64 = 10 * 60 * 1_000;

fn crash_report_in(reports: &Path, exit: &AbnormalExit) -> Option<PathBuf> {
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(reports).ok()? {
        let Ok(entry) = entry else { continue };
        let path = entry.path();
        let names_a_runner = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("cairn-runner-") && name.ends_with(".ips"));
        if !names_a_runner {
            continue;
        }
        let Ok(written_at) = entry.metadata().and_then(|meta| meta.modified()) else {
            continue;
        };
        let written_unix_ms = written_at
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or_default();
        // Written after the process decided to die, and within a bounded
        // interval of that decision.
        if written_unix_ms < exit.at_unix_ms
            || written_unix_ms - exit.at_unix_ms > CRASH_REPORT_WINDOW_MS
        {
            continue;
        }
        if reported_pid(&path) != Some(exit.pid) {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|(best_at, _)| written_unix_ms < *best_at)
        {
            best = Some((written_unix_ms, path));
        }
    }
    best.map(|(_, path)| path)
}

/// The pid a crash report is about.
///
/// An `.ips` is two JSON documents separated by a newline: a short header, then
/// a body carrying `pid`. A report this cannot parse yields `None` and is
/// skipped rather than guessed at -- an unattributable report is a gap, not a
/// match.
fn reported_pid(path: &Path) -> Option<u32> {
    let text = std::fs::read_to_string(path).ok()?;
    let (_header, body) = text.split_once('\n')?;
    let parsed: serde_json::Value = serde_json::from_str(body).ok()?;
    u32::try_from(parsed.get("pid")?.as_u64()?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_recorded_marker_round_trips_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let exit = AbnormalExit::new(4242, "transport watchdog aborted".to_string());
        record(dir.path(), &exit);

        let bytes = std::fs::read(marker_path(dir.path())).unwrap();
        let decoded: AbnormalExit = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, exit);
    }

    /// Adoption must CONSUME the marker. A marker that survives its adoption
    /// makes every later boot report a restart that already happened, which is
    /// the same silence in the opposite direction: a warning that is always on
    /// carries no information.
    /// Reading the marker must also CONSUME it. A marker that survives makes
    /// every later boot report a restart that already happened, which is the
    /// same silence in the opposite direction: a warning that is always on
    /// carries no information.
    #[test]
    fn taking_the_marker_reports_it_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let exit = AbnormalExit::new(7, "aborted".to_string());
        record(dir.path(), &exit);

        assert_eq!(take_marker(dir.path()), Some(exit));
        assert!(
            !marker_path(dir.path()).exists(),
            "reading must consume the marker"
        );
        assert_eq!(
            take_marker(dir.path()),
            None,
            "one abort must not be reported by two boots"
        );
    }

    #[test]
    fn a_clean_predecessor_leaves_nothing_to_report() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(take_marker(dir.path()), None);
    }

    /// Write a crash report the way macOS does: a header line, then a body
    /// document carrying the pid of the process that died.
    fn write_report(dir: &Path, name: &str, pid: u32) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(
            &path,
            format!("{{\"app_name\":\"cairn-runner\"}}\n{{\"pid\":{pid},\"procName\":\"cairn-runner\"}}"),
        )
        .unwrap();
        path
    }

    /// Timing cannot attribute a crash report on its own. Several runner
    /// reports can land in the same interval -- a dev instance, or a successor
    /// that crashed on its own way up -- and picking the earliest one after the
    /// abort would hand an operator the wrong process's threads to read.
    #[test]
    fn a_crash_report_is_attributed_by_the_pid_that_died() {
        let dir = tempfile::tempdir().unwrap();
        let exit = AbnormalExit::new(74402, "aborted".to_string());

        // An unrelated runner crash lands FIRST, so only the pid can tell them
        // apart: an earliest-wins rule would choose this one.
        write_report(dir.path(), "cairn-runner-2026-08-01-153830.ips", 99999);
        std::thread::sleep(Duration::from_millis(20));
        let ours = write_report(dir.path(), "cairn-runner-2026-08-01-153834.ips", 74402);

        assert_eq!(crash_report_in(dir.path(), &exit), Some(ours));
    }

    /// No report for this pid is a NAMED GAP, never the nearest other report.
    #[test]
    fn an_unattributable_report_is_not_offered_as_this_abort() {
        let dir = tempfile::tempdir().unwrap();
        let exit = AbnormalExit::new(74402, "aborted".to_string());
        write_report(dir.path(), "cairn-runner-2026-08-01-153834.ips", 12345);
        // A report whose body cannot be parsed is skipped rather than guessed at.
        std::fs::write(
            dir.path().join("cairn-runner-2026-08-01-153835.ips"),
            b"junk",
        )
        .unwrap();
        assert_eq!(crash_report_in(dir.path(), &exit), None);
    }

    /// A pid is reused, so identity alone cannot bound the search: a report
    /// written long after the abort is a different process's.
    #[test]
    fn a_report_outside_the_window_is_not_attributed_to_an_old_abort() {
        let dir = tempfile::tempdir().unwrap();
        let mut exit = AbnormalExit::new(74402, "aborted".to_string());
        write_report(dir.path(), "cairn-runner-2026-08-01-153834.ips", 74402);
        assert!(crash_report_in(dir.path(), &exit).is_some());

        exit.at_unix_ms -= CRASH_REPORT_WINDOW_MS + 60_000;
        assert_eq!(crash_report_in(dir.path(), &exit), None);
    }

    /// A marker whose contents cannot be parsed is still removed, so a corrupt
    /// write cannot wedge every future boot into warning about it forever.
    #[test]
    fn an_unreadable_marker_is_cleared_rather_than_left_to_repeat() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(marker_path(dir.path()), b"{ not json").unwrap();
        assert_eq!(take_marker(dir.path()), None);
        assert!(!marker_path(dir.path()).exists());
    }
}
