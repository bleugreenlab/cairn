//! A stack sample of a Codex app-server whose request is about to time out.
//!
//! A request Cairn gives up on says only that it timed out, and the app-server
//! logs nothing about where it was parked. So once a request has waited
//! [`SAMPLE_AFTER`], its server's stacks are sampled on a thread of their own,
//! within a limit of their own: the full report is written under the Codex
//! home's `log` directory (where Codex keeps its own logs), and the timeout
//! error carries an excerpt of at most [`EXCERPT_BYTES`]. Sampling never
//! delays the timeout: a sample still running when the request gives up is
//! reported as running, and a sampler that fails or is missing changes
//! nothing but the note.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver};

/// How long a request waits before its server's stacks are sampled.
pub const SAMPLE_AFTER: Duration = Duration::from_secs(30);
/// What one sample may take, macOS `sample`'s five seconds of sampling and
/// its symbolication included.
const SAMPLE_LIMIT: Duration = Duration::from_secs(20);
/// The most of a sample a timeout error carries.
pub const EXCERPT_BYTES: usize = 4096;

/// Takes a report of a running process's stacks.
pub trait StackSampler: Send + Sync {
    /// A report of `pid`'s stacks, taken within `limit`, or why none was.
    fn sample(&self, pid: u32, limit: Duration) -> Result<String, String>;
}

/// This machine's sampler. On macOS, `sample <pid> 5`. On Linux every stack
/// sampler needs ptrace rights over the process, which a sibling of Cairn's
/// child does not have without root, so the report is each thread's name,
/// state and kernel wait channel from `/proc` instead. Elsewhere, none.
#[derive(Debug, Default, Clone, Copy)]
pub struct PlatformSampler;

impl StackSampler for PlatformSampler {
    #[cfg(target_os = "macos")]
    fn sample(&self, pid: u32, limit: Duration) -> Result<String, String> {
        use std::io::Read;
        use std::process::{Command, Stdio};
        use std::time::Instant;

        // `-file` keeps `sample` from leaving its own copy in /tmp.
        let report = tempfile::NamedTempFile::new()
            .map_err(|error| format!("no file for the sample: {error}"))?;
        let mut child = Command::new("/usr/bin/sample")
            .arg(pid.to_string())
            .arg("5")
            .arg("-file")
            .arg(report.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("could not run sample: {error}"))?;
        let deadline = Instant::now() + limit;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("sample did not finish within {}s", limit.as_secs()));
                }
                Err(error) => return Err(format!("could not wait for sample: {error}")),
            }
        };
        if !status.success() {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            return Err(format!("sample exited with {status}: {}", stderr.trim()));
        }
        std::fs::read_to_string(report.path())
            .map_err(|error| format!("could not read the sample: {error}"))
    }

    #[cfg(target_os = "linux")]
    fn sample(&self, pid: u32, _limit: Duration) -> Result<String, String> {
        let tasks = Path::new("/proc").join(pid.to_string()).join("task");
        let mut threads: Vec<PathBuf> = std::fs::read_dir(&tasks)
            .map_err(|error| format!("could not read {}: {error}", tasks.display()))?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .collect();
        threads.sort();
        let mut report = format!(
            "Threads of pid {pid}: id, name, state, kernel wait channel. Linux offers no \
             stack sample of this process without ptrace rights.\n"
        );
        for task in threads {
            let read = |file: &str| {
                std::fs::read_to_string(task.join(file))
                    .map(|text| text.trim().to_string())
                    .unwrap_or_else(|_| "?".to_string())
            };
            // The state is the first field after the parenthesized name.
            let stat = read("stat");
            let state = stat
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().next())
                .unwrap_or("?")
                .to_string();
            let tid = task
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            report.push_str(&format!(
                "{tid} {} {state} {}\n",
                read("comm"),
                read("wchan")
            ));
        }
        Ok(report)
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn sample(&self, _pid: u32, _limit: Duration) -> Result<String, String> {
        Err("there is no stack sampler on this platform".to_string())
    }
}

/// The sample of a request that has waited [`SAMPLE_AFTER`].
pub(crate) struct PendingSample(Sample);

enum Sample {
    Running {
        pid: u32,
        file: Option<PathBuf>,
        done: Receiver<String>,
    },
    Skipped(String),
}

impl PendingSample {
    /// Start sampling `pid`, the server `method` is waiting on, on a thread
    /// of its own; `log_dir` is where the full report goes.
    pub(crate) fn start(
        sampler: Arc<dyn StackSampler>,
        pid: Option<u32>,
        log_dir: Option<&Path>,
        method: &str,
        waited: Duration,
    ) -> Self {
        let Some(pid) = pid else {
            log::warn!(
                "codex app-server request {method} still pending after {}s; no process to take a stack sample of",
                waited.as_secs()
            );
            return Self(Sample::Skipped(
                "no stack sample was taken: no app-server process".to_string(),
            ));
        };
        log::warn!(
            "codex app-server request {method} still pending after {}s; sampling the stacks of pid {pid}",
            waited.as_secs()
        );
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or_default();
        let file = log_dir.map(|dir| dir.join(format!("app-server-stack-{pid}-{stamp}.txt")));
        let target = file.clone();
        let (tx, done) = bounded(1);
        let spawned = thread::Builder::new()
            .name("codex-stack-sample".to_string())
            .spawn(move || {
                let _ = tx.send(take(sampler.as_ref(), pid, target.as_deref()));
            });
        match spawned {
            Ok(_) => Self(Sample::Running { pid, file, done }),
            Err(error) => Self(Sample::Skipped(format!(
                "no stack sample was taken of pid {pid}: {error}"
            ))),
        }
    }

    /// What the timeout error says about the sample, now, without waiting.
    pub(crate) fn report(&self) -> String {
        match &self.0 {
            Sample::Skipped(note) => note.clone(),
            Sample::Running { pid, file, done } => done.try_recv().unwrap_or_else(|_| {
                let destination = file
                    .as_ref()
                    .map(|file| format!("; it will be written to {}", file.display()))
                    .unwrap_or_default();
                format!("the stack sample of pid {pid} was still running{destination}")
            }),
        }
    }
}

/// Take the sample, save it whole to `file`, and say what the timeout error
/// carries of it.
fn take(sampler: &dyn StackSampler, pid: u32, file: Option<&Path>) -> String {
    let report = match sampler.sample(pid, SAMPLE_LIMIT) {
        Ok(report) => report,
        Err(reason) => {
            log::warn!("No stack sample was taken of codex app-server pid {pid}: {reason}");
            return format!("no stack sample was taken of pid {pid}: {reason}");
        }
    };
    let saved = match file {
        Some(file) => match save(file, &report) {
            Ok(()) => {
                log::warn!(
                    "Stack sample of codex app-server pid {pid} written to {}",
                    file.display()
                );
                format!("full output: {}", file.display())
            }
            Err(error) => format!("full output not saved: {error}"),
        },
        None => "full output not saved: the server has no Codex home".to_string(),
    };
    format!("stack sample of pid {pid} ({saved}):\n{}", excerpt(&report))
}

fn save(file: &Path, report: &str) -> std::io::Result<()> {
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(file, report)
}

/// The head of `report`, at most [`EXCERPT_BYTES`] of it.
fn excerpt(report: &str) -> String {
    if report.len() <= EXCERPT_BYTES {
        return report.to_string();
    }
    let mut end = EXCERPT_BYTES;
    while !report.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[{} more bytes in the full output]",
        &report[..end],
        report.len() - end
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_excerpt_is_capped_on_a_character_boundary() {
        let report = "é".repeat(EXCERPT_BYTES);
        let cut = excerpt(&report);
        assert!(cut.len() < EXCERPT_BYTES + 64, "{}", cut.len());
        assert!(cut.ends_with(&format!(
            "[{} more bytes in the full output]",
            EXCERPT_BYTES
        )));
        assert_eq!(excerpt("short"), "short");
    }

    /// The machine's own sampler reads a live process, or says why it
    /// cannot; either way it answers within its limit.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn the_platform_sampler_reports_on_a_live_process() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let started = std::time::Instant::now();
        let outcome = PlatformSampler.sample(child.id(), SAMPLE_LIMIT);
        let _ = child.kill();
        let _ = child.wait();
        assert!(started.elapsed() <= SAMPLE_LIMIT + Duration::from_secs(2));
        match outcome {
            Ok(report) => assert!(report.contains(&child.id().to_string()), "{report}"),
            // A confined test shell may be refused the task port; the
            // refusal is still an answer.
            Err(reason) => assert!(!reason.is_empty()),
        }
    }
}
