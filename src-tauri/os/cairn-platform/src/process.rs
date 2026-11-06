use std::io::Read;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

pub const NETWORK_TIMEOUT: Duration = Duration::from_secs(600);
const PIPE_READER_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
/// A `posix_spawn` returns in well under this. A spawn that takes longer is
/// almost certainly a `fork()` copying a large address space while holding the
/// allocator's locks, which stalls every allocating thread in the process.
const SLOW_SPAWN: Duration = Duration::from_millis(100);
/// How long a completed spawn's duration stays in [`spawn_stats`].
const RECENT_SPAWN_WINDOW: Duration = Duration::from_secs(60);

#[derive(Default)]
struct SpawnLedger {
    next_id: u64,
    in_flight: Vec<(u64, Instant)>,
    longest_recent: Option<(Instant, Duration)>,
}

static SPAWNS: Mutex<SpawnLedger> = Mutex::new(SpawnLedger {
    next_id: 0,
    in_flight: Vec::new(),
    longest_recent: None,
});

/// Spawns this process has in progress, and the longest seen recently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpawnStats {
    pub in_flight: usize,
    /// The longest spawn completed in the last minute, or the age of the
    /// oldest one still in flight, whichever is longer.
    pub longest_recent: Option<Duration>,
}

pub fn spawn_stats() -> SpawnStats {
    let Ok(ledger) = SPAWNS.lock() else {
        return SpawnStats {
            in_flight: 0,
            longest_recent: None,
        };
    };
    let oldest_in_flight = ledger
        .in_flight
        .iter()
        .map(|(_, started)| started.elapsed())
        .max();
    let recent = ledger
        .longest_recent
        .filter(|(finished, _)| finished.elapsed() <= RECENT_SPAWN_WINDOW)
        .map(|(_, duration)| duration);
    SpawnStats {
        in_flight: ledger.in_flight.len(),
        longest_recent: oldest_in_flight.max(recent),
    }
}

/// `command.spawn()`, accounted: counted in [`spawn_stats`] while in flight,
/// tagged as the `process.spawn` region for the stall watchdog, and logged
/// when it takes long enough to mean the spawn forked.
pub fn observed_spawn(command: &mut Command) -> std::io::Result<Child> {
    let program = command.get_program().to_owned();
    observed(&program, || command.spawn())
}

/// Any spawn of `program`, accounted as [`observed_spawn`] accounts one.
pub(crate) fn observed<T>(
    program: &std::ffi::OsStr,
    spawn: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    let started = Instant::now();
    let id = SPAWNS.lock().ok().map(|mut ledger| {
        let id = ledger.next_id;
        ledger.next_id += 1;
        ledger.in_flight.push((id, started));
        id
    });
    let result = {
        let _region = crate::runtime_regions::enter("process.spawn");
        spawn()
    };
    let elapsed = started.elapsed();
    if let (Some(id), Ok(mut ledger)) = (id, SPAWNS.lock()) {
        ledger.in_flight.retain(|(entry, _)| *entry != id);
        let replace = match ledger.longest_recent {
            Some((finished, longest)) => {
                finished.elapsed() > RECENT_SPAWN_WINDOW || elapsed >= longest
            }
            None => true,
        };
        if replace {
            ledger.longest_recent = Some((Instant::now(), elapsed));
        }
    }
    if elapsed >= SLOW_SPAWN {
        log::warn!(
            "slow process spawn: program={} elapsed_ms={}",
            program.to_string_lossy(),
            elapsed.as_millis()
        );
    }
    result
}

/// `command.output()`, with the spawn accounted as [`observed_spawn`] accounts
/// it. As `output` does, it captures stdout and stderr and gives the child a
/// null stdin.
pub fn observed_output(command: &mut Command) -> std::io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    observed_spawn(command)?.wait_with_output()
}

/// `command.status()`, with the spawn accounted as [`observed_spawn`] accounts
/// it. The child inherits whatever stdio the command was given, as with
/// `status`.
pub fn observed_status(command: &mut Command) -> std::io::Result<ExitStatus> {
    observed_spawn(command)?.wait()
}

fn spawn_pipe_reader<R: Read + Send + 'static>(
    mut reader: R,
) -> (
    std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    thread::JoinHandle<()>,
) {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let handle = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = reader.read_to_end(&mut bytes).map(|_| bytes);
        let _ = tx.send(result);
    });
    (rx, handle)
}

fn finish_pipe_reader(
    receiver: std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    handle: thread::JoinHandle<()>,
    ctx: &str,
    stream: &str,
    deadline: Option<Instant>,
) -> Result<Vec<u8>, String> {
    // Within a bounded reap, pipe draining spends the same allowance rather than
    // adding its own two seconds per stream.
    let wait = deadline.map_or(PIPE_READER_JOIN_TIMEOUT, |deadline| {
        PIPE_READER_JOIN_TIMEOUT.min(deadline.saturating_duration_since(Instant::now()))
    });
    match receiver.recv_timeout(wait) {
        Ok(result) => {
            handle
                .join()
                .map_err(|_| format!("{ctx}: {stream} reader panicked"))?;
            result.map_err(|error| format!("{ctx}: read {stream}: {error}"))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "{ctx}: {stream} pipe remained open more than {}s after child exit; reader detached",
            PIPE_READER_JOIN_TIMEOUT.as_secs()
        )),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            let _ = handle.join();
            Err(format!("{ctx}: {stream} reader disconnected"))
        }
    }
}

/// A process a timed-out command left behind: killed, and not proven gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurvivingProcess {
    pub pid: u32,
    /// The group the child led, which its descendants joined.
    pub process_group: u32,
    /// The leader's kernel start token, read right after spawn, so a later
    /// check can tell this process from a reuse of its pid.
    pub start_token: Option<String>,
    /// What was still outstanding when the reap bound expired.
    pub outstanding: String,
}

/// Why a [`bounded_command_output_reaped`] command produced no output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedCommandError {
    /// The command could not be spawned, failed while running, or ran out of
    /// time and was killed with its whole process group proven gone. Whatever
    /// it changed, nothing it started is still running.
    Failed(String),
    /// The command ran out of time and was killed, but its leader was not
    /// reaped, or its process group not proven empty, before the reap bound.
    /// A writer may still be running.
    TerminationUnproven {
        message: String,
        survivor: SurvivingProcess,
    },
}

impl std::fmt::Display for BoundedCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(message) | Self::TerminationUnproven { message, .. } => {
                f.write_str(message)
            }
        }
    }
}

/// Run a subprocess with drained output pipes and a hard deadline. The child is
/// placed in its own process group on Unix so timeout cleanup also reaches git,
/// ssh, credential helpers, and any other descendants spawned by jj.
///
/// A timed-out child is waited on without bound, exactly as before
/// [`bounded_command_output_reaped`] existed: callers that have no fence to
/// fall back on keep the guarantee that nothing they started outlives the call.
pub fn bounded_command_output(
    command: &mut Command,
    timeout: Duration,
    ctx: &str,
) -> Result<Output, String> {
    run_bounded(command, timeout, ctx, None).map_err(|error| error.to_string())
}

/// [`bounded_command_output`] whose timeout reap is itself bounded.
///
/// When the child outlives `timeout`, its group is killed and `reap_deadline`
/// is called once, at that moment, for the instant by which termination must be
/// proven: the leader reaped, and no live process left in its group. Short of
/// that proof the call returns [`BoundedCommandError::TerminationUnproven`]
/// naming the survivor, and a detached thread keeps waiting on the leader so it
/// is reaped whenever the kernel lets it go. It never waits past the bound, so a
/// caller holding a lock can release it, provided it first fences what the
/// survivor may still be writing.
pub fn bounded_command_output_reaped(
    command: &mut Command,
    timeout: Duration,
    ctx: &str,
    reap_deadline: &dyn Fn() -> Instant,
) -> Result<Output, BoundedCommandError> {
    run_bounded(command, timeout, ctx, Some(reap_deadline))
}

fn run_bounded(
    command: &mut Command,
    timeout: Duration,
    ctx: &str,
    reap_deadline: Option<&dyn Fn() -> Instant>,
) -> Result<Output, BoundedCommandError> {
    let failed = |error: String| BoundedCommandError::Failed(error);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(unix)]
    {
        // `process_group`, never a `pre_exec(setpgid)`: std can carry the group
        // through `posix_spawn`, but any `pre_exec` closure forces `fork()`,
        // and on macOS fork holds every malloc zone lock process-wide while it
        // copies the address space.
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    let mut child = observed_spawn(command).map_err(|e| failed(format!("{ctx}: {e}")))?;
    // Read now, while the child certainly runs: after a kill the token is what
    // tells a survivor from a stranger that reused its pid.
    let start_token =
        reap_deadline.and_then(|_| cairn_common::process_identity::process_start_token(child.id()));
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (stdout_rx, stdout_reader) = spawn_pipe_reader(stdout);
    let (stderr_rx, stderr_reader) = spawn_pipe_reader(stderr);

    let deadline = Instant::now() + timeout;
    let mut pipe_deadline = None;
    let (status, timed_out) = loop {
        match child
            .try_wait()
            .map_err(|e| failed(format!("{ctx}: {e}")))?
        {
            Some(status) => break (status, false),
            None if Instant::now() >= deadline => {
                kill_group(&mut child);
                let Some(reap_deadline) = reap_deadline else {
                    let status = child.wait().map_err(|e| failed(format!("{ctx}: {e}")))?;
                    break (status, true);
                };
                let reap_by = reap_deadline();
                match reap_within(&mut child, reap_by) {
                    Ok(status) => {
                        pipe_deadline = Some(reap_by);
                        break (status, true);
                    }
                    Err(outstanding) => {
                        let pid = child.id();
                        // Keep waiting where no lock is held, so the leader is
                        // still reaped once the kernel releases it.
                        thread::spawn(move || {
                            let _ = child.wait();
                        });
                        return Err(BoundedCommandError::TerminationUnproven {
                            message: format!(
                                "{ctx} timed out after {}s and was killed, but its termination \
                                 was not proven before the reap bound: {outstanding}",
                                timeout.as_secs_f64()
                            ),
                            survivor: SurvivingProcess {
                                pid,
                                process_group: pid,
                                start_token,
                                outstanding,
                            },
                        });
                    }
                }
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    };
    let stdout = finish_pipe_reader(stdout_rx, stdout_reader, ctx, "stdout", pipe_deadline)
        .map_err(failed)?;
    let stderr = finish_pipe_reader(stderr_rx, stderr_reader, ctx, "stderr", pipe_deadline)
        .map_err(failed)?;
    if timed_out {
        return Err(failed(format!(
            "{ctx} timed out after {}s and was killed",
            timeout.as_secs_f64()
        )));
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn kill_group(child: &mut Child) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let _ = child.kill();
}

/// Reap the killed leader, then prove its group empty, by `reap_by`.
///
/// The leader is reaped first, deliberately: Darwin answers a group probe with
/// `EPERM` rather than `ESRCH` while the group holds only an unreaped zombie
/// leader (cairn/6159), so a membership probe before the reap could never
/// conclude. Membership is read through the shared process-identity stack,
/// which leaves zombies out; an unreadable listing is not proof of absence.
fn reap_within(child: &mut Child, reap_by: Instant) -> Result<ExitStatus, String> {
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= reap_by => {
                return Err(format!("leader pid {} was not reaped", child.id()));
            }
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(error) => return Err(format!("waiting on leader pid {}: {error}", child.id())),
        }
    };
    #[cfg(unix)]
    {
        let group = child.id();
        loop {
            match cairn_common::process_identity::process_group_members(group) {
                Ok(members) if members.is_empty() => break,
                outcome if Instant::now() >= reap_by => {
                    return Err(match outcome {
                        Ok(members) => format!(
                            "process group {group} still held {} live process(es): {}",
                            members.len(),
                            members
                                .iter()
                                .map(|member| member.pid.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        Err(error) => {
                            format!("process group {group} membership unreadable: {error}")
                        }
                    });
                }
                _ => {
                    unsafe {
                        libc::kill(-(group as i32), libc::SIGKILL);
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }
    Ok(status)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// The accounted forms answer exactly as `output` and `status` do, and
    /// the spawn is recorded in the ledger stall lines read.
    #[test]
    fn observed_output_and_status_answer_as_std_does_and_are_accounted() {
        let output =
            observed_output(Command::new("sh").args(["-c", "echo out; echo err >&2; exit 3"]))
                .unwrap();
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"out\n");
        assert_eq!(output.stderr, b"err\n");
        assert!(
            spawn_stats().longest_recent.is_some(),
            "the spawn is in the recent-spawn record"
        );

        let status = observed_status(Command::new("sh").args(["-c", "exit 0"])).unwrap();
        assert!(status.success());
        let refused = observed_output(&mut Command::new("/nonexistent/cairn-test-binary"));
        assert!(
            refused.is_err(),
            "a spawn that fails is an error, as with std"
        );
        assert_eq!(
            spawn_stats().in_flight,
            0,
            "nothing is left in flight once a spawn returns, failed or not"
        );
    }

    /// The child leads its own process group, so the timeout's `kill(-pid)`
    /// reaches everything it started, and a timed-out child is still reaped.
    #[test]
    fn the_child_leads_its_own_group_and_a_timeout_reaps_it() {
        let temp = tempfile::tempdir().unwrap();
        let pids = temp.path().join("pids");
        let script = format!(
            "/bin/sleep 30 & echo \"$$ $!\" > '{}.tmp' && mv '{0}.tmp' '{0}'; wait",
            pids.display()
        );
        let run = thread::spawn(move || {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", &script]);
            bounded_command_output(&mut command, Duration::from_secs(2), "probe")
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        let recorded = loop {
            if let Ok(recorded) = std::fs::read_to_string(&pids) {
                break recorded;
            }
            assert!(Instant::now() < deadline, "child never recorded its pids");
            thread::sleep(Duration::from_millis(10));
        };
        let mut recorded = recorded
            .split_whitespace()
            .map(|pid| pid.parse::<i32>().unwrap());
        let (shell, grandchild) = (recorded.next().unwrap(), recorded.next().unwrap());
        // The child leads a group of its own, and what it starts joins it.
        assert_eq!(unsafe { libc::getpgid(shell) }, shell);
        assert_eq!(unsafe { libc::getpgid(grandchild) }, shell);

        let error = run.join().unwrap().unwrap_err();
        assert!(error.contains("timed out"), "{error}");
        // The group kill reached the grandchild, not just the shell.
        let deadline = Instant::now() + Duration::from_secs(5);
        while unsafe { libc::kill(grandchild, 0) } == 0 {
            assert!(Instant::now() < deadline, "grandchild survived the timeout");
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn spawns_are_recorded_after_they_finish() {
        let mut command = Command::new("/usr/bin/true");
        observed_spawn(&mut command).unwrap().wait().unwrap();
        let stats = spawn_stats();
        assert!(stats.longest_recent.is_some());
    }
}
