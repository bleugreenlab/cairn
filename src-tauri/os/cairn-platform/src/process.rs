use std::io::Read;
use std::process::{Child, Command, Output, Stdio};
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
    let started = Instant::now();
    let id = SPAWNS.lock().ok().map(|mut ledger| {
        let id = ledger.next_id;
        ledger.next_id += 1;
        ledger.in_flight.push((id, started));
        id
    });
    let result = {
        let _region = crate::runtime_regions::enter("process.spawn");
        command.spawn()
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
            command.get_program().to_string_lossy(),
            elapsed.as_millis()
        );
    }
    result
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
) -> Result<Vec<u8>, String> {
    match receiver.recv_timeout(PIPE_READER_JOIN_TIMEOUT) {
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

/// Run a subprocess with drained output pipes and a hard deadline. The child is
/// placed in its own process group on Unix so timeout cleanup also reaches git,
/// ssh, credential helpers, and any other descendants spawned by jj.
pub fn bounded_command_output(
    command: &mut Command,
    timeout: Duration,
    ctx: &str,
) -> Result<Output, String> {
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

    let mut child = observed_spawn(command).map_err(|e| format!("{ctx}: {e}"))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (stdout_rx, stdout_reader) = spawn_pipe_reader(stdout);
    let (stderr_rx, stderr_reader) = spawn_pipe_reader(stderr);

    let deadline = Instant::now() + timeout;
    let (status, timed_out) = loop {
        match child.try_wait().map_err(|e| format!("{ctx}: {e}"))? {
            Some(status) => break (status, false),
            None if Instant::now() >= deadline => {
                #[cfg(unix)]
                unsafe {
                    libc::kill(-(child.id() as i32), libc::SIGKILL);
                }
                let _ = child.kill();
                let status = child.wait().map_err(|e| format!("{ctx}: {e}"))?;
                break (status, true);
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    };
    let stdout = finish_pipe_reader(stdout_rx, stdout_reader, ctx, "stdout")?;
    let stderr = finish_pipe_reader(stderr_rx, stderr_reader, ctx, "stderr")?;
    if timed_out {
        return Err(format!(
            "{ctx} timed out after {}s and was killed",
            timeout.as_secs_f64()
        ));
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

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
