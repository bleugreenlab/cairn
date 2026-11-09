//! Scheduling priority for the threads that orchestrate work, relative to the
//! work they place on their own host.
//!
//! A runner shares its machine with the builds and test binaries its colocated
//! executor runs. Those are lowered (nice 10, a utility QoS clamp and throttled
//! disk I/O on macOS, the idle-adjacent I/O class on Linux; see the executor's
//! child spawn), and the runner's own runtime threads are raised here, so under
//! a saturated host the scheduler keeps choosing the process every agent's tool
//! call is waiting on. On 10-16 the runner competed on equal terms with the
//! rustc processes it placed and lost for minutes at a time.
//!
//! Raising a thread's QoS needs no privilege on macOS. Linux has no
//! unprivileged way to raise a thread above the default, so there the lowering
//! of bulk work is the whole mechanism.
//!
//! The agent CLIs the runner launches (every Claude CLI and Codex app-server)
//! sit between the two: below the runner, above the builds. They spend most of
//! their lives waiting on their provider, so a lower CPU priority costs them
//! little, while one preempted runner stalls every agent's tool calls at once.
//! Only CPU priority is lowered for them, never I/O or a background QoS, which
//! would also throttle the disk and network their streaming and tool results
//! ride on.

use std::fmt;

use serde::Serialize;

/// macOS `QOS_CLASS_USER_INITIATED`: work a person is waiting on.
#[cfg(target_os = "macos")]
const QOS_CLASS_USER_INITIATED: u32 = 0x19;

#[cfg(target_os = "macos")]
extern "C" {
    fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: libc::c_int)
        -> libc::c_int;
    #[cfg(test)]
    fn qos_class_self() -> u32;
}

/// Run the calling thread ahead of bulk work on the same host: user-initiated
/// QoS on macOS, nothing elsewhere. Best effort; a refusal leaves the thread
/// at the default and is not worth failing a runtime over.
pub fn raise_current_thread_for_orchestration() {
    #[cfg(target_os = "macos")]
    // SAFETY: sets only the calling thread's own QoS; no pointers.
    unsafe {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INITIATED, 0);
    }
}

/// How many nice steps below the spawning process an agent CLI runs.
///
/// From a runner at nice 0 that is nice 5, halfway to the executor's build
/// cells at nice 10. Under Linux's CFS each step is a 1.25x weight, so five
/// steps give the runner about three times an agent's CPU share and an agent
/// about three times a cell's. On macOS each step lowers the base scheduling
/// priority by about one level (measured: 31 at nice 0, 26 at nice 5, 20 for a
/// cell at nice 10 under its utility clamp), with the runner's runtime threads
/// raised above all of them to user-initiated QoS.
///
/// Relative rather than absolute, so an agent is below its runner whatever
/// nice the runner itself was started at.
pub const AGENT_NICE_STEPS: i32 = 5;

/// The lowest Unix scheduling priority every supported kernel accepts.
const NICE_MAX: i32 = 19;

/// The nice an agent spawned by a process at `spawner` runs at.
pub fn agent_nice(spawner: i32) -> i32 {
    spawner
        .saturating_add(AGENT_NICE_STEPS)
        .min(NICE_MAX)
        .max(spawner)
}

/// The CPU priority a spawn starts its process at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpawnPriority {
    /// The spawning process's own, as any child gets it.
    #[default]
    Inherited,
    /// [`AGENT_NICE_STEPS`] below the spawner on Unix, the below-normal
    /// priority class on Windows. Every descendant inherits it: through fork
    /// and exec on Unix, and on Windows because a below-normal process's
    /// children default to its class.
    Agent,
}

/// A process's CPU scheduling priority, as the OS reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
pub enum CpuPriority {
    /// A Unix nice value: 0 is the default, higher runs later.
    Nice(i32),
    /// A Windows priority class.
    Class(PriorityClass),
}

/// The Windows process priority classes, lowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PriorityClass {
    Idle,
    BelowNormal,
    Normal,
    AboveNormal,
    High,
    Realtime,
}

impl fmt::Display for CpuPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CpuPriority::Nice(nice) => write!(f, "nice {nice}"),
            CpuPriority::Class(class) => f.write_str(match class {
                PriorityClass::Idle => "idle",
                PriorityClass::BelowNormal => "below normal",
                PriorityClass::Normal => "normal",
                PriorityClass::AboveNormal => "above normal",
                PriorityClass::High => "high",
                PriorityClass::Realtime => "realtime",
            }),
        }
    }
}

/// `pid`'s CPU priority now, or `None` when it cannot be read (the process is
/// gone, or not this user's).
pub fn of_process(pid: u32) -> Option<CpuPriority> {
    imp::of_process(pid)
}

/// This process's own CPU priority: on Unix, the calling thread's nice, which
/// is what a child spawned from it starts with.
pub fn of_current_process() -> Option<CpuPriority> {
    imp::of_current_process()
}

#[cfg(unix)]
pub(crate) use imp::apply_to_spawned_group;
#[cfg(windows)]
pub(crate) use imp::creation_flags;

#[cfg(unix)]
mod imp {
    use super::{agent_nice, CpuPriority, SpawnPriority};

    /// A `getpriority` result, told apart from its error: -1 is a legal nice.
    fn checked(read: impl FnOnce() -> libc::c_int) -> std::io::Result<i32> {
        nix::errno::Errno::clear();
        let value = read();
        if value == -1 && nix::errno::Errno::last_raw() != 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(value)
        }
    }

    fn own_nice() -> std::io::Result<i32> {
        // SAFETY: reads the calling thread's own priority; no pointers.
        checked(|| unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) })
    }

    pub(super) fn of_process(pid: u32) -> Option<CpuPriority> {
        // SAFETY: reads a priority; no pointers.
        checked(|| unsafe { libc::getpriority(libc::PRIO_PROCESS, pid as libc::id_t) })
            .ok()
            .map(CpuPriority::Nice)
    }

    pub(super) fn of_current_process() -> Option<CpuPriority> {
        own_nice().ok().map(CpuPriority::Nice)
    }

    /// Apply `priority` to the process group a spawn just started, which
    /// `pgid` leads.
    ///
    /// After the spawn rather than inside it, because a `pre_exec` hook would
    /// push the spawn onto `fork()` (see `docs/environment.md`, "Spawning
    /// without fork"). The whole group rather than the leader, so anything the
    /// child forked before this call is lowered with it, and on Linux, where a
    /// nice belongs to a thread, so is every thread of every member.
    /// Descendants started afterwards inherit it through fork and exec.
    ///
    /// Only ever lowers: a spawner already at the floor leaves its child where
    /// it is, since raising one needs privilege.
    pub(crate) fn apply_to_spawned_group(
        pgid: u32,
        priority: SpawnPriority,
    ) -> std::io::Result<()> {
        if priority == SpawnPriority::Inherited {
            return Ok(());
        }
        let own = own_nice()?;
        let target = agent_nice(own);
        if target == own {
            return Ok(());
        }
        // SAFETY: sets a priority on a process group this process just created;
        // no pointers.
        if unsafe { libc::setpriority(libc::PRIO_PGRP, pgid as libc::id_t, target) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(windows)]
mod imp {
    use super::{CpuPriority, PriorityClass, SpawnPriority};
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetCurrentProcessId, GetPriorityClass, OpenProcess,
        ABOVE_NORMAL_PRIORITY_CLASS, BELOW_NORMAL_PRIORITY_CLASS, HIGH_PRIORITY_CLASS,
        IDLE_PRIORITY_CLASS, NORMAL_PRIORITY_CLASS, PROCESS_QUERY_LIMITED_INFORMATION,
        REALTIME_PRIORITY_CLASS,
    };

    fn class(raw: u32) -> Option<PriorityClass> {
        Some(match raw {
            IDLE_PRIORITY_CLASS => PriorityClass::Idle,
            BELOW_NORMAL_PRIORITY_CLASS => PriorityClass::BelowNormal,
            NORMAL_PRIORITY_CLASS => PriorityClass::Normal,
            ABOVE_NORMAL_PRIORITY_CLASS => PriorityClass::AboveNormal,
            HIGH_PRIORITY_CLASS => PriorityClass::High,
            REALTIME_PRIORITY_CLASS => PriorityClass::Realtime,
            _ => return None,
        })
    }

    fn own_class() -> Option<PriorityClass> {
        // SAFETY: the pseudo-handle needs no closing and is always valid.
        class(unsafe { GetPriorityClass(GetCurrentProcess()) })
    }

    pub(super) fn of_process(pid: u32) -> Option<CpuPriority> {
        // SAFETY: GetCurrentProcessId takes nothing.
        if pid == unsafe { GetCurrentProcessId() } {
            return of_current_process();
        }
        // SAFETY: the handle is checked before use and closed exactly once.
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return None;
            }
            let raw = GetPriorityClass(handle);
            CloseHandle(handle);
            class(raw).map(CpuPriority::Class)
        }
    }

    pub(super) fn of_current_process() -> Option<CpuPriority> {
        own_class().map(CpuPriority::Class)
    }

    /// The priority-class creation flag a spawn at `priority` carries, or 0.
    /// Set at creation, so the child never runs a moment above it. A spawner
    /// already below normal passes its own class on by default, which is
    /// already below it.
    pub(crate) fn creation_flags(priority: SpawnPriority) -> u32 {
        match (priority, own_class()) {
            (SpawnPriority::Inherited, _) => 0,
            (SpawnPriority::Agent, Some(own)) if own <= PriorityClass::BelowNormal => 0,
            (SpawnPriority::Agent, _) => BELOW_NORMAL_PRIORITY_CLASS,
        }
    }
}

#[cfg(not(any(unix, windows)))]
mod imp {
    use super::CpuPriority;

    pub(super) fn of_process(_pid: u32) -> Option<CpuPriority> {
        None
    }

    pub(super) fn of_current_process() -> Option<CpuPriority> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(unix, windows))]
    use crate::services::process::{ProcessSpawner, RealProcessSpawner, SpawnConfig};
    #[cfg(any(unix, windows))]
    use std::io::BufRead;

    #[cfg(target_os = "macos")]
    #[test]
    fn an_orchestration_thread_runs_at_user_initiated_qos() {
        let qos = std::thread::spawn(|| {
            super::raise_current_thread_for_orchestration();
            // SAFETY: reads the calling thread's own QoS.
            unsafe { super::qos_class_self() }
        })
        .join()
        .unwrap();
        assert_eq!(qos, super::QOS_CLASS_USER_INITIATED);
    }

    #[test]
    fn an_agent_runs_five_steps_below_its_spawner_and_never_above_it() {
        assert_eq!(agent_nice(0), 5);
        assert_eq!(agent_nice(10), 15);
        assert_eq!(agent_nice(17), 19);
        assert_eq!(agent_nice(19), 19);
        assert_eq!(agent_nice(-5), 0);
    }

    #[test]
    fn a_priority_reads_as_its_os_names_it() {
        assert_eq!(CpuPriority::Nice(5).to_string(), "nice 5");
        assert_eq!(
            CpuPriority::Class(PriorityClass::BelowNormal).to_string(),
            "below normal"
        );
        assert_eq!(
            serde_json::to_value(CpuPriority::Nice(5)).unwrap(),
            serde_json::json!({"kind": "nice", "value": 5})
        );
    }

    /// A shell that waits for a line on stdin, then starts a `sleep` in the
    /// background and prints its pid: the grandchild is started only after
    /// the spawn has returned, so it shows what a CLI's own tool subprocess
    /// inherits.
    #[cfg(unix)]
    fn parent_of_a_sleeper() -> SpawnConfig {
        SpawnConfig::new("/bin/sh")
            .args(["-c", "read _; sleep 30 & echo $!; wait"])
            .stdin(true)
    }

    /// The pids of `child` and the grandchild it starts once told to.
    #[cfg(unix)]
    fn child_and_grandchild(
        child: &mut Box<dyn crate::services::process::ChildProcess>,
    ) -> (u32, u32) {
        use std::io::Write;
        let mut stdin = child.take_stdin().unwrap();
        writeln!(stdin).unwrap();
        let mut line = String::new();
        child.take_stdout().unwrap().read_line(&mut line).unwrap();
        (child.id(), line.trim().parse().expect("the sleeper's pid"))
    }

    /// An agent spawn, by either Unix spawn path (std's, and the direct
    /// `posix_spawn` a Claude CLI's descriptor credential takes), runs below
    /// this process, and so does what it starts; this process stays where it
    /// was.
    #[cfg(unix)]
    #[test]
    fn an_agent_spawn_and_its_descendants_run_below_the_spawner_on_unix() {
        use crate::services::process::InheritedSecret;
        let before = of_current_process().expect("own priority");
        let CpuPriority::Nice(own) = before else {
            panic!("a Unix priority is a nice value")
        };
        let expected = CpuPriority::Nice(agent_nice(own));
        for (path, config) in [
            ("std", parent_of_a_sleeper()),
            (
                "inherited secret",
                parent_of_a_sleeper().inherited_secret(InheritedSecret::new(3, "secret")),
            ),
        ] {
            let mut child = RealProcessSpawner
                .spawn(config.priority(SpawnPriority::Agent))
                .unwrap();
            let (pid, grandchild) = child_and_grandchild(&mut child);
            assert_eq!(of_process(pid), Some(expected), "{path}: the child");
            assert_eq!(
                of_process(grandchild),
                Some(expected),
                "{path}: what the child started"
            );
            let _ = child.kill();
        }
        assert_eq!(of_current_process(), Some(before), "the spawner itself");
    }

    #[cfg(unix)]
    #[test]
    fn an_ordinary_spawn_runs_at_the_spawners_priority_on_unix() {
        let own = of_current_process().unwrap();
        let mut child = RealProcessSpawner.spawn(parent_of_a_sleeper()).unwrap();
        let (pid, grandchild) = child_and_grandchild(&mut child);
        assert_eq!(of_process(pid), Some(own));
        assert_eq!(of_process(grandchild), Some(own));
        let _ = child.kill();
    }

    /// A below-normal process's children default to its class, so the
    /// PowerShell `cmd` starts reports the agent's class as its own.
    #[cfg(windows)]
    #[test]
    fn an_agent_spawn_and_its_descendants_run_below_the_spawner_on_windows() {
        let before = of_current_process().expect("own priority");
        let expected = match before {
            CpuPriority::Class(own) if own <= PriorityClass::BelowNormal => before,
            _ => CpuPriority::Class(PriorityClass::BelowNormal),
        };
        let mut child = RealProcessSpawner
            .spawn(
                SpawnConfig::new("cmd")
                    .args([
                        "/d",
                        "/c",
                        "powershell",
                        "-NoProfile",
                        "-NonInteractive",
                        "-Command",
                        "[System.Diagnostics.Process]::GetCurrentProcess().PriorityClass",
                    ])
                    .priority(SpawnPriority::Agent),
            )
            .unwrap();
        assert_eq!(of_process(child.id()), Some(expected), "the child");
        let mut line = String::new();
        child.take_stdout().unwrap().read_line(&mut line).unwrap();
        let reported = match expected {
            CpuPriority::Class(PriorityClass::Idle) => "Idle",
            _ => "BelowNormal",
        };
        assert_eq!(line.trim(), reported, "what the child started");
        assert_eq!(of_current_process(), Some(before), "the spawner itself");
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn a_run_to_completion_refuses_an_agent_priority() {
        let error = RealProcessSpawner
            .run(SpawnConfig::new("true").priority(SpawnPriority::Agent))
            .unwrap_err();
        assert!(error.contains("only by spawn"), "{error}");
    }
}
