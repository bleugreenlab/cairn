//! Confinement for an **isolated child**: a process that needs nothing from
//! the host but its stdio pipes, its own program, an allow-listed set of files
//! to read, and one scratch directory to write. The headless page renderer is
//! one: it runs agent-authored code whose output becomes model context, so the
//! boundary is what it may READ as much as what it may write or reach.
//!
//! Unlike the worktree fence ([`crate::SandboxPolicy`]), which allows reads
//! broadly and confines writes, this allows nothing it does not name:
//!
//! - **Reads** only beneath the allow-list: the program, the directories the
//!   caller names, the scratch dir, the system libraries a process needs to
//!   start, and a few device and time-zone files. Metadata (`stat`) stays
//!   readable everywhere on macOS, because path resolution needs it; contents
//!   and directory listings do not.
//! - **Writes** only beneath the scratch dir (and `/dev/null`).
//! - **No network and no local IPC.** macOS denies `network*` and every
//!   `mach-lookup`. Linux refuses `socket(2)` and `socketpair(2)` for every
//!   address family with a seccomp filter, which holds on any kernel, unlike
//!   Landlock's network rights (TCP only, ABI 4). It also refuses `io_uring`,
//!   whose `IORING_OP_SOCKET` would create a socket past the filter.
//! - **No new processes.** The program is an interpreter, so a path allow-list
//!   cannot stop it starting another copy of itself without its host's
//!   lockdown; the boundary is refusing process creation outright. macOS denies
//!   `process-fork` (which `posix_spawn` passes through) and `process-exec` of
//!   anything but the program. Linux's seccomp filter refuses `fork`, `vfork`
//!   and a `clone` without `CLONE_THREAD`, and answers `clone3` with ENOSYS so
//!   the C library creates threads with `clone`, whose flags the filter can
//!   read. It also refuses `setsid` and `setpgid`, so a process that re-execs
//!   itself stays in the group the caller kills.
//! - **Resource limits** on the child: CPU seconds, open files, and the largest
//!   file it may write. Address space is not limited: JavaScriptCore reserves
//!   far more virtual memory than it uses, so `RLIMIT_AS` would stop the
//!   runtime from starting; the caller bounds resident memory instead.
//!
//! On macOS the profile is one composed SBPL text closed by the operator
//! credential floor, never a profile nested inside another. Where no kernel
//! primitive exists (Windows) [`IsolatedChild::availability`] says so and the
//! caller must refuse rather than run unconfined.

use std::path::{Path, PathBuf};

#[cfg(any(test, target_os = "macos"))]
use crate::OperatorCredentialFloor;

/// What an isolated child may do, and the limits it runs under.
#[derive(Debug, Clone)]
pub struct IsolatedChild {
    /// The program the child execs: readable and executable.
    pub program: PathBuf,
    /// Directories (or files) the child may read beneath, besides the system
    /// libraries every process needs.
    pub readable: Vec<PathBuf>,
    /// The child's one writable directory: its cwd, `HOME` and `TMPDIR`.
    pub scratch: PathBuf,
    pub limits: IsolatedLimits,
    /// Carried by every deny rule of the macOS profile into the violation log,
    /// so what this child was refused can be read back and reported.
    pub violation_tag: crate::ViolationTag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsolatedLimits {
    /// CPU seconds before the kernel stops the child (`RLIMIT_CPU`).
    pub cpu_seconds: u64,
    /// Open file descriptors (`RLIMIT_NOFILE`).
    pub open_files: u64,
    /// The largest file the child may write, in bytes (`RLIMIT_FSIZE`).
    pub file_bytes: u64,
}

impl Default for IsolatedLimits {
    fn default() -> Self {
        Self {
            cpu_seconds: 30,
            open_files: 256,
            file_bytes: 64 * 1024 * 1024,
        }
    }
}

impl IsolatedChild {
    pub fn new(program: &Path, readable: Vec<PathBuf>, scratch: &Path) -> Self {
        Self {
            program: program.to_path_buf(),
            readable,
            scratch: scratch.to_path_buf(),
            limits: IsolatedLimits::default(),
            violation_tag: crate::ViolationTag::fresh(),
        }
    }

    /// Start watching for this host's sandbox denials, so a child spawned after
    /// this call can have its own read back by [`Self::denials_since`]. Blocks
    /// for up to two seconds the first time; a no-op off macOS.
    pub fn watch_violations(&self) {
        #[cfg(target_os = "macos")]
        crate::macos::watch_violations();
    }

    /// What the kernel refused this child since `since`, as `operation target`
    /// lines, waiting briefly for the first to arrive. Empty off macOS, and when
    /// the violation log cannot be streamed.
    pub fn denials_since(&self, since: std::time::SystemTime) -> Vec<String> {
        #[cfg(target_os = "macos")]
        {
            crate::macos::denials_since(
                &self.violation_tag,
                since,
                std::time::Duration::from_millis(1500),
            )
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = since;
            Vec::new()
        }
    }

    /// Whether this host can apply the confinement, or why not.
    pub fn availability() -> Result<(), String> {
        #[cfg(target_os = "macos")]
        {
            if crate::current_process_is_confined() {
                return Err(
                    "this process already runs inside a Seatbelt profile, and macOS cannot apply a second one to its child"
                        .to_string(),
                );
            }
            Ok(())
        }
        #[cfg(target_os = "linux")]
        {
            linux::availability()
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err(
                "this platform has no kernel primitive Cairn uses to confine a process's reads and network"
                    .to_string(),
            )
        }
    }

    /// The `(program, args)` to spawn: wrapped in `sandbox-exec` on macOS,
    /// unchanged elsewhere (Linux confines in [`Self::install_pre_exec`]).
    pub fn wrap_argv(&self, args: &[String]) -> (String, Vec<String>) {
        let program = self.program.to_string_lossy().into_owned();
        #[cfg(target_os = "macos")]
        {
            crate::macos::wrap_argv_with_profile(
                &program,
                args,
                self.sbpl_profile(&OperatorCredentialFloor::for_this_machine()),
            )
        }
        #[cfg(not(target_os = "macos"))]
        {
            (program, args.to_vec())
        }
    }

    /// Install the child-side half: resource limits everywhere, and on Linux
    /// the Landlock read/write allow-list and the seccomp filter. Each
    /// fails the spawn rather than running the child with less.
    #[cfg(unix)]
    pub fn install_pre_exec(&self, command: &mut std::process::Command) {
        use std::os::unix::process::CommandExt;
        let limits = self.limits;
        #[cfg(target_os = "linux")]
        let rules = linux::Rules::for_child(self);
        // SAFETY: runs in the forked child before exec. setrlimit, prctl and the
        // Landlock syscalls are async-signal-safe; the Landlock crate allocates,
        // exactly as the worktree fence's hook already does.
        unsafe {
            command.pre_exec(move || {
                apply_limits(limits)?;
                #[cfg(target_os = "linux")]
                rules.apply()?;
                Ok(())
            });
        }
    }

    /// Every path the child may read, in the order the rules list them.
    fn read_allow(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.program.clone(), self.scratch.clone()];
        paths.extend(self.readable.iter().cloned());
        paths.extend(system_read_paths());
        paths
    }

    /// The one composed profile: closed by default for reads, writes, network,
    /// IPC lookups, and exec; then the allow-lists; then the credential floor.
    #[cfg(any(test, target_os = "macos"))]
    pub(crate) fn sbpl_profile(&self, floor: &OperatorCredentialFloor) -> String {
        let mut out = String::from("(version 1)\n(allow default)\n");
        // Every deny carries this child's tag into the violation log.
        let report = format!("(with message \"{}\")", self.violation_tag.as_str());
        out.push_str(";; isolated child: no network, no IPC lookups, no new processes\n");
        out.push_str(&format!("(deny network* {report})\n"));
        out.push_str(&format!("(deny mach-lookup {report})\n"));
        out.push_str(&format!("(deny process-fork {report})\n"));
        out.push_str(&format!("(deny process-exec* {report})\n"));
        out.push_str(&format!(
            "(allow process-exec {})\n",
            literal(&self.program)
        ));
        out.push_str(
            ";; reads: nothing but the allow-list; metadata stays visible for path resolution\n",
        );
        out.push_str(&format!("(deny file-read* {report})\n"));
        out.push_str("(allow file-read-metadata)\n");
        let reads: Vec<String> = self.read_allow().iter().map(|path| subpath(path)).collect();
        out.push_str(&format!("(allow file-read* {})\n", reads.join(" ")));
        // The root directory node itself, none of what is beneath it: bun opens
        // `/` at startup and aborts (SIGABRT, silently) when Seatbelt refuses
        // it. Measured by `bun_starts_under_the_isolated_profile_live`: of every
        // read grant, this one alone lets it start. It shows the names of the
        // top-level directories and nothing in them.
        out.push_str("(allow file-read* (literal \"/\"))\n");
        out.push_str(";; writes: the scratch dir and /dev/null\n");
        out.push_str(&format!("(deny file-write* {report})\n"));
        out.push_str(&format!(
            "(allow file-write* {} (literal \"/dev/null\"))\n",
            subpath(&self.scratch)
        ));
        out.push_str(&floor.sbpl_rules());
        out
    }
}

/// The system paths a dynamically linked program reads to start: the loader,
/// its shared cache, the system libraries, and the time-zone database. Nothing
/// here holds user data.
fn system_read_paths() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    let paths: &[&str] = &[
        "/usr/lib",
        "/System/Library",
        "/System/Cryptexes",
        "/System/Volumes/Preboot/Cryptexes",
        "/Library/Apple/usr/lib",
        "/private/var/db/dyld",
        "/private/var/db/timezone",
        "/usr/share/zoneinfo",
        // The system ICU's locale data: without it `Intl` cannot format a number.
        "/usr/share/icu",
        "/private/etc/localtime",
        "/dev/null",
        "/dev/random",
        "/dev/urandom",
    ];
    #[cfg(not(target_os = "macos"))]
    let paths: &[&str] = &[
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/etc/ld.so.cache",
        "/etc/localtime",
        "/usr/share/zoneinfo",
        "/dev/null",
        "/dev/random",
        "/dev/urandom",
        // The runtime sizes its heap and thread pool from these.
        "/proc/meminfo",
        "/proc/cpuinfo",
        "/sys/devices/system/cpu",
        "/sys/fs/cgroup",
    ];
    paths.iter().map(PathBuf::from).collect()
}

#[cfg(any(test, target_os = "macos"))]
fn sbpl_string(path: &Path) -> String {
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    resolved
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

#[cfg(any(test, target_os = "macos"))]
fn subpath(path: &Path) -> String {
    format!("(subpath \"{}\")", sbpl_string(path))
}

#[cfg(any(test, target_os = "macos"))]
fn literal(path: &Path) -> String {
    format!("(literal \"{}\")", sbpl_string(path))
}

#[cfg(unix)]
fn apply_limits(limits: IsolatedLimits) -> std::io::Result<()> {
    let set = |resource, value: u64| -> std::io::Result<()> {
        let limit = libc::rlimit {
            rlim_cur: value as libc::rlim_t,
            rlim_max: value as libc::rlim_t,
        };
        // SAFETY: setrlimit reads one initialized struct.
        if unsafe { libc::setrlimit(resource, &limit) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    };
    set(libc::RLIMIT_CPU, limits.cpu_seconds)?;
    set(libc::RLIMIT_NOFILE, limits.open_files)?;
    set(libc::RLIMIT_FSIZE, limits.file_bytes)?;
    set(libc::RLIMIT_CORE, 0)?;
    Ok(())
}

#[cfg(target_os = "linux")]
mod linux {
    //! Landlock for the read/write allow-list; seccomp for sockets and process
    //! creation.

    use std::io;
    use std::path::PathBuf;

    use landlock::{
        Access, AccessFs, BitFlags, CompatLevel, Compatible, Ruleset, RulesetAttr,
        RulesetCreatedAttr, Scope, ABI,
    };

    use super::IsolatedChild;

    /// Reads are confined from ABI 1 and cross-directory refer from ABI 2;
    /// the isolated child requires both.
    const FLOOR_ABI: ABI = ABI::V2;
    const TARGET_ABI: ABI = ABI::V9;

    pub(super) fn availability() -> Result<(), String> {
        let landlock = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(FLOOR_ABI))
            .and_then(|ruleset| ruleset.create());
        if let Err(error) = landlock {
            return Err(format!(
                "this kernel cannot confine a process's reads with Landlock (ABI 2 or later is required): {error}"
            ));
        }
        // SAFETY: PR_GET_SECCOMP reads the calling thread's mode.
        if unsafe { libc::prctl(libc::PR_GET_SECCOMP, 0, 0, 0, 0) } < 0 {
            return Err("this kernel does not support seccomp filters".to_string());
        }
        Ok(())
    }

    pub(super) struct Rules {
        readable: Vec<PathBuf>,
        writable: Vec<PathBuf>,
    }

    impl Rules {
        pub(super) fn for_child(child: &IsolatedChild) -> Self {
            let mut readable = child.read_allow();
            // `/proc/self` resolves in the child, where this hook runs, so the
            // rule names the child's own process directory and no other.
            readable.push(PathBuf::from("/proc/self"));
            Self {
                readable,
                writable: vec![child.scratch.clone(), PathBuf::from("/dev/null")],
            }
        }

        pub(super) fn apply(&self) -> io::Result<()> {
            restrict(&self.readable, &self.writable)?;
            install_filter()
        }
    }

    fn read_grant() -> BitFlags<AccessFs> {
        AccessFs::from_read(TARGET_ABI)
    }

    fn write_grant() -> BitFlags<AccessFs> {
        // Everything but connecting to a pathname socket, which a scratch dir
        // never needs and the seccomp filter already makes moot.
        AccessFs::from_all(TARGET_ABI) & !AccessFs::ResolveUnix
    }

    fn restrict(readable: &[PathBuf], writable: &[PathBuf]) -> io::Result<()> {
        let ruleset = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(FLOOR_ABI))
            .map_err(to_io)?
            .set_compatibility(CompatLevel::BestEffort)
            .handle_access(AccessFs::from_all(TARGET_ABI))
            .map_err(to_io)?
            .scope(Scope::AbstractUnixSocket | Scope::Signal)
            .map_err(to_io)?
            .create()
            .map_err(to_io)?;
        ruleset
            .add_rules(landlock::path_beneath_rules(readable, read_grant()))
            .map_err(to_io)?
            .add_rules(landlock::path_beneath_rules(writable, write_grant()))
            .map_err(to_io)?
            // Also sets no_new_privs, which the seccomp filter below requires.
            .restrict_self()
            .map_err(to_io)?;
        Ok(())
    }

    fn to_io<E: std::fmt::Display>(error: E) -> io::Error {
        io::Error::other(format!("landlock: {error}"))
    }

    // Classic BPF, as `linux/filter.h` and `linux/seccomp.h` define it.
    const BPF_LD: u16 = 0x00;
    const BPF_W: u16 = 0x00;
    const BPF_ABS: u16 = 0x20;
    const BPF_JMP: u16 = 0x05;
    const BPF_JEQ: u16 = 0x10;
    const BPF_JSET: u16 = 0x40;
    const BPF_K: u16 = 0x00;
    const BPF_RET: u16 = 0x06;
    const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
    const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
    const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
    const SECCOMP_MODE_FILTER: libc::c_ulong = 2;
    /// `offsetof(struct seccomp_data, nr)`, `arch`, and the low word of
    /// `args[0]` (both supported architectures are little-endian).
    const DATA_NR: u32 = 0;
    const DATA_ARCH: u32 = 4;
    const DATA_ARG0_LOW: u32 = 16;
    /// `clone`'s flags are its first argument on x86_64 and aarch64.
    const CLONE_THREAD: u32 = libc::CLONE_THREAD as u32;

    #[cfg(target_arch = "x86_64")]
    const AUDIT_ARCH: u32 = 0xC000_003E;
    #[cfg(target_arch = "aarch64")]
    const AUDIT_ARCH: u32 = 0xC000_00B7;

    /// The x32 ABI's syscall numbers carry this bit; refusing it keeps them
    /// from reaching `socket` under a different number.
    #[cfg(target_arch = "x86_64")]
    const X32_SYSCALL_BIT: u32 = 0x4000_0000;

    fn statement(code: u16, k: u32) -> libc::sock_filter {
        libc::sock_filter {
            code,
            jt: 0,
            jf: 0,
            k,
        }
    }

    fn jump(code: u16, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
        libc::sock_filter { code, jt, jf, k }
    }

    /// The filter: a foreign architecture is killed; the x32 ABI, sockets,
    /// process creation and leaving the process group fail with EPERM;
    /// `io_uring` and `clone3` fail with ENOSYS, so a runtime that probes for
    /// them falls back to syscalls the filter can judge; a `clone` is allowed
    /// only when it makes a thread; everything else is allowed (Landlock and
    /// the rlimits confine it).
    pub(super) fn seccomp_filter() -> Vec<libc::sock_filter> {
        let errno = |code: i32| SECCOMP_RET_ERRNO | (code as u32 & 0xffff);
        let mut refused = vec![
            libc::SYS_socket,
            libc::SYS_socketpair,
            libc::SYS_setsid,
            libc::SYS_setpgid,
        ];
        #[cfg(target_arch = "x86_64")]
        refused.extend([libc::SYS_fork, libc::SYS_vfork]);
        let unsupported = [
            libc::SYS_io_uring_setup,
            libc::SYS_io_uring_enter,
            libc::SYS_io_uring_register,
            libc::SYS_clone3,
        ];
        let mut program = vec![
            statement(BPF_LD | BPF_W | BPF_ABS, DATA_ARCH),
            jump(BPF_JMP | BPF_JEQ | BPF_K, AUDIT_ARCH, 1, 0),
            statement(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
            statement(BPF_LD | BPF_W | BPF_ABS, DATA_NR),
        ];
        #[cfg(target_arch = "x86_64")]
        {
            program.push(jump(BPF_JMP | BPF_JSET | BPF_K, X32_SYSCALL_BIT, 0, 1));
            program.push(statement(BPF_RET | BPF_K, errno(libc::EPERM)));
        }
        for nr in refused {
            program.push(jump(BPF_JMP | BPF_JEQ | BPF_K, nr as u32, 0, 1));
            program.push(statement(BPF_RET | BPF_K, errno(libc::EPERM)));
        }
        for nr in unsupported {
            program.push(jump(BPF_JMP | BPF_JEQ | BPF_K, nr as u32, 0, 1));
            program.push(statement(BPF_RET | BPF_K, errno(libc::ENOSYS)));
        }
        // clone: a thread (CLONE_THREAD set) is allowed, a process refused.
        program.push(jump(
            BPF_JMP | BPF_JEQ | BPF_K,
            libc::SYS_clone as u32,
            0,
            4,
        ));
        program.push(statement(BPF_LD | BPF_W | BPF_ABS, DATA_ARG0_LOW));
        program.push(jump(BPF_JMP | BPF_JSET | BPF_K, CLONE_THREAD, 0, 1));
        program.push(statement(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));
        program.push(statement(BPF_RET | BPF_K, errno(libc::EPERM)));
        program.push(statement(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));
        program
    }

    fn install_filter() -> io::Result<()> {
        let mut filter = seccomp_filter();
        let program = libc::sock_fprog {
            len: filter.len() as libc::c_ushort,
            filter: filter.as_mut_ptr(),
        };
        // SAFETY: no_new_privs is set (Landlock's restrict_self set it, and it
        // is set again here in case that ever changes); the program pointer is
        // valid for the duration of the call, which copies it.
        unsafe {
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::prctl(
                libc::PR_SET_SECCOMP,
                SECCOMP_MODE_FILTER,
                &program as *const libc::sock_fprog,
            ) != 0
            {
                return Err(io::Error::other(format!(
                    "seccomp: {}",
                    io::Error::last_os_error()
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child() -> IsolatedChild {
        IsolatedChild::new(
            Path::new("/opt/cairn/bun"),
            vec![PathBuf::from("/opt/cairn/page-render")],
            Path::new("/tmp/cairn-render-x"),
        )
    }

    #[test]
    fn the_profile_closes_reads_network_ipc_and_exec_then_ends_with_the_floor() {
        let floor = OperatorCredentialFloor::new(Some(PathBuf::from("/Users/op")), []);
        let child = child();
        let profile = child.sbpl_profile(&floor);
        let report = format!("(with message \"{}\")", child.violation_tag.as_str());
        for rule in [
            "network*",
            "mach-lookup",
            "process-fork",
            "process-exec*",
            "file-read*",
            "file-write*",
        ] {
            // Each deny reports this child's tag, so its denials can be read back.
            let deny = format!("(deny {rule} {report})");
            assert!(profile.contains(&deny), "{deny} missing from:\n{profile}");
        }
        assert!(profile.contains("(allow process-exec (literal \"/opt/cairn/bun\"))"));
        assert!(profile.contains("(subpath \"/opt/cairn/page-render\")"));
        // Reading /System wholesale would reach the data volume's firmlink.
        assert!(!profile.contains("(subpath \"/System\")"), "{profile}");
        assert!(!profile.contains("(subpath \"/\")"), "{profile}");
        // The root directory node alone, which bun needs to start.
        assert!(
            profile.contains("(allow file-read* (literal \"/\"))"),
            "{profile}"
        );
        // The credential floor closes the one composed profile: nothing after it reopens it.
        let floor_at = profile
            .find(";; operator credential boundary")
            .expect("the floor is part of the profile");
        assert!(profile[floor_at..]
            .lines()
            .all(|line| !line.starts_with("(allow")));
        assert_eq!(profile.matches("(version 1)").count(), 1);
    }

    /// Whether this process can apply a Seatbelt profile to a child; a process
    /// already inside one cannot, the declared `nested-sandbox-exec` gap.
    #[cfg(target_os = "macos")]
    fn skip_inside_seatbelt(test: &str) -> bool {
        match IsolatedChild::availability() {
            Ok(()) => false,
            Err(why) => {
                eprintln!("skipping {test}: {why}");
                cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
                true
            }
        }
    }

    /// The pinned bun the app ships, staged by `ensure-sidecar` beside the crates.
    #[cfg(target_os = "macos")]
    fn staged_bun() -> PathBuf {
        let binaries = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../binaries");
        let bun = std::fs::read_dir(&binaries)
            .unwrap_or_else(|error| panic!("{}: {error}", binaries.display()))
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    // `bun-<triple>` itself, not its `.pin` record beside it.
                    .is_some_and(|name| name.starts_with("bun-") && !name.contains('.'))
            })
            .unwrap_or_else(|| panic!("no staged bun in {}", binaries.display()));
        bun.canonicalize().unwrap()
    }

    /// Live: bun starts, and formats a number and a date, under exactly the
    /// confinement a rendered page gets. When
    /// it does not, the failure bisects the profile in the same run (without
    /// the rlimits, without each deny rule in turn, without the credential
    /// floor), so one unconfined lane names the rule that stops it.
    #[cfg(target_os = "macos")]
    #[test]
    fn bun_starts_under_the_isolated_profile_live() {
        use std::process::Command;
        if skip_inside_seatbelt("bun_starts_under_the_isolated_profile_live") {
            return;
        }
        let bun = staged_bun();
        let scratch = tempfile::tempdir().unwrap();
        let scratch = scratch.path().canonicalize().unwrap();
        let isolated = IsolatedChild::new(&bun, Vec::new(), &scratch);
        let profile = isolated.sbpl_profile(&OperatorCredentialFloor::for_this_machine());
        let run = |profile: &str, limits: bool| -> String {
            let (program, args) = crate::macos::wrap_argv_with_profile(
                bun.to_str().unwrap(),
                // `Intl` reads the system ICU's locale data, which a page that
                // formats a number or a date needs as much as bun needs to start.
                &[
                    "-e".to_string(),
                    "console.log('alive ' + new Intl.NumberFormat('en-US').format(1234.5) + ' ' + new Date(0).toLocaleString('en-US'))".to_string(),
                ],
                profile.to_string(),
            );
            let mut command = Command::new(program);
            command
                .args(args)
                .env_clear()
                .env("HOME", &scratch)
                .env("TMPDIR", &scratch)
                .current_dir(&scratch);
            if limits {
                isolated.install_pre_exec(&mut command);
            }
            match command.output() {
                Ok(out) if String::from_utf8_lossy(&out.stdout).contains("alive") => {
                    "starts".to_string()
                }
                Ok(out) => format!(
                    "{} {}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                        .chars()
                        .take(300)
                        .collect::<String>()
                        .trim()
                ),
                Err(error) => format!("did not spawn: {error}"),
            }
        };
        let production = run(&profile, true);
        if production == "starts" {
            return;
        }
        let without = |rule: &str| -> String {
            profile
                .lines()
                .filter(|line| !line.starts_with(rule))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let mut report = vec![
            format!("production (profile and rlimits): {production}"),
            format!("profile without rlimits: {}", run(&profile, false)),
        ];
        for rule in [
            "(deny mach-lookup",
            "(deny file-read*",
            "(deny process-fork",
            "(deny process-exec*",
            "(deny network*",
            "(deny file-write*",
        ] {
            report.push(format!("without `{rule}`: {}", run(&without(rule), false)));
        }
        let floor_at = profile
            .find(";; operator credential boundary")
            .unwrap_or(profile.len());
        report.push(format!(
            "without the credential floor: {}",
            run(&profile[..floor_at], false)
        ));
        report.push(format!(
            "with `(allow default)` only: {}",
            run("(version 1)\n(allow default)\n", false)
        ));
        // The read allow-list misses something bun needs: find the smallest
        // subtree whose read, added to the profile, lets it start, by walking
        // down from `/` one directory at a time.
        let with_grant = |grant: String| -> String {
            match profile.find(";; operator credential boundary") {
                Some(at) => format!("{}{grant}{}", &profile[..at], &profile[at..]),
                None => format!("{profile}{grant}"),
            }
        };
        let granting = |path: &Path| with_grant(format!("(allow file-read* {})\n", subpath(path)));
        // Directories themselves, not what is in them: a `literal` matches the
        // directory node only.
        let listing = |paths: &[PathBuf]| {
            let literals: Vec<String> = paths.iter().map(|path| literal(path)).collect();
            with_grant(format!("(allow file-read* {})\n", literals.join(" ")))
        };
        let cwd_ancestors: Vec<PathBuf> =
            scratch.ancestors().skip(1).map(Path::to_path_buf).collect();
        report.push(format!(
            "reading `/` itself: {}",
            run(&listing(&[PathBuf::from("/")]), false)
        ));
        report.push(format!(
            "reading the working directory's ancestors themselves ({}): {}",
            cwd_ancestors.len(),
            run(&listing(&cwd_ancestors), false)
        ));
        let mut needed = PathBuf::from("/");
        if run(&granting(&needed), false) == "starts" {
            for _ in 0..12 {
                let mut children: Vec<PathBuf> = match std::fs::read_dir(&needed) {
                    Ok(entries) => entries
                        .filter_map(Result::ok)
                        .map(|entry| entry.path())
                        .collect(),
                    Err(_) => break,
                };
                children.sort();
                match children
                    .into_iter()
                    .find(|child| run(&granting(child), false) == "starts")
                {
                    Some(child) => needed = child,
                    None => break,
                }
            }
            report.push(format!(
                "the smallest read grant that lets bun start: {}",
                needed.display()
            ));
        } else {
            report.push("no read grant alone lets bun start".to_string());
        }
        panic!(
            "bun does not start under the isolated profile:\n{}",
            report.join("\n")
        );
    }

    #[test]
    fn the_default_limits_bound_cpu_files_and_writes() {
        let limits = IsolatedLimits::default();
        assert!(limits.cpu_seconds > 0 && limits.cpu_seconds <= 60);
        assert!(limits.open_files >= 64);
        assert!(limits.file_bytes > 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_filter_refuses_sockets_processes_and_io_uring_and_kills_a_foreign_arch() {
        let program = linux::seccomp_filter();
        let has = |k: u32| program.iter().any(|insn| insn.k == k);
        for nr in [
            libc::SYS_socket,
            libc::SYS_socketpair,
            libc::SYS_io_uring_setup,
            libc::SYS_clone,
            libc::SYS_clone3,
            libc::SYS_setsid,
            libc::SYS_setpgid,
        ] {
            assert!(has(nr as u32), "syscall {nr} is not judged");
        }
        #[cfg(target_arch = "x86_64")]
        assert!(has(libc::SYS_fork as u32) && has(libc::SYS_vfork as u32));
        assert!(has(libc::CLONE_THREAD as u32), "clone's flags are read");
        assert_eq!(program[2].k, 0x8000_0000, "foreign arch is killed");
        assert_eq!(
            program.last().unwrap().k,
            0x7fff_0000,
            "the default is allow"
        );
    }

    /// Live: under the confinement a child cannot open a socket of any family,
    /// cannot fork or leave its process group, cannot read outside the
    /// allow-list, and can read inside it.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_confined_child_has_no_sockets_and_reads_only_its_allow_list_live() {
        use std::io::Read;
        use std::os::unix::process::CommandExt;

        if let Err(why) = IsolatedChild::availability() {
            eprintln!("skipping: {why}");
            cairn_common::skip_ledger::record_skip(
                "a_confined_child_has_no_sockets_and_reads_only_its_allow_list_live",
                "isolated-child-unavailable",
            );
            return;
        }
        let scratch = tempfile::tempdir().unwrap();
        let allowed = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(allowed.path().join("ok"), "inside").unwrap();
        std::fs::write(outside.path().join("canary"), "CANARY-5514").unwrap();
        let isolated = IsolatedChild::new(
            Path::new("/bin/true"),
            vec![allowed.path().to_path_buf()],
            scratch.path(),
        );
        let (mut reader, writer) = std::os::unix::net::UnixStream::pair().unwrap();
        let writer_fd = std::os::fd::AsRawFd::as_raw_fd(&writer);
        let canary =
            std::ffi::CString::new(outside.path().join("canary").to_str().unwrap()).unwrap();
        let ok = std::ffi::CString::new(allowed.path().join("ok").to_str().unwrap()).unwrap();
        let mut command = std::process::Command::new("/bin/true");
        isolated.install_pre_exec(&mut command);
        // SAFETY: raw syscalls only, after the confinement hook ran.
        unsafe {
            command.pre_exec(move || {
                let inet = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
                let unix = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
                let mut pair = [0; 2];
                let paired =
                    libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, pair.as_mut_ptr());
                let canary_fd = libc::open(canary.as_ptr(), libc::O_RDONLY);
                let ok_fd = libc::open(ok.as_ptr(), libc::O_RDONLY);
                // The raw syscalls, not libc's wrappers, which may route fork
                // through clone or clone3 depending on the C library.
                let forked = libc::syscall(
                    libc::SYS_clone,
                    libc::SIGCHLD as libc::c_long,
                    0 as libc::c_long,
                );
                if forked == 0 {
                    libc::_exit(0);
                }
                let left_group = libc::syscall(libc::SYS_setsid);
                let report = [
                    (inet < 0) as u8,
                    (unix < 0) as u8,
                    (paired < 0) as u8,
                    (canary_fd < 0) as u8,
                    (ok_fd >= 0) as u8,
                    (forked < 0) as u8,
                    (left_group < 0) as u8,
                ];
                libc::write(writer_fd, report.as_ptr().cast(), report.len());
                Ok(())
            });
        }
        let status = command.status().expect("the confined child spawns");
        assert!(status.success());
        drop(writer);
        let mut report = Vec::new();
        reader.read_to_end(&mut report).unwrap();
        assert_eq!(
            report,
            vec![1, 1, 1, 1, 1, 1, 1],
            "[inet refused, unix refused, socketpair refused, canary unreadable, allow-list readable, fork refused, setsid refused]"
        );
    }
}
