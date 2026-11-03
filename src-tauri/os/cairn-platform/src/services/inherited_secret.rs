//! Handing a spawned process a secret on an inherited file descriptor.
//!
//! A process's launch environment and argument vector are readable by every
//! process of the same user (`KERN_PROCARGS2` on macOS, `/proc/<pid>/environ`
//! on Linux), for as long as the process lives. A secret placed there is
//! everyone's. An [`InheritedSecret`] goes instead into a pipe: the whole value
//! is written and the write end closed before the child exists, and the child
//! finds the read end at [`InheritedSecret::child_fd`], reads the value, then
//! end-of-stream. Whatever later inherits that descriptor from the child reads
//! only end-of-stream.
//!
//! Both ends of the pipe are close-on-exec in this process from the moment it
//! is created, so a spawn on another thread can never inherit them. Only the
//! child gets a copy, and only because the spawn's own file actions
//! `dup2` the read end onto `child_fd`, which clears close-on-exec in the child
//! alone.
//!
//! The spawn is a direct `posix_spawn` rather than std's `Command`: std has no
//! stable way to map a descriptor into a child, and a `pre_exec` closure would
//! push the spawn onto `fork()`, which in a long-lived process stalls every
//! allocating thread (see `docs/environment.md`, "Spawning without fork"). The
//! program, arguments, environment, and working directory are still composed by
//! `build_command`, so this path launches exactly what the std path would.

use std::fmt;

/// A secret a spawned process reads once from an inherited file descriptor.
///
/// `Debug` names the descriptor and never the value.
#[derive(Clone, PartialEq, Eq)]
pub struct InheritedSecret {
    child_fd: i32,
    bytes: Vec<u8>,
}

impl InheritedSecret {
    /// The largest secret delivered. The value is written into the pipe in full
    /// before the child exists, so it must fit a pipe's buffer, which on every
    /// supported platform holds at least a page.
    pub const MAX_LEN: usize = 4096;

    /// `bytes`, to be found by the child on descriptor `child_fd`.
    pub fn new(child_fd: i32, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            child_fd,
            bytes: bytes.into(),
        }
    }

    /// The descriptor number the child reads the secret from.
    pub fn child_fd(&self) -> i32 {
        self.child_fd
    }

    /// Whether this secret is `value`. For tests that prove a launch carries
    /// the secret it should, without exposing it.
    pub fn is(&self, value: &[u8]) -> bool {
        self.bytes == value
    }

    /// Why this secret cannot be delivered, if it cannot.
    fn refusal(&self) -> Option<String> {
        if self.child_fd <= 2 {
            Some(format!(
                "an inherited secret cannot take descriptor {}, which is stdio",
                self.child_fd
            ))
        } else if self.bytes.len() > Self::MAX_LEN {
            Some(format!(
                "an inherited secret is at most {} bytes",
                Self::MAX_LEN
            ))
        } else {
            None
        }
    }
}

impl fmt::Debug for InheritedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InheritedSecret")
            .field("child_fd", &self.child_fd)
            .field("bytes", &"<redacted>")
            .finish()
    }
}

#[cfg(unix)]
pub(super) use unix::spawn;

/// No descriptor delivery exists here, so a spawn that asks for one is refused
/// rather than handed the secret some other way.
#[cfg(not(unix))]
pub(super) fn spawn(
    _config: &super::process::SpawnConfig,
    secret: &InheritedSecret,
) -> Result<Box<dyn super::process::ChildProcess>, String> {
    Err(secret
        .refusal()
        .unwrap_or_else(|| "an inherited secret has no delivery on this platform".to_string()))
}

#[cfg(unix)]
mod unix {
    use std::ffi::{CString, OsStr, OsString};
    use std::io::{BufRead, BufReader, PipeReader, PipeWriter, Write};
    use std::mem::MaybeUninit;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;

    use cairn_sandbox::Confinement;

    use super::InheritedSecret;
    use crate::services::process::{build_command, ChildProcess, SpawnConfig};

    /// Spawn `config` with `secret` on its descriptor, as
    /// `RealProcessSpawner::spawn` would spawn it otherwise: in its own
    /// process group, with the stdio it asks to capture piped.
    pub(in crate::services) fn spawn(
        config: &SpawnConfig,
        secret: &InheritedSecret,
    ) -> Result<Box<dyn ChildProcess>, String> {
        if let Some(refusal) = secret.refusal() {
            return Err(refusal);
        }
        // A confined spawn on Linux is confined by a `pre_exec` hook, which a
        // direct `posix_spawn` cannot run, and no confined spawn needs one.
        if !matches!(config.confinement, Confinement::Unconfined) {
            return Err("an inherited secret is delivered only to an unconfined spawn".to_string());
        }
        let command = build_command(config);
        let program = command.get_program();
        if !std::path::Path::new(program).is_absolute() {
            return Err(format!(
                "Failed to spawn process: {} was not found on the PATH it was given",
                program.to_string_lossy()
            ));
        }
        let path = c_string(program.as_bytes())?;
        let argv: Vec<CString> = std::iter::once(OsStr::new(&config.program))
            .chain(command.get_args())
            .map(|arg| c_string(arg.as_bytes()))
            .collect::<Result<_, _>>()?;
        let envp = child_environment(&command)?;
        let cwd = command
            .get_current_dir()
            .map(|dir| c_string(dir.as_os_str().as_bytes()))
            .transpose()?;

        let io = |error: std::io::Error| format!("Failed to spawn process: {error}");
        // Every descriptor handed to the child is moved clear of the numbers
        // the file actions write to, so no `dup2` can overwrite a source a
        // later one still reads.
        let floor = secret.child_fd + 1;
        let secret_fd = {
            let (reader, mut writer) = std::io::pipe().map_err(io)?;
            writer.write_all(&secret.bytes).map_err(io)?;
            drop(writer);
            above(OwnedFd::from(reader), floor).map_err(io)?
        };
        let mut stdio: [Option<(OwnedFd, Parent)>; 3] = [None, None, None];
        for (slot, captured) in [
            config.capture_stdin,
            config.capture_stdout,
            config.capture_stderr,
        ]
        .into_iter()
        .enumerate()
        {
            if !captured {
                continue;
            }
            let (reader, writer) = std::io::pipe().map_err(io)?;
            stdio[slot] = Some(if slot == 0 {
                (
                    above(OwnedFd::from(reader), floor).map_err(io)?,
                    Parent::Writer(writer),
                )
            } else {
                (
                    above(OwnedFd::from(writer), floor).map_err(io)?,
                    Parent::Reader(reader),
                )
            });
        }

        let mut actions = FileActions::new().map_err(io)?;
        for (target, entry) in stdio.iter().enumerate() {
            if let Some((child_end, _)) = entry {
                actions
                    .dup2(child_end.as_raw_fd(), target as RawFd)
                    .map_err(io)?;
            }
        }
        actions
            .dup2(secret_fd.as_raw_fd(), secret.child_fd)
            .map_err(io)?;
        if let Some(cwd) = &cwd {
            actions.chdir(cwd).map_err(io)?;
        }
        let attrs = Attributes::new().map_err(io)?;

        let mut argv_ptrs: Vec<*mut libc::c_char> =
            argv.iter().map(|arg| arg.as_ptr().cast_mut()).collect();
        argv_ptrs.push(std::ptr::null_mut());
        let mut envp_ptrs: Vec<*mut libc::c_char> =
            envp.iter().map(|var| var.as_ptr().cast_mut()).collect();
        envp_ptrs.push(std::ptr::null_mut());

        let pid = crate::process::observed(program, || {
            let mut pid: libc::pid_t = 0;
            // SAFETY: every pointer is valid for the call: `path`, `argv`, and
            // `envp` own NUL-terminated strings and the pointer arrays end in
            // null; `actions` and `attrs` are initialised and outlive the call.
            let rc = unsafe {
                libc::posix_spawn(
                    &mut pid,
                    path.as_ptr(),
                    actions.as_ptr(),
                    attrs.as_ptr(),
                    argv_ptrs.as_ptr(),
                    envp_ptrs.as_ptr(),
                )
            };
            if rc == 0 {
                Ok(pid)
            } else {
                Err(std::io::Error::from_raw_os_error(rc))
            }
        })
        .map_err(io)?;
        // The child holds its copies now; this process keeps only its own ends.
        drop(secret_fd);
        let [stdin, stdout, stderr] = stdio.map(|entry| entry.map(|(_, parent)| parent));
        Ok(Box::new(SpawnedChild {
            pid,
            status: None,
            stdin: stdin.and_then(Parent::into_writer),
            stdout: stdout.and_then(Parent::into_reader),
            stderr: stderr.and_then(Parent::into_reader),
        }))
    }

    /// This process's end of one of the child's stdio pipes.
    enum Parent {
        Reader(PipeReader),
        Writer(PipeWriter),
    }

    impl Parent {
        fn into_reader(self) -> Option<PipeReader> {
            match self {
                Parent::Reader(reader) => Some(reader),
                Parent::Writer(_) => None,
            }
        }

        fn into_writer(self) -> Option<PipeWriter> {
            match self {
                Parent::Writer(writer) => Some(writer),
                Parent::Reader(_) => None,
            }
        }
    }

    fn c_string(bytes: &[u8]) -> Result<CString, String> {
        CString::new(bytes)
            .map_err(|_| "Failed to spawn process: an argument holds a NUL byte".to_string())
    }

    /// The environment `command` would give its child: this process's own,
    /// with the command's overlay applied and its removals taken out.
    fn child_environment(command: &std::process::Command) -> Result<Vec<CString>, String> {
        let mut env: std::collections::BTreeMap<OsString, OsString> = std::env::vars_os().collect();
        for (name, value) in command.get_envs() {
            match value {
                Some(value) => env.insert(name.to_owned(), value.to_owned()),
                None => env.remove(name),
            };
        }
        env.into_iter()
            .map(|(name, value)| {
                let mut var = name.into_vec();
                var.push(b'=');
                var.extend_from_slice(value.as_bytes());
                c_string(&var)
            })
            .collect()
    }

    /// `fd`, renumbered to at least `floor` if it is below, still close-on-exec.
    fn above(fd: OwnedFd, floor: RawFd) -> std::io::Result<OwnedFd> {
        if fd.as_raw_fd() >= floor {
            return Ok(fd);
        }
        // SAFETY: `fd` is open; `F_DUPFD_CLOEXEC` returns a new descriptor this
        // function then owns.
        let raised = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, floor) };
        if raised < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `raised` is a fresh descriptor nothing else owns.
        Ok(unsafe { OwnedFd::from_raw_fd(raised) })
    }

    #[cfg(not(target_vendor = "apple"))]
    use libc::posix_spawn_file_actions_addchdir_np as addchdir_np;

    // The libc crate Cairn locks binds this only on Linux; macOS has had it
    // since 10.15, Cairn's deployment target.
    #[cfg(target_vendor = "apple")]
    extern "C" {
        #[link_name = "posix_spawn_file_actions_addchdir_np"]
        fn addchdir_np(
            actions: *mut libc::posix_spawn_file_actions_t,
            path: *const libc::c_char,
        ) -> libc::c_int;
    }

    fn check(rc: libc::c_int) -> std::io::Result<()> {
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(rc))
        }
    }

    struct FileActions(Box<MaybeUninit<libc::posix_spawn_file_actions_t>>);

    impl FileActions {
        fn new() -> std::io::Result<Self> {
            let mut actions = Box::new(MaybeUninit::uninit());
            // SAFETY: initialises the boxed storage, which `Drop` destroys.
            check(unsafe { libc::posix_spawn_file_actions_init(actions.as_mut_ptr()) })?;
            Ok(Self(actions))
        }

        fn dup2(&mut self, from: RawFd, to: RawFd) -> std::io::Result<()> {
            // SAFETY: `self.0` was initialised by `new`.
            check(unsafe { libc::posix_spawn_file_actions_adddup2(self.0.as_mut_ptr(), from, to) })
        }

        fn chdir(&mut self, dir: &CString) -> std::io::Result<()> {
            // SAFETY: `self.0` was initialised by `new`; `dir` is NUL-terminated
            // and the action copies it.
            check(unsafe { addchdir_np(self.0.as_mut_ptr(), dir.as_ptr()) })
        }

        fn as_ptr(&self) -> *const libc::posix_spawn_file_actions_t {
            self.0.as_ptr()
        }
    }

    impl Drop for FileActions {
        fn drop(&mut self) {
            // SAFETY: initialised by `new`, destroyed exactly once.
            unsafe { libc::posix_spawn_file_actions_destroy(self.0.as_mut_ptr()) };
        }
    }

    /// The attributes std's own `posix_spawn` gives a spawn in its own process
    /// group: that group, and `SIGPIPE` back at its default, since this
    /// process ignores it.
    struct Attributes(Box<MaybeUninit<libc::posix_spawnattr_t>>);

    impl Attributes {
        fn new() -> std::io::Result<Self> {
            let mut attrs = Box::new(MaybeUninit::uninit());
            // SAFETY: initialises the boxed storage, which `Drop` destroys.
            check(unsafe { libc::posix_spawnattr_init(attrs.as_mut_ptr()) })?;
            let mut attrs = Self(attrs);
            // SAFETY: `attrs` is initialised; `default_set` is initialised by
            // `sigemptyset` before it is read.
            unsafe {
                check(libc::posix_spawnattr_setpgroup(attrs.0.as_mut_ptr(), 0))?;
                let mut default_set = MaybeUninit::<libc::sigset_t>::uninit();
                libc::sigemptyset(default_set.as_mut_ptr());
                libc::sigaddset(default_set.as_mut_ptr(), libc::SIGPIPE);
                check(libc::posix_spawnattr_setsigdefault(
                    attrs.0.as_mut_ptr(),
                    default_set.as_ptr(),
                ))?;
                let flags = libc::POSIX_SPAWN_SETPGROUP | libc::POSIX_SPAWN_SETSIGDEF;
                check(libc::posix_spawnattr_setflags(
                    attrs.0.as_mut_ptr(),
                    flags as _,
                ))?;
            }
            Ok(attrs)
        }

        fn as_ptr(&self) -> *const libc::posix_spawnattr_t {
            self.0.as_ptr()
        }
    }

    impl Drop for Attributes {
        fn drop(&mut self) {
            // SAFETY: initialised by `new`, destroyed exactly once.
            unsafe { libc::posix_spawnattr_destroy(self.0.as_mut_ptr()) };
        }
    }

    /// A child started by [`spawn`], driven as `RealChildProcess` drives a std
    /// child.
    struct SpawnedChild {
        pid: libc::pid_t,
        /// Set once the child has been reaped, after which its pid may belong
        /// to some other process and is never signalled.
        status: Option<ExitStatus>,
        stdin: Option<PipeWriter>,
        stdout: Option<PipeReader>,
        stderr: Option<PipeReader>,
    }

    impl ChildProcess for SpawnedChild {
        fn id(&self) -> u32 {
            self.pid as u32
        }

        fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
            self.stdout
                .take()
                .map(|out| Box::new(BufReader::new(out)) as Box<dyn BufRead + Send>)
        }

        fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
            self.stderr
                .take()
                .map(|err| Box::new(BufReader::new(err)) as Box<dyn BufRead + Send>)
        }

        fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
            self.stdin
                .take()
                .map(|stdin| Box::new(stdin) as Box<dyn Write + Send>)
        }

        fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
            if let Some(status) = self.status {
                return Ok(Some(status));
            }
            let mut raw = 0;
            loop {
                // SAFETY: `raw` is a valid out-pointer.
                let reaped = unsafe { libc::waitpid(self.pid, &mut raw, libc::WNOHANG) };
                if reaped == 0 {
                    return Ok(None);
                }
                if reaped == self.pid {
                    let status = ExitStatus::from_raw(raw);
                    self.status = Some(status);
                    return Ok(Some(status));
                }
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }

        fn kill(&mut self) -> std::io::Result<()> {
            if self.try_wait()?.is_some() {
                return Ok(());
            }
            let group = nix::unistd::Pid::from_raw(self.pid);
            match nix::sys::signal::killpg(group, nix::sys::signal::Signal::SIGKILL) {
                Ok(()) => Ok(()),
                Err(error) => {
                    log::debug!("failed to kill process group {}: {error}", self.pid);
                    nix::sys::signal::kill(group, nix::sys::signal::Signal::SIGKILL)
                        .map_err(std::io::Error::from)
                }
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::{Read, Write};

    use super::InheritedSecret;
    use crate::services::process::{ChildProcess, ProcessSpawner, RealProcessSpawner, SpawnConfig};

    const SECRET: &str = "sk-INHERITED-dummy";

    fn wait(child: &mut Box<dyn ChildProcess>) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the child never exited"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Run `script` under `/bin/sh` with the secret on descriptor 3 and
    /// answer its stdout. Callers assert on the output without printing it,
    /// since it may hold the secret.
    fn run_with_secret(script: &str, config: SpawnConfig) -> String {
        let config = config
            .args(["-c", script])
            .inherited_secret(InheritedSecret::new(3, SECRET));
        let mut child = RealProcessSpawner.spawn(config).unwrap();
        let mut out = String::new();
        child
            .take_stdout()
            .unwrap()
            .read_to_string(&mut out)
            .unwrap();
        assert!(wait(&mut child).success(), "the child failed");
        out
    }

    #[test]
    fn the_child_reads_the_secret_on_its_descriptor_and_never_in_its_environment() {
        let out = run_with_secret("cat <&3", SpawnConfig::new("/bin/sh"));
        assert!(out == SECRET, "the child did not read the secret on fd 3");

        let env = run_with_secret("env", SpawnConfig::new("/bin/sh"));
        assert!(!env.is_empty());
        assert!(
            !env.contains(SECRET),
            "the secret reached the child's environment"
        );
    }

    /// The secret spawn launches what the std path would: the same overlay
    /// and removals, the working directory, captured stdio both ways, and a
    /// process group of its own.
    #[test]
    fn the_secret_spawn_keeps_the_rest_of_the_spawn_config() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().canonicalize().unwrap();
        let config = SpawnConfig::new("sh")
            .args([
                "-c",
                "read line; printf '%s|%s|%s|%s|%s' \"$line\" \"$PWD\" \"$OVERLAY\" \"${PATH:+path}\" \"${HOME-unset}\"; \
                 echo err >&2",
            ])
            .cwd(&cwd.display().to_string())
            .env("OVERLAY", "on")
            .env_remove("HOME")
            .stdin(true)
            .inherited_secret(InheritedSecret::new(3, SECRET));
        let mut child = RealProcessSpawner.spawn(config).unwrap();
        let pid = child.id() as libc::pid_t;
        // The child is blocked reading stdin, so it is still there to ask.
        // SAFETY: `getpgid` only reads.
        assert_eq!(
            unsafe { libc::getpgid(pid) },
            pid,
            "the child leads its own group"
        );
        let mut stdin = child.take_stdin().unwrap();
        stdin.write_all(b"hello\n").unwrap();
        drop(stdin);
        let mut out = String::new();
        child
            .take_stdout()
            .unwrap()
            .read_to_string(&mut out)
            .unwrap();
        let mut err = String::new();
        child
            .take_stderr()
            .unwrap()
            .read_to_string(&mut err)
            .unwrap();
        assert!(wait(&mut child).success());
        assert_eq!(
            out,
            format!("hello|{}|on|path|unset", cwd.display()),
            "stdin, cwd, overlay, the resolved PATH, and a removal"
        );
        assert_eq!(err, "err\n", "stderr is captured apart from stdout");
    }

    /// What the child passes on reads end-of-stream, never the secret: the
    /// pipe's write end was closed before the child existed, so once the child
    /// has read the value nothing is left in it.
    #[test]
    fn a_grandchild_holding_the_descriptor_reads_nothing() {
        let out = run_with_secret(
            "cat <&3 >/dev/null; /bin/sh -c 'cat <&3; echo done'",
            SpawnConfig::new("/bin/sh"),
        );
        assert!(
            out == "done\n",
            "the grandchild read something from the inherited descriptor"
        );
    }

    /// The pipe is close-on-exec in this process from the moment it exists, so
    /// processes spawned on other threads while secrets are in flight never
    /// hold it. Each sibling reads whatever its descriptor 3 is, if anything.
    #[test]
    fn a_sibling_spawned_concurrently_never_holds_the_secret() {
        let secrets = std::thread::spawn(|| {
            for _ in 0..40 {
                let out = run_with_secret("cat <&3", SpawnConfig::new("/bin/sh"));
                assert!(out == SECRET, "a secret child lost its secret");
            }
        });
        let mut leaked = 0;
        for _ in 0..40 {
            let output = RealProcessSpawner
                .run(SpawnConfig::new("/bin/sh").args(["-c", "cat <&3 2>/dev/null; true"]))
                .unwrap();
            if output.stdout.contains(SECRET) {
                leaked += 1;
            }
        }
        secrets.join().unwrap();
        assert_eq!(leaked, 0, "a sibling spawn inherited the secret's pipe");
    }

    #[test]
    fn what_cannot_be_delivered_is_refused() {
        let refused = |config: SpawnConfig| match RealProcessSpawner.spawn(config) {
            Ok(_) => panic!("the spawn was not refused"),
            Err(error) => error,
        };
        let stdio = refused(
            SpawnConfig::new("/bin/true").inherited_secret(InheritedSecret::new(2, SECRET)),
        );
        assert!(stdio.contains("stdio"), "{stdio}");
        let long = refused(
            SpawnConfig::new("/bin/true").inherited_secret(InheritedSecret::new(
                3,
                vec![b'x'; InheritedSecret::MAX_LEN + 1],
            )),
        );
        assert!(long.contains("at most"), "{long}");
        let missing = refused(
            SpawnConfig::new("cairn-no-such-program")
                .inherited_secret(InheritedSecret::new(3, SECRET)),
        );
        assert!(missing.contains("not found"), "{missing}");
        let ran = RealProcessSpawner
            .run(SpawnConfig::new("/bin/true").inherited_secret(InheritedSecret::new(3, SECRET)));
        assert!(ran.is_err(), "run never delivers an inherited secret");
    }

    #[test]
    fn debug_never_shows_the_secret() {
        let secret = InheritedSecret::new(3, SECRET);
        let config = SpawnConfig::new("/bin/true").inherited_secret(secret.clone());
        assert!(!format!("{secret:?}").contains(SECRET));
        assert!(!format!("{config:?}").contains(SECRET));
        assert!(secret.is(SECRET.as_bytes()));
    }
}
