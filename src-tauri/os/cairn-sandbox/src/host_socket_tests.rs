//! The Linux fence against host-service sockets and other processes, measured
//! against the running kernel.
//!
//! Every test asserts the outcome this kernel should give rather than skipping
//! on an old one: below Landlock ABI 9 a pathname socket served from outside
//! the fence must be *reachable*, because that is the truth the containment
//! probe has to report, and a test that only ran where the boundary held would
//! never notice the probe claiming more than the kernel enforces.
//!
//! The specimens are the ones cairn/5083 was filed on: the session bus that
//! `systemd-run --user` rides (any pathname socket served from outside), a
//! tmux server under a fixed `-L` label, the abstract namespace, and
//! cairn://posts/461's read of another process's environment.

use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use crate::containment::socket_probe::{address, open_pty, report_pipe, short_tempdir};
use crate::linux::{kernel_abi, ABSTRACT_SCOPE_ABI, RESOLVE_UNIX_ABI};
use crate::SandboxPolicy;

type Address = (libc::sockaddr_un, libc::socklen_t);

fn pathname(path: &Path) -> Address {
    address(path.as_os_str().as_bytes(), false).expect("socket path fits")
}

fn sockets_refused() -> bool {
    kernel_abi().is_some_and(|abi| abi >= RESOLVE_UNIX_ABI)
}

fn abstract_refused() -> bool {
    kernel_abi().is_some_and(|abi| abi >= ABSTRACT_SCOPE_ABI)
}

/// A fenced job: its worktree, its cell scratch, and the policy a confined
/// batch in it runs under.
struct Job {
    worktree: tempfile::TempDir,
    scratch: tempfile::TempDir,
}

impl Job {
    fn new() -> Self {
        Self {
            worktree: short_tempdir().unwrap(),
            scratch: short_tempdir().unwrap(),
        }
    }

    fn policy(&self) -> SandboxPolicy {
        let grant = [self.scratch.path().to_string_lossy().into_owned()];
        let mut policy = SandboxPolicy::for_run(self.worktree.path(), &grant, Vec::new());
        policy.allow_job_sockets_in(self.scratch.path());
        policy
    }
}

/// `connect(2)` to `address`, as an errno: 0 for success.
///
/// # Safety
/// Async-signal-safe; callable in a forked child.
unsafe fn connect_errno((address, length): &Address, kind: libc::c_int) -> u8 {
    let socket = libc::socket(libc::AF_UNIX, kind | libc::SOCK_CLOEXEC, 0);
    if socket < 0 {
        return u8::MAX;
    }
    let rc = libc::connect(
        socket,
        (address as *const libc::sockaddr_un).cast(),
        *length,
    );
    let errno = if rc == 0 {
        0
    } else {
        *libc::__errno_location() as u8
    };
    libc::close(socket);
    errno
}

/// `sendto(2)` one byte to a datagram socket at `address`, as an errno.
unsafe fn sendto_errno((address, length): &Address) -> u8 {
    let socket = libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0);
    if socket < 0 {
        return u8::MAX;
    }
    let byte = 0u8;
    let rc = libc::sendto(
        socket,
        (&byte as *const u8).cast(),
        1,
        0,
        (address as *const libc::sockaddr_un).cast(),
        *length,
    );
    let errno = if rc >= 0 {
        0
    } else {
        *libc::__errno_location() as u8
    };
    libc::close(socket);
    errno
}

/// Run `attempt` inside a spawn confined by `policy`, after the seam's own
/// hooks, and return what it reported. `pty` spawns through the terminal seam.
fn fenced<const N: usize>(
    policy: Option<&SandboxPolicy>,
    pty: bool,
    attempt: impl Fn() -> [u8; N] + Send + Sync + 'static,
) -> [u8; N] {
    let (report_read, report_write) = report_pipe().unwrap();
    let mut command = Command::new("/bin/true");
    let _master = if pty {
        let (master, slave) = open_pty().unwrap();
        crate::attach_pty_session(&mut command, slave).unwrap();
        Some(master)
    } else {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        None
    };
    if let Some(policy) = policy {
        crate::install_pre_exec(&mut command, policy);
    }
    let report = report_write.as_raw_fd();
    // SAFETY: the attempt makes only async-signal-safe calls.
    unsafe {
        command.pre_exec(move || {
            let result = attempt();
            libc::write(report, result.as_ptr().cast(), N);
            Ok(())
        });
    }
    let mut child = command.spawn().expect("spawn the fenced attempt");
    drop(report_write);
    let mut result = [0u8; N];
    std::io::Read::read_exact(&mut std::fs::File::from(report_read), &mut result)
        .expect("the fenced attempt reports");
    child.wait().unwrap();
    result
}

/// A server a fenced spawn starts for itself: it binds `at` inside its own
/// domain, then execs `sleep` with the listener still open.
fn fenced_server(policy: &SandboxPolicy, at: &Path) -> Child {
    let address = pathname(at);
    let mut command = Command::new("sleep");
    command.arg("30").stdin(Stdio::null());
    crate::install_pre_exec(&mut command, policy);
    // SAFETY: async-signal-safe socket calls on captured stack data. The
    // listener is deliberately not close-on-exec, so it outlives the exec.
    unsafe {
        command.pre_exec(move || {
            let (addr, length) = &address;
            let listener = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
            if libc::bind(listener, (addr as *const libc::sockaddr_un).cast(), *length) != 0
                || libc::listen(listener, 8) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().expect("start a fenced server");
    for _ in 0..200 {
        if at.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    child
}

fn expect_refused_or_reachable(errno: u8, what: &str) {
    if sockets_refused() {
        assert_eq!(
            errno,
            libc::EACCES as u8,
            "{what} must be refused at ABI 9+"
        );
    } else {
        assert_eq!(
            errno, 0,
            "{what} is reachable below ABI 9 ({:?}); the probe must say so rather than this test pass",
            kernel_abi()
        );
    }
}

/// The bus-shaped specimen: a pathname socket served from outside, reached by
/// `connect` and by `sendmsg` with an explicit address.
#[test]
fn a_pathname_socket_served_from_outside_is_refused() {
    let job = Job::new();
    let host = short_tempdir().unwrap();
    let stream_path = host.path().join("bus");
    let _stream = std::os::unix::net::UnixListener::bind(&stream_path).unwrap();
    let datagram_path = host.path().join("log");
    let _datagram = std::os::unix::net::UnixDatagram::bind(&datagram_path).unwrap();
    let (stream, datagram) = (pathname(&stream_path), pathname(&datagram_path));

    let [connected, sent] = fenced(Some(&job.policy()), false, move || unsafe {
        [
            connect_errno(&stream, libc::SOCK_STREAM),
            sendto_errno(&datagram),
        ]
    });
    expect_refused_or_reachable(connected, "connect to an outside stream socket");
    expect_refused_or_reachable(sent, "sendto an outside datagram socket");

    // Unfenced, the same attempt succeeds: the refusal is the fence's.
    let [unfenced] = fenced(None, false, move || unsafe {
        [connect_errno(&stream, libc::SOCK_STREAM)]
    });
    assert_eq!(unfenced, 0);
}

/// A tmux server started outside the fence under a fixed label resolves to a
/// predictable `tmux-<uid>/<label>` path; the label form and `-S` are the same
/// crossing at the socket.
#[test]
fn a_tmux_server_outside_the_fence_is_unreachable_by_label_and_by_path() {
    let job = Job::new();
    let tmux_tmpdir = short_tempdir().unwrap();
    // SAFETY: getuid cannot fail.
    let uid = unsafe { libc::getuid() };
    let label_dir = tmux_tmpdir.path().join(format!("tmux-{uid}"));
    std::fs::create_dir(&label_dir).unwrap();
    let by_label = label_dir.join("run");
    let _server = std::os::unix::net::UnixListener::bind(&by_label).unwrap();
    let explicit = tmux_tmpdir.path().join("explicit.sock");
    let _explicit = std::os::unix::net::UnixListener::bind(&explicit).unwrap();
    let (label, path) = (pathname(&by_label), pathname(&explicit));

    let [via_label, via_path] = fenced(Some(&job.policy()), false, move || unsafe {
        [
            connect_errno(&label, libc::SOCK_STREAM),
            connect_errno(&path, libc::SOCK_STREAM),
        ]
    });
    expect_refused_or_reachable(via_label, "tmux -L run");
    expect_refused_or_reachable(via_path, "tmux -S");
}

/// The real thing, when tmux is installed: `run-shell` through a server the
/// fence did not start must not execute. The mechanism-level test above runs
/// whether or not tmux is present.
#[test]
fn tmux_run_shell_through_an_outside_server_does_not_execute() {
    if Command::new("tmux").arg("-V").output().is_err() {
        return;
    }
    let job = Job::new();
    // Under /tmp rather than the cell's TMPDIR: a deep scratch path plus
    // `tmux-<uid>/<label>` can overrun the 108-byte socket address limit.
    let tmux_tmpdir = short_tempdir().unwrap();
    let label = format!("c{}", std::process::id());
    let started = Command::new("tmux")
        .env("TMUX_TMPDIR", tmux_tmpdir.path())
        .args(["-L", &label, "new-session", "-d", "sleep 60"])
        .status()
        .unwrap();
    assert!(started.success(), "start the outside tmux server");
    let marker = job.worktree.path().join("escaped");

    let mut attempt = Command::new("tmux");
    attempt
        .env("TMUX_TMPDIR", tmux_tmpdir.path())
        .args([
            "-L",
            &label,
            "run-shell",
            &format!("touch {}", marker.display()),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    crate::install_pre_exec(&mut attempt, &job.policy());
    let _ = attempt.status();
    std::thread::sleep(std::time::Duration::from_millis(300));
    let escaped = marker.exists();

    let _ = Command::new("tmux")
        .env("TMUX_TMPDIR", tmux_tmpdir.path())
        .args(["-L", &label, "kill-server"])
        .status();
    if sockets_refused() {
        assert!(
            !escaped,
            "run-shell executed through a server outside the fence"
        );
    } else {
        assert!(
            escaped,
            "below ABI 9 the outside server is reachable; if this stopped holding, the probe's Ambient report is stale"
        );
    }
}

#[test]
fn an_abstract_socket_served_from_outside_is_refused() {
    use std::os::linux::net::SocketAddrExt;

    let job = Job::new();
    let name = format!("cairn-5083-test-{}", uuid::Uuid::new_v4().simple());
    let _host = std::os::unix::net::SocketAddr::from_abstract_name(&name)
        .and_then(|address| std::os::unix::net::UnixListener::bind_addr(&address))
        .unwrap();
    let target = address(name.as_bytes(), true).unwrap();

    let [errno] = fenced(Some(&job.policy()), false, move || unsafe {
        [connect_errno(&target, libc::SOCK_STREAM)]
    });
    if abstract_refused() {
        assert_eq!(errno, libc::EPERM as u8, "abstract scope must refuse");
    } else {
        assert_eq!(errno, 0);
    }
}

/// cairn://posts/461: another same-uid process's environment and cwd are
/// refused on every ABI by landlock's ptrace rule, while the job's own
/// descendant within one spawn stays readable.
#[test]
fn another_process_environment_is_refused_but_a_descendant_is_readable() {
    let job = Job::new();
    let mut sibling = Command::new("sleep").arg("30").spawn().unwrap();
    let pid = sibling.id();
    let script = format!(
        "cat /proc/{pid}/environ >/dev/null 2>&1 && echo environ-read; \
         readlink /proc/{pid}/cwd >/dev/null 2>&1 && echo cwd-read; \
         sleep 5 & own=$!; cat /proc/$own/environ >/dev/null 2>&1 && echo own-read; kill $own"
    );
    let mut command = Command::new("/bin/sh");
    command.args(["-c", &script]).stdin(Stdio::null());
    crate::install_pre_exec(&mut command, &job.policy());
    let output = command.output().unwrap();
    let _ = sibling.kill();
    let _ = sibling.wait();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("environ-read"), "{stdout}");
    assert!(!stdout.contains("cwd-read"), "{stdout}");
    assert!(
        stdout.contains("own-read"),
        "a descendant must stay inspectable: {stdout}"
    );
}

/// A later batch of the same job is a sibling landlock domain. It reaches a
/// server an earlier batch started in the cell scratch, and not one that
/// batch started in `/tmp`, which is writable but never resolvable.
#[test]
fn a_later_batch_reconnects_to_its_job_scratch_server_only() {
    let job = Job::new();
    let elsewhere = short_tempdir().unwrap();
    let in_scratch = job.scratch.path().join("t.sock");
    let in_tmp = elsewhere.path().join("t.sock");
    let mut first = fenced_server(&job.policy(), &in_scratch);
    let mut stray = fenced_server(&job.policy(), &in_tmp);
    let (scratch_server, tmp_server) = (pathname(&in_scratch), pathname(&in_tmp));

    let [own, tmp] = fenced(Some(&job.policy()), false, move || unsafe {
        [
            connect_errno(&scratch_server, libc::SOCK_STREAM),
            connect_errno(&tmp_server, libc::SOCK_STREAM),
        ]
    });
    let _ = first.kill();
    let _ = stray.kill();
    let _ = first.wait();
    let _ = stray.wait();
    assert_eq!(own, 0, "the job's own scratch server must stay reachable");
    expect_refused_or_reachable(tmp, "a sibling batch's server in /tmp");
}

/// cairn/5497: the `$TMPDIR` shapes an executor exports, built by the layout's
/// own constructors under a home exactly as long as fedora's
/// `/home/dev/.cairn-exec`, leave room for a socket at
/// `$TMPDIR/<24-char dir>/<16-char name>`, and a server one fenced batch binds
/// there is reached from the job's next. Nested under the cell's authority, the
/// same socket did not fit `sun_path` at all.
#[test]
fn a_socket_deep_in_an_executor_scratch_fits_and_is_reached_from_the_next_batch() {
    use cairn_common::executor_paths::{
        cell_scratch_dir, scratch_tree_name, ScratchTreeKind, NODE_SCRATCH_TREE_DIR,
        SCRATCH_TREES_DIR,
    };
    const FEDORA_HOME: &str = "/home/dev/.cairn-exec";
    let base = short_tempdir().unwrap();
    let pad = FEDORA_HOME
        .len()
        .saturating_sub(base.path().as_os_str().len() + 1)
        .max(1);
    let home = base.path().join("h".repeat(pad));
    let authority = home.join("build-slots/cairn/.authority/slot-1088");
    let cell = cell_scratch_dir(&authority).unwrap();
    let replica = home
        .join(SCRATCH_TREES_DIR)
        .join(scratch_tree_name(
            ScratchTreeKind::Node,
            b"a0445167-6499-499f-8aec-96ec56da3742",
        ))
        .join(NODE_SCRATCH_TREE_DIR);
    let socket_under = |tmpdir: &Path| tmpdir.join("s".repeat(24)).join("n".repeat(16));
    // The cliff this replaced: the same socket under the old cell scratch.
    assert!(
        address(
            socket_under(&authority.join("scratch"))
                .as_os_str()
                .as_bytes(),
            false
        )
        .is_err(),
        "the legacy scratch path was expected not to fit"
    );

    for tmpdir in [cell, replica] {
        let at = socket_under(&tmpdir);
        std::fs::create_dir_all(at.parent().unwrap()).unwrap();
        let grant = [tmpdir.to_string_lossy().into_owned()];
        let worktree = short_tempdir().unwrap();
        let mut policy = SandboxPolicy::for_run(worktree.path(), &grant, Vec::new());
        policy.allow_job_sockets_in(&tmpdir);

        let mut first = fenced_server(&policy, &at);
        let server = pathname(&at);
        let [reached] = fenced(Some(&policy), false, move || unsafe {
            [connect_errno(&server, libc::SOCK_STREAM)]
        });
        let _ = first.kill();
        let _ = first.wait();
        assert_eq!(
            reached,
            0,
            "a later batch must reach the server at {} ({} bytes)",
            at.display(),
            at.as_os_str().len()
        );
    }
}

/// A symlink resolves to the socket's real location, and a hard link that
/// would carry an outside socket into a resolvable directory is refused by
/// landlock's rule that a link may not gain rights.
#[test]
fn links_do_not_launder_an_outside_socket() {
    let job = Job::new();
    let host = short_tempdir().unwrap();
    let outside = host.path().join("agent.sock");
    let _server = std::os::unix::net::UnixListener::bind(&outside).unwrap();
    let symlink = job.worktree.path().join("agent-link");
    std::os::unix::fs::symlink(&outside, &symlink).unwrap();
    let hard = job.scratch.path().join("agent-hard");
    let via_symlink = pathname(&symlink);
    let via_hard = pathname(&hard);
    let source = std::ffi::CString::new(outside.as_os_str().as_bytes()).unwrap();
    let destination = std::ffi::CString::new(hard.as_os_str().as_bytes()).unwrap();

    let [symlinked, linked, through_link] = fenced(Some(&job.policy()), false, move || unsafe {
        let linked = libc::link(source.as_ptr(), destination.as_ptr()) == 0;
        [
            connect_errno(&via_symlink, libc::SOCK_STREAM),
            u8::from(linked),
            if linked {
                connect_errno(&via_hard, libc::SOCK_STREAM)
            } else {
                libc::EACCES as u8
            },
        ]
    });
    expect_refused_or_reachable(symlinked, "a symlink to an outside socket");
    if sockets_refused() {
        assert!(
            linked == 0 || through_link == libc::EACCES as u8,
            "a hard link must not carry an outside socket into the resolvable set"
        );
    }
}

/// A PTY resident spawns through the terminal seam and is fenced like a
/// batch: no write into `$HOME`, no outside socket.
#[test]
fn a_pty_resident_is_fenced() {
    let job = Job::new();
    let host = short_tempdir().unwrap();
    let outside = host.path().join("bus");
    let _server = std::os::unix::net::UnixListener::bind(&outside).unwrap();
    let target = pathname(&outside);
    let home = std::env::var_os("HOME").map(PathBuf::from).expect("HOME");
    let escape = home.join(format!("cairn-5083-pty-{}", uuid::Uuid::new_v4().simple()));
    let escape_c = std::ffi::CString::new(escape.as_os_str().as_bytes()).unwrap();

    let [wrote, connected] = fenced(Some(&job.policy()), true, move || unsafe {
        let fd = libc::open(
            escape_c.as_ptr(),
            libc::O_CREAT | libc::O_WRONLY | libc::O_CLOEXEC,
            0o600,
        );
        let wrote = fd >= 0;
        if wrote {
            libc::close(fd);
        }
        [u8::from(wrote), connect_errno(&target, libc::SOCK_STREAM)]
    });
    let _ = std::fs::remove_file(&escape);
    assert_eq!(wrote, 0, "a fenced PTY resident must not write into $HOME");
    expect_refused_or_reachable(connected, "a PTY resident's connect to an outside socket");
}

/// The probe the inventory carries reports what this kernel enforces, for each
/// seam it covers.
#[test]
fn the_host_socket_probe_reports_what_this_kernel_enforces() {
    use cairn_common::executor_protocol::{PolicyMode, ProcessClass};

    for class in [
        ProcessClass::BatchSpawn,
        ProcessClass::PipedResident,
        ProcessClass::PtyResident,
    ] {
        for mode in [PolicyMode::Confined, PolicyMode::ReadOnlyCheckout] {
            let witness = crate::containment::probe_host_socket(class, mode).unwrap();
            assert!(
                witness.positive_control_passed(),
                "{class:?} {mode:?}: {}",
                witness.detail()
            );
            assert_eq!(
                witness.crossing_permitted(),
                !(sockets_refused() && abstract_refused()),
                "{class:?} {mode:?}: {}",
                witness.detail()
            );
            if !sockets_refused() {
                assert!(
                    witness.detail().contains("kernel Landlock ABI")
                        && witness.detail().contains("< 9"),
                    "{}",
                    witness.detail()
                );
            }
        }
        let unconfined =
            crate::containment::probe_host_socket(class, PolicyMode::Unconfined).unwrap();
        assert!(unconfined.positive_control_passed());
        assert!(
            unconfined.crossing_permitted(),
            "an unconfined spawn reaches host sockets"
        );
    }
}
