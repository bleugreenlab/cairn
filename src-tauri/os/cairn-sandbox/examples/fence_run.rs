//! Run one command under this build's fence, the way a confined executor spawn
//! would: the batch policy for the current directory, the given scratch as
//! `$TMPDIR`/`TMUX_TMPDIR` and as the socket-resolvable job scratch, and with
//! `--pty` the terminal seam (`attach_pty_session`).
//!
//! It exists to measure a sandbox change on a host before that host's executor
//! runs the build that carries it: run it inside an ordinary batch there, and
//! landlock's layering makes the effective rules the intersection of the
//! executor's fence and this one.
//!
//! ```sh
//! cargo run -q -p cairn-sandbox --example fence_run -- [--pty] <scratch> -- <command> [args...]
//! ```

use std::path::PathBuf;
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let pty = args.first().is_some_and(|arg| arg == "--pty");
    if pty {
        args.remove(0);
    }
    let Some(separator) = args.iter().position(|arg| arg == "--") else {
        eprintln!("usage: fence_run [--pty] <scratch> -- <command> [args...]");
        return ExitCode::from(2);
    };
    let (head, command) = (&args[..separator], &args[separator + 1..]);
    let (Some(scratch), Some((program, rest))) = (head.first(), command.split_first()) else {
        eprintln!("usage: fence_run [--pty] <scratch> -- <command> [args...]");
        return ExitCode::from(2);
    };
    let scratch = PathBuf::from(scratch);
    let _ = std::fs::create_dir_all(&scratch);
    let cwd = std::env::current_dir().expect("current directory");

    let mut policy = cairn_sandbox::SandboxPolicy::for_run(
        &cwd,
        &[scratch.to_string_lossy().into_owned()],
        cairn_sandbox::default_deny_read(),
    );
    policy.allow_job_sockets_in(&scratch);

    let mut child = Command::new(program);
    child
        .args(rest)
        .env("TMPDIR", &scratch)
        .env("TMUX_TMPDIR", &scratch)
        .env("CAIRN_SANDBOXED", "1");
    #[cfg(unix)]
    let _master = if pty {
        Some(attach_pty(&mut child))
    } else {
        None
    };
    cairn_sandbox::install_pre_exec(&mut child, &policy);
    let status = child.status().expect("spawn the fenced command");
    ExitCode::from(status.code().unwrap_or(1).clamp(0, 255) as u8)
}

/// Put the child on a fresh PTY and copy what it writes to our stdout.
#[cfg(unix)]
fn attach_pty(child: &mut Command) -> std::fs::File {
    use std::os::fd::FromRawFd;
    let (mut master, mut slave) = (0, 0);
    // SAFETY: openpty writes two descriptors; the optional arguments are null.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, 0, "openpty: {}", std::io::Error::last_os_error());
    // SAFETY: openpty returned two fresh descriptors we now own.
    let (master, slave) = unsafe {
        (
            std::fs::File::from_raw_fd(master),
            std::fs::File::from_raw_fd(slave),
        )
    };
    cairn_sandbox::attach_pty_session(child, slave).expect("attach the PTY session");
    let mut reader = master.try_clone().expect("clone the PTY master");
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut reader, &mut std::io::stdout());
    });
    master
}
