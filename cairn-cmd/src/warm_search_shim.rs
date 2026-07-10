//! Silent `rg`/`grep` executable shim client.

use std::ffi::OsString;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use base64::Engine;
use cairn_common::protocol::{WarmSearchRequest, WarmSearchResponse, WarmSearchStdin};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(100);

pub fn invoked_program() -> Option<&'static str> {
    let argv0 = std::env::args_os().next()?;
    match Path::new(&argv0).file_name()?.to_str()? {
        "rg" | "rg.exe" => Some("rg"),
        "grep" | "grep.exe" => Some("grep"),
        _ => None,
    }
}

pub fn run(program: &str) -> ! {
    restore_sigpipe();
    let argv_os: Vec<OsString> = std::env::args_os().skip(1).collect();
    let real = real_binary(program);

    if config_environment_is_sensitive(program) {
        fallback(real, &argv_os);
    }
    let stdin = stdin_kind();
    if matches!(
        stdin,
        WarmSearchStdin::Pipe | WarmSearchStdin::File | WarmSearchStdin::Other
    ) {
        fallback(real, &argv_os);
    }
    let Some(run_id) = std::env::var("CAIRN_RUN_ID").ok() else {
        fallback(real, &argv_os);
    };
    let Some(secret) = std::env::var("CAIRN_MCP_SECRET").ok() else {
        fallback(real, &argv_os);
    };
    let Some(url) = std::env::var("CAIRN_WARM_SEARCH_URL").ok() else {
        fallback(real, &argv_os);
    };
    let Some(argv) = argv_os
        .iter()
        .map(|arg| arg.to_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>()
    else {
        fallback(real, &argv_os);
    };
    let Some(cwd) = std::env::current_dir()
        .ok()
        .and_then(|path| path.into_os_string().into_string().ok())
    else {
        fallback(real, &argv_os);
    };

    let request = WarmSearchRequest {
        run_id,
        cwd,
        program: program.to_string(),
        argv,
        stdin,
    };
    let response = reqwest::blocking::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .ok()
        .and_then(|client| {
            client
                .post(url)
                .bearer_auth(secret)
                .json(&request)
                .send()
                .ok()
        })
        .filter(|response| response.status().is_success())
        .and_then(|response| response.json::<WarmSearchResponse>().ok());

    let Some(WarmSearchResponse::Serve {
        stdout_base64,
        stderr_base64,
        exit_code,
    }) = response
    else {
        fallback(real, &argv_os);
    };
    let engine = base64::engine::general_purpose::STANDARD;
    let (Ok(stdout), Ok(stderr)) = (engine.decode(stdout_base64), engine.decode(stderr_base64))
    else {
        fallback(real, &argv_os);
    };

    if program == "rg" {
        ignore_sigpipe();
    }
    if write_inherited(std::io::stdout().lock(), &stdout).is_err()
        || write_inherited(std::io::stderr().lock(), &stderr).is_err()
    {
        // For the directory searches this service can serve, ripgrep treats a
        // downstream early close as successful completion; POSIX grep is killed
        // by SIGPIPE (128 + 13). Preserve those program-specific contracts.
        std::process::exit(if program == "rg" { 0 } else { 141 });
    }
    std::process::exit(exit_code);
}

fn real_binary(program: &str) -> Option<OsString> {
    let key = match program {
        "rg" => "CAIRN_REAL_RG",
        "grep" => "CAIRN_REAL_GREP",
        _ => return None,
    };
    std::env::var_os(key)
}

fn config_environment_is_sensitive(program: &str) -> bool {
    match program {
        "rg" => std::env::var_os("RIPGREP_CONFIG_PATH").is_some(),
        "grep" => std::env::var_os("GREP_OPTIONS").is_some(),
        _ => true,
    }
}

fn write_inherited(mut stream: impl Write, bytes: &[u8]) -> std::io::Result<()> {
    stream.write_all(bytes)?;
    stream.flush()
}

#[cfg(unix)]
fn restore_sigpipe() {
    // Rust ignores SIGPIPE by default. Executable shims must restore the native
    // command-line contract before writing into pipelines such as `rg | head`.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

#[cfg(not(unix))]
fn restore_sigpipe() {}

#[cfg(unix)]
fn ignore_sigpipe() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

#[cfg(not(unix))]
fn ignore_sigpipe() {}

#[cfg(unix)]
fn stdin_kind() -> WarmSearchStdin {
    unsafe {
        if libc::isatty(libc::STDIN_FILENO) == 1 {
            return WarmSearchStdin::Terminal;
        }
        let mut stat: libc::stat = std::mem::zeroed();
        if libc::fstat(libc::STDIN_FILENO, &mut stat) != 0 {
            return WarmSearchStdin::Other;
        }
        match stat.st_mode & libc::S_IFMT {
            libc::S_IFCHR => WarmSearchStdin::Inherited,
            libc::S_IFIFO => WarmSearchStdin::Pipe,
            libc::S_IFREG => WarmSearchStdin::File,
            _ => WarmSearchStdin::Other,
        }
    }
}

#[cfg(not(unix))]
fn stdin_kind() -> WarmSearchStdin {
    WarmSearchStdin::Other
}

#[cfg(unix)]
fn fallback(real: Option<OsString>, argv: &[OsString]) -> ! {
    use std::os::unix::process::CommandExt;

    let Some(real) = real else {
        std::process::exit(127);
    };
    let error = std::process::Command::new(real).args(argv).exec();
    let _ = error;
    std::process::exit(127);
}

#[cfg(not(unix))]
fn fallback(real: Option<OsString>, argv: &[OsString]) -> ! {
    let Some(real) = real else {
        std::process::exit(127);
    };
    let code = std::process::Command::new(real)
        .args(argv)
        .status()
        .ok()
        .and_then(|status| status.code())
        .unwrap_or(127);
    std::process::exit(code);
}
