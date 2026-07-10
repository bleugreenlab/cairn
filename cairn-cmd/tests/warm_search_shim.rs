#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;

use base64::Engine;
use cairn_common::protocol::{WarmSearchDeclineReason, WarmSearchRequest, WarmSearchResponse};

fn real_rg() -> PathBuf {
    let output = Command::new("sh")
        .args(["-c", "command -v rg"])
        .output()
        .unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
}

fn shim_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    symlink(env!("CARGO_BIN_EXE_cairn-cmd"), dir.path().join("rg")).unwrap();
    symlink(env!("CARGO_BIN_EXE_cairn-cmd"), dir.path().join("grep")).unwrap();
    dir
}

fn run_bash(cwd: &Path, command: &str, shim: Option<(&Path, &str)>) -> Output {
    let mut process = Command::new("bash");
    process
        .args(["-c", command])
        .current_dir(cwd)
        .stdin(Stdio::null());
    if let Some((shim_dir, url)) = shim {
        let path = format!(
            "{}:{}",
            shim_dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        process
            .env("PATH", path)
            .env("CAIRN_REAL_RG", real_rg())
            .env("CAIRN_REAL_GREP", "/usr/bin/grep")
            .env("CAIRN_WARM_SEARCH_URL", url)
            .env("CAIRN_RUN_ID", "run-test")
            .env("CAIRN_MCP_SECRET", "secret")
            .env_remove("RIPGREP_CONFIG_PATH")
            .env_remove("GREP_OPTIONS");
    }
    process.output().unwrap()
}

fn one_response_server(
    response: WarmSearchResponse,
) -> (
    String,
    mpsc::Receiver<WarmSearchRequest>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(str::trim)
                    .and_then(|value| value.parse::<usize>().ok())
            })
            .unwrap();
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
        }
        let request: WarmSearchRequest =
            serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap();
        let _ = tx.send(request);

        let body = serde_json::to_vec(&response).unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    });
    (format!("http://{address}/api/warm-search"), rx, handle)
}

fn served(stdout: &[u8], exit_code: i32) -> WarmSearchResponse {
    WarmSearchResponse::Serve {
        stdout_base64: base64::engine::general_purpose::STANDARD.encode(stdout),
        stderr_base64: String::new(),
        exit_code,
    }
}

#[test]
fn compound_shell_uses_runtime_cwd_and_preserves_redirection() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("nested")).unwrap();
    fs::write(root.path().join("nested/a.txt"), "needle\n").unwrap();
    let shim = shim_dir();
    let (url, request_rx, server) = one_response_server(served(b"./a.txt:needle\n", 0));

    let output = run_bash(
        root.path(),
        "cd nested && rg needle . > result.txt; printf done",
        Some((shim.path(), &url)),
    );
    server.join().unwrap();

    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout, b"done");
    assert_eq!(
        fs::read(root.path().join("nested/result.txt")).unwrap(),
        b"./a.txt:needle\n"
    );
    let request = request_rx.recv().unwrap();
    assert_eq!(request.program, "rg");
    assert_eq!(request.argv, ["needle", "."]);
    assert_eq!(
        fs::canonicalize(Path::new(&request.cwd)).unwrap(),
        fs::canonicalize(root.path().join("nested")).unwrap()
    );
}

#[test]
fn pass_through_and_transport_failure_are_silent_native_fallbacks() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("src")).unwrap();
    fs::write(root.path().join("src/a.txt"), "needle\n").unwrap();
    let shim = shim_dir();

    let native = run_bash(root.path(), "rg --json needle src | head -1", None);
    let response = WarmSearchResponse::PassThrough {
        reason: WarmSearchDeclineReason::UnsupportedFlag("--json".to_string()),
    };
    let (url, _, server) = one_response_server(response);
    let declined = run_bash(
        root.path(),
        "rg --json needle src | head -1",
        Some((shim.path(), &url)),
    );
    server.join().unwrap();
    assert_eq!(declined.stdout, native.stdout);
    assert_eq!(declined.stderr, native.stderr);
    assert_eq!(declined.status.code(), native.status.code());

    let unavailable = run_bash(
        root.path(),
        "rg needle src | sort",
        Some((shim.path(), "http://127.0.0.1:9/api/warm-search")),
    );
    let native = run_bash(root.path(), "rg needle src | sort", None);
    assert_eq!(unavailable.stdout, native.stdout);
    assert_eq!(unavailable.stderr, native.stderr);
    assert_eq!(unavailable.status.code(), native.status.code());
}

#[test]
fn piped_stdin_bypasses_transport_and_execs_native_rg() {
    let root = tempfile::tempdir().unwrap();
    let shim = shim_dir();
    let native = run_bash(root.path(), "printf 'needle\n' | rg needle", None);
    let shimmed = run_bash(
        root.path(),
        "printf 'needle\n' | rg needle",
        Some((shim.path(), "http://127.0.0.1:9/api/warm-search")),
    );
    assert_eq!(shimmed.stdout, native.stdout);
    assert_eq!(shimmed.stderr, native.stderr);
    assert_eq!(shimmed.status.code(), native.status.code());
}

#[test]
fn served_rg_preserves_broken_pipe_status_under_pipefail() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("src")).unwrap();
    let lines = "needle\n".repeat(100_000);
    fs::write(root.path().join("src/a.txt"), &lines).unwrap();
    let shim = shim_dir();
    let served_lines = "src/a.txt:needle\n".repeat(100_000);
    let (url, _, server) = one_response_server(served(served_lines.as_bytes(), 0));

    let native = run_bash(
        root.path(),
        "set -o pipefail; rg needle src | head -1 >/dev/null",
        None,
    );
    let shimmed = run_bash(
        root.path(),
        "set -o pipefail; rg needle src | head -1 >/dev/null",
        Some((shim.path(), &url)),
    );
    server.join().unwrap();
    assert_eq!(shimmed.status.code(), native.status.code());
    assert!(shimmed.stderr.is_empty(), "{:?}", shimmed.stderr);
}

#[test]
fn downstream_pipeline_and_shell_control_flow_remain_bash_owned() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("src")).unwrap();
    let shim = shim_dir();

    let (url, _, server) = one_response_server(served(
        b"src/a.txt:needle keep\nsrc/b.txt:needle fixture\n",
        0,
    ));
    let pipeline = run_bash(
        root.path(),
        "rg needle src | grep -v fixture | head -1",
        Some((shim.path(), &url)),
    );
    server.join().unwrap();
    assert!(pipeline.status.success());
    assert_eq!(pipeline.stdout, b"src/a.txt:needle keep\n");

    let (url, _, server) = one_response_server(served(b"", 1));
    let fallback = run_bash(
        root.path(),
        "rg absent . || printf fallback",
        Some((shim.path(), &url)),
    );
    server.join().unwrap();
    assert!(fallback.status.success());
    assert_eq!(fallback.stdout, b"fallback");

    let (url, request_rx, server) = one_response_server(served(b"src/a.txt:needle\n", 0));
    let variable = run_bash(
        root.path(),
        "root=src; if rg needle \"$root\" >/dev/null; then printf found; fi",
        Some((shim.path(), &url)),
    );
    server.join().unwrap();
    assert!(variable.status.success());
    assert_eq!(variable.stdout, b"found");
    assert_eq!(request_rx.recv().unwrap().argv, ["needle", "src"]);
}

#[test]
fn shell_functions_and_absolute_paths_bypass_the_path_shim() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("src")).unwrap();
    fs::write(root.path().join("src/a.txt"), "needle\n").unwrap();
    let shim = shim_dir();
    let unavailable = "http://127.0.0.1:9/api/warm-search";

    let function = run_bash(
        root.path(),
        "rg() { printf function; }; rg needle src",
        Some((shim.path(), unavailable)),
    );
    assert!(function.status.success());
    assert_eq!(function.stdout, b"function");

    let command = format!("{} needle src", real_rg().display());
    let absolute = run_bash(root.path(), &command, Some((shim.path(), unavailable)));
    let native = run_bash(root.path(), &command, None);
    assert_eq!(absolute.stdout, native.stdout);
    assert_eq!(absolute.stderr, native.stderr);
    assert_eq!(absolute.status.code(), native.status.code());
}
