//! The CLI a session launches is the one Cairn installed, never whichever
//! `codex` or `claude` comes first on the user's `PATH`.
//!
//! Its own test binary: it rewrites `PATH` and `HOME`, which the user-PATH
//! composition reads once per process.
#![cfg(unix)]

#[cfg(not(feature = "test-utils"))]
compile_error!("cairn-models' integration tests need `--features test-utils`; `bun run test:rust -p cairn-models` adds it");

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use cairn_models::interface::session::{Cli, SessionHost};
use cairn_models::managed::claude::CLAUDE;
use cairn_models::managed::codex::CODEX;
use cairn_models::managed::testing::installed_release;
use cairn_models::managed::{ManagedTool, ManagedTools, ToolSpec};
use cairn_models::session::local::LocalSessionHost;
use cairn_platform::services::process::RealProcessSpawner;

fn script(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A different `codex` and `claude`, first on `PATH` and in the first
/// directory the user-PATH composition searches under `HOME`. Set once: both
/// tests share the process environment.
fn decoys() -> &'static Path {
    static SCRATCH: OnceLock<PathBuf> = OnceLock::new();
    SCRATCH.get_or_init(|| {
        let scratch = tempfile::tempdir().unwrap().keep();
        let home = scratch.join("home");
        let decoy_dir = home.join(".claude").join("local").join("bin");
        script(&decoy_dir.join("codex"), "echo 'codex-cli 9.9.9'");
        script(&decoy_dir.join("claude"), "echo '9.9.9 (Claude Code)'");
        let path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("HOME", &home);
        std::env::set_var("PATH", format!("{}:{path}", decoy_dir.display()));
        scratch
    })
}

/// The host, with `spec`'s release installed under a fresh root and the
/// other tool left uninstalled.
fn host_with(spec: &'static ToolSpec) -> (LocalSessionHost, PathBuf, tempfile::TempDir) {
    decoys();
    let scratch = tempfile::tempdir().unwrap();
    let installed = installed_release(
        spec,
        scratch.path(),
        &format!("echo '{} (managed)'", spec.embedded().version),
    );
    let entrypoint = installed.installed().expect("planted").path;
    let other = |other: &'static ToolSpec| {
        Arc::new(ManagedTool::new(
            other,
            scratch.path().join("other"),
            scratch.path().join("bin"),
            Arc::new(cairn_models::managed::RefusingSource),
            Arc::new(cairn_models::managed::RefusingSource),
        ))
    };
    let managed = if spec.name == "codex" {
        ManagedTools {
            codex: Arc::new(installed),
            claude: other(&CLAUDE),
        }
    } else {
        ManagedTools {
            codex: other(&CODEX),
            claude: Arc::new(installed),
        }
    };
    let host = LocalSessionHost::new(
        Arc::new(RealProcessSpawner),
        scratch.path().join("codex-home"),
        managed,
    );
    (host, entrypoint, scratch)
}

#[test]
fn the_managed_codex_wins_over_a_codex_first_on_path() {
    if (CODEX.host_target)().is_none() {
        return;
    }
    let (host, entrypoint, _scratch) = host_with(&CODEX);
    let resolved = host
        .resolve_cli(Cli::Codex)
        .expect("the managed Codex resolves");
    assert_eq!(resolved, entrypoint.to_string_lossy());
}

#[test]
fn claude_resolves_to_the_managed_copy_over_path() {
    if (CLAUDE.host_target)().is_none() {
        return;
    }
    let (host, entrypoint, _scratch) = host_with(&CLAUDE);
    let resolved = host
        .resolve_cli(Cli::Claude)
        .expect("the managed Claude resolves");
    assert_eq!(resolved, entrypoint.to_string_lossy());
    assert!(
        resolved.ends_with(&format!(
            "/tools/claude/{}-{}/claude",
            CLAUDE.embedded().version,
            (CLAUDE.host_target)().unwrap()
        )),
        "{resolved}"
    );
}

/// With nothing installed and nothing to install from, Claude is refused
/// with the managed install's own text; the decoy on `PATH` is never
/// offered in its place.
#[test]
fn an_uninstalled_claude_is_refused_not_found_on_path() {
    decoys();
    let (host, _, _scratch) = host_with(&CODEX);
    let error = host.resolve_cli(Cli::Claude).unwrap_err().to_string();
    assert!(
        error.contains("Cairn does not fall back to a `claude` found on your PATH"),
        "{error}"
    );
}
