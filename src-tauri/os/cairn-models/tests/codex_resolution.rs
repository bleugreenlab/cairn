//! The Codex a session launches is the one Cairn installed, never whichever
//! `codex` comes first on the user's `PATH`.
//!
//! Its own test binary: it rewrites `PATH` and `HOME`, which the user-PATH
//! composition reads once per process.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cairn_models::interface::session::{Cli, SessionHost};
use cairn_models::session::codex::managed::{Fetch, ManagedCodex, PinFetch, PinSource};
use cairn_models::session::codex::pin;
use cairn_models::session::local::LocalSessionHost;
use cairn_platform::services::process::RealProcessSpawner;

/// Resolving an installed Codex is local: it never asks for a pin or a
/// download.
struct Offline;

impl PinSource for Offline {
    fn fetch(&self, _etag: Option<&str>) -> Result<PinFetch, String> {
        panic!("resolving an installed Codex consulted the published pin");
    }
}

impl Fetch for Offline {
    fn download(&self, url: &str, _dest: &Path, _max_bytes: u64) -> Result<(), String> {
        panic!("resolving an installed Codex downloaded {url}");
    }
}

fn script(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A complete managed release of `version`, active, laid out as
/// `ManagedCodex` installs one. Returns its entrypoint.
fn install_release(root: &Path, version: &str, target: &str) -> PathBuf {
    let release = root.join(format!("{version}-{target}"));
    let entrypoint = release.join("bin").join("codex");
    script(&entrypoint, &format!("echo 'codex-cli {version}'"));
    std::fs::write(
        release.join("codex-package.json"),
        serde_json::json!({
            "layoutVersion": 1,
            "version": version,
            "target": target,
            "entrypoint": "bin/codex",
            "resourcesDir": "codex-resources",
            "pathDir": "codex-path",
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        root.join("current.json"),
        serde_json::json!({
            "version": version,
            "target": target,
            "integration": pin::CODEX_INTEGRATION,
            "pin_source": "embedded",
        })
        .to_string(),
    )
    .unwrap();
    entrypoint
}

#[test]
fn the_managed_codex_wins_over_a_codex_first_on_path() {
    let Some(target) = pin::host_target() else {
        return;
    };
    let version = pin::embedded_pin().version;
    let scratch = tempfile::tempdir().unwrap();

    // A different Codex, first on PATH and in the first directory the
    // user-PATH composition searches under HOME.
    let home = scratch.path().join("home");
    let decoy_dir = home.join(".claude").join("local").join("bin");
    script(&decoy_dir.join("codex"), "echo 'codex-cli 9.9.9'");
    let path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("HOME", &home);
    std::env::set_var("PATH", format!("{}:{path}", decoy_dir.display()));

    let root = scratch.path().join("tools").join("codex");
    let entrypoint = install_release(&root, &version, target);
    let codex = Arc::new(ManagedCodex::new(
        &root,
        scratch.path().join("bin"),
        Arc::new(Offline),
        Arc::new(Offline),
    ));
    let host = LocalSessionHost::new(
        Arc::new(RealProcessSpawner),
        scratch.path().join("codex-home"),
        codex,
    );

    let resolved = host
        .resolve_cli(Cli::Codex)
        .expect("the managed Codex resolves");
    assert_eq!(resolved, entrypoint.to_string_lossy());
}
