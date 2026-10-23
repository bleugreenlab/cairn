//! Check execution provenance shared by the runner and executor.
//!
//! Its source-sensitive build identity lives above the shared protocol and
//! storage crates, so a workload implementation edit cannot invalidate them.

use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};

const ENVIRONMENT_IDENTITY_VERSION: &str = "check-environment-v1";
const VERDICT_ENVIRONMENT_IDENTITY_VERSION: &str = "verdict-environment-v1";

/// Source-sensitive identity embedded by `build.rs`, deliberately equal in every
/// runner and executor linked against the same workspace source. The build fails
/// rather than falling back to package semver when this identity cannot be made.
pub const fn implementation_identity() -> &'static str {
    env!("CAIRN_CHECK_IMPLEMENTATION_ID")
}

/// Hash only the environment variables a check declares as verdict inputs.
/// Platform, architecture, toolchains, and build identity are deliberately not
/// part of this value; they are recorded as separate execution facts.
pub fn verdict_environment_hash(
    variable_names: impl IntoIterator<Item = String>,
    read_variable: impl Fn(&str) -> Option<String>,
) -> String {
    let mut names = variable_names.into_iter().collect::<Vec<_>>();
    names.sort();
    names.dedup();

    let mut hasher = Sha256::new();
    field(&mut hasher, VERDICT_ENVIRONMENT_IDENTITY_VERSION);
    for name in names {
        field(&mut hasher, &name);
        match read_variable(&name) {
            Some(value) => {
                field(&mut hasher, "present");
                field(&mut hasher, &value);
            }
            None => field(&mut hasher, "missing"),
        }
    }
    digest(hasher)
}

fn field(hasher: &mut Sha256, value: &str) {
    hasher.update(value.len().to_be_bytes());
    hasher.update(value.as_bytes());
}

fn option(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            field(hasher, "some");
            field(hasher, value);
        }
        None => field(hasher, "none"),
    }
}

fn strings(hasher: &mut Sha256, values: &[String]) {
    field(hasher, &values.len().to_string());
    for value in values {
        field(hasher, value);
    }
}

fn digest(hasher: Sha256) -> String {
    format!("{:x}", hasher.finalize())
}

pub fn fingerprint(
    os: &str,
    arch: &str,
    capabilities: impl IntoIterator<Item = String>,
    runner_build_id: Option<&str>,
    variable_names: impl IntoIterator<Item = String>,
    read_variable: impl Fn(&str) -> Option<String>,
) -> String {
    let mut capabilities = capabilities.into_iter().collect::<Vec<_>>();
    capabilities.sort();
    capabilities.dedup();
    let mut names = variable_names.into_iter().collect::<Vec<_>>();
    names.sort();
    names.dedup();

    let mut hasher = Sha256::new();
    field(&mut hasher, ENVIRONMENT_IDENTITY_VERSION);
    field(&mut hasher, os);
    field(&mut hasher, arch);
    option(&mut hasher, None);
    option(&mut hasher, None);
    strings(&mut hasher, &capabilities);
    option(&mut hasher, runner_build_id);
    for name in names {
        let mut value = Sha256::new();
        match read_variable(&name) {
            Some(secret) => {
                field(&mut value, "present");
                field(&mut value, &secret);
            }
            None => field(&mut value, "missing"),
        }
        field(&mut hasher, &name);
        field(&mut hasher, &digest(value));
    }
    digest(hasher)
}

const UNAVAILABLE: &str = "unavailable";
const RUSTC_VERSION_ARGS: &[&str] = &["--version", "--verbose"];

/// One tool's version output, or `None` (logged) when it cannot be read.
fn probe(command: &mut Command, program: &str) -> Option<String> {
    match command.output() {
        Ok(output) if output.status.success() => {
            let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if value.is_empty() {
                tracing::warn!(program, "toolchain probe returned empty output");
                None
            } else {
                Some(value)
            }
        }
        Ok(output) => {
            tracing::warn!(program, status = %output.status, stderr = %String::from_utf8_lossy(&output.stderr).trim(), "toolchain probe failed");
            None
        }
        Err(error) => {
            tracing::warn!(program, %error, "toolchain probe could not start");
            None
        }
    }
}

fn compose_identity(rustc: Option<String>, bun: Option<String>) -> String {
    format!(
        "rustc={};bun={}",
        rustc.as_deref().unwrap_or(UNAVAILABLE),
        bun.as_deref().unwrap_or(UNAVAILABLE)
    )
}

/// The toolchains this process resolves from its own working directory and the
/// agent shell's PATH, which is the machine's default unless the process happens
/// to start beneath a `rust-toolchain` pin. It identifies the host, not any
/// particular check: use [`toolchain_identity_in`] for what a command ran under.
pub fn toolchain_identity() -> &'static str {
    static IDENTITY: OnceLock<String> = OnceLock::new();
    IDENTITY.get_or_init(|| {
        let path = cairn_common::toolchain_path::agent_shell_path();
        let host = |program: &str, args: &[&str]| {
            probe(Command::new(program).args(args).env("PATH", &path), program)
        };
        compose_identity(
            host("rustc", RUSTC_VERSION_ARGS),
            host("bun", &["--version"]),
        )
    })
}

/// The toolchains a command launched in `dir` with `env` layered over this
/// process's environment resolves to, which is what that command actually ran
/// under. rustup chooses the Rust toolchain from the launch context (a
/// `RUSTUP_TOOLCHAIN` variable, a directory override, a `rust-toolchain[.toml]`
/// in `dir` or an ancestor, then the machine default), and `env` decides which
/// executables PATH finds, so the caller passes exactly the environment it
/// launches the command with.
///
/// Nothing is cached: no key short of the full launch context is sound, and a
/// probe costs a few milliseconds. The probe never installs a toolchain (a pin
/// this machine lacks is installed by the command that needs it); a probe that
/// fails reports `unavailable`.
pub fn toolchain_identity_in<K, V>(dir: &Path, env: impl IntoIterator<Item = (K, V)>) -> String
where
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    let env = env
        .into_iter()
        .map(|(name, value)| (name.as_ref().to_os_string(), value.as_ref().to_os_string()))
        .collect::<Vec<_>>();
    let launched = |program: &str, args: &[&str]| {
        probe(
            Command::new(program)
                .args(args)
                .current_dir(dir)
                .envs(env.iter().map(|(name, value)| (name, value)))
                .env("RUSTUP_AUTO_INSTALL", "0"),
            program,
        )
    };
    compose_identity(
        launched("rustc", RUSTC_VERSION_ARGS),
        launched("bun", &["--version"]),
    )
}

pub fn local_fingerprint(variable_names: impl IntoIterator<Item = String>) -> String {
    fingerprint(
        std::env::consts::OS,
        std::env::consts::ARCH,
        [toolchain_identity().to_string()],
        Some(implementation_identity()),
        variable_names,
        |name| std::env::var(name).ok(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_build_identity_participates_in_the_fingerprint() {
        let compose = |build: &str| {
            fingerprint(
                "linux",
                "x86_64",
                ["rustc=example;bun=example".to_string()],
                Some(build),
                Vec::<String>::new(),
                |_| None,
            )
        };
        assert_ne!(compose("source-v1:first"), compose("source-v1:second"));
    }

    #[test]
    fn verdict_environment_hash_is_order_independent_and_declared_only() {
        let read = |name: &str| Some(format!("value-{name}"));
        let first = verdict_environment_hash(["B".into(), "A".into(), "A".into()], read);
        let second = verdict_environment_hash(["A".into(), "B".into()], read);
        assert_eq!(first, second);
        assert_ne!(first, verdict_environment_hash(["A".into()], read));
    }

    /// Stand-ins for rustc and bun that report the launch context they were
    /// given, so the test sees exactly what a real toolchain resolver would.
    /// They use only shell builtins and expansions: the probe's PATH holds just
    /// this directory, so an external program such as `dirname` is not found.
    #[cfg(unix)]
    fn reporting_toolchain(root: &Path) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let bin = root.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        for (name, body) in [
            (
                "rustc",
                "echo \"dir=$(pwd -P) toolchain=${RUSTUP_TOOLCHAIN:-none} auto=${RUSTUP_AUTO_INSTALL:-unset}\"",
            ),
            ("bun", "echo \"bun-from-${0%/*}\""),
        ] {
            let path = bin.join(name);
            std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        bin
    }

    #[cfg(unix)]
    #[test]
    fn each_launch_context_is_probed_as_the_command_would_see_it() {
        let root = tempfile::tempdir().unwrap();
        let bin = reporting_toolchain(root.path());
        let first = root.path().join("first");
        let second = root.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let path = bin.to_str().unwrap();

        let pinned =
            toolchain_identity_in(&first, [("PATH", path), ("RUSTUP_TOOLCHAIN", "1.97.1")]);
        let overridden =
            toolchain_identity_in(&second, [("PATH", path), ("RUSTUP_TOOLCHAIN", "stable")]);

        let first = first.canonicalize().unwrap();
        let second = second.canonicalize().unwrap();
        assert_eq!(
            pinned,
            format!(
                "rustc=dir={} toolchain=1.97.1 auto=0;bun=bun-from-{path}",
                first.display()
            ),
            "the probe runs where the command runs, with its declared variables, without installing"
        );
        assert_eq!(
            overridden,
            format!(
                "rustc=dir={} toolchain=stable auto=0;bun=bun-from-{path}",
                second.display()
            ),
            "a second launch context is probed afresh rather than served the first one's answer"
        );
    }

    #[test]
    fn a_toolchain_that_cannot_be_started_is_reported_unavailable() {
        let root = tempfile::tempdir().unwrap();
        let empty = root.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert_eq!(
            toolchain_identity_in(root.path(), [("PATH", empty.as_os_str())]),
            "rustc=unavailable;bun=unavailable"
        );
    }

    #[test]
    fn verdict_environment_hash_distinguishes_missing_and_present_empty() {
        let missing = verdict_environment_hash(["A".into()], |_| None);
        let empty = verdict_environment_hash(["A".into()], |_| Some(String::new()));
        assert_ne!(missing, empty);
    }
}
