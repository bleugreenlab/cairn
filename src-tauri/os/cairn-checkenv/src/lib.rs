//! Check execution provenance shared by the runner and executor.
//!
//! Its source-sensitive build identity lives above the shared protocol and
//! storage crates, so a workload implementation edit cannot invalidate them.

use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, RwLock};

use sha2::{Digest, Sha256};

const ENVIRONMENT_IDENTITY_VERSION: &str = "check-environment-v1";
const VERDICT_ENVIRONMENT_IDENTITY_VERSION: &str = "verdict-environment-v1";
const VERDICT_RUNTIME_IDENTITY_VERSION: &str = "verdict-runtime-v1";

/// The toolchains [`toolchain_identity`] and [`toolchain_identity_in`] probe,
/// by the name a check declares one as a runtime instrument under (`runtime:`
/// in its definition). Every composed identity carries exactly these.
pub const RUNTIME_INSTRUMENTS: &[&str] = &["rustc", "bun"];

/// The version an instrument reads as when the run's toolchain was never
/// measured (a lane whose executor report was lost). No probe produces it, so a
/// verdict whose runtime nobody measured never answers a lookup that expects one.
const UNRECORDED: &str = "unrecorded";

/// Source-sensitive identity embedded by `build.rs`, deliberately equal in every
/// runner and executor linked against the same workspace source. The build fails
/// rather than falling back to package semver when this identity cannot be made.
pub const fn implementation_identity() -> &'static str {
    env!("CAIRN_CHECK_IMPLEMENTATION_ID")
}

/// Hash only the environment variables a check declares as verdict inputs.
/// Platform, architecture, and build identity are deliberately not part of this
/// value; they are recorded as separate execution facts. The toolchains a check
/// declares as its runtime instruments join it through
/// [`verdict_environment_with_runtime`], which every reuse key is made with.
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

/// A check's verdict environment: its declared-variable hash
/// ([`verdict_environment_hash`]) extended by the version of every toolchain
/// the check declares as a runtime instrument.
///
/// A check's runtime is an axis of its verdict when the check runs under it: the
/// same `bun test` on the same tree fails 101 of 839 tests under Bun 1.3.14 and
/// none under 1.4.4. Reuse matches this value exactly, so a verdict produced
/// under one instrument version never answers a lookup that expects another.
/// Only the declared instruments enter it, so a Bun bump leaves a rustc-only
/// check's verdicts standing and a rustc bump leaves a Bun-only check's.
///
/// `runtime` pairs each instrument with its version, from [`runtime_versions`];
/// order and repeats do not matter.
pub fn verdict_environment_with_runtime(declared: &str, runtime: &[(String, String)]) -> String {
    let mut runtime = runtime.to_vec();
    runtime.sort();
    runtime.dedup();
    let mut hasher = Sha256::new();
    field(&mut hasher, VERDICT_RUNTIME_IDENTITY_VERSION);
    field(&mut hasher, declared);
    field(&mut hasher, &runtime.len().to_string());
    for (instrument, version) in &runtime {
        field(&mut hasher, instrument);
        field(&mut hasher, version);
    }
    digest(hasher)
}

/// The version of one instrument in a composed identity (`rustc=…;bun=…`).
///
/// rustc's component is `rustc --version --verbose`, whose later lines name the
/// host triple and LLVM build. Only its first line (`rustc 1.97.1 (8bab26f4f
/// 2026-07-14)`) is the release, so a Linux run and a macOS run of one pinned
/// compiler read as the same version, which a check that trusts both platforms
/// needs. `None` for an instrument this crate does not probe, or for a value
/// that is not a composed identity.
pub fn instrument_version<'a>(identity: &'a str, instrument: &str) -> Option<&'a str> {
    let (rustc, bun) = identity.strip_prefix("rustc=")?.rsplit_once(";bun=")?;
    match instrument {
        "rustc" => rustc.lines().next().map(str::trim),
        "bun" => Some(bun.trim()),
        _ => None,
    }
}

/// Each of `instruments` paired with the version `identity` records for it,
/// the input [`verdict_environment_with_runtime`] takes.
///
/// An identity that was never recorded reads `unrecorded` for every
/// instrument, and one that cannot be parsed reads the same, so neither can
/// ever equal a probed expectation.
pub fn runtime_versions<'a>(
    instruments: impl IntoIterator<Item = &'a str>,
    identity: Option<&str>,
) -> Vec<(String, String)> {
    instruments
        .into_iter()
        .map(|instrument| {
            let version = identity
                .and_then(|identity| instrument_version(identity, instrument))
                .unwrap_or(UNRECORDED);
            (instrument.to_string(), version.to_string())
        })
        .collect()
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

/// The host identity [`toolchain_identity`] last probed. Each value is leaked so
/// callers keep a `&'static str`; a new one is minted only when
/// [`refresh_toolchain_identity`] runs, which is once per tool install, so the
/// leak is bounded by how often a machine's tools actually change.
static IDENTITY: RwLock<Option<&'static str>> = RwLock::new(None);

/// The toolchains this process resolves from its own working directory and the
/// PATH it installed for itself at startup (the agent shell PATH for a runner,
/// the executor's PATH led by its own home's `bin` for an executor), which is
/// the machine's default unless the process happens to start beneath a
/// `rust-toolchain` pin. It identifies the host, not any particular check: use
/// [`toolchain_identity_in`] for what a command ran under.
///
/// Probed once and then held, because it keys every check verdict this process
/// computes. A process that changes what its PATH resolves (an executor
/// installing the published `bun` into its own bin) calls
/// [`refresh_toolchain_identity`], or the held answer would keep naming the
/// tool it replaced until the process restarted.
pub fn toolchain_identity() -> &'static str {
    if let Some(identity) = *IDENTITY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        return identity;
    }
    let mut held = IDENTITY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *held.get_or_insert_with(|| Box::leak(probe_host_identity().into_boxed_str()))
}

/// Probe the host identity again and hold the new answer, returning it. Call
/// after anything that changes which `rustc` or `bun` this process's PATH
/// resolves.
pub fn refresh_toolchain_identity() -> &'static str {
    let identity: &'static str = Box::leak(probe_host_identity().into_boxed_str());
    *IDENTITY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(identity);
    EXPECTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    identity
}

/// Expectations [`expected_toolchain_identity_in`] has probed, keyed by the
/// Rust toolchain pin in force where each was probed.
static EXPECTED: Mutex<Vec<(Option<String>, String)>> = Mutex::new(Vec::new());

/// The toolchains a check launched by this process in the checkout at `dir`
/// would resolve, which is what a reuse lookup expects a stored verdict's
/// runtime instruments to be.
///
/// It is the same question [`toolchain_identity_in`] answers for a command that
/// ran, asked before anything runs, because the two must be comparable: an
/// executor records the toolchain resolved inside the checkout, where rustup
/// applies the tree's `rust-toolchain` pin. The host identity
/// ([`toolchain_identity`]) is probed outside any checkout and names rustup's
/// default instead, which a `rustup update` moves off the pin while every lane
/// keeps compiling with the pin.
///
/// Held per pin rather than per directory, because every worktree of one tree
/// resolves the same pin and planning runs on every commit; the probe itself
/// costs two process starts. An answer in which any instrument is unavailable
/// is not held, so a toolchain installed later is seen on the next plan. A
/// stale answer costs reuse, never correctness: a stored verdict answers only
/// when its own measured runtime equals the expectation, so a wrong expectation
/// can only miss. [`refresh_toolchain_identity`] drops every held answer. A
/// `dir` that does not exist has no launch context, and reads as the host.
pub fn expected_toolchain_identity_in(dir: &Path) -> String {
    if !dir.is_dir() {
        return toolchain_identity().to_string();
    }
    let pin = rust_toolchain_pin(dir);
    let mut held = EXPECTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some((_, identity)) = held.iter().find(|(key, _)| *key == pin) {
        return identity.clone();
    }
    let identity = toolchain_identity_in(dir, std::iter::empty::<(String, String)>());
    if !identity.contains(UNAVAILABLE) {
        held.push((pin, identity.clone()));
    }
    identity
}

/// The `rust-toolchain` file rustup would apply in `dir`: the nearest one in
/// `dir` or an ancestor, named and with its contents, since either decides
/// which compiler resolves. `None` when no ancestor holds one.
fn rust_toolchain_pin(dir: &Path) -> Option<String> {
    dir.ancestors().find_map(|ancestor| {
        ["rust-toolchain", "rust-toolchain.toml"]
            .into_iter()
            .find_map(|name| {
                std::fs::read_to_string(ancestor.join(name))
                    .ok()
                    .map(|contents| format!("{name}\n{contents}"))
            })
    })
}

fn probe_host_identity() -> String {
    let path = host_probe_path();
    let host = |program: &str, args: &[&str]| {
        probe(Command::new(program).args(args).env("PATH", &path), program)
    };
    compose_identity(
        host("rustc", RUSTC_VERSION_ARGS),
        host("bun", &["--version"]),
    )
}

/// The PATH the host identity is probed under: this process's own, which both
/// the runner (`install_process_path`) and the executor
/// (`install_executor_process_path`) set before anything else runs. Only a
/// process that installed none falls back to the agent shell PATH. Probing the
/// agent shell PATH from an executor would skip the executor's own bin and
/// fingerprint whatever older install `~/.cairn/bin` happens to hold.
fn host_probe_path() -> std::ffi::OsString {
    std::env::var_os("PATH")
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| cairn_common::toolchain_path::agent_shell_path().into())
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

    /// The held identity is what every verdict this process keys reads, so a
    /// refresh has to replace it rather than leave the first probe standing.
    #[test]
    fn a_refresh_replaces_the_held_identity() {
        let held = toolchain_identity();
        assert_eq!(
            toolchain_identity(),
            held,
            "the identity is held, not re-probed"
        );
        let refreshed = refresh_toolchain_identity();
        assert_eq!(toolchain_identity(), refreshed);
        assert!(refreshed.starts_with("rustc=") && refreshed.contains(";bun="));
    }

    const RUSTC_VERBOSE: &str = "rustc 1.97.1 (8bab26f4f 2026-07-14)\nbinary: rustc\ncommit-hash: 8bab26f4f\nhost: x86_64-unknown-linux-gnu\nrelease: 1.97.1";

    fn identity(rustc: &str, bun: &str) -> String {
        compose_identity(Some(rustc.to_string()), Some(bun.to_string()))
    }

    /// The version of each instrument is read from the composed identity, and
    /// rustc's is its release line alone, so one pinned compiler reads the same
    /// on every host triple.
    #[test]
    fn instrument_versions_are_read_from_a_composed_identity() {
        let linux = identity(RUSTC_VERBOSE, "1.4.4");
        let macos = identity(
            &RUSTC_VERBOSE.replace("x86_64-unknown-linux-gnu", "aarch64-apple-darwin"),
            "1.4.4",
        );
        assert_eq!(
            instrument_version(&linux, "rustc"),
            Some("rustc 1.97.1 (8bab26f4f 2026-07-14)")
        );
        assert_eq!(instrument_version(&linux, "bun"), Some("1.4.4"));
        assert_eq!(
            instrument_version(&linux, "rustc"),
            instrument_version(&macos, "rustc")
        );
        assert_eq!(
            instrument_version("rustc=unavailable;bun=1.3.14", "rustc"),
            Some("unavailable")
        );
        assert_eq!(instrument_version(&linux, "node"), None);
        assert_eq!(instrument_version("tools-1", "bun"), None);
    }

    /// Only the declared instruments enter the verdict environment: two runs a
    /// Bun version apart separate a Bun check and leave a rustc check whole.
    #[test]
    fn only_declared_instruments_enter_the_verdict_environment() {
        let old_bun = identity(RUSTC_VERBOSE, "1.3.14");
        let new_bun = identity(RUSTC_VERBOSE, "1.4.4");
        let key = |instruments: &[&str], toolchain: &str| {
            verdict_environment_with_runtime(
                "declared",
                &runtime_versions(instruments.iter().copied(), Some(toolchain)),
            )
        };
        assert_ne!(key(&["bun"], &old_bun), key(&["bun"], &new_bun));
        assert_eq!(key(&["rustc"], &old_bun), key(&["rustc"], &new_bun));
        assert_ne!(
            key(&["rustc", "bun"], &old_bun),
            key(&["rustc", "bun"], &new_bun)
        );
        assert_eq!(
            key(&["bun", "rustc", "bun"], &new_bun),
            key(&["rustc", "bun"], &new_bun),
            "order and repeats do not move the key"
        );
        assert_ne!(
            key(&[], &new_bun),
            verdict_environment_with_runtime("other-declared", &[]),
            "the declared variables still decide the key with no instrument"
        );
    }

    /// A run whose toolchain nobody measured has a runtime no probe produces,
    /// so it cannot stand in for any expectation, even an unavailable one.
    #[test]
    fn an_unrecorded_runtime_matches_no_expectation() {
        let unrecorded = runtime_versions(["bun"], None);
        assert_eq!(
            unrecorded,
            vec![("bun".to_string(), "unrecorded".to_string())]
        );
        for probed in ["rustc=unavailable;bun=unavailable", "rustc=x;bun=1.4.4"] {
            assert_ne!(runtime_versions(["bun"], Some(probed)), unrecorded);
        }
    }

    #[test]
    fn the_nearest_rust_toolchain_pin_decides_the_expectation_key() {
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("a/b");
        std::fs::create_dir_all(&nested).unwrap();
        assert_eq!(rust_toolchain_pin(&nested), None);
        std::fs::write(
            root.path().join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.97.1\"\n",
        )
        .unwrap();
        let pinned = rust_toolchain_pin(&nested).expect("an ancestor's pin applies");
        assert!(pinned.starts_with("rust-toolchain.toml\n") && pinned.contains("1.97.1"));
        std::fs::write(root.path().join("a/rust-toolchain"), "1.98.0\n").unwrap();
        assert_eq!(
            rust_toolchain_pin(&nested).as_deref(),
            Some("rust-toolchain\n1.98.0\n"),
            "the nearer pin wins"
        );
    }

    #[test]
    fn verdict_environment_hash_distinguishes_missing_and_present_empty() {
        let missing = verdict_environment_hash(["A".into()], |_| None);
        let empty = verdict_environment_hash(["A".into()], |_| Some(String::new()));
        assert_ne!(missing, empty);
    }
}
