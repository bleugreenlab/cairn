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
/// in its definition), in the order a composed identity carries them. Every
/// identity composed now carries exactly these; one recorded before an
/// instrument joined simply lacks it, and reads it as `unrecorded`.
///
/// Probed is not the same as declarable: `node` is measured and recorded on
/// every row, but no verdict key carries it yet (see `RuntimeInstrument` in
/// `cairn-domain`), because the runner's expectation cannot resolve the pinned
/// Node an executor's lane runs.
pub const RUNTIME_INSTRUMENTS: &[&str] = &["rustc", "bun", "node"];

/// The probed instruments a verdict's keys and environment fingerprints may
/// carry: the declarable ones (`RuntimeInstrument::ALL` in `cairn-domain`, held
/// equal by a `cairn-core` test). Every key and fingerprint is built from
/// [`keyed_identity`]; the full identity, Node included, is the recorded
/// runtime and nothing else. A runner-side key built from the full identity
/// would move whenever the runner's host Node did (a `brew upgrade node`) and
/// strand every stored verdict.
pub const KEYED_INSTRUMENTS: &[&str] = &["rustc", "bun"];

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

/// The version of one instrument in a composed identity
/// (`rustc=…;bun=…;node=…`).
///
/// rustc's component is `rustc --version --verbose`, whose later lines name the
/// host triple and LLVM build. Only its first line (`rustc 1.97.1 (8bab26f4f
/// 2026-07-14)`) is the release, so a Linux run and a macOS run of one pinned
/// compiler read as the same version, which a check that trusts both platforms
/// needs. `None` for an instrument the identity does not record (one composed
/// before that instrument was probed, such as a `rustc=…;bun=…` row and
/// `node`), or for a value that is not a composed identity.
pub fn instrument_version<'a>(identity: &'a str, instrument: &str) -> Option<&'a str> {
    let value = identity_fields(identity)?
        .into_iter()
        .find_map(|(name, value)| (name == instrument).then_some(value))?;
    match instrument {
        "rustc" => value.lines().next().map(str::trim),
        _ => Some(value.trim()),
    }
}

/// The `name=value` fields of a composed identity, in order.
///
/// A field opens at the start of the identity or after a `;`, and only where a
/// probed instrument's `name=` follows; any other `;` belongs to the value
/// before it. So a tool whose version output happened to contain `;` cannot
/// open a field of its own. `None` unless the identity opens with a field.
fn identity_fields(identity: &str) -> Option<Vec<(&'static str, &str)>> {
    let opens = |rest: &str| {
        RUNTIME_INSTRUMENTS.iter().copied().find(|name| {
            rest.strip_prefix(*name)
                .is_some_and(|after| after.starts_with('='))
        })
    };
    let first = opens(identity)?;
    let mut starts = vec![(0, first)];
    for (at, _) in identity.match_indices(';') {
        if let Some(name) = opens(&identity[at + 1..]) {
            starts.push((at + 1, name));
        }
    }
    let fields = starts
        .iter()
        .enumerate()
        .map(|(index, &(start, name))| {
            let end = starts
                .get(index + 1)
                .map_or(identity.len(), |&(next, _)| next - 1);
            (name, &identity[start + name.len() + 1..end])
        })
        .collect();
    Some(fields)
}

/// `identity` with only its [`KEYED_INSTRUMENTS`] fields, each value exactly as
/// probed, in probe order: `rustc=…;bun=…`, which is byte-for-byte the identity
/// composed before Node was probed. A value that is not a composed identity is
/// returned as it is.
pub fn keyed_identity(identity: &str) -> String {
    let Some(fields) = identity_fields(identity) else {
        return identity.to_string();
    };
    fields
        .into_iter()
        .filter(|(name, _)| KEYED_INSTRUMENTS.contains(name))
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(";")
}

/// The instruments `identity` actually records, each with its version, in
/// probe order. What a page shows for a stored run: an instrument the run was
/// never probed for is left out rather than printed as `unrecorded`.
pub fn recorded_runtime(identity: &str) -> Vec<(String, String)> {
    RUNTIME_INSTRUMENTS
        .iter()
        .filter_map(|instrument| {
            instrument_version(identity, instrument)
                .map(|version| (instrument.to_string(), version.to_string()))
        })
        .collect()
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

/// The arguments that make `instrument` print its version.
fn version_args(instrument: &str) -> &'static [&'static str] {
    match instrument {
        "rustc" => RUSTC_VERSION_ARGS,
        _ => &["--version"],
    }
}

/// One probe's output as the identity records it. `node --version` prints
/// `v24.22.2` where the pin, and every other instrument, says `24.22.2`.
fn normalized_version<'a>(instrument: &str, output: &'a str) -> &'a str {
    match instrument {
        "node" => output.strip_prefix('v').unwrap_or(output),
        _ => output,
    }
}

/// `rustc=…;bun=…;node=…`: every [`RUNTIME_INSTRUMENTS`] entry, probed with
/// `probe(program, args)`, in order, with `unavailable` for one that could not
/// be read.
fn compose_identity(mut probe: impl FnMut(&str, &[&str]) -> Option<String>) -> String {
    RUNTIME_INSTRUMENTS
        .iter()
        .map(|instrument| {
            let output = probe(instrument, version_args(instrument));
            let version = output
                .as_deref()
                .map_or(UNAVAILABLE, |output| normalized_version(instrument, output));
            format!("{instrument}={version}")
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// The host identity [`toolchain_identity`] last probed. Each value is leaked so
/// callers keep a `&'static str`; a new one is minted only when
/// [`refresh_toolchain_identity`] finds the host's tools changed, so the leak is
/// bounded by how often a machine's tools actually change, not by how often
/// they are looked at.
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

/// [`keyed_identity`] of [`toolchain_identity`]: what every key and environment
/// fingerprint this process computes is made with. Minted again only when the
/// held identity is replaced.
pub fn keyed_toolchain_identity() -> &'static str {
    static KEYED: Mutex<Option<(&'static str, &'static str)>> = Mutex::new(None);
    let full = toolchain_identity();
    let mut held = KEYED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some((source, keyed)) = *held {
        if std::ptr::eq(source, full) {
            return keyed;
        }
    }
    let keyed: &'static str = Box::leak(keyed_identity(full).into_boxed_str());
    *held = Some((full, keyed));
    keyed
}

/// Probe the host identity again and, when it differs from the held one, hold
/// the new answer and drop every expectation probed under the old one. Returns
/// the new identity when it changed, `None` when the host still resolves the
/// same tools.
///
/// Cheap enough to call on a timer: it costs one probe of each instrument, and
/// mints nothing when nothing moved. That is what lets a host-side change no
/// install of ours made (an `apt install nodejs` on a resident machine) reach
/// the next verdict's fingerprint without a restart.
pub fn refresh_toolchain_identity() -> Option<&'static str> {
    hold_identity(probe_host_identity())
}

fn hold_identity(probed: String) -> Option<&'static str> {
    let mut held = IDENTITY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if held.is_some_and(|current| current == probed) {
        return None;
    }
    let identity: &'static str = Box::leak(probed.into_boxed_str());
    *held = Some(identity);
    EXPECTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    Some(identity)
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
    compose_identity(|program, args| {
        probe(Command::new(program).args(args).env("PATH", &path), program)
    })
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
    compose_identity(|program, args| {
        probe(
            Command::new(program)
                .args(args)
                .current_dir(dir)
                .envs(env.iter().map(|(name, value)| (name, value)))
                .env("RUSTUP_AUTO_INSTALL", "0"),
            program,
        )
    })
}

pub fn local_fingerprint(variable_names: impl IntoIterator<Item = String>) -> String {
    fingerprint(
        std::env::consts::OS,
        std::env::consts::ARCH,
        [keyed_toolchain_identity().to_string()],
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
                ["rustc=example;bun=example;node=example".to_string()],
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
            ("node", "echo \"v24.22.2-from-${0%/*}\""),
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
                "rustc=dir={} toolchain=1.97.1 auto=0;bun=bun-from-{path};node=24.22.2-from-{path}",
                first.display()
            ),
            "the probe runs where the command runs, with its declared variables, without installing"
        );
        assert_eq!(
            overridden,
            format!(
                "rustc=dir={} toolchain=stable auto=0;bun=bun-from-{path};node=24.22.2-from-{path}",
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
            "rustc=unavailable;bun=unavailable;node=unavailable"
        );
    }

    /// The held identity is what every verdict this process keys reads, so a
    /// refresh has to replace it rather than leave the first probe standing,
    /// and only when the tools moved, since it runs on every refresh pass.
    /// One test, because the held identity is process-wide.
    #[test]
    fn a_refresh_replaces_the_held_identity_only_when_it_changed() {
        let held = toolchain_identity();
        assert_eq!(
            toolchain_identity(),
            held,
            "the identity is held, not re-probed"
        );
        assert!(held.starts_with("rustc=") && held.contains(";bun=") && held.contains(";node="));

        let moved = "rustc=r;bun=1.4.4;node=18.19.1".to_string();
        assert_eq!(hold_identity(moved.clone()), Some(moved.as_str()));
        assert_eq!(toolchain_identity(), moved);
        assert_eq!(
            hold_identity(moved.clone()),
            None,
            "an unchanged probe mints and replaces nothing"
        );
        let installed = "rustc=r;bun=1.4.4;node=24.22.2".to_string();
        assert_eq!(hold_identity(installed.clone()), Some(installed.as_str()));
        assert_eq!(toolchain_identity(), installed);

        // Back to what this host really resolves, for anything after.
        refresh_toolchain_identity();
        assert_eq!(refresh_toolchain_identity(), None);
    }

    const RUSTC_VERBOSE: &str = "rustc 1.97.1 (8bab26f4f 2026-07-14)\nbinary: rustc\ncommit-hash: 8bab26f4f\nhost: x86_64-unknown-linux-gnu\nrelease: 1.97.1";

    fn identity(rustc: &str, bun: &str) -> String {
        identity_with_node(rustc, bun, "v24.22.2")
    }

    fn identity_with_node(rustc: &str, bun: &str, node: &str) -> String {
        compose_identity(|program, _| {
            Some(
                match program {
                    "rustc" => rustc,
                    "bun" => bun,
                    "node" => node,
                    other => panic!("probed {other}"),
                }
                .to_string(),
            )
        })
    }

    /// Keys are made from the keyed projection, which drops node and keeps
    /// rustc and bun byte for byte, so a key is the one main made and does not
    /// move with the host's Node.
    #[test]
    fn the_keyed_identity_is_the_pre_node_identity_whatever_node_is() {
        let before_node = format!("rustc={RUSTC_VERBOSE};bun=1.4.4");
        let homebrew = identity_with_node(RUSTC_VERBOSE, "1.4.4", "v26.12.1");
        let pinned = identity_with_node(RUSTC_VERBOSE, "1.4.4", "v24.22.2");
        assert_eq!(keyed_identity(&homebrew), before_node);
        assert_eq!(keyed_identity(&pinned), before_node);
        assert_eq!(keyed_identity(&before_node), before_node);
        assert_eq!(keyed_identity("tools-1"), "tools-1");
        let key = |toolchain: &str| {
            fingerprint(
                "linux",
                "x86_64",
                [keyed_identity(toolchain)],
                Some("build"),
                Vec::<String>::new(),
                |_| None,
            )
        };
        assert_eq!(key(&homebrew), key(&before_node));
        assert!(keyed_toolchain_identity().starts_with("rustc="));
        assert!(!keyed_toolchain_identity().contains(";node="));
    }

    /// Rows recorded before node was probed are `rustc=…;bun=…`; they must keep
    /// answering for rustc and bun exactly as before, and read node as absent.
    #[test]
    fn an_identity_recorded_before_node_still_reads_rustc_and_bun() {
        let old = format!("rustc={RUSTC_VERBOSE};bun=1.4.4");
        assert_eq!(
            instrument_version(&old, "rustc"),
            Some("rustc 1.97.1 (8bab26f4f 2026-07-14)")
        );
        assert_eq!(instrument_version(&old, "bun"), Some("1.4.4"));
        assert_eq!(instrument_version(&old, "node"), None);
        assert_eq!(
            runtime_versions(["node"], Some(&old)),
            vec![("node".to_string(), "unrecorded".to_string())]
        );
        assert_eq!(
            recorded_runtime(&old),
            vec![
                (
                    "rustc".to_string(),
                    "rustc 1.97.1 (8bab26f4f 2026-07-14)".to_string()
                ),
                ("bun".to_string(), "1.4.4".to_string()),
            ]
        );
    }

    #[test]
    fn a_three_instrument_identity_reads_each_and_drops_nodes_v() {
        let composed = identity_with_node(RUSTC_VERBOSE, "1.4.4", "v18.19.1");
        assert!(composed.ends_with(";bun=1.4.4;node=18.19.1"), "{composed}");
        assert_eq!(instrument_version(&composed, "bun"), Some("1.4.4"));
        assert_eq!(instrument_version(&composed, "node"), Some("18.19.1"));
        assert_eq!(
            instrument_version(&composed, "rustc"),
            Some("rustc 1.97.1 (8bab26f4f 2026-07-14)")
        );
        // A `;` inside a value that does not open an instrument's field stays in
        // the value.
        assert_eq!(
            instrument_version("rustc=a;b;bun=1.4.4;node=24.22.2", "rustc"),
            Some("a;b")
        );
        assert_eq!(
            instrument_version("rustc=a;bunx=1;bun=1.4.4", "bun"),
            Some("1.4.4")
        );
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
        assert_eq!(instrument_version(&linux, "node"), Some("24.22.2"));
        assert_eq!(instrument_version(&linux, "deno"), None);
        assert_eq!(instrument_version("tools-1", "bun"), None);
    }

    /// Only the declared instruments enter the verdict environment: two runs a
    /// Bun version apart separate a Bun check and leave a rustc check whole, and
    /// two runs a Node apart separate a Node check and leave a Bun check whole.
    #[test]
    fn only_declared_instruments_enter_the_verdict_environment() {
        let host_node = identity_with_node(RUSTC_VERBOSE, "1.4.4", "v18.19.1");
        let pinned_node = identity_with_node(RUSTC_VERBOSE, "1.4.4", "v24.22.2");
        let runtime_key = |instruments: &[&str], toolchain: &str| {
            verdict_environment_with_runtime(
                "declared",
                &runtime_versions(instruments.iter().copied(), Some(toolchain)),
            )
        };
        assert_ne!(
            runtime_key(&["node"], &host_node),
            runtime_key(&["node"], &pinned_node)
        );
        assert_eq!(
            runtime_key(&["bun"], &host_node),
            runtime_key(&["bun"], &pinned_node)
        );

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
        for probed in [
            "rustc=unavailable;bun=unavailable;node=unavailable",
            "rustc=x;bun=1.4.4;node=24.22.2",
        ] {
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
