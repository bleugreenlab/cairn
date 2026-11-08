//! Shared secret utilities for the two credentials a Cairn install holds.
//!
//! `mcp_auth_secret` authenticates MCP callbacks from outside any run, and
//! reaches no process Cairn spawns ([`FORBIDDEN_LAUNCH_ENV`]). `operator_auth_secret` authenticates a desktop operator
//! answering an authority prompt and is deliberately handed to nothing but the
//! native desktop shell. They are the same shape and must never be the same
//! value, which is why the two file names live here beside each other.

use std::fmt;

/// Number of bytes in a shared secret.
pub const SECRET_LEN: usize = 32;

/// File name of the MCP callback credential, under a Cairn home.
pub const MCP_SECRET_FILE: &str = "mcp_auth_secret";

/// File name of the desktop operator credential, under a Cairn home.
///
/// This one is a capability: presenting it to `/api/invoke` is what lets an
/// answer to an authority prompt mint an authority grant. It is never placed in
/// an agent's environment. On macOS, agent shells are refused the file in every
/// Cairn home by the kernel (`cairn_sandbox::operator_credential`, which also
/// lists what it does not yet cover and how Linux and Windows report it), and
/// `cairn_core::authorization::protected` refuses it to the structured verbs.
pub const OPERATOR_SECRET_FILE: &str = "operator_auth_secret";

/// The HTTP header the desktop operator credential is presented in.
///
/// A wire contract between the native desktop shell that sends it and the
/// runner transport that checks it, which share no other type. It lives beside
/// the file name so a rename cannot move one without the other.
pub const OPERATOR_TOKEN_HEADER: &str = "x-cairn-operator-token";

/// A shared secret whose bytes cannot escape by accident.
///
/// There is no `Serialize`, so a struct that holds one and derives `Serialize`
/// does not compile — wholesale serialization of app or session state cannot
/// carry a secret out. `Debug` and `Display` redact, so an incidental `{:?}` in
/// a log line cannot either. Everything else has to go through [`Self::expose`]
/// or [`Self::expose_base64`], which makes every deliberate exposure a named,
/// greppable, countable call site rather than a discipline someone has to
/// remember.
///
/// The wrapper narrows the surface; it does not close it. What pins the set of
/// deliberate exposures is a test that enumerates them
/// (`cairn_core::mcp::auth::tests::the_operator_credential_has_no_unexpected_exposure_sites`),
/// which fails naming the new site on the change that adds one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SharedSecret([u8; SECRET_LEN]);

impl SharedSecret {
    /// Adopt raw bytes as a secret. Rejects any length but [`SECRET_LEN`], so a
    /// truncated or corrupt secret file can never become a short credential
    /// that still validates.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SECRET_LEN {
            return None;
        }
        let mut secret = [0u8; SECRET_LEN];
        secret.copy_from_slice(bytes);
        Some(Self(secret))
    }

    /// Adopt a base64-encoded secret.
    pub fn from_base64(encoded: &str) -> Option<Self> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        Self::from_bytes(&bytes)
    }

    /// The raw bytes. A deliberate exposure; see the type's documentation.
    pub fn expose(&self) -> &[u8; SECRET_LEN] {
        &self.0
    }

    /// The wire encoding used by both bearer-token paths. A deliberate
    /// exposure; see the type's documentation.
    pub fn expose_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(self.0)
    }

    /// Whether `token` is this secret's base64 encoding.
    ///
    /// Compared in constant time over the decoded bytes. A wrong length short
    /// circuits, which leaks only the length of a fixed public-format token,
    /// while a same-length candidate is compared without an early exit so the
    /// number of leading correct bytes is not observable through timing.
    pub fn matches_token(&self, token: &str) -> bool {
        let Some(candidate) = Self::from_base64(token) else {
            return false;
        };
        let mut difference = 0u8;
        for (left, right) in self.0.iter().zip(candidate.0.iter()) {
            difference |= left ^ right;
        }
        difference == 0
    }
}

impl fmt::Debug for SharedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SharedSecret(<redacted>)")
    }
}

impl fmt::Display for SharedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Decode a base64-encoded secret.
pub fn decode_secret(encoded: &str) -> Option<[u8; SECRET_LEN]> {
    SharedSecret::from_base64(encoded).map(|secret| *secret.expose())
}

/// Load a bearer token from a named secret file under a Cairn home.
fn load_token_from(home: &std::path::Path, file_name: &str) -> Option<String> {
    let secret = std::fs::read(home.join(file_name)).ok()?;
    SharedSecret::from_bytes(&secret).map(|secret| secret.expose_base64())
}

/// Load the MCP bearer token from the `mcp_auth_secret` under a specific home
/// directory. Used to authenticate to a *different* Cairn instance (e.g. a
/// running `dev:instance`) whose secret lives under its own `CAIRN_HOME`.
pub fn load_mcp_token_from(home: &std::path::Path) -> Option<String> {
    load_token_from(home, MCP_SECRET_FILE)
}

/// Load the local MCP bearer token from the on-disk secret under this process's
/// own Cairn home.
pub fn load_local_mcp_token() -> Option<String> {
    load_mcp_token_from(&crate::paths::cairn_home())
}

/// The callback credential a local client presents: the one its environment
/// gave it, or, for a process outside any run, this home's machine credential.
///
/// A process in a run never falls back to the file. The machine credential
/// speaks for no run, so a run reaching for it can only have lost the credential
/// it was given, and that is said here rather than left to surface as a refused
/// call.
pub fn callback_token(
    explicit: Option<String>,
    run_id: Option<&str>,
    load_local: impl FnOnce() -> Option<String>,
) -> Option<String> {
    if explicit.is_some() {
        return explicit;
    }
    match run_id.filter(|run_id| !run_id.is_empty()) {
        Some(run_id) => {
            eprintln!(
                "cairn: this process belongs to run {run_id} but has no CAIRN_MCP_SECRET, and a \
                 run does not read the machine credential from disk"
            );
            None
        }
        None => load_local(),
    }
}

/// The variable a callback credential travels in where one still sits in a
/// launch environment: Windows, which has no lineage socket. `cairn-cmd` and
/// the SDKs read it there.
pub const CALLBACK_CREDENTIAL_ENV: &str = "CAIRN_MCP_SECRET";

/// The credentials an executor is launched with: its runner token (the machine
/// credential, on a colocated executor), its enrollment token, and the nonce its
/// supervisor admits it by. They are the executor's alone, on every platform.
pub const EXECUTOR_LAUNCH_CREDENTIAL_ENV: [&str; 3] = [
    crate::executor_protocol::EXECUTOR_AUTH_TOKEN_ENV,
    crate::executor_protocol::EXECUTOR_ENROLLMENT_TOKEN_ENV,
    crate::executor_protocol::EXECUTOR_SUPERVISION_NONCE_ENV,
];

/// Every credential that never reaches the launch environment of a process
/// Cairn spawns, whatever the spawning process's own environment holds (tenet
/// #1381: credentials travel by inherited descriptor or broker). On macOS and
/// Linux that includes the callback credential, because a caller there is
/// identified by its lineage on the callback socket. Windows has no socket, so
/// each process there still carries its run-bound callback credential, the
/// residual `docs/identity.md` names.
#[cfg(unix)]
pub const FORBIDDEN_LAUNCH_ENV: [&str; 4] = [
    CALLBACK_CREDENTIAL_ENV,
    EXECUTOR_LAUNCH_CREDENTIAL_ENV[0],
    EXECUTOR_LAUNCH_CREDENTIAL_ENV[1],
    EXECUTOR_LAUNCH_CREDENTIAL_ENV[2],
];
#[cfg(not(unix))]
pub const FORBIDDEN_LAUNCH_ENV: [&str; 3] = EXECUTOR_LAUNCH_CREDENTIAL_ENV;

/// Dummy credentials for the tests that pin [`FORBIDDEN_LAUNCH_ENV`], one per
/// process class: a value for each credential, planted wherever a launch could
/// pick it up, and a check that finds any of them in what a spawned process
/// saw, by name or by value, so a credential moved under a new name is found
/// too.
#[cfg(any(test, feature = "test-utils"))]
pub mod launch_canary {
    use super::{CALLBACK_CREDENTIAL_ENV, EXECUTOR_LAUNCH_CREDENTIAL_ENV, FORBIDDEN_LAUNCH_ENV};

    /// One dummy value per credential name.
    pub const CANARIES: [(&str, &str); 4] = [
        (CALLBACK_CREDENTIAL_ENV, "canary-callback-credential-6454"),
        (
            EXECUTOR_LAUNCH_CREDENTIAL_ENV[0],
            "canary-runner-token-6454",
        ),
        (
            EXECUTOR_LAUNCH_CREDENTIAL_ENV[1],
            "canary-enrollment-token-6454",
        ),
        (
            EXECUTOR_LAUNCH_CREDENTIAL_ENV[2],
            "canary-supervision-nonce-6454",
        ),
    ];

    /// Set only in the child [`in_planted_child`] starts.
    const CHILD_ENV: &str = "CAIRN_LAUNCH_CANARY_CHILD";

    /// Whether this is the planted child of `test`, which should now run its
    /// body.
    ///
    /// Called first in the test's own process, it runs `test` again in a child
    /// of this test binary whose own environment holds every canary, as a
    /// runner's or an executor's environment could, panics unless that child
    /// ran it and passed, and answers `false`, so the caller returns. In the
    /// child it answers `true`. The canaries never enter the shared test
    /// process's environment. `module` is the caller's `module_path!()`, and
    /// `test` its function's name.
    pub fn in_planted_child(module: &str, test: &str) -> bool {
        if std::env::var_os(CHILD_ENV).is_some() {
            return true;
        }
        // A libtest name omits the crate, the first segment of a module path.
        let name = match module.split_once("::") {
            Some((_, path)) => format!("{path}::{test}"),
            None => test.to_owned(),
        };
        let output =
            std::process::Command::new(std::env::current_exe().expect("the running test binary"))
                .args(["--exact", &name, "--nocapture", "--test-threads=1"])
                .env(CHILD_ENV, "1")
                .envs(CANARIES)
                .output()
                .expect("run the test binary again");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{name} failed with every canary planted:\n{stdout}\n{stderr}"
        );
        assert!(
            stdout.contains(" 1 passed"),
            "the planted child did not run {name}:\n{stdout}\n{stderr}"
        );
        false
    }

    /// Every credential `observed` shows: a name in [`FORBIDDEN_LAUNCH_ENV`]
    /// set to anything; a canary or one of `composed` (a credential the test
    /// knows was minted for this launch) under any name; or, under any name,
    /// any credential registered in this process for output scrubbing
    /// (`security::registry`), which every batch context, resident capability
    /// and run bearer is while it lives. A test that runs the runner and its
    /// executors in one process therefore catches a credential it never learns
    /// the value of, provided it looks while the process still holds it.
    /// `observed` is a process's environment as `NAME=value` lines (what `env`
    /// prints), as JSON, or as any text derived from it, such as its argv.
    pub fn leaks(observed: &str, composed: &[&str]) -> Vec<String> {
        let mut found = Vec::new();
        for name in FORBIDDEN_LAUNCH_ENV {
            let assignment = format!("{name}=");
            if observed.lines().any(|line| line.starts_with(&assignment))
                || observed.contains(&format!("\"{name}\""))
            {
                found.push(format!("`{name}` is set"));
            }
        }
        for value in CANARIES
            .iter()
            .map(|(_, value)| *value)
            .chain(composed.iter().copied())
            .filter(|value| !value.is_empty())
        {
            if observed.contains(value) {
                found.push(format!("the credential `{value}` appears"));
            }
        }
        let mut registered = crate::security::sanitize::Sanitizer::exact();
        let _ = registered.text(observed);
        for detection in registered.detections().entries() {
            if detection.rule.is_exact() {
                found.push(format!(
                    "the registered credential {:?} ({:?}) appears",
                    detection.secret_id, detection.category
                ));
            }
        }
        found
    }

    /// Panic, naming `class`, if `observed` shows any credential.
    pub fn assert_no_credential(class: &str, observed: &str, composed: &[&str]) {
        let leaks = leaks(observed, composed);
        assert!(
            leaks.is_empty(),
            "{class}'s launch environment carries a credential: {leaks:?}\n{observed}"
        );
    }

    /// Panic, naming `class`, unless `observed` names the callback socket
    /// `socket` under `CAIRN_CALLBACK_SOCKET`, as an `env` line or within a
    /// rendered MCP server entry.
    #[cfg(unix)]
    pub fn assert_names_socket(class: &str, observed: &str, socket: &str) {
        let name = crate::lineage::CALLBACK_SOCKET_ENV;
        let named = observed
            .lines()
            .any(|line| line == format!("{name}={socket}"))
            || observed.lines().any(|line| {
                line.find(name)
                    .is_some_and(|at| line[at..].contains(&format!("\"{socket}\"")))
            });
        assert!(
            named,
            "{class} is not named its callback socket `{socket}`:\n{observed}"
        );
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// The executor's own credentials reach no child anywhere. The callback
        /// credential is forbidden on macOS and Linux only: Windows has no
        /// lineage socket, so each seam there carries a run-bound one in
        /// `CAIRN_MCP_SECRET`, the residual `docs/identity.md` names and the
        /// seams' own Windows tests assert (`configure_relay_environment`'s in
        /// `cairn-exec`, `CAIRN_STDIO_INHERITED_ENV`'s in `cairn-models`).
        #[test]
        fn the_callback_credential_is_a_named_residual_on_windows_alone() {
            for name in EXECUTOR_LAUNCH_CREDENTIAL_ENV {
                assert!(FORBIDDEN_LAUNCH_ENV.contains(&name), "{name}");
            }
            assert_eq!(
                FORBIDDEN_LAUNCH_ENV.contains(&CALLBACK_CREDENTIAL_ENV),
                cfg!(unix)
            );
        }

        #[test]
        fn a_credential_is_found_by_its_name_or_by_its_value() {
            assert!(leaks("PATH=/bin\nHOME=/h\n", &["minted"]).is_empty());
            for (name, value) in CANARIES {
                if FORBIDDEN_LAUNCH_ENV.contains(&name) {
                    // A name is matched whole, not as a prefix.
                    assert!(leaks(&format!("{name}_HINT=x"), &[]).is_empty());
                    assert_eq!(
                        leaks(&format!("{name}=x"), &[]),
                        [format!("`{name}` is set")]
                    );
                    assert_eq!(leaks(&format!("{{\"{name}\":\"x\"}}"), &[]).len(), 1);
                }
                assert_eq!(
                    leaks(&format!("RENAMED={value}"), &[]),
                    [format!("the credential `{value}` appears")]
                );
            }
            assert_eq!(leaks("--flag minted", &["minted"]).len(), 1);
        }

        /// A credential this process registered for scrubbing is found under
        /// any name, though the test never learned its value.
        #[test]
        fn a_registered_credential_is_found_under_any_name() {
            use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};
            const MINTED: &str = "canary-registered-capability-6454-Qz8";
            assert!(leaks(&format!("RENAMED={MINTED}"), &[]).is_empty());
            let _held = registry()
                .register(
                    SecretId::new("launch-canary:registered"),
                    SecretCategory::CallbackCredential,
                    "unit test",
                    SecretMaterial::from_bytes(MINTED.as_bytes()),
                )
                .expect("registerable");
            let found = leaks(&format!("RENAMED={MINTED}"), &[]);
            assert_eq!(found.len(), 1, "{found:?}");
            assert!(found[0].contains("launch-canary:registered"), "{found:?}");
            assert!(
                !found[0].contains(MINTED),
                "a finding never repeats the value"
            );
        }
    }
}

/// Load the desktop operator token from the `operator_auth_secret` under a
/// specific home directory.
///
/// The native desktop shell reads it on behalf of a human operator, forwarding
/// an operator answer to its own runner with the caller's own permissions; where the operator credential boundary is enforced, an agent
/// shell that runs the CLI is refused the file like any other read. Nothing in an agent's process tree calls
/// this, and adding a caller that runs inside one would defeat the boundary the
/// credential exists to draw.
pub fn load_operator_token_from(home: &std::path::Path) -> Option<String> {
    load_token_from(home, OPERATOR_SECRET_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_credential_files_are_distinct() {
        // Same shape, same directory, opposite audiences: the MCP secret is in
        // every agent's environment and the operator secret must never be. If
        // these ever named one file, the desktop credential would be the token
        // the agent already holds and the boundary would be decorative.
        assert_ne!(MCP_SECRET_FILE, OPERATOR_SECRET_FILE);
    }

    #[test]
    fn a_secret_redacts_itself_in_every_formatting_path() {
        let secret = SharedSecret::from_bytes(&[0x42; SECRET_LEN]).unwrap();
        let encoded = secret.expose_base64();

        for rendered in [format!("{secret:?}"), format!("{secret}")] {
            assert!(
                !rendered.contains(&encoded),
                "a formatting path emitted the secret: {rendered}"
            );
            assert!(rendered.contains("redacted"), "got {rendered}");
        }
    }

    #[test]
    fn token_matching_accepts_only_the_exact_secret() {
        let secret = SharedSecret::from_bytes(&[0x42; SECRET_LEN]).unwrap();

        assert!(secret.matches_token(&secret.expose_base64()));
        assert!(!secret.matches_token(""));
        assert!(!secret.matches_token("not base64 at all!!"));
        assert!(!secret.matches_token(
            &SharedSecret::from_bytes(&[0x43; SECRET_LEN])
                .unwrap()
                .expose_base64()
        ));

        // A prefix of the right secret is not the right secret, and neither is
        // a longer value that starts with it.
        let mut truncated = [0x42u8; SECRET_LEN];
        truncated[SECRET_LEN - 1] = 0;
        assert!(!secret.matches_token(
            &SharedSecret::from_bytes(&truncated)
                .unwrap()
                .expose_base64()
        ));
    }

    #[test]
    fn a_wrong_length_secret_file_yields_no_token() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join(OPERATOR_SECRET_FILE), [0u8; 8]).unwrap();

        assert!(load_operator_token_from(home.path()).is_none());
    }

    #[test]
    fn only_a_process_outside_any_run_falls_back_to_the_machine_credential() {
        let unreached = || panic!("this call must not read the machine credential");
        assert_eq!(
            callback_token(Some("given".into()), Some("run-a"), unreached),
            Some("given".into())
        );
        assert_eq!(
            callback_token(Some("given".into()), None, unreached),
            Some("given".into())
        );
        assert_eq!(callback_token(None, Some("run-a"), unreached), None);

        let local = || Some("machine".to_string());
        assert_eq!(callback_token(None, None, local), Some("machine".into()));
        assert_eq!(
            callback_token(None, Some(""), local),
            Some("machine".into())
        );
    }

    #[test]
    fn each_loader_reads_only_its_own_file() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join(MCP_SECRET_FILE), [0x11u8; SECRET_LEN]).unwrap();

        assert!(load_mcp_token_from(home.path()).is_some());
        assert!(
            load_operator_token_from(home.path()).is_none(),
            "the operator loader must not fall back to the MCP secret"
        );
    }
}
