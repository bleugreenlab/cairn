//! Shared-secret bearer authentication for the two credentials a Cairn install
//! holds, and the reason they are two.
//!
//! [`McpAuthState`] guards `~/.cairn/mcp_auth_secret`, which authenticates MCP
//! callbacks. It is passed to every agent process as `CAIRN_MCP_AUTH_TOKEN`
//! because that is what it is for: an agent has to be able to call back.
//!
//! [`OperatorAuthState`] guards `~/.cairn/operator_auth_secret`, which
//! authenticates a **desktop operator** answering an authority prompt. Holding
//! it is what separates "the person at this machine approved this" from
//! "something on this machine opened a loopback socket". It is therefore never
//! placed in an agent environment, never returned by an invoke command, and
//! never handed to the executor — and this module deliberately gives it no
//! token accessor at all, so the runner can check the credential without ever
//! being able to disclose it.
//!
//! The credential is a file, and a file is only out of reach if something keeps
//! it out of reach. `cairn_core::authorization::protected` refuses agent reads
//! and writes of this path, and the executor sandbox denies reads of it
//! unconditionally. See `docs/authorization.md` for what that does and does not
//! establish.

use crate::security::SecretCategory;
use cairn_common::auth::{SharedSecret, MCP_SECRET_FILE, OPERATOR_SECRET_FILE, SECRET_LEN};
use rand::Rng;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// A random secret persisted under a config directory, generated on first use
/// and cached in memory afterwards.
///
/// One implementation for both credentials, because the mechanics that matter
/// — fixed length, `0600` on Unix, regenerate rather than trust a corrupt file,
/// resolve once and cache, register for scrubbing before the value can be held
/// — are exactly the properties that must not differ between them.
#[derive(Debug)]
pub struct SecretStore {
    /// Cached secret (loaded once on first use).
    secret: Mutex<Option<SharedSecret>>,
    /// Config directory for secret file storage.
    config_dir: PathBuf,
    /// File name under `config_dir`.
    file_name: &'static str,
    /// What this credential is, for the scrubbing registry.
    category: SecretCategory,
    /// Non-secret id prefix naming the producer, scoped by config directory so
    /// two runners sharing one process (only tests do this) cannot displace
    /// each other's registration.
    registry_prefix: &'static str,
}

impl SecretStore {
    pub fn new(
        config_dir: PathBuf,
        file_name: &'static str,
        category: SecretCategory,
        registry_prefix: &'static str,
    ) -> Self {
        Self {
            secret: Mutex::new(None),
            config_dir,
            file_name,
            category,
            registry_prefix,
        }
    }

    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Where this credential lives.
    pub fn path(&self) -> PathBuf {
        self.config_dir.join(self.file_name)
    }

    /// Get or create the secret. First call reads the file or generates a new
    /// one; later calls return the cached value.
    fn get(&self) -> Result<SharedSecret, String> {
        let mut guard = self.secret.lock().map_err(|e| e.to_string())?;
        if let Some(secret) = *guard {
            return Ok(secret);
        }
        let secret = get_or_create_secret(&self.config_dir, self.file_name)?;
        self.register_for_scrubbing(&secret);
        *guard = Some(secret);
        Ok(secret)
    }

    /// Register this credential with the secret registry, so it is scrubbed out
    /// of anything an agent observes (CAIRN-3822).
    ///
    /// Called on first materialization, which is strictly before the first use:
    /// every consumer reaches the value through [`Self::get`]. The ordering is
    /// the point — a registration moved after the return would leave a window
    /// during which the value exists but is not scrubbed for.
    ///
    /// The registration is held for the life of the process. A credential can
    /// appear in observed output at any later moment, so no narrower lifetime is
    /// honest: releasing early would unregister while output carrying the value
    /// is still in flight.
    fn register_for_scrubbing(&self, secret: &SharedSecret) {
        use crate::security::{registry, SecretId, SecretMaterial};

        let id = SecretId::new(format!(
            "{}:{}",
            self.registry_prefix,
            self.config_dir.display()
        ));
        // Registered as RAW BYTES, not as the base64 token. `derived_forms`
        // computes encodings of what it is given, so raw material covers both
        // the 32 bytes as they sit in the file (what `cat` or `xxd` of it would
        // emit) and the base64 token both credentials travel as. Registering the
        // token instead would cover the token and base64-of-token, and leave the
        // file's own contents uncovered.
        match registry().register(
            id,
            self.category,
            self.file_name,
            SecretMaterial::from_bytes(secret.expose()),
        ) {
            Ok(guard) => guard.retain_for_process(),
            // Loud, and non-secret: a refusal means observed output is NOT
            // protected against this credential, which an operator needs to know.
            Err(error) => log::warn!("{} not registered for scrubbing: {error}", self.file_name),
        }
    }

    /// Materialize the secret on disk if it is not there yet. Idempotent.
    pub fn ensure(&self) -> Result<(), String> {
        self.get().map(|_| ())
    }

    /// The bearer token for this secret. A deliberate exposure — see
    /// [`SharedSecret`].
    pub fn token(&self) -> Result<String, String> {
        Ok(self.get()?.expose_base64())
    }

    /// Whether `token` is this credential.
    pub fn validate_token(&self, token: &str) -> Result<(), String> {
        if self.get()?.matches_token(token) {
            Ok(())
        } else {
            Err("Invalid token".to_string())
        }
    }
}

/// Shared state for MCP callback authentication.
#[derive(Debug)]
pub struct McpAuthState(SecretStore);

impl McpAuthState {
    pub(crate) fn config_dir(&self) -> &Path {
        self.0.config_dir()
    }

    pub fn new(config_dir: PathBuf) -> Self {
        Self(SecretStore::new(
            config_dir,
            MCP_SECRET_FILE,
            SecretCategory::CallbackCredential,
            "mcp-callback",
        ))
    }

    /// Get the secret as base64 for passing to the MCP binary via env var.
    pub fn get_secret_for_mcp(&self) -> Result<String, String> {
        self.0.token()
    }

    /// Materialize the shared secret on disk if it is not present yet.
    ///
    /// The secret is otherwise created lazily on the first agent spawn or
    /// inbound callback. Calling this when the callback server starts makes the
    /// invariant "a running server has a readable `mcp_auth_secret`" hold, so
    /// tooling that authenticates to a running instance by reading its secret
    /// file (e.g. `cairn://dev/db`/`cairn://dev/pid` querying a `dev:instance`) works even before
    /// the instance has done any MCP work. Idempotent.
    pub fn ensure_secret(&self) -> Result<(), String> {
        self.0.ensure()
    }

    /// Validate a bearer token from an MCP callback request.
    pub fn validate_token(&self, token: &str) -> Result<(), String> {
        self.0.validate_token(token)
    }
}

/// Shared state for desktop **operator** authentication.
///
/// Deliberately narrower than [`McpAuthState`]: it can materialize the
/// credential and check one, and that is all. There is no accessor that returns
/// the token, so no runner code path — an invoke handler, a log line, a health
/// payload, a spawn environment — can hand it out even by mistake. The one
/// process that legitimately needs the bytes is the native desktop shell, which
/// reads the file itself through `cairn_common::auth::load_operator_token_from`.
///
/// It is persisted rather than held in the desktop's memory because the runner
/// and the desktop have independent lifetimes: a value the desktop invented at
/// launch would give a runner that started before it nothing to check against.
#[derive(Debug)]
pub struct OperatorAuthState(SecretStore);

impl OperatorAuthState {
    pub fn new(config_dir: PathBuf) -> Self {
        Self(SecretStore::new(
            config_dir,
            OPERATOR_SECRET_FILE,
            SecretCategory::OperatorCredential,
            "operator-credential",
        ))
    }

    /// Where the credential lives, for the protected-path and sandbox rules
    /// that keep an agent away from it.
    pub fn path(&self) -> PathBuf {
        self.0.path()
    }

    /// Materialize the credential on disk if it is not present yet, so a
    /// desktop that starts after the runner finds something to read.
    pub fn ensure_secret(&self) -> Result<(), String> {
        self.0.ensure()
    }

    /// Whether a presented `X-Cairn-Operator-Token` is this install's operator
    /// credential.
    pub fn validate_token(&self, token: &str) -> Result<(), String> {
        self.0.validate_token(token)
    }
}

/// Create a file containing `contents`, owner-readable only from the moment it
/// exists.
///
/// `std::fs::write` creates at the process umask (`0644` on a stock macOS
/// install) and a follow-up `set_permissions` closes that only afterwards — a
/// window in which the file is world-readable, and in which a reader that got
/// an fd keeps its access across the later chmod. Small, but a credential whose
/// entire value is read-secrecy should not have one, so the mode is part of the
/// `open(2)` rather than a second step.
fn write_owner_only(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.sync_all()
}

/// Get or create a secret file in the config directory.
fn get_or_create_secret(config_dir: &Path, file_name: &str) -> Result<SharedSecret, String> {
    let path = config_dir.join(file_name);

    if path.exists() {
        let contents =
            std::fs::read(&path).map_err(|e| format!("Failed to read secret file: {}", e))?;

        if let Some(secret) = SharedSecret::from_bytes(&contents) {
            log::info!("Loaded auth secret from {:?}", path);
            return Ok(secret);
        }

        // Invalid file, will regenerate
        log::warn!(
            "Invalid auth secret file at {:?} (len={}), regenerating",
            path,
            contents.len()
        );
    }

    let bytes: [u8; SECRET_LEN] = rand::thread_rng().gen();
    let secret = SharedSecret::from_bytes(&bytes).expect("a SECRET_LEN array is a valid secret");

    std::fs::create_dir_all(config_dir)
        .map_err(|e| format!("Failed to create config directory: {}", e))?;

    // A stale or corrupt file is removed first so the create below is always a
    // fresh create, which is what lets the mode be set AT creation.
    if path.exists() {
        std::fs::remove_file(&path)
            .map_err(|e| format!("Failed to remove invalid secret file: {}", e))?;
    }
    write_owner_only(&path, secret.expose())
        .map_err(|e| format!("Failed to write secret file: {}", e))?;

    log::info!("Generated new auth secret at {:?}", path);
    Ok(secret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::auth::decode_secret;

    fn store_with(secret: [u8; SECRET_LEN], file_name: &'static str) -> SecretStore {
        SecretStore {
            secret: Mutex::new(Some(SharedSecret::from_bytes(&secret).unwrap())),
            config_dir: PathBuf::from("/tmp"),
            file_name,
            category: SecretCategory::CallbackCredential,
            registry_prefix: "mcp-callback",
        }
    }

    #[test]
    fn test_validate_token_correct() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState(store_with(secret, MCP_SECRET_FILE));

        let token = SharedSecret::from_bytes(&secret).unwrap().expose_base64();
        assert!(auth_state.validate_token(&token).is_ok());
    }

    #[test]
    fn test_validate_token_invalid() {
        let auth_state = McpAuthState(store_with([0x42; 32], MCP_SECRET_FILE));

        assert!(auth_state.validate_token("invalid").is_err());
        assert!(auth_state.validate_token("").is_err());
        assert!(auth_state.validate_token("0000000000000000").is_err());
    }

    #[test]
    fn test_validate_token_wrong_secret() {
        let auth_state = McpAuthState(store_with([0x42; 32], MCP_SECRET_FILE));

        let wrong_token = SharedSecret::from_bytes(&[0xab; 32])
            .unwrap()
            .expose_base64();
        assert!(auth_state.validate_token(&wrong_token).is_err());
    }

    #[test]
    fn test_get_secret_for_mcp() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState(store_with(secret, MCP_SECRET_FILE));

        let encoded = auth_state.get_secret_for_mcp().unwrap();
        let decoded = decode_secret(&encoded).unwrap();

        assert_eq!(secret, decoded);
    }

    /// The registration must be in place by the time anyone can hold the value.
    ///
    /// Ordering is the whole point: this asserts against the *first* value the
    /// state ever produces, so a registration moved after the return would fail
    /// here rather than leaving a silent window during which the credential is
    /// injected but not scrubbed for.
    #[test]
    fn materializing_the_callback_credential_registers_it_before_returning_it() {
        use crate::security::{registry, Sanitizer, SecretId};

        let dir = tempfile::tempdir().unwrap();
        let auth = McpAuthState::new(dir.path().to_path_buf());
        let encoded = auth.get_secret_for_mcp().expect("secret materializes");

        let id = SecretId::new(format!("mcp-callback:{}", dir.path().display()));
        assert!(
            registry().metadata().iter().any(|entry| entry.id == id),
            "the callback credential must be registered"
        );

        let mut sanitizer = Sanitizer::exact();
        assert_eq!(
            sanitizer.text(&format!("CAIRN_MCP_SECRET={encoded}")),
            "CAIRN_MCP_SECRET=[REDACTED]",
            "the value handed to every child must already be scrubbable"
        );
    }

    /// The operator credential is registered too, in **both** the form it sits
    /// in on disk and the form it travels in.
    ///
    /// Nothing injects this credential into an agent, so unlike the callback
    /// secret it is not protecting an intentional exposure. It matters because
    /// the sandbox is switched off entirely at `fence: allow`, leaving a shell
    /// read of the file as the one route an agent has to it.
    ///
    /// Which form is registered is the whole point, and getting it backwards is
    /// silent: `derived_forms` computes encodings OF WHAT IT IS GIVEN, so
    /// registering the base64 token would cover the token and base64-of-token
    /// and leave `cat` of the file — the actual read — uncovered. This asserts
    /// the raw bytes are scrubbed, which is the assertion that fails if someone
    /// later switches the registration back to the encoded form.
    #[test]
    fn materializing_the_operator_credential_registers_it_before_returning_it() {
        use crate::security::{registry, Sanitizer, SecretId};

        let dir = tempfile::tempdir().unwrap();
        let auth = OperatorAuthState::new(dir.path().to_path_buf());
        auth.ensure_secret().expect("secret materializes");
        let encoded = cairn_common::auth::load_operator_token_from(dir.path()).expect("token");
        let raw = std::fs::read(auth.path()).expect("credential file");

        let id = SecretId::new(format!("operator-credential:{}", dir.path().display()));
        assert!(
            registry().metadata().iter().any(|entry| entry.id == id),
            "the operator credential must be registered"
        );

        // The token, through the text sanitizer: this is the form the header
        // and any log line would carry.
        let mut sanitizer = Sanitizer::exact();
        assert_eq!(
            sanitizer.text(&format!("header {encoded}")),
            "header [REDACTED]",
            "the form the credential travels in must be scrubbed"
        );

        // The file's own bytes, through the BYTE-oriented scrubber, which is
        // what command output actually passes through. It has to be this
        // surface: 32 random bytes are not valid UTF-8, so they cannot even be
        // expressed to the text sanitizer without being mangled first.
        let mut stream = crate::security::StreamingScrubber::new();
        let mut scrubbed = stream.push(b"file ");
        scrubbed.extend(stream.push(&raw));
        scrubbed.extend(stream.flush());
        assert_eq!(
            String::from_utf8_lossy(&scrubbed),
            "file [REDACTED]",
            "the form the credential sits in on disk must be scrubbed too — a `cat` of \
             the file is the read this is defending against"
        );
    }

    #[test]
    fn a_generated_secret_reloads_as_the_same_value() {
        let dir = tempfile::tempdir().unwrap();
        let first = OperatorAuthState::new(dir.path().to_path_buf());
        first.ensure_secret().unwrap();

        // A second process reading the same home must reach the same credential,
        // or a desktop that outlives a runner restart stops being able to answer.
        let token = cairn_common::auth::load_operator_token_from(dir.path())
            .expect("the credential is readable from disk");
        let second = OperatorAuthState::new(dir.path().to_path_buf());
        assert!(second.validate_token(&token).is_ok());
    }

    #[test]
    fn an_incorrect_operator_token_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = OperatorAuthState::new(dir.path().to_path_buf());
        store.ensure_secret().unwrap();

        assert!(store.validate_token("").is_err());
        assert!(store.validate_token("not-base64!!").is_err());
        assert!(store
            .validate_token(
                &SharedSecret::from_bytes(&[0x7f; SECRET_LEN])
                    .unwrap()
                    .expose_base64()
            )
            .is_err());
    }

    #[test]
    fn the_two_credentials_are_independent_values() {
        // Same directory, same mechanics, and they must never coincide: the MCP
        // secret is in every agent's environment, so an operator credential
        // equal to it would authenticate the agent as the operator.
        let dir = tempfile::tempdir().unwrap();
        let mcp = McpAuthState::new(dir.path().to_path_buf());
        let operator = OperatorAuthState::new(dir.path().to_path_buf());
        mcp.ensure_secret().unwrap();
        operator.ensure_secret().unwrap();

        let mcp_token = mcp.get_secret_for_mcp().unwrap();
        assert!(
            operator.validate_token(&mcp_token).is_err(),
            "the MCP token — which every agent holds — must not authenticate an operator"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_generated_secret_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let store = OperatorAuthState::new(dir.path().to_path_buf());
        store.ensure_secret().unwrap();

        let mode = std::fs::metadata(store.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "got mode {:o}", mode & 0o777);
    }

    #[test]
    fn a_corrupt_secret_file_is_regenerated_rather_than_adopted() {
        // A short file must not become a short credential that still validates.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OPERATOR_SECRET_FILE);
        std::fs::write(&path, [0u8; 8]).unwrap();

        let store = OperatorAuthState::new(dir.path().to_path_buf());
        store.ensure_secret().unwrap();

        assert_eq!(std::fs::read(&path).unwrap().len(), SECRET_LEN);
    }

    /// The wrapper type narrows where the operator credential can escape to
    /// deliberate call sites; this pins what that set actually is.
    ///
    /// The compiler cannot answer "is this the complete set of places the
    /// credential is reachable", but a test enumerating the files that name it
    /// can, and it fails naming the new file on the change that adds one. Same
    /// shape as the exhaustive destructure in `fingerprint_mcp_config`: the
    /// obligation breaks on its own rather than waiting for a reviewer to
    /// notice.
    ///
    /// Adding a file here is not forbidden — it is a decision that has to be
    /// made out loud. Before adding one, check that the new site does not run
    /// inside an agent's process tree and does not put the credential into a
    /// response body, a log line, a config file, or a spawn environment.
    #[test]
    fn the_operator_credential_has_no_unexpected_exposure_sites() {
        const EXPECTED: &[&str] = &[
            // The names, the header, and the loader.
            "src-tauri/os/cairn-common/src/auth.rs",
            // The store: materialize, register for scrubbing, validate. No
            // token accessor, so the runner cannot disclose it.
            "src-tauri/os/cairn-core/src/mcp/auth.rs",
            "src-tauri/os/cairn-core/src/mcp/mod.rs",
            // Names the path so agent reads and writes of it are refused.
            "src-tauri/os/cairn-core/src/authorization/protected.rs",
            // Hosts: the runner materializes it, the hosted server carries an
            // (never-matching) validator so both reach the same authorization.
            "src-tauri/cairn-runner/src/main.rs",
            "src-tauri/cairn-server/src/main.rs",
            // Transport: validates a presented header into a request extension.
            "src-tauri/cairn-transport/src/auth.rs",
            "src-tauri/cairn-transport/src/runtime.rs",
            "src-tauri/cairn-transport/src/state.rs",
            // The native desktop shell. `runner_client` is the only reader of
            // the bytes; the command modules name the privileged forwarder and
            // nothing else, so a webview reaches it without the credential ever
            // crossing into JavaScript.
            "src-tauri/src/runner_client.rs",
            "src-tauri/src/commands/permission.rs",
            "src-tauri/src/commands/voice.rs",
            // Test fixtures, which mint their own throwaway credentials.
            "src-tauri/cairn-runner/tests/transport.rs",
            "src-tauri/cairn-transport/src/routes/invoke/tests.rs",
        ];

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("repo root is three levels above cairn-core")
            .to_path_buf();

        let mut found: Vec<String> = Vec::new();
        let mut stack = vec![root.join("src-tauri")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if path.is_dir() {
                    if !matches!(name, "target" | "node_modules" | "gen" | "dev-binaries") {
                        stack.push(path);
                    }
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                // Deliberately terms that reach the VALUE — the file name
                // constant, the loader, the store type, the header, the
                // privileged forwarder. Prose mentioning the credential is not
                // reach, and counting it would bury the sites that matter.
                let names_it = text.contains("OPERATOR_SECRET_FILE")
                    || text.contains("load_operator_token_from")
                    || text.contains("OperatorAuthState")
                    || text.contains("OPERATOR_TOKEN_HEADER")
                    || text.contains("invoke_as_operator");
                if names_it {
                    if let Ok(relative) = path.strip_prefix(&root) {
                        found.push(relative.to_string_lossy().replace('\\', "/"));
                    }
                }
            }
        }
        found.sort();
        let mut expected: Vec<String> = EXPECTED.iter().map(|p| (*p).to_string()).collect();
        expected.sort();

        assert_eq!(
            found, expected,
            "the set of files that can reach the desktop operator credential changed. Every \
             entry here is a place the credential could escape from, so adding or removing one \
             is a security decision: confirm the new site does not run inside an agent's \
             process tree and does not put the credential into a response body, a log line, a \
             config file, or a spawn environment — then update EXPECTED."
        );
    }
}
