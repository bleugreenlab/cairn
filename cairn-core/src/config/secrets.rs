//! OS keychain storage for external MCP server secrets.
//!
//! `${ENV_VAR}` interpolation in MCP server config (command/args/env/url/
//! headers) resolves from the process environment, but a signed app launched
//! from Finder/Dock inherits a minimal environment — so token references resolve
//! to empty strings and token-bearing servers (remote APIs, Linear, …) fail.
//!
//! This module stores secret values in the OS keychain instead, keyed by
//! `(scope, server, var)`, so they resolve at connect time regardless of how the
//! app was launched. The connect-time expander (`McpServerConfig::expanded`)
//! checks the keychain first, then falls back to the process environment.
//!
//! Values live in the keychain, never in `settings.yaml`. The settings file
//! keeps the `${VAR}` reference; the secret behind it is stored here.
//!
//! ## Scoping
//!
//! The keychain service name embeds the dev/prod mode (`computer.cairn.mcp.dev`
//! vs `.prod`) so a dev build and a prod build never read each other's secrets,
//! mirroring the `~/.cairn-dev` vs `~/.cairn` home split. The account is
//! `"{server}/{var}"` for workspace servers and
//! `"project/{project_path}/{server}/{var}"` for project servers, so a same-named
//! project server can authenticate to a different tenant/workspace.

use keyring::{Entry, Error};
use std::path::Path;

/// Build the keychain service name for the current dev/prod mode.
fn service() -> String {
    format!("computer.cairn.mcp.{}", cairn_common::paths::env_str())
}

/// The keychain account for a `(server, var)` pair.
fn account(server: &str, var: &str) -> String {
    format!("{server}/{var}")
}

/// The server component used in keychain accounts for workspace or project MCP
/// credentials. Workspace keys are intentionally unchanged for compatibility;
/// project keys include the repo path so same-named servers in different
/// projects can hold independent static secrets and OAuth tokens.
pub fn credential_key(server: &str, project_path: Option<&Path>) -> String {
    match project_path {
        Some(path) => format!("project/{}/{server}", path.to_string_lossy()),
        None => server.to_string(),
    }
}

fn entry(server: &str, var: &str) -> Result<Entry, String> {
    Entry::new(&service(), &account(server, var))
        .map_err(|e| format!("Failed to open keychain entry: {e}"))
}

/// Store (or replace) the secret value for `var` referenced by `server`.
pub fn set_secret(server: &str, var: &str, value: &str) -> Result<(), String> {
    entry(server, var)?
        .set_password(value)
        .map_err(|e| format!("Failed to store secret in keychain: {e}"))
}

/// Read the secret value for `var` referenced by `server`, if one is stored.
///
/// Returns `None` when no secret exists or the keychain is unavailable — a
/// missing secret degrades to the env fallback / empty string at expand time
/// rather than failing the whole connection here.
pub fn get_secret(server: &str, var: &str) -> Option<String> {
    match entry(server, var).ok()?.get_password() {
        Ok(value) => Some(value),
        Err(Error::NoEntry) => None,
        Err(e) => {
            log::warn!("Failed to read MCP secret {server}/{var} from keychain: {e}");
            None
        }
    }
}

/// Whether a secret is stored for `var` referenced by `server`.
pub fn has_secret(server: &str, var: &str) -> bool {
    get_secret(server, var).is_some()
}

/// Remove the stored secret for `var` referenced by `server`. Succeeds when no
/// secret exists, so callers can purge unconditionally.
pub fn delete_secret(server: &str, var: &str) -> Result<(), String> {
    match entry(server, var)?.delete_credential() {
        Ok(()) | Err(Error::NoEntry) => Ok(()),
        Err(e) => Err(format!("Failed to delete secret from keychain: {e}")),
    }
}

// A persistent in-memory keychain backend for tests, shared across the whole
// crate's test build. keyring's bundled `mock` builds a fresh, non-persistent
// credential per `Entry::new`, so a value set through one handle is invisible to
// the next. The secrets API opens a new `Entry` per call (matching the real
// keychain, which persists), so tests need a backend that actually stores values
// keyed by (service, account). `set_default_credential_builder` is process-wide
// and effectively one-shot, so every module that needs a mock keychain must go
// through this single installer rather than registering its own builder.
#[cfg(test)]
pub(crate) mod mock_keychain {
    use keyring::credential::{Credential, CredentialApi, CredentialBuilder, CredentialBuilderApi};
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};

    type Store = Arc<Mutex<HashMap<String, Vec<u8>>>>;

    #[derive(Debug)]
    struct MemCredential {
        key: String,
        store: Store,
    }

    impl CredentialApi for MemCredential {
        fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
            self.store
                .lock()
                .unwrap()
                .insert(self.key.clone(), secret.to_vec());
            Ok(())
        }
        fn get_secret(&self) -> keyring::Result<Vec<u8>> {
            self.store
                .lock()
                .unwrap()
                .get(&self.key)
                .cloned()
                .ok_or(keyring::Error::NoEntry)
        }
        fn delete_credential(&self) -> keyring::Result<()> {
            if self.store.lock().unwrap().remove(&self.key).is_some() {
                Ok(())
            } else {
                Err(keyring::Error::NoEntry)
            }
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MemBuilder {
        store: Store,
    }

    impl CredentialBuilderApi for MemBuilder {
        fn build(
            &self,
            _target: Option<&str>,
            service: &str,
            user: &str,
        ) -> keyring::Result<Box<Credential>> {
            Ok(Box::new(MemCredential {
                key: format!("{service}\u{0}{user}"),
                store: self.store.clone(),
            }))
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// Install the persistent in-memory backend once for the whole test process.
    /// keyring stores the override in an `RwLock`, so this safely redirects every
    /// subsequent `Entry::new` regardless of which module calls it first.
    pub(crate) fn install() {
        static INSTALLED: OnceLock<()> = OnceLock::new();
        INSTALLED.get_or_init(|| {
            let store: Store = Arc::new(Mutex::new(HashMap::new()));
            let builder: Box<CredentialBuilder> = Box::new(MemBuilder { store });
            keyring::set_default_credential_builder(builder);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::mock_keychain::install as use_mock_keychain;

    #[test]
    fn set_get_delete_roundtrip() {
        use_mock_keychain();
        let (server, var) = ("roundtrip-server", "TOKEN");

        assert_eq!(get_secret(server, var), None);
        assert!(!has_secret(server, var));

        set_secret(server, var, "s3cr3t").unwrap();
        assert_eq!(get_secret(server, var).as_deref(), Some("s3cr3t"));
        assert!(has_secret(server, var));

        // Replace in place.
        set_secret(server, var, "rotated").unwrap();
        assert_eq!(get_secret(server, var).as_deref(), Some("rotated"));

        delete_secret(server, var).unwrap();
        assert_eq!(get_secret(server, var), None);
        assert!(!has_secret(server, var));
        // Deleting an absent secret is a no-op success.
        delete_secret(server, var).unwrap();
    }

    #[test]
    fn secrets_are_scoped_per_server() {
        use_mock_keychain();
        set_secret("server-a", "KEY", "a-value").unwrap();
        set_secret("server-b", "KEY", "b-value").unwrap();
        assert_eq!(get_secret("server-a", "KEY").as_deref(), Some("a-value"));
        assert_eq!(get_secret("server-b", "KEY").as_deref(), Some("b-value"));
    }

    #[test]
    fn credential_key_scopes_projects_without_changing_workspace_keys() {
        assert_eq!(credential_key("linear", None), "linear");
        assert_eq!(
            credential_key("linear", Some(Path::new("/repo/a"))),
            "project//repo/a/linear"
        );
        assert_ne!(
            credential_key("linear", Some(Path::new("/repo/a"))),
            credential_key("linear", Some(Path::new("/repo/b")))
        );
    }
}
