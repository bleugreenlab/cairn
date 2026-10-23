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
//!
//! ## Credential witness
//!
//! Every mutation of a credential key's values (secrets and non-credential
//! configuration values alike, since both feed `${VAR}` expansion) advances a
//! witness kept in its own keychain service, so cached MCP catalogs can be bound
//! to the exact credential state they were fetched under. A key's values live in
//! many keychain entries and no single write can change them atomically, so the
//! witness is a seqlock held under [`super::credential_witness`]'s mutation lock:
//! `Stable(n)` becomes `Updating(n + 1)`, the values change, and `Stable(n + 2)`
//! publishes them. A mutation that fails or dies part way leaves `Updating`,
//! which no reader may cache under and no ordinary mutation may clear. Only an
//! explicit user reconcile ([`reconcile_static_witness`]) does, by starting a
//! newer generation. Deleting values never deletes the witness: its history is
//! what keeps a deleted-then-recreated credential from reusing a generation.

use cairn_packs::server_config::CredentialGeneration;
use keyring::{Entry, Error};
use serde::{Deserialize, Serialize};
use std::path::Path;

use super::credential_witness::{self, CredentialMutationGuard};

/// Build the keychain service name for the current dev/prod mode.
fn service() -> String {
    format!("computer.cairn.mcp.{}", cairn_common::paths::env_str())
}

/// The keychain account for a `(server, var)` pair.
fn account(server: &str, var: &str) -> String {
    format!("{server}/{var}")
}

/// The keychain account for a `(server, var)` pair whose value is **not** a
/// credential. See [`set_config_value`].
fn config_account(server: &str, var: &str) -> String {
    format!("config:{server}/{var}")
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

fn config_entry(server: &str, var: &str) -> Result<Entry, String> {
    Entry::new(&service(), &config_account(server, var))
        .map_err(|e| format!("Failed to open keychain entry: {e}"))
}

/// Store (or replace) a **non-credential** value for `var` referenced by
/// `server` — a path to something the user installed, supplied through the pack
/// setup affordance.
///
/// # Why a second store rather than a second meaning for the first
///
/// The keychain is the only store a Finder- or Dock-launched app can read a
/// user-supplied value from, so a pack server's executable path has to live
/// here alongside real credentials. But the credential broker's declared-secret
/// signal is "a value in the keychain is a secret, because a human typed it
/// into a secret field" — and registering a resolved secret makes it a
/// process-global scrub target for the life of the process, which *refuses*
/// every model-authored write that mentions it. A user's MATLAB install path
/// registered that way is an undiagnosable agent lockout.
///
/// The declaration therefore has to be durable and attached to the entry, not
/// inferred from the value at read time. Two things go wrong with inference:
/// it silently reclassifies values stored through the ordinary secret field
/// that happen to look like whatever it is sniffing for, and — because
/// registration is process-lifetime and cannot be undone — one resolution taken
/// while the filesystem disagreed would poison the value for the rest of the
/// process, even after the world caught up.
///
/// So the account namespace carries the declaration: written here, it is
/// configuration, always, on every read, forever. See
/// [`crate::security::broker`].
pub fn set_config_value(server: &str, var: &str, value: &str) -> Result<(), String> {
    mutate_values(server, || {
        config_entry(server, var)?
            .set_password(value)
            .map_err(|e| format!("Failed to store value in keychain: {e}"))
    })
}

/// Read the non-credential value for `var` referenced by `server`, if stored.
pub fn get_config_value(server: &str, var: &str) -> Option<String> {
    match config_entry(server, var).ok()?.get_password() {
        Ok(value) => Some(value),
        Err(Error::NoEntry) => None,
        Err(e) => {
            log::warn!("Failed to read MCP value {server}/{var} from keychain: {e}");
            None
        }
    }
}

/// Remove the stored non-credential value for `var` referenced by `server`.
pub fn delete_config_value(server: &str, var: &str) -> Result<(), String> {
    mutate_values(server, || {
        match config_entry(server, var)?.delete_credential() {
            Ok(()) | Err(Error::NoEntry) => Ok(()),
            Err(e) => Err(format!("Failed to delete value from keychain: {e}")),
        }
    })
}

/// Store (or replace) the secret value for `var` referenced by `server`.
pub fn set_secret(server: &str, var: &str, value: &str) -> Result<(), String> {
    mutate_values(server, || {
        entry(server, var)?
            .set_password(value)
            .map_err(|e| format!("Failed to store secret in keychain: {e}"))
    })
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
    mutate_values(server, || match entry(server, var)?.delete_credential() {
        Ok(()) | Err(Error::NoEntry) => Ok(()),
        Err(e) => Err(format!("Failed to delete secret from keychain: {e}")),
    })
}

/// Whether `var` resolves from a witnessed Cairn store rather than from the
/// process environment. Environment values are not covered by the witness, so a
/// server depending on one is never cached.
pub fn has_witnessed_value(server: &str, var: &str) -> bool {
    get_secret(server, var).is_some() || get_config_value(server, var).is_some()
}

fn witness_service() -> String {
    format!(
        "computer.cairn.mcp-witness.{}",
        cairn_common::paths::env_str()
    )
}

fn witness_entry(server: &str) -> Result<Entry, String> {
    Entry::new(&witness_service(), server)
        .map_err(|e| format!("Failed to open keychain witness entry: {e}"))
}

const WITNESS_RECORD_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum WitnessState {
    Stable,
    Updating,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WitnessRecord {
    v: u32,
    incarnation: String,
    counter: u64,
    state: WitnessState,
}

impl WitnessRecord {
    fn generation(&self) -> CredentialGeneration {
        CredentialGeneration {
            incarnation: self.incarnation.clone(),
            counter: self.counter,
        }
    }
}

/// What a reader may conclude about the static values behind a credential key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaticWitness {
    /// No witness has been written yet: a new key, or values stored before
    /// witnesses existed. Establish one under the lock before relying on it.
    Absent,
    /// No mutation is in progress; catalogs may bind to this generation.
    Stable(CredentialGeneration),
    /// A mutation began and did not commit, or is committing now. Nothing may be
    /// cached, and only an explicit reconcile clears it.
    Updating(CredentialGeneration),
    /// The keychain could not be read, or the witness is corrupt. Fail closed.
    Unavailable(String),
}

enum RawWitness {
    Absent,
    Record(WitnessRecord),
    Corrupt,
    Unavailable(String),
}

fn read_witness(server: &str) -> RawWitness {
    let raw = match witness_entry(server).and_then(|entry| {
        entry.get_password().map(Some).or_else(|e| match e {
            Error::NoEntry => Ok(None),
            e => Err(format!("Failed to read MCP credential witness: {e}")),
        })
    }) {
        Ok(Some(raw)) => raw,
        Ok(None) => return RawWitness::Absent,
        Err(e) => return RawWitness::Unavailable(e),
    };
    match serde_json::from_str::<WitnessRecord>(&raw) {
        Ok(record) if record.v == WITNESS_RECORD_VERSION && !record.incarnation.is_empty() => {
            RawWitness::Record(record)
        }
        _ => RawWitness::Corrupt,
    }
}

fn write_witness(
    _guard: &CredentialMutationGuard,
    server: &str,
    record: &WitnessRecord,
) -> Result<(), String> {
    let blob = serde_json::to_string(record)
        .map_err(|e| format!("Failed to serialize MCP credential witness: {e}"))?;
    witness_entry(server)?
        .set_password(&blob)
        .map_err(|e| format!("Failed to store MCP credential witness: {e}"))
}

/// The current witness for `server`'s static values, without taking the lock.
/// A single keychain read is atomic, which is all a reader needs.
pub fn static_witness(server: &str) -> StaticWitness {
    match read_witness(server) {
        RawWitness::Absent => StaticWitness::Absent,
        RawWitness::Record(record) => match record.state {
            WitnessState::Stable => StaticWitness::Stable(record.generation()),
            WitnessState::Updating => StaticWitness::Updating(record.generation()),
        },
        RawWitness::Corrupt => StaticWitness::Unavailable(
            "MCP credential witness is unreadable; reconcile it in Settings".to_string(),
        ),
        RawWitness::Unavailable(e) => StaticWitness::Unavailable(e),
    }
}

/// The current witness, creating a stable one under the lock if none exists.
///
/// Creating one changes no value; it only begins witnessing them. The fresh
/// incarnation means no catalog recorded under an earlier, since-deleted
/// witness can ever match it.
pub fn establish_static_witness(server: &str) -> StaticWitness {
    match static_witness(server) {
        StaticWitness::Absent => {}
        witness => return witness,
    }
    let guard = match credential_witness::acquire() {
        Ok(guard) => guard,
        Err(e) => return StaticWitness::Unavailable(e),
    };
    if let RawWitness::Absent = read_witness(server) {
        let record = WitnessRecord {
            v: WITNESS_RECORD_VERSION,
            incarnation: credential_witness::new_incarnation(),
            counter: 0,
            state: WitnessState::Stable,
        };
        if let Err(e) = write_witness(&guard, server, &record) {
            return StaticWitness::Unavailable(e);
        }
    }
    drop(guard);
    static_witness(server)
}

fn exhausted(server: &str) -> String {
    format!("MCP credential generation for `{server}` is exhausted")
}

/// Change `server`'s values under the witness protocol. A failure after the
/// witness reads `Updating` leaves it there: the values may have changed.
fn mutate_values(server: &str, change: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
    let guard = credential_witness::acquire()?;
    let base = match read_witness(server) {
        RawWitness::Absent => WitnessRecord {
            v: WITNESS_RECORD_VERSION,
            incarnation: credential_witness::new_incarnation(),
            counter: 0,
            state: WitnessState::Stable,
        },
        RawWitness::Record(record) if record.state == WitnessState::Stable => record,
        RawWitness::Record(_) => {
            return Err(format!(
                "MCP credentials for `{server}` were left mid-update by an interrupted change. \
                 Reconcile them in Settings before changing them again."
            ))
        }
        RawWitness::Corrupt => {
            return Err(format!(
                "MCP credential witness for `{server}` is unreadable. Reconcile it in Settings."
            ))
        }
        RawWitness::Unavailable(e) => return Err(e),
    };
    let updating = base
        .counter
        .checked_add(1)
        .ok_or_else(|| exhausted(server))?;
    let stable = updating.checked_add(1).ok_or_else(|| exhausted(server))?;
    write_witness(
        &guard,
        server,
        &WitnessRecord {
            counter: updating,
            state: WitnessState::Updating,
            ..base.clone()
        },
    )?;
    change()?;
    write_witness(
        &guard,
        server,
        &WitnessRecord {
            counter: stable,
            state: WitnessState::Stable,
            ..base
        },
    )
}

/// Leave `server`'s witness as a crashed mutation would: `Updating`.
#[cfg(test)]
pub(crate) fn interrupt_mutation_for_test(server: &str) {
    assert!(mutate_values(server, || Err("interrupted".into())).is_err());
}

/// The explicit user reconcile: accept the values currently stored for
/// `server` as its credentials, and publish a generation newer than any
/// catalog could be bound to.
///
/// This is the only path out of `Updating`. Nothing calls it automatically: a
/// refetch cannot tell whether an interrupted change left the values as the
/// user intended, only the user can. An unreadable witness is replaced with a
/// fresh incarnation, which is equally unmatched by any recorded catalog.
pub fn reconcile_static_witness(server: &str) -> Result<CredentialGeneration, String> {
    let guard = credential_witness::acquire()?;
    let next = match read_witness(server) {
        RawWitness::Record(record) => WitnessRecord {
            counter: record
                .counter
                .checked_add(1)
                .ok_or_else(|| exhausted(server))?,
            state: WitnessState::Stable,
            ..record
        },
        RawWitness::Absent | RawWitness::Corrupt => WitnessRecord {
            v: WITNESS_RECORD_VERSION,
            incarnation: credential_witness::new_incarnation(),
            counter: 0,
            state: WitnessState::Stable,
        },
        RawWitness::Unavailable(e) => return Err(e),
    };
    write_witness(&guard, server, &next)?;
    Ok(next.generation())
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

    fn stable(server: &str) -> CredentialGeneration {
        match static_witness(server) {
            StaticWitness::Stable(generation) => generation,
            other => panic!("expected a stable witness, got {other:?}"),
        }
    }

    #[test]
    fn every_mutation_advances_a_stable_witness_and_deletion_keeps_history() {
        use_mock_keychain();
        let server = "witness-history";
        assert_eq!(static_witness(server), StaticWitness::Absent);
        set_secret(server, "TOKEN", "account-a").unwrap();
        let first = stable(server);
        delete_secret(server, "TOKEN").unwrap();
        let deleted = stable(server);
        set_secret(server, "TOKEN", "account-a").unwrap();
        let recreated = stable(server);
        set_config_value(server, "BIN", "/opt/tool").unwrap();
        let configured = stable(server);
        // Same values as `first`, yet never the same witness: delete/recreate
        // cannot bring an old catalog's generation back.
        for generation in [&deleted, &recreated, &configured] {
            assert_eq!(generation.incarnation, first.incarnation);
        }
        assert!(first.counter < deleted.counter);
        assert!(deleted.counter < recreated.counter);
        assert!(recreated.counter < configured.counter);
    }

    #[test]
    fn concurrent_writers_never_publish_the_same_generation() {
        use_mock_keychain();
        let server = "witness-concurrent";
        let writers: Vec<_> = (0..8)
            .map(|writer| {
                std::thread::spawn(move || {
                    for round in 0..10 {
                        set_secret(server, "TOKEN", &format!("{writer}-{round}")).unwrap();
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        // Each of the 80 mutations advanced by exactly two from the value it
        // read under the lock. A lost or duplicated increment would come up short.
        assert_eq!(stable(server).counter, 160);
    }

    #[test]
    fn an_interrupted_mutation_stays_updating_until_explicitly_reconciled() {
        use_mock_keychain();
        let server = "witness-interrupted";
        set_secret(server, "TOKEN", "before").unwrap();
        let before = stable(server);
        assert!(mutate_values(server, || Err("keychain write failed".into())).is_err());
        let StaticWitness::Updating(stuck) = static_witness(server) else {
            panic!("an interrupted mutation must leave the witness updating");
        };
        // Ordinary mutations and witness establishment cannot clear it.
        assert!(set_secret(server, "TOKEN", "after").is_err());
        assert!(delete_secret(server, "TOKEN").is_err());
        assert_eq!(
            establish_static_witness(server),
            StaticWitness::Updating(stuck.clone())
        );
        let reconciled = reconcile_static_witness(server).unwrap();
        assert_eq!(stable(server), reconciled);
        assert!(reconciled.counter > stuck.counter && stuck.counter > before.counter);
        set_secret(server, "TOKEN", "after").unwrap();
        assert!(stable(server).counter > reconciled.counter);
    }

    #[test]
    fn a_witness_recreated_outside_cairn_starts_a_new_incarnation() {
        use_mock_keychain();
        let server = "witness-external-delete";
        set_secret(server, "TOKEN", "value").unwrap();
        let original = stable(server);
        witness_entry(server).unwrap().delete_credential().unwrap();
        assert_eq!(static_witness(server), StaticWitness::Absent);
        let StaticWitness::Stable(established) = establish_static_witness(server) else {
            panic!("establishing an absent witness must yield a stable one");
        };
        assert_ne!(established.incarnation, original.incarnation);
        // Establishing an existing witness changes nothing.
        assert_eq!(
            establish_static_witness(server),
            StaticWitness::Stable(established)
        );
    }

    #[test]
    fn a_corrupt_witness_fails_closed_until_reconciled() {
        use_mock_keychain();
        let server = "witness-corrupt";
        witness_entry(server)
            .unwrap()
            .set_password("{not json")
            .unwrap();
        assert!(matches!(
            static_witness(server),
            StaticWitness::Unavailable(_)
        ));
        assert!(matches!(
            establish_static_witness(server),
            StaticWitness::Unavailable(_)
        ));
        assert!(set_secret(server, "TOKEN", "value").is_err());
        let reconciled = reconcile_static_witness(server).unwrap();
        assert_eq!(stable(server), reconciled);
    }

    #[test]
    fn exhausted_generations_refuse_rather_than_wrap() {
        use_mock_keychain();
        let server = "witness-exhausted";
        let guard = credential_witness::acquire().unwrap();
        write_witness(
            &guard,
            server,
            &WitnessRecord {
                v: WITNESS_RECORD_VERSION,
                incarnation: "near-end".into(),
                counter: u64::MAX - 1,
                state: WitnessState::Stable,
            },
        )
        .unwrap();
        drop(guard);
        assert!(set_secret(server, "TOKEN", "value").is_err());
        // Refused before anything was written: still stable, value untouched.
        assert_eq!(stable(server).counter, u64::MAX - 1);
        assert_eq!(get_secret(server, "TOKEN"), None);
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
