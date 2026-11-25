//! Each inference deployment's API key: minted here, kept in the keychain,
//! and sent by everything that reaches the deployment's engine.
//!
//! A deployment's engine listens on the network, so it requires a key of
//! every request. [`ensure`] mints one when a start is sent and none is kept,
//! [`key`] is the one resolver every caller asks (the act side's start, stop
//! and probes, routing, verification and the metrics scrape, as
//! [`cairn_inference::Addresses`] is for where the engine is), and [`forget`]
//! drops it once the deployment has stopped, so the next start is given a
//! fresh one. A running server's key never changes.
//!
//! The key lives in the keychain (service
//! `computer.cairn.inference-deployment.{dev|prod}`, account = the deployment
//! id) and in this process's memory, registered with the secret registry so
//! any output carrying it is scrubbed. It is never in the inventory, a
//! rendered script, a log or a `cairn://` read: the head is handed it in the
//! command ([`DeploymentApiKey`], whose `Debug` hides it) and gives it to the
//! scripts in their environment.
//!
//! Its own keychain service rather than `config::secrets`, for the reason
//! ingress endpoint secrets have theirs: that store is the MCP `${VAR}` store,
//! whose every write advances an MCP credential witness.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use base64::Engine as _;
use cairn_common::executor_protocol::DeploymentApiKey;
use cairn_common::security::registry::{registry, SecretGuard};
use cairn_common::security::secret::{SecretCategory, SecretId, SecretMaterial};
use cairn_inference::DeploymentId;
use rand::RngCore as _;

/// What this process knows of one deployment's key.
enum Known {
    /// The keychain holds none.
    Absent,
    /// The key, with its scrubbing registration held for as long as it is.
    Held {
        key: DeploymentApiKey,
        _registered: Option<SecretGuard<'static>>,
    },
}

fn known() -> MutexGuard<'static, HashMap<String, Known>> {
    static KNOWN: OnceLock<Mutex<HashMap<String, Known>>> = OnceLock::new();
    KNOWN
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

/// The keychain account, and the memory entry, of `id`'s key.
fn account(id: &DeploymentId) -> String {
    id.to_string()
}

fn entry(account: &str) -> Result<keyring::Entry, String> {
    // The act side's tests start deployments, which keeps their keys: never
    // in the machine's real keychain.
    #[cfg(test)]
    crate::config::secrets::mock_keychain::install();
    keyring::Entry::new(
        &format!(
            "computer.cairn.inference-deployment.{}",
            cairn_common::paths::env_str()
        ),
        account,
    )
    .map_err(|error| format!("could not open the keychain entry: {error}"))
}

fn held(id: &DeploymentId, key: DeploymentApiKey) -> Known {
    let registered = registry()
        .register(
            SecretId::new(format!("inference-deployment:{id}")),
            SecretCategory::InferenceDeploymentKey,
            format!("inference deployment {id}"),
            SecretMaterial::from_string(key.expose().to_string()),
        )
        .ok();
    Known::Held {
        key,
        _registered: registered,
    }
}

/// `id`'s key, when one is kept: the one resolver for what a request to the
/// deployment's engine must carry. A keychain that cannot be read answers
/// none, and is asked again next time.
pub(crate) fn key(id: &DeploymentId) -> Option<DeploymentApiKey> {
    let account = account(id);
    let mut known = known();
    match known.get(&account) {
        Some(Known::Held { key, .. }) => return Some(key.clone()),
        Some(Known::Absent) => return None,
        None => {}
    }
    let stored = entry(&account).and_then(|entry| match entry.get_password() {
        Ok(value) => Ok(Some(value)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(error) => Err(format!("could not read it from the keychain: {error}")),
    });
    match stored {
        Ok(Some(value)) => {
            let key = DeploymentApiKey::new(value);
            known.insert(account, held(id, key.clone()));
            Some(key)
        }
        Ok(None) => {
            known.insert(account, Known::Absent);
            None
        }
        Err(why) => {
            log::warn!("inference: deployment {id}'s API key is unknown: {why}");
            None
        }
    }
}

/// A fresh key: 32 random bytes, URL-safe, with a prefix that says what it is.
fn mint() -> DeploymentApiKey {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    DeploymentApiKey::new(format!(
        "cairn-inference-{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    ))
}

/// `id`'s key, minting and keeping one when none is kept: what a start is
/// sent with.
pub(crate) fn ensure(id: &DeploymentId) -> Result<DeploymentApiKey, String> {
    if let Some(key) = key(id) {
        return Ok(key);
    }
    let key = mint();
    let account = account(id);
    entry(&account)?
        .set_password(key.expose())
        .map_err(|error| format!("could not keep it in the keychain: {error}"))?;
    known().insert(account, held(id, key.clone()));
    Ok(key)
}

/// Drop `id`'s key: its server has stopped, so its next start is given a new
/// one. Succeeds when there was none.
pub(crate) fn forget(id: &DeploymentId) -> Result<(), String> {
    let account = account(id);
    let deleted = entry(&account)?.delete_credential();
    known().insert(account, Known::Absent);
    match deleted {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) => Err(format!("could not delete it from the keychain: {error}")),
    }
}

/// A request to a deployment's engine, carrying its key when it has one.
pub(crate) trait Keyed {
    fn keyed(self, key: Option<&DeploymentApiKey>) -> Self;
}

impl Keyed for reqwest::RequestBuilder {
    fn keyed(self, key: Option<&DeploymentApiKey>) -> Self {
        match key {
            Some(key) => self.header(reqwest::header::AUTHORIZATION, key.bearer()),
            None => self,
        }
    }
}
