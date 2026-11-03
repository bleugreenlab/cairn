//! Where an endpoint's secret lives: this machine's keychain, and nowhere a
//! model or a web page can read it.
//!
//! The api shows a generated secret exactly once, in the mint or rotate
//! response. [`store`] writes it to the keychain (service
//! `computer.cairn.ingress-endpoint.{dev|prod}`, account = the endpoint id)
//! and registers it with the process secret registry, so any output that
//! carries it is scrubbed and any model-authored write that mentions it is
//! refused. No `cairn://` read or write result renders it, and no invoke
//! returns it: the operator's Copy writes it to the clipboard inside the
//! runner.
//!
//! Its own keychain service rather than `config::secrets`: that store is the
//! MCP `${VAR}` store, and every write there advances an MCP credential
//! witness under a cross-process lock that an endpoint secret has no part in.

use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use cairn_common::security::{
    registry::{registry, RegistrationRefused, SecretGuard},
    secret::{SecretCategory, SecretId, SecretMaterial},
};

use crate::storage::LocalDb;

/// The keychain entry for an endpoint's secret.
fn entry(endpoint_id: &str) -> Result<keyring::Entry, String> {
    keyring::Entry::new(
        &format!(
            "computer.cairn.ingress-endpoint.{}",
            cairn_common::paths::env_str()
        ),
        endpoint_id,
    )
    .map_err(|error| format!("could not open the keychain entry: {error}"))
}

fn read(endpoint_id: &str) -> Option<String> {
    match entry(endpoint_id).ok()?.get_password() {
        Ok(secret) => Some(secret),
        Err(keyring::Error::NoEntry) => None,
        Err(error) => {
            log::warn!(
                "could not read ingress endpoint {endpoint_id}'s secret from the keychain: {error}"
            );
            None
        }
    }
}

/// The endpoints whose secret this machine's keychain holds, so answering
/// "is it held" never reads a secret (or raises a keychain prompt).
fn held_ids() -> &'static Mutex<std::collections::HashSet<String>> {
    static HELD: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();
    HELD.get_or_init(Mutex::default)
}

/// One registry hold per endpoint whose secret this process knows.
fn guards() -> &'static Mutex<HashMap<String, SecretGuard<'static>>> {
    static GUARDS: OnceLock<Mutex<HashMap<String, SecretGuard<'static>>>> = OnceLock::new();
    GUARDS.get_or_init(Mutex::default)
}

/// The non-secret reference to an endpoint's secret, as provenance and
/// identity records name it.
pub fn credential_ref(endpoint_id: &str) -> String {
    format!("ingress-endpoint:{endpoint_id}")
}

fn hold(endpoint_id: &str, secret: &str) {
    match registry().register(
        SecretId::new(credential_ref(endpoint_id)),
        SecretCategory::IngressEndpointSecret,
        format!("ingress endpoint {endpoint_id}"),
        SecretMaterial::from_string(secret.to_string()),
    ) {
        // Replacing the entry drops the previous hold after the new one is
        // taken, so a rotation never leaves a gap with nothing registered.
        Ok(guard) => {
            guards()
                .lock()
                .expect("ingress custody poisoned")
                .insert(endpoint_id.to_string(), guard);
        }
        // A server-generated secret is 32 random bytes, so only a caller's
        // provider secret can be refused. It is still stored; it cannot be
        // scrubbed for without false positives.
        Err(RegistrationRefused::BelowThreshold) => log::warn!(
            "ingress endpoint {endpoint_id}'s secret is too short or repetitive to scrub for; it is kept in the keychain but not registered"
        ),
        Err(RegistrationRefused::Empty) => {
            log::warn!("ingress endpoint {endpoint_id} was given an empty secret")
        }
    }
}

/// Keep `secret` for `endpoint_id` in the keychain and register it. A
/// rotation replaces both.
pub fn store(endpoint_id: &str, secret: &str) -> Result<(), String> {
    entry(endpoint_id)?
        .set_password(secret)
        .map_err(|error| format!("could not store the secret in the keychain: {error}"))?;
    hold(endpoint_id, secret);
    held_ids()
        .lock()
        .expect("ingress custody poisoned")
        .insert(endpoint_id.to_string());
    Ok(())
}

/// Register a provider's own signing secret, which lives with the provider's
/// credentials (the GitHub App's `webhook_secret`) rather than in the
/// keychain. Nothing is written and nothing is marked held: there is no Copy
/// for a secret the provider already has.
pub fn hold_provider_secret(endpoint_id: &str, secret: &str) {
    hold(endpoint_id, secret);
}

/// Drop the registration and delete the keychain entry. Succeeds when there
/// was none.
pub fn forget(endpoint_id: &str) -> Result<(), String> {
    guards()
        .lock()
        .expect("ingress custody poisoned")
        .remove(endpoint_id);
    held_ids()
        .lock()
        .expect("ingress custody poisoned")
        .remove(endpoint_id);
    match entry(endpoint_id)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) => Err(format!(
            "could not delete the secret from the keychain: {error}"
        )),
    }
}

/// Whether this machine holds the endpoint's secret, as [`store`] and
/// [`restore`] recorded it.
pub fn held(endpoint_id: &str) -> bool {
    held_ids()
        .lock()
        .expect("ingress custody poisoned")
        .contains(endpoint_id)
}

/// The secret itself. Its one caller is the operator-gated Copy, which hands
/// it to the clipboard and returns nothing.
pub fn reveal_for_copy(endpoint_id: &str) -> Option<String> {
    read(endpoint_id)
}

/// Register every live endpoint's stored secret, so scrubbing covers them
/// from startup rather than from the next mint.
pub async fn restore(db: &LocalDb) {
    let endpoints = match super::active_endpoints(db).await {
        Ok(endpoints) => endpoints,
        Err(error) => {
            log::warn!("could not list ingress endpoints to restore their secrets: {error}");
            return;
        }
    };
    for endpoint in endpoints
        .iter()
        .filter(|endpoint| endpoint.provider != "first-party")
    {
        if let Some(secret) = read(&endpoint.id) {
            hold(&endpoint.id, &secret);
            held_ids()
                .lock()
                .expect("ingress custody poisoned")
                .insert(endpoint.id.clone());
        }
    }
}

#[cfg(test)]
pub(crate) fn is_registered(endpoint_id: &str) -> bool {
    guards()
        .lock()
        .expect("ingress custody poisoned")
        .contains_key(endpoint_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keychain_round_trips_rotation_replaces_and_revoke_deletes() {
        crate::config::secrets::mock_keychain::install();
        let id = "src_custody_roundtrip";
        store(id, "Zr8pQ1x-generated-secret-value-one").unwrap();
        assert!(held(id));
        assert!(is_registered(id));
        assert_eq!(
            reveal_for_copy(id).as_deref(),
            Some("Zr8pQ1x-generated-secret-value-one")
        );
        store(id, "Kq2vW9y-generated-secret-value-two").unwrap();
        assert_eq!(
            reveal_for_copy(id).as_deref(),
            Some("Kq2vW9y-generated-secret-value-two")
        );
        forget(id).unwrap();
        assert!(!held(id));
        assert!(!is_registered(id));
    }

    #[test]
    fn a_provider_secret_is_registered_but_not_held_or_stored() {
        crate::config::secrets::mock_keychain::install();
        let id = "src_custody_provider";
        hold_provider_secret(id, "Pw3kX8m-provider-signing-secret-value");
        assert!(is_registered(id));
        assert!(!held(id));
        assert_eq!(reveal_for_copy(id), None);
        forget(id).unwrap();
        assert!(!is_registered(id));
    }

    #[test]
    fn a_registered_secret_is_scrubbed_from_output() {
        crate::config::secrets::mock_keychain::install();
        let id = "src_custody_scrub";
        let secret = "Hn4tR7s-generated-secret-value-scrub";
        store(id, secret).unwrap();
        let mut found = cairn_common::security::registry::Detections::new();
        let scrubbed = registry()
            .snapshot()
            .scrub_str(&format!("the body said {secret}"), &mut found)
            .expect("the secret is found");
        assert!(!scrubbed.contains(secret), "{scrubbed}");
        forget(id).unwrap();
    }
}
