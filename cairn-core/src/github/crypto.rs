//! At-rest encryption for GitHub credentials stored in the local DB.
//!
//! Uses ChaCha20-Poly1305 with a machine-derived key and per-field domain
//! separation. The encryption key is derived from a machine identifier
//! (`get_machine_id`), so encrypted values are **not portable across machines** —
//! the same constraint that already applies to the relay E2E key. This is
//! acceptable for local at-rest protection: the DB file alone never yields a
//! usable secret. On read, legacy plaintext values are passed through unchanged
//! and re-encrypted on the next write, so existing installs migrate transparently.
//!
//! Both the desktop app and the headless `cairn-server` read GitHub credentials
//! through `credentials.rs`, which is why this lives in cairn-core rather than the
//! Tauri layer.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Nonce,
};
use crypto_box::{aead::OsRng, PublicKey, SalsaBox, SecretKey};
use rand::RngCore;
use sha2::{Digest, Sha256};

/// Machine identifier used to derive the at-rest key. Re-exported from the
/// identity module so there is a single implementation across the crate.
pub use crate::identity::crypto::get_machine_id;

/// Generate a new X25519 relay keypair.
///
/// Returns `(public_key_base64, secret_key_base64)`.
pub fn generate_keypair() -> (String, String) {
    let secret_key = SecretKey::generate(&mut OsRng);
    let public_key = secret_key.public_key();

    (
        BASE64.encode(public_key.as_bytes()),
        BASE64.encode(secret_key.to_bytes()),
    )
}

/// Decrypt a relay sealed-box payload using a base64-encoded X25519 secret key.
pub fn decrypt_payload(encrypted_b64: &str, secret_key_b64: &str) -> Result<String, String> {
    let encrypted = BASE64
        .decode(encrypted_b64)
        .map_err(|e| format!("Invalid base64: {e}"))?;
    let secret_bytes = BASE64
        .decode(secret_key_b64)
        .map_err(|e| format!("Invalid secret key base64: {e}"))?;

    if secret_bytes.len() != 32 {
        return Err("Invalid secret key length".to_string());
    }

    let secret_key = SecretKey::from_slice(&secret_bytes)
        .map_err(|_| "Invalid secret key format".to_string())?;
    if encrypted.len() < 32 {
        return Err("Encrypted payload too short".to_string());
    }

    let ephemeral_public_bytes: [u8; 32] = encrypted[..32]
        .try_into()
        .map_err(|_| "Invalid ephemeral public key")?;
    let ciphertext = &encrypted[32..];

    let ephemeral_public_key = PublicKey::from(ephemeral_public_bytes);
    let recipient_public_key = secret_key.public_key();

    let mut hasher = Sha256::new();
    hasher.update(ephemeral_public_key.as_bytes());
    hasher.update(recipient_public_key.as_bytes());
    let hash_result = hasher.finalize();
    let nonce: [u8; 24] = hash_result[..24]
        .try_into()
        .map_err(|_| "Failed to derive nonce")?;

    let salsa_box = SalsaBox::new(&ephemeral_public_key, &secret_key);
    let decrypted = salsa_box
        .decrypt((&nonce).into(), ciphertext)
        .map_err(|_| "Decryption failed")?;

    String::from_utf8(decrypted).map_err(|e| format!("Invalid UTF-8: {e}"))
}

/// Version byte prefixing the ChaCha20-Poly1305 ciphertext format.
const VERSION: u8 = 0x01;

/// Domain separator for the relay E2E private key (`relay_private_key_encrypted`).
const RELAY_KEY_DOMAIN: &[u8] = b"cairn-relay-key-v1";
/// Domain separator for the webhook secret (`webhook_secret`).
pub(crate) const WEBHOOK_SECRET_DOMAIN: &[u8] = b"cairn-webhook-secret-v1";
/// Domain separator for the GitHub App private key (`private_key`).
pub(crate) const APP_PRIVATE_KEY_DOMAIN: &[u8] = b"cairn-github-app-key-v1";
/// Domain separator for the relay channel shared secret (`relay_secret`).
pub(crate) const RELAY_SECRET_DOMAIN: &[u8] = b"cairn-relay-secret-v1";

/// Derive a 32-byte key from the machine ID and a domain separator.
fn derive_storage_key(machine_id: &str, domain: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(machine_id.as_bytes());
    hasher.update(domain);
    hasher.finalize().into()
}

/// Encrypt a value for at-rest storage under the given domain.
///
/// Format: `[version: 1 byte][nonce: 12 bytes][ciphertext + 16-byte auth tag]`,
/// base64-encoded.
pub(crate) fn encrypt_at_rest(
    plaintext: &str,
    machine_id: &str,
    domain: &[u8],
) -> Result<String, String> {
    let key = derive_storage_key(machine_id, domain);
    let cipher = ChaCha20Poly1305::new_from_slice(&key)
        .map_err(|e| format!("Failed to create cipher: {e}"))?;

    let mut nonce_bytes = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| format!("Encryption failed: {e}"))?;

    let mut out = Vec::with_capacity(1 + 12 + ciphertext.len());
    out.push(VERSION);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(BASE64.encode(&out))
}

/// Decrypt an at-rest value under the given domain.
///
/// Legacy plaintext values (a PEM key, or anything that is not our versioned
/// ciphertext) pass through unchanged so existing installs migrate on the next
/// write. A value that *is* in our ciphertext format but fails to decrypt (wrong
/// machine or tampering) returns an error rather than leaking ciphertext.
pub(crate) fn decrypt_at_rest(
    value: &str,
    machine_id: &str,
    domain: &[u8],
) -> Result<String, String> {
    // PEM private keys are unambiguously plaintext.
    if value.contains("-----BEGIN") {
        return Ok(value.to_string());
    }

    let Ok(bytes) = BASE64.decode(value) else {
        // Not base64 → legacy plaintext.
        return Ok(value.to_string());
    };

    if bytes.first() == Some(&VERSION) && bytes.len() >= 29 {
        decrypt_chacha20(&bytes, machine_id, domain)
    } else {
        // Not our ciphertext format → legacy plaintext.
        Ok(value.to_string())
    }
}

fn decrypt_chacha20(bytes: &[u8], machine_id: &str, domain: &[u8]) -> Result<String, String> {
    if bytes.len() < 29 {
        return Err("Encrypted data too short for ChaCha20 format".to_string());
    }
    let nonce_bytes: [u8; 12] = bytes[1..13].try_into().map_err(|_| "Invalid nonce")?;
    let ciphertext = &bytes[13..];

    let key = derive_storage_key(machine_id, domain);
    let cipher = ChaCha20Poly1305::new_from_slice(&key)
        .map_err(|e| format!("Failed to create cipher: {e}"))?;

    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce_bytes), ciphertext)
        .map_err(|_| "Decryption failed: authentication error or wrong machine".to_string())?;
    String::from_utf8(plaintext).map_err(|e| format!("Invalid UTF-8: {e}"))
}

/// Encrypt the relay E2E private key for storage (`RELAY_KEY_DOMAIN`).
pub fn encrypt_private_key(private_key: &str, machine_id: &str) -> Result<String, String> {
    encrypt_at_rest(private_key, machine_id, RELAY_KEY_DOMAIN)
}

/// Decrypt the relay E2E private key from storage.
///
/// Supports both the current ChaCha20-Poly1305 format and the legacy XOR format
/// written by older builds.
pub fn decrypt_private_key(encrypted_b64: &str, machine_id: &str) -> Result<String, String> {
    let encrypted = BASE64
        .decode(encrypted_b64)
        .map_err(|e| format!("Invalid base64: {e}"))?;

    if encrypted.is_empty() {
        return Err("Empty encrypted data".to_string());
    }

    if encrypted[0] == VERSION && encrypted.len() >= 29 {
        decrypt_chacha20(&encrypted, machine_id, RELAY_KEY_DOMAIN)
    } else {
        // Legacy XOR format.
        let key = derive_storage_key(machine_id, RELAY_KEY_DOMAIN);
        let decrypted: Vec<u8> = encrypted
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 32])
            .collect();
        String::from_utf8(decrypted).map_err(|e| format!("Invalid UTF-8: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_relay_keypair_is_base64_x25519() {
        let (public_key, secret_key) = generate_keypair();
        assert_eq!(BASE64.decode(public_key).unwrap().len(), 32);
        assert_eq!(BASE64.decode(secret_key).unwrap().len(), 32);
    }

    #[test]
    fn relay_payload_rejects_short_ciphertext() {
        let secret_key = BASE64.encode([0u8; 32]);
        let payload = BASE64.encode([1, 2, 3, 4]);
        let err = decrypt_payload(&payload, &secret_key).unwrap_err();
        assert!(err.contains("too short"));
    }

    #[test]
    fn roundtrip_under_app_key_domain() {
        let pt = "-----BEGIN RSA PRIVATE KEY-----\nMII...\n-----END RSA PRIVATE KEY-----";
        let machine = "machine-A";
        let enc = encrypt_at_rest(pt, machine, APP_PRIVATE_KEY_DOMAIN).unwrap();
        // Ciphertext must not contain the plaintext.
        assert!(!enc.contains("BEGIN RSA"));
        let dec = decrypt_at_rest(&enc, machine, APP_PRIVATE_KEY_DOMAIN).unwrap();
        assert_eq!(dec, pt);
    }

    #[test]
    fn wrong_machine_fails_to_decrypt() {
        let enc = encrypt_at_rest("secret", "machine-A", RELAY_SECRET_DOMAIN).unwrap();
        let err = decrypt_at_rest(&enc, "machine-B", RELAY_SECRET_DOMAIN).unwrap_err();
        assert!(err.contains("authentication error"));
    }

    #[test]
    fn wrong_domain_fails_to_decrypt() {
        let enc = encrypt_at_rest("secret", "machine-A", WEBHOOK_SECRET_DOMAIN).unwrap();
        // Same machine, different field domain must not cross-decrypt.
        let err = decrypt_at_rest(&enc, "machine-A", APP_PRIVATE_KEY_DOMAIN).unwrap_err();
        assert!(err.contains("authentication error"));
    }

    #[test]
    fn tampering_is_detected() {
        let enc = encrypt_at_rest("secret", "machine-A", APP_PRIVATE_KEY_DOMAIN).unwrap();
        let mut bytes = BASE64.decode(&enc).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        let tampered = BASE64.encode(&bytes);
        let err = decrypt_at_rest(&tampered, "machine-A", APP_PRIVATE_KEY_DOMAIN).unwrap_err();
        assert!(err.contains("authentication error"));
    }

    #[test]
    fn legacy_plaintext_pem_passes_through() {
        let pem = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----";
        let out = decrypt_at_rest(pem, "any-machine", APP_PRIVATE_KEY_DOMAIN).unwrap();
        assert_eq!(out, pem);
    }

    #[test]
    fn legacy_plaintext_non_base64_passes_through() {
        // A relay secret with characters outside the standard base64 alphabet.
        let secret = "relay-secret-with-dashes_and_underscores";
        let out = decrypt_at_rest(secret, "any-machine", RELAY_SECRET_DOMAIN).unwrap();
        assert_eq!(out, secret);
    }

    #[test]
    fn relay_private_key_chacha_roundtrip() {
        let enc = encrypt_private_key("relay-priv", "machine-A").unwrap();
        assert_eq!(
            decrypt_private_key(&enc, "machine-A").unwrap(),
            "relay-priv"
        );
    }

    #[test]
    fn relay_private_key_legacy_xor_roundtrip() {
        // Reproduce the legacy XOR format and ensure the fallback still reads it.
        let machine = "machine-A";
        let key = derive_storage_key(machine, RELAY_KEY_DOMAIN);
        let pt = "legacy-relay-key";
        let xor: Vec<u8> = pt
            .as_bytes()
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 32])
            .collect();
        let legacy = BASE64.encode(&xor);
        assert_eq!(decrypt_private_key(&legacy, machine).unwrap(), pt);
    }
}
