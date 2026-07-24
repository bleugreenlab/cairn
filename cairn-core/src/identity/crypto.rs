//! Encryption helpers for identity credential storage.
//!
//! Uses ChaCha20Poly1305 with a machine-derived key to encrypt sensitive
//! identity fields (Claude tokens, API keys) at rest in identity.yaml.
//! Same pattern as `src-tauri/src/github/crypto.rs`.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Nonce,
};
use rand::RngCore;
use sha2::{Digest, Sha256};

/// Domain separator for identity credential encryption
const IDENTITY_KEY_DOMAIN: &[u8] = b"cairn-identity-v1";

/// Encrypt a credential value for storage.
///
/// Format: [version: 1 byte][nonce: 12 bytes][ciphertext + 16-byte auth tag]
pub(crate) fn encrypt_credential(plaintext: &str, machine_id: &str) -> Result<String, String> {
    const VERSION: u8 = 0x01;

    let derived_key = derive_key(machine_id);
    let cipher = ChaCha20Poly1305::new_from_slice(&derived_key)
        .map_err(|e| format!("Failed to create cipher: {}", e))?;

    let mut nonce_bytes = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| format!("Encryption failed: {}", e))?;

    let mut output = Vec::with_capacity(1 + 12 + ciphertext.len());
    output.push(VERSION);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);

    Ok(BASE64.encode(&output))
}

/// Decrypt a credential value from storage.
pub(crate) fn decrypt_credential(encrypted_b64: &str, machine_id: &str) -> Result<String, String> {
    let encrypted = BASE64
        .decode(encrypted_b64)
        .map_err(|e| format!("Invalid base64: {}", e))?;

    if encrypted.len() < 29 {
        return Err("Encrypted data too short".to_string());
    }

    if encrypted[0] != 0x01 {
        return Err("Unknown encryption format version".to_string());
    }

    let nonce_bytes: [u8; 12] = encrypted[1..13].try_into().map_err(|_| "Invalid nonce")?;
    let ciphertext = &encrypted[13..];

    let derived_key = derive_key(machine_id);
    let cipher = ChaCha20Poly1305::new_from_slice(&derived_key)
        .map_err(|e| format!("Failed to create cipher: {}", e))?;

    let nonce = Nonce::from_slice(&nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| "Decryption failed: authentication error or wrong machine".to_string())?;

    String::from_utf8(plaintext).map_err(|e| format!("Invalid UTF-8: {}", e))
}

/// Derive a 32-byte key from machine ID for identity credential encryption.
fn derive_key(machine_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(machine_id.as_bytes());
    hasher.update(IDENTITY_KEY_DOMAIN);
    hasher.finalize().into()
}

/// Get a machine-specific identifier for key derivation.
///
/// Uses hardware UUID on macOS, falls back to hostname+username.
pub fn get_machine_id() -> String {
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("ioreg")
            .args(["-rd1", "-c", "IOPlatformExpertDevice"])
            .output()
        {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                if line.contains("IOPlatformUUID") {
                    if let Some(uuid) = line.split('"').nth(3) {
                        return uuid.to_string();
                    }
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(id) = std::fs::read_to_string("/etc/machine-id") {
            let id = id.trim();
            if !id.is_empty() {
                return id.to_string();
            }
        }
    }

    // Fallback: hostname + username
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown-host".to_string());
    let username = whoami::username();
    format!("{}-{}", hostname, username)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let plaintext = "sk-ant-oat01-test-token-12345";
        let machine_id = "test-machine";

        let encrypted = encrypt_credential(plaintext, machine_id).unwrap();
        let decrypted = decrypt_credential(&encrypted, machine_id).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_wrong_machine_id_fails() {
        let plaintext = "secret-token";
        let encrypted = encrypt_credential(plaintext, "machine-1").unwrap();

        let result = decrypt_credential(&encrypted, "machine-2");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("authentication error"));
    }

    #[test]
    fn test_different_machine_ids_produce_different_ciphertext() {
        let plaintext = "same-token";
        let enc1 = encrypt_credential(plaintext, "machine-1").unwrap();
        let enc2 = encrypt_credential(plaintext, "machine-2").unwrap();
        assert_ne!(enc1, enc2);
    }

    #[test]
    fn test_tampering_detected() {
        let encrypted = encrypt_credential("token", "machine").unwrap();
        let mut bytes = BASE64.decode(&encrypted).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        let tampered = BASE64.encode(&bytes);

        assert!(decrypt_credential(&tampered, "machine").is_err());
    }

    #[test]
    fn test_get_machine_id_not_empty() {
        let id = get_machine_id();
        assert!(!id.is_empty());
    }
}
