//! Shared TOTP-style passcode functions for MCP authentication.
//!
//! Both the Tauri backend and MCP binary use these functions to generate
//! and validate time-based passcodes from a shared secret.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

/// Number of bytes in the shared secret.
pub const SECRET_LEN: usize = 32;

/// Time step for passcode generation (60 seconds).
pub const TIME_STEP_SECS: u64 = 60;

/// Get current time step (floor(current_time / TIME_STEP_SECS)).
pub fn current_time_step() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / TIME_STEP_SECS
}

/// Generate a passcode for a specific time step.
pub fn generate_passcode_for_step(secret: &[u8; SECRET_LEN], time_step: u64) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(&time_step.to_be_bytes());
    let result = mac.finalize().into_bytes();

    // Take first 8 bytes, encode as hex (16 hex chars = 64 bits)
    hex::encode(&result[..8])
}

/// Generate passcode for current time step.
pub fn generate_passcode(secret: &[u8; SECRET_LEN]) -> String {
    generate_passcode_for_step(secret, current_time_step())
}

/// Decode a base64-encoded secret.
pub fn decode_secret(encoded: &str) -> Option<[u8; SECRET_LEN]> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;

    if bytes.len() != SECRET_LEN {
        return None;
    }

    let mut secret = [0u8; SECRET_LEN];
    secret.copy_from_slice(&bytes);
    Some(secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_passcode_deterministic() {
        let secret: [u8; 32] = [0xab; 32];
        let step = 12345u64;

        let p1 = generate_passcode_for_step(&secret, step);
        let p2 = generate_passcode_for_step(&secret, step);

        assert_eq!(p1, p2, "Same secret and step should produce same passcode");
        assert_eq!(p1.len(), 16, "Passcode should be 16 hex chars");
    }

    #[test]
    fn test_generate_passcode_different_steps() {
        let secret: [u8; 32] = [0xab; 32];

        let p1 = generate_passcode_for_step(&secret, 100);
        let p2 = generate_passcode_for_step(&secret, 101);

        assert_ne!(p1, p2, "Different steps should produce different passcodes");
    }

    #[test]
    fn test_generate_passcode_different_secrets() {
        let secret1: [u8; 32] = [0xab; 32];
        let secret2: [u8; 32] = [0xcd; 32];
        let step = 100u64;

        let p1 = generate_passcode_for_step(&secret1, step);
        let p2 = generate_passcode_for_step(&secret2, step);

        assert_ne!(
            p1, p2,
            "Different secrets should produce different passcodes"
        );
    }

    #[test]
    fn test_decode_secret_roundtrip() {
        use base64::Engine;
        let secret: [u8; 32] = [0xab; 32];
        let encoded = base64::engine::general_purpose::STANDARD.encode(secret);
        let decoded = decode_secret(&encoded).unwrap();
        assert_eq!(secret, decoded);
    }

    #[test]
    fn test_decode_secret_invalid_length() {
        use base64::Engine;
        let short = base64::engine::general_purpose::STANDARD.encode(&[0u8; 16]);
        assert!(decode_secret(&short).is_none());
    }

    #[test]
    fn test_decode_secret_invalid_base64() {
        assert!(decode_secret("not valid base64!!!").is_none());
    }
}
