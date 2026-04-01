//! JWT encoding/decoding and encryption for account storage.
//!
//! Moved from the legacy teams module. Used by AccountManager for JWT lifecycle.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

use crate::identity::crypto;

/// Domain separator for JWT encryption
const JWT_KEY_DOMAIN: &[u8] = b"cairn-team-v1";

/// Claims extracted from a JWT payload (no signature verification — API already verified).
#[derive(Debug, Clone)]
pub struct JwtClaims {
    pub sub: String,
    pub org_id: String,
    pub org_role: String,
    pub exp: i64,
    pub name: Option<String>,
    pub email: Option<String>,
}

/// Decode JWT claims from the payload segment without verifying signature.
pub fn decode_jwt_claims(jwt: &str) -> Result<JwtClaims, String> {
    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() != 3 {
        return Err("Invalid JWT format".to_string());
    }

    let payload_bytes = BASE64
        .decode(parts[1])
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]))
        .map_err(|e| format!("Failed to decode JWT payload: {}", e))?;

    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|e| format!("Failed to parse JWT payload: {}", e))?;

    Ok(JwtClaims {
        sub: payload["sub"]
            .as_str()
            .ok_or("Missing 'sub' claim")?
            .to_string(),
        org_id: payload["org_id"]
            .as_str()
            .ok_or("Missing 'org_id' claim")?
            .to_string(),
        org_role: payload["org_role"]
            .as_str()
            .ok_or("Missing 'org_role' claim")?
            .to_string(),
        exp: payload["exp"].as_i64().ok_or("Missing 'exp' claim")?,
        name: payload["name"].as_str().map(|s| s.to_string()),
        email: payload["email"].as_str().map(|s| s.to_string()),
    })
}

// === Encryption helpers ===

fn derive_key(machine_id: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(machine_id.as_bytes());
    hasher.update(JWT_KEY_DOMAIN);
    hasher.finalize().into()
}

fn encrypt_jwt(plaintext: &str, machine_id: &str) -> Result<String, String> {
    use chacha20poly1305::{
        aead::{Aead, KeyInit},
        ChaCha20Poly1305, Nonce,
    };
    use rand::RngCore;

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

fn decrypt_jwt(encrypted_b64: &str, machine_id: &str) -> Result<String, String> {
    use chacha20poly1305::{
        aead::{Aead, KeyInit},
        ChaCha20Poly1305, Nonce,
    };

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
        .map_err(|_| "Decryption failed: wrong machine or corrupted data".to_string())?;

    String::from_utf8(plaintext).map_err(|e| format!("Invalid UTF-8: {}", e))
}

/// Encrypt a JWT for storage using the current machine's identity.
pub fn encrypt_jwt_for_storage(jwt: &str) -> Result<String, String> {
    let machine_id = crypto::get_machine_id();
    encrypt_jwt(jwt, &machine_id)
}

/// Decrypt a stored JWT using the current machine's identity.
pub fn decrypt_jwt_from_storage(encrypted: &str) -> Result<String, String> {
    let machine_id = crypto::get_machine_id();
    decrypt_jwt(encrypted, &machine_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_jwt_claims() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"EdDSA"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            r#"{"sub":"user-1","org_id":"org-1","org_role":"admin","exp":9999999999,"name":"Test","email":"t@t.com"}"#,
        );
        let token = format!("{}.{}.fake-sig", header, payload);

        let claims = decode_jwt_claims(&token).unwrap();
        assert_eq!(claims.sub, "user-1");
        assert_eq!(claims.org_id, "org-1");
        assert_eq!(claims.org_role, "admin");
        assert_eq!(claims.exp, 9999999999);
        assert_eq!(claims.name, Some("Test".to_string()));
    }

    #[test]
    fn test_jwt_encrypt_decrypt_roundtrip() {
        let jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSJ9.test-payload.test-sig";
        let machine_id = "test-machine-123";

        let encrypted = encrypt_jwt(jwt, machine_id).unwrap();
        let decrypted = decrypt_jwt(&encrypted, machine_id).unwrap();

        assert_eq!(jwt, decrypted);
    }

    #[test]
    fn test_jwt_wrong_machine_fails() {
        let jwt = "test-jwt-token";
        let encrypted = encrypt_jwt(jwt, "machine-a").unwrap();
        assert!(decrypt_jwt(&encrypted, "machine-b").is_err());
    }
}
