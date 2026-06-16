//! Shared secret utilities for MCP authentication.
//!
//! Both the Tauri backend and MCP binary use these functions to work
//! with the shared secret that authenticates MCP callback requests.

/// Number of bytes in the shared secret.
pub const SECRET_LEN: usize = 32;

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

/// Load the local MCP bearer token from the on-disk secret.
pub fn load_local_mcp_token() -> Option<String> {
    use base64::Engine;

    let config_dir = crate::paths::cairn_home();
    let secret = std::fs::read(config_dir.join("mcp_auth_secret")).ok()?;

    if secret.len() != SECRET_LEN {
        return None;
    }

    Some(base64::engine::general_purpose::STANDARD.encode(secret))
}

#[cfg(test)]
mod tests {
    use super::*;

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
