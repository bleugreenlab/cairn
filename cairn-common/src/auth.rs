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

/// Load the MCP bearer token from the `mcp_auth_secret` under a specific home
/// directory. Used to authenticate to a *different* Cairn instance (e.g. a
/// running `dev:instance`) whose secret lives under its own `CAIRN_HOME`.
pub fn load_mcp_token_from(home: &std::path::Path) -> Option<String> {
    use base64::Engine;

    let secret = std::fs::read(home.join("mcp_auth_secret")).ok()?;

    if secret.len() != SECRET_LEN {
        return None;
    }

    Some(base64::engine::general_purpose::STANDARD.encode(secret))
}

/// Load the local MCP bearer token from the on-disk secret under this process's
/// own Cairn home.
pub fn load_local_mcp_token() -> Option<String> {
    load_mcp_token_from(&crate::paths::cairn_home())
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
        let short = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);
        assert!(decode_secret(&short).is_none());
    }

    #[test]
    fn test_decode_secret_invalid_base64() {
        assert!(decode_secret("not valid base64!!!").is_none());
    }
}
