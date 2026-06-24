//! Shared secret bearer token authentication for MCP callbacks.
//!
//! The shared secret is stored in `~/.cairn/mcp_auth_secret` and passed to
//! the MCP binary as a base64-encoded env var. The binary sends it directly
//! as a static bearer token; the callback server compares it against the
//! locally-stored secret.

use cairn_common::auth::SECRET_LEN;
use rand::Rng;
use std::path::PathBuf;
use std::sync::Mutex;

/// Shared state for MCP authentication
#[derive(Debug)]
pub struct McpAuthState {
    /// Cached secret (loaded once on first use)
    secret: Mutex<Option<[u8; SECRET_LEN]>>,
    /// Config directory for secret file storage
    config_dir: PathBuf,
}

impl McpAuthState {
    pub fn new(config_dir: PathBuf) -> Self {
        Self {
            secret: Mutex::new(None),
            config_dir,
        }
    }

    /// Get or create the shared secret.
    ///
    /// On first call, reads from `<config_dir>/mcp_auth_secret` or generates a new one.
    /// Subsequent calls return the cached value.
    fn get_secret(&self) -> Result<[u8; SECRET_LEN], String> {
        let mut guard = self.secret.lock().map_err(|e| e.to_string())?;
        if let Some(secret) = *guard {
            return Ok(secret);
        }
        let secret = get_or_create_secret(&self.config_dir)?;
        *guard = Some(secret);
        Ok(secret)
    }

    /// Get the secret as base64 for passing to MCP binary via env var.
    pub fn get_secret_for_mcp(&self) -> Result<String, String> {
        let secret = self.get_secret()?;
        Ok(base64_encode(&secret))
    }

    /// Materialize the shared secret on disk if it is not present yet.
    ///
    /// The secret is otherwise created lazily on the first agent spawn or
    /// inbound callback. Calling this when the callback server starts makes the
    /// invariant "a running server has a readable `mcp_auth_secret`" hold, so
    /// tooling that authenticates to a running instance by reading its secret
    /// file (e.g. `cairn://dev/db`/`cairn://dev/pid` querying a `dev:instance`) works even before
    /// the instance has done any MCP work. Idempotent.
    pub fn ensure_secret(&self) -> Result<(), String> {
        self.get_secret().map(|_| ())
    }

    /// Validate a bearer token from an MCP callback request.
    ///
    /// Compares the token directly against the base64-encoded shared secret.
    pub fn validate_token(&self, token: &str) -> Result<(), String> {
        let secret = self.get_secret()?;
        let expected = base64_encode(&secret);

        if token == expected {
            Ok(())
        } else {
            Err("Invalid token".to_string())
        }
    }
}

/// Simple base64 encoding for the secret.
fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Get or create the shared secret from the config directory.
fn get_or_create_secret(config_dir: &std::path::Path) -> Result<[u8; SECRET_LEN], String> {
    let path = config_dir.join("mcp_auth_secret");

    // Try to read existing secret
    if path.exists() {
        let contents =
            std::fs::read(&path).map_err(|e| format!("Failed to read secret file: {}", e))?;

        if contents.len() == SECRET_LEN {
            let mut secret = [0u8; SECRET_LEN];
            secret.copy_from_slice(&contents);
            log::info!("Loaded MCP auth secret from {:?}", path);
            return Ok(secret);
        }

        // Invalid file, will regenerate
        log::warn!(
            "Invalid MCP auth secret file (len={}), regenerating",
            contents.len()
        );
    }

    // Generate new secret
    let secret: [u8; SECRET_LEN] = rand::thread_rng().gen();

    // Ensure config directory exists
    std::fs::create_dir_all(config_dir)
        .map_err(|e| format!("Failed to create config directory: {}", e))?;

    // Write secret to file
    std::fs::write(&path, secret).map_err(|e| format!("Failed to write secret file: {}", e))?;

    // Set restrictive permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("Failed to set permissions on secret file: {}", e))?;
    }

    log::info!("Generated new MCP auth secret at {:?}", path);
    Ok(secret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::auth::decode_secret;

    #[test]
    fn test_validate_token_correct() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret)),
            config_dir: PathBuf::from("/tmp"),
        };

        let token = base64_encode(&secret);
        assert!(auth_state.validate_token(&token).is_ok());
    }

    #[test]
    fn test_validate_token_invalid() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret)),
            config_dir: PathBuf::from("/tmp"),
        };

        assert!(auth_state.validate_token("invalid").is_err());
        assert!(auth_state.validate_token("").is_err());
        assert!(auth_state.validate_token("0000000000000000").is_err());
    }

    #[test]
    fn test_validate_token_wrong_secret() {
        let secret1: [u8; 32] = [0x42; 32];
        let secret2: [u8; 32] = [0xab; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret1)),
            config_dir: PathBuf::from("/tmp"),
        };

        let wrong_token = base64_encode(&secret2);
        assert!(auth_state.validate_token(&wrong_token).is_err());
    }

    #[test]
    fn test_base64_roundtrip() {
        let secret: [u8; 32] = [0xab; 32];
        let encoded = base64_encode(&secret);
        let decoded = decode_secret(&encoded).unwrap();

        assert_eq!(secret, decoded);
    }

    #[test]
    fn test_get_secret_for_mcp() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret)),
            config_dir: PathBuf::from("/tmp"),
        };

        let encoded = auth_state.get_secret_for_mcp().unwrap();
        let decoded = decode_secret(&encoded).unwrap();

        assert_eq!(secret, decoded);
    }
}
