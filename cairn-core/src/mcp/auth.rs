//! TOTP-style rolling passcode authentication for MCP callbacks.
//!
//! Instead of per-session tokens with lifecycle management, we use a shared
//! secret with time-based passcodes:
//! - Secret is generated once and stored in `~/.cairn/mcp_auth_secret`
//! - Both Tauri backend and MCP binary compute passcodes independently
//! - Passcodes are valid for 60-second windows, accepting current + previous
//!
//! This eliminates token lifecycle issues with warm processes.

use cairn_common::auth::{current_time_step, generate_passcode_for_step, SECRET_LEN};
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

    /// Validate a passcode from an MCP callback request.
    ///
    /// Accepts passcodes from current or previous time window (2-minute effective validity).
    pub fn validate_passcode(&self, passcode: &str) -> Result<(), String> {
        let secret = self.get_secret()?;

        // Check current window
        let current = generate_passcode_for_step(&secret, current_time_step());
        if passcode == current {
            return Ok(());
        }

        // Check previous window (handles clock edge cases)
        let prev = generate_passcode_for_step(&secret, current_time_step().saturating_sub(1));
        if passcode == prev {
            return Ok(());
        }

        Err("Invalid passcode".to_string())
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
    use cairn_common::auth::{decode_secret, generate_passcode};

    #[test]
    fn test_validate_passcode_current() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret)),
            config_dir: PathBuf::from("/tmp"),
        };

        let passcode = generate_passcode(&secret);
        assert!(auth_state.validate_passcode(&passcode).is_ok());
    }

    #[test]
    fn test_validate_passcode_previous_window() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret)),
            config_dir: PathBuf::from("/tmp"),
        };

        // Generate passcode for previous window
        let prev_step = current_time_step().saturating_sub(1);
        let passcode = generate_passcode_for_step(&secret, prev_step);

        assert!(auth_state.validate_passcode(&passcode).is_ok());
    }

    #[test]
    fn test_validate_passcode_old_window() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret)),
            config_dir: PathBuf::from("/tmp"),
        };

        // Generate passcode for two windows ago (should fail)
        let old_step = current_time_step().saturating_sub(2);
        let passcode = generate_passcode_for_step(&secret, old_step);

        assert!(auth_state.validate_passcode(&passcode).is_err());
    }

    #[test]
    fn test_validate_passcode_invalid() {
        let secret: [u8; 32] = [0x42; 32];
        let auth_state = McpAuthState {
            secret: Mutex::new(Some(secret)),
            config_dir: PathBuf::from("/tmp"),
        };

        assert!(auth_state.validate_passcode("invalid").is_err());
        assert!(auth_state.validate_passcode("").is_err());
        assert!(auth_state.validate_passcode("0000000000000000").is_err());
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
