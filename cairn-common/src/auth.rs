//! Shared secret utilities for the two credentials a Cairn install holds.
//!
//! `mcp_auth_secret` authenticates MCP callbacks and is deliberately handed to
//! every agent process. `operator_auth_secret` authenticates a desktop operator
//! answering an authority prompt and is deliberately handed to nothing but the
//! native desktop shell. They are the same shape and must never be the same
//! value, which is why the two file names live here beside each other.

use std::fmt;

/// Number of bytes in a shared secret.
pub const SECRET_LEN: usize = 32;

/// File name of the MCP callback credential, under a Cairn home.
pub const MCP_SECRET_FILE: &str = "mcp_auth_secret";

/// File name of the desktop operator credential, under a Cairn home.
///
/// This one is a capability: presenting it to `/api/invoke` is what lets an
/// answer to an authority prompt mint an authority grant. It is never placed in
/// an agent's environment, and `cairn_core::authorization::protected` refuses
/// agent reads and writes of the file itself.
pub const OPERATOR_SECRET_FILE: &str = "operator_auth_secret";

/// The HTTP header the desktop operator credential is presented in.
///
/// A wire contract between the native desktop shell that sends it and the
/// runner transport that checks it, which share no other type. It lives beside
/// the file name so a rename cannot move one without the other.
pub const OPERATOR_TOKEN_HEADER: &str = "x-cairn-operator-token";

/// A shared secret whose bytes cannot escape by accident.
///
/// There is no `Serialize`, so a struct that holds one and derives `Serialize`
/// does not compile — wholesale serialization of app or session state cannot
/// carry a secret out. `Debug` and `Display` redact, so an incidental `{:?}` in
/// a log line cannot either. Everything else has to go through [`Self::expose`]
/// or [`Self::expose_base64`], which makes every deliberate exposure a named,
/// greppable, countable call site rather than a discipline someone has to
/// remember.
///
/// The wrapper narrows the surface; it does not close it. What pins the set of
/// deliberate exposures is a test that enumerates them
/// (`cairn_core::mcp::auth::tests::the_operator_credential_has_no_unexpected_exposure_sites`),
/// which fails naming the new site on the change that adds one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SharedSecret([u8; SECRET_LEN]);

impl SharedSecret {
    /// Adopt raw bytes as a secret. Rejects any length but [`SECRET_LEN`], so a
    /// truncated or corrupt secret file can never become a short credential
    /// that still validates.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SECRET_LEN {
            return None;
        }
        let mut secret = [0u8; SECRET_LEN];
        secret.copy_from_slice(bytes);
        Some(Self(secret))
    }

    /// Adopt a base64-encoded secret.
    pub fn from_base64(encoded: &str) -> Option<Self> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        Self::from_bytes(&bytes)
    }

    /// The raw bytes. A deliberate exposure; see the type's documentation.
    pub fn expose(&self) -> &[u8; SECRET_LEN] {
        &self.0
    }

    /// The wire encoding used by both bearer-token paths. A deliberate
    /// exposure; see the type's documentation.
    pub fn expose_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(self.0)
    }

    /// Whether `token` is this secret's base64 encoding.
    ///
    /// Compared in constant time over the decoded bytes. A wrong length short
    /// circuits, which leaks only the length of a fixed public-format token,
    /// while a same-length candidate is compared without an early exit so the
    /// number of leading correct bytes is not observable through timing.
    pub fn matches_token(&self, token: &str) -> bool {
        let Some(candidate) = Self::from_base64(token) else {
            return false;
        };
        let mut difference = 0u8;
        for (left, right) in self.0.iter().zip(candidate.0.iter()) {
            difference |= left ^ right;
        }
        difference == 0
    }
}

impl fmt::Debug for SharedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SharedSecret(<redacted>)")
    }
}

impl fmt::Display for SharedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Decode a base64-encoded secret.
pub fn decode_secret(encoded: &str) -> Option<[u8; SECRET_LEN]> {
    SharedSecret::from_base64(encoded).map(|secret| *secret.expose())
}

/// Load a bearer token from a named secret file under a Cairn home.
fn load_token_from(home: &std::path::Path, file_name: &str) -> Option<String> {
    let secret = std::fs::read(home.join(file_name)).ok()?;
    SharedSecret::from_bytes(&secret).map(|secret| secret.expose_base64())
}

/// Load the MCP bearer token from the `mcp_auth_secret` under a specific home
/// directory. Used to authenticate to a *different* Cairn instance (e.g. a
/// running `dev:instance`) whose secret lives under its own `CAIRN_HOME`.
pub fn load_mcp_token_from(home: &std::path::Path) -> Option<String> {
    load_token_from(home, MCP_SECRET_FILE)
}

/// Load the local MCP bearer token from the on-disk secret under this process's
/// own Cairn home.
pub fn load_local_mcp_token() -> Option<String> {
    load_mcp_token_from(&crate::paths::cairn_home())
}

/// Load the desktop operator token from the `operator_auth_secret` under a
/// specific home directory.
///
/// The single caller is the native desktop shell, forwarding a permission answer
/// to its own runner. Nothing in an agent's process tree calls this, and adding
/// a caller that runs inside one would defeat the boundary the credential
/// exists to draw.
pub fn load_operator_token_from(home: &std::path::Path) -> Option<String> {
    load_token_from(home, OPERATOR_SECRET_FILE)
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

    #[test]
    fn the_two_credential_files_are_distinct() {
        // Same shape, same directory, opposite audiences: the MCP secret is in
        // every agent's environment and the operator secret must never be. If
        // these ever named one file, the desktop credential would be the token
        // the agent already holds and the boundary would be decorative.
        assert_ne!(MCP_SECRET_FILE, OPERATOR_SECRET_FILE);
    }

    #[test]
    fn a_secret_redacts_itself_in_every_formatting_path() {
        let secret = SharedSecret::from_bytes(&[0x42; SECRET_LEN]).unwrap();
        let encoded = secret.expose_base64();

        for rendered in [format!("{secret:?}"), format!("{secret}")] {
            assert!(
                !rendered.contains(&encoded),
                "a formatting path emitted the secret: {rendered}"
            );
            assert!(rendered.contains("redacted"), "got {rendered}");
        }
    }

    #[test]
    fn token_matching_accepts_only_the_exact_secret() {
        let secret = SharedSecret::from_bytes(&[0x42; SECRET_LEN]).unwrap();

        assert!(secret.matches_token(&secret.expose_base64()));
        assert!(!secret.matches_token(""));
        assert!(!secret.matches_token("not base64 at all!!"));
        assert!(!secret.matches_token(
            &SharedSecret::from_bytes(&[0x43; SECRET_LEN])
                .unwrap()
                .expose_base64()
        ));

        // A prefix of the right secret is not the right secret, and neither is
        // a longer value that starts with it.
        let mut truncated = [0x42u8; SECRET_LEN];
        truncated[SECRET_LEN - 1] = 0;
        assert!(!secret.matches_token(
            &SharedSecret::from_bytes(&truncated)
                .unwrap()
                .expose_base64()
        ));
    }

    #[test]
    fn a_wrong_length_secret_file_yields_no_token() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join(OPERATOR_SECRET_FILE), [0u8; 8]).unwrap();

        assert!(load_operator_token_from(home.path()).is_none());
    }

    #[test]
    fn each_loader_reads_only_its_own_file() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join(MCP_SECRET_FILE), [0x11u8; SECRET_LEN]).unwrap();

        assert!(load_mcp_token_from(home.path()).is_some());
        assert!(
            load_operator_token_from(home.path()).is_none(),
            "the operator loader must not fall back to the MCP secret"
        );
    }
}
