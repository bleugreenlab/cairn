//! The pasteable connection string that lets a desktop watch *another* runner.
//!
//! A runner's two bearer credentials — the session token (`mcp_auth_secret`) and
//! the desktop operator token (`operator_auth_secret`) — live as files under its
//! Cairn home. A desktop on a different machine cannot read them, so
//! `cairn-orch peer-credentials` packs both into one opaque string the operator
//! copies across, and the desktop's add-peer flow parses it back.
//!
//! The string carries operator authority over the peer. It is printed once to
//! the terminal of whoever ran the command, stored by the receiving desktop in
//! the OS keychain, and never logged: [`PeerConnection`]'s `Debug` is redacted
//! so it cannot leak through a stray `{:?}`.

use std::fmt;

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::auth::decode_secret;

/// Version tag on every connection string. A future layout gets a new tag
/// rather than a silently different payload under this one.
pub const PEER_CONNECTION_PREFIX: &str = "cairn-peer-v1:";

/// Both credentials a desktop needs to act as the operator of a peer runner,
/// plus the loopback port the peer's runner listens on (informational: the
/// desktop reaches the peer through whatever URL the operator fronted it with).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerConnection {
    /// The runner session token, base64 of the 32-byte `mcp_auth_secret`.
    pub session: String,
    /// The desktop operator token, base64 of the 32-byte `operator_auth_secret`.
    pub operator: String,
    /// The peer runner's loopback port.
    pub port: u16,
}

impl fmt::Debug for PeerConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerConnection")
            .field("session", &"<redacted>")
            .field("operator", &"<redacted>")
            .field("port", &self.port)
            .finish()
    }
}

/// Why a pasted string is not a usable connection string. Messages are shown to
/// the operator verbatim and never echo the input, which may be a real secret
/// pasted into the wrong field.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeerConnectionError {
    #[error("not a Cairn peer connection string (expected it to start with `{PEER_CONNECTION_PREFIX}`); run `cairn-orch peer-credentials` on the peer and paste its output")]
    MissingPrefix,
    #[error("the connection string is truncated or corrupted; copy it again in full")]
    Malformed,
    #[error("the connection string's {0} token is not a Cairn runner credential")]
    InvalidToken(&'static str),
}

impl PeerConnection {
    /// Render as `cairn-peer-v1:<base64url(JSON)>`.
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("PeerConnection serializes");
        format!(
            "{PEER_CONNECTION_PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
        )
    }

    /// Parse a pasted connection string. Surrounding whitespace is tolerated,
    /// because terminals and chat clients add it; nothing else is.
    pub fn parse(input: &str) -> Result<Self, PeerConnectionError> {
        let payload = input
            .trim()
            .strip_prefix(PEER_CONNECTION_PREFIX)
            .ok_or(PeerConnectionError::MissingPrefix)?;
        let json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| PeerConnectionError::Malformed)?;
        let connection: Self =
            serde_json::from_slice(&json).map_err(|_| PeerConnectionError::Malformed)?;
        if decode_secret(&connection.session).is_none() {
            return Err(PeerConnectionError::InvalidToken("session"));
        }
        if decode_secret(&connection.operator).is_none() {
            return Err(PeerConnectionError::InvalidToken("operator"));
        }
        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(byte: u8) -> String {
        base64::engine::general_purpose::STANDARD.encode([byte; 32])
    }

    fn sample() -> PeerConnection {
        PeerConnection {
            session: token(0x11),
            operator: token(0x22),
            port: 3849,
        }
    }

    #[test]
    fn connection_string_round_trips() {
        let encoded = sample().encode();
        assert!(encoded.starts_with(PEER_CONNECTION_PREFIX));
        assert_eq!(PeerConnection::parse(&encoded).unwrap(), sample());
        assert_eq!(
            PeerConnection::parse(&format!("  {encoded}\n")).unwrap(),
            sample(),
            "pasted whitespace is tolerated"
        );
    }

    #[test]
    fn malformed_strings_are_named_without_echoing_input() {
        assert_eq!(
            PeerConnection::parse(&token(0x11)),
            Err(PeerConnectionError::MissingPrefix)
        );
        assert_eq!(
            PeerConnection::parse("cairn-peer-v1:!!!"),
            Err(PeerConnectionError::Malformed)
        );
        let encoded = sample().encode();
        assert_eq!(
            PeerConnection::parse(&encoded[..encoded.len() - 6]),
            Err(PeerConnectionError::Malformed)
        );

        let mut short_operator = sample();
        short_operator.operator = base64::engine::general_purpose::STANDARD.encode([0u8; 8]);
        let error = PeerConnection::parse(&short_operator.encode()).unwrap_err();
        assert_eq!(error, PeerConnectionError::InvalidToken("operator"));
        assert!(!error.to_string().contains(&short_operator.operator));
    }

    #[test]
    fn debug_output_never_contains_a_token() {
        let rendered = format!("{:?}", sample());
        assert!(!rendered.contains(&token(0x11)));
        assert!(!rendered.contains(&token(0x22)));
        assert!(rendered.contains("3849"));
    }
}
