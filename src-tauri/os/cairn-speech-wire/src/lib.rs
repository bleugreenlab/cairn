//! Cairn's speech wires: the provider protocols the cairn-models facet's
//! `speak` verb drives.
//!
//! - [`elevenlabs`]: ElevenLabs' text-to-speech WebSocket
//!   (`/v1/text-to-speech/{voice_id}/stream-input`), which takes text as it is
//!   produced and streams audio back.
//! - [`openai_speech`]: OpenAI-compatible `audio/speech`, which takes whole
//!   text and streams audio back, raw or as server-sent events. Any compatible
//!   server (Kokoro-FastAPI, for one) speaks it too.
//!
//! Like `cairn-llm-wire`, families are named after the protocol, endpoints are
//! data passed in, and nothing here knows about catalogs, accounts or usage
//! meters. A leaf: it reaches no other workspace crate. The facet maps these
//! DTOs and [`SpeechWireError`] onto its own vocabulary.

pub mod elevenlabs;
pub mod openai_speech;
mod tls;

/// Why a speech request failed, classified so a host can tell what to do next.
/// Every message names the provider and, where there was one, the status.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpeechWireError {
    /// The key is missing, wrong, or not permitted to do this.
    #[error("{0}")]
    Auth(String),
    /// Too many requests right now; retrying later can succeed.
    #[error("{0}")]
    RateLimited(String),
    /// Credits or quota are exhausted; retrying will not help until the
    /// account is topped up.
    #[error("{0}")]
    QuotaExceeded(String),
    /// The request itself is wrong (an unknown voice, a bad parameter).
    #[error("{0}")]
    InvalidRequest(String),
    /// The provider served everything else, but the requested output format is
    /// one the account's plan may not include. Retrying silently with another
    /// format would change what the caller gets, so this is its own error.
    #[error("{message}")]
    FormatNotOnPlan { format: String, message: String },
    /// The provider declined to speak the text (content policy, moderation).
    #[error("{0}")]
    Refused(String),
    /// The provider failed. `status` is the HTTP status, or the WebSocket close
    /// code when the failure arrived as a close frame.
    #[error("{message}")]
    Upstream { status: u16, message: String },
    /// The connection failed, stalled, or closed unexpectedly.
    #[error("{0}")]
    Transport(String),
    /// The provider answered with something this wire cannot read.
    #[error("{0}")]
    InvalidResponse(String),
}

/// `value` percent-encoded for a URL path segment or query value.
pub(crate) fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn percent_encoding_keeps_only_unreserved_bytes() {
        assert_eq!(
            super::percent_encode("eleven_flash_v2.5~x"),
            "eleven_flash_v2.5~x"
        );
        assert_eq!(super::percent_encode("a b/c?é"), "a%20b%2Fc%3F%C3%A9");
    }
}
