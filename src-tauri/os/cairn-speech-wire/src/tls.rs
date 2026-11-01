//! The TLS client configuration every `wss://` connection uses: the process's
//! shared one from `cairn_tls`, whose platform root store is loaded once per
//! process. (HTTP goes through reqwest, built on the same config.)

use std::sync::Arc;

use rustls::ClientConfig;

use crate::SpeechWireError;

/// Refused when the platform yielded no roots, so the dial fails with that
/// reason rather than as an unknown-issuer handshake.
pub(crate) fn client_config() -> Result<Arc<ClientConfig>, SpeechWireError> {
    if cairn_tls::root_store().is_empty() {
        return Err(SpeechWireError::Transport(
            "no native root certificates were found, so a TLS connection cannot be verified"
                .to_string(),
        ));
    }
    Ok(cairn_tls::websocket_config())
}
