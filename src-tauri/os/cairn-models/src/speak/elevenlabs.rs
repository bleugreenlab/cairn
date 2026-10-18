//! Routes onto ElevenLabs' stream-input WebSocket.

use crate::interface::speak::SpeechRoute;
use crate::speech_wire::elevenlabs::{ElevenLabsEndpoint, DEFAULT_BASE};

/// The route to ElevenLabs with `api_key`, on its global endpoint or on
/// `base` (a regional or data-residency host such as
/// `wss://api.eu.residency.elevenlabs.io`). Operators paste the REST address,
/// and Settings stores it as the API root (`https://api.elevenlabs.io/v1`), so
/// an `https://` base is taken as its `wss://` equivalent and a trailing `/v1`
/// is dropped: the socket's own path starts with it.
pub fn route_for(api_key: &str, base: Option<&str>) -> SpeechRoute {
    let base = base
        .map(str::trim)
        .filter(|base| !base.is_empty())
        .unwrap_or(DEFAULT_BASE);
    let base = match base.split_once("://") {
        Some(("https", rest)) => format!("wss://{rest}"),
        Some(("http", rest)) => format!("ws://{rest}"),
        _ => base.to_string(),
    };
    let base = base.trim_end_matches('/');
    let base = base.strip_suffix("/v1").unwrap_or(base);
    SpeechRoute::ElevenLabs(ElevenLabsEndpoint {
        base: base.to_string(),
        api_key: api_key.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(route: SpeechRoute) -> String {
        match route {
            SpeechRoute::ElevenLabs(endpoint) => endpoint.base,
            other => panic!("expected ElevenLabs, got {other:?}"),
        }
    }

    #[test]
    fn the_base_defaults_and_a_rest_address_becomes_its_socket() {
        assert_eq!(base(route_for("k", None)), "wss://api.elevenlabs.io");
        assert_eq!(base(route_for("k", Some(" "))), "wss://api.elevenlabs.io");
        assert_eq!(
            base(route_for(
                "k",
                Some("https://api.eu.residency.elevenlabs.io/")
            )),
            "wss://api.eu.residency.elevenlabs.io"
        );
        assert_eq!(
            base(route_for("k", Some("ws://127.0.0.1:9"))),
            "ws://127.0.0.1:9"
        );
        // The API root Settings stores.
        assert_eq!(
            base(route_for("k", Some("https://api.elevenlabs.io/v1"))),
            "wss://api.elevenlabs.io"
        );
    }
}
