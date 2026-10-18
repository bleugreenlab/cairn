//! Routes onto OpenAI-compatible `audio/speech`.

use crate::interface::speak::SpeechRoute;
use crate::speech_wire::openai_speech::OpenAiSpeechEndpoint;

/// The route to `{base_url}/audio/speech`: OpenAI at
/// `https://api.openai.com/v1`, or a compatible server such as Kokoro-FastAPI
/// at `http://host:8880/v1`. The key, when there is one, rides as a bearer
/// token; `headers` are added after it.
pub fn route_for(
    provider_name: &str,
    base_url: &str,
    api_key: Option<&str>,
    headers: &[(String, String)],
) -> SpeechRoute {
    let mut all = vec![("content-type".to_string(), "application/json".to_string())];
    if let Some(key) = api_key.filter(|key| !key.is_empty()) {
        all.push(("authorization".to_string(), format!("Bearer {key}")));
    }
    all.extend(headers.iter().cloned());
    SpeechRoute::OpenAiSpeech(OpenAiSpeechEndpoint {
        provider_name: provider_name.to_string(),
        url: format!("{}/audio/speech", base_url.trim_end_matches('/')),
        headers: all,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_route_joins_the_path_and_carries_the_key_as_a_bearer() {
        let SpeechRoute::OpenAiSpeech(endpoint) = route_for(
            "OpenAI",
            "https://api.openai.com/v1/",
            Some("sk-1"),
            &[("x-org".into(), "o".into())],
        ) else {
            panic!("expected an OpenAI speech route");
        };
        assert_eq!(endpoint.url, "https://api.openai.com/v1/audio/speech");
        assert_eq!(
            endpoint.headers,
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("authorization".to_string(), "Bearer sk-1".to_string()),
                ("x-org".to_string(), "o".to_string()),
            ]
        );
        let SpeechRoute::OpenAiSpeech(local) = route_for("Kokoro", "http://h:8880/v1", None, &[])
        else {
            panic!("expected an OpenAI speech route");
        };
        assert_eq!(local.headers.len(), 1);
    }
}
