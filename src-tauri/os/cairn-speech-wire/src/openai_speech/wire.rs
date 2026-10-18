//! `audio/speech` request bodies, SSE events, and error classification.

use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::SpeechWireError;

/// How the audio comes back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFormat {
    /// The encoded bytes as the response body.
    Audio,
    /// Server-sent events with base64 audio and a final usage event.
    Sse,
}

/// One speech request.
#[derive(Debug, Clone, Copy)]
pub struct SpeechBody<'a> {
    pub model: &'a str,
    /// The whole text to speak.
    pub input: &'a str,
    /// A built-in voice name, or a custom voice id (`voice_...`).
    pub voice: &'a str,
    /// `pcm`, `mp3`, `opus`, ...
    pub response_format: &'a str,
    pub speed: Option<f32>,
    /// Free-text delivery instructions; OpenAI's `tts-1` ignores them.
    pub instructions: Option<&'a str>,
    pub stream_format: StreamFormat,
    /// Provider-specific fields (a compatible server such as Kokoro takes its
    /// own parameters). They never replace a typed field: those carry the
    /// contract the request was admitted under.
    pub extra_body: Option<&'a Map<String, Value>>,
}

/// The voice as OpenAI takes it: a name as a string, a custom voice as
/// `{"id": ...}`.
pub fn voice_value(voice: &str) -> Value {
    if voice.starts_with("voice_") {
        json!({ "id": voice })
    } else {
        json!(voice)
    }
}

/// The body fields this wire sets itself, which extras may not replace.
pub const TYPED_FIELDS: &[&str] = &[
    "model",
    "input",
    "voice",
    "response_format",
    "speed",
    "instructions",
    "stream_format",
];

/// The request body. `stream_format` is sent only for SSE, since raw audio is
/// the base protocol and some compatible servers know no such field. Extras
/// go in first and every typed field after, so no extra can replace one.
pub fn build_body(body: &SpeechBody<'_>) -> Value {
    let mut out = Map::new();
    if let Some(extra) = body.extra_body {
        out.extend(
            extra
                .iter()
                .filter(|(key, _)| !TYPED_FIELDS.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
    out.insert("model".into(), json!(body.model));
    out.insert("input".into(), json!(body.input));
    out.insert("voice".into(), voice_value(body.voice));
    out.insert("response_format".into(), json!(body.response_format));
    if let Some(speed) = body.speed {
        out.insert("speed".into(), json!(speed));
    }
    if let Some(instructions) = body.instructions {
        out.insert("instructions".into(), json!(instructions));
    }
    if body.stream_format == StreamFormat::Sse {
        out.insert("stream_format".into(), json!("sse"));
    }
    Value::Object(out)
}

/// Token usage from `speech.audio.done`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct SpeechUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: Option<u64>,
}

/// One SSE event's meaning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseEvent {
    Audio(Vec<u8>),
    Done(Option<SpeechUsage>),
}

#[derive(Deserialize)]
struct RawEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    audio: Option<String>,
    #[serde(default)]
    usage: Option<SpeechUsage>,
    #[serde(default)]
    error: Option<Value>,
}

/// Decode the payload of one `data:` line. `Ok(None)` is an event this wire
/// does not act on.
pub fn decode_sse_data(provider: &str, data: &str) -> Result<Option<SseEvent>, SpeechWireError> {
    let data = data.trim();
    if data.is_empty() || data == "[DONE]" {
        return Ok(None);
    }
    let event: RawEvent = serde_json::from_str(data).map_err(|error| {
        SpeechWireError::InvalidResponse(format!(
            "{provider} sent an unreadable speech event: {error}"
        ))
    })?;
    if let Some(error) = event.error {
        return Err(classify_error(
            provider,
            0,
            &json!({ "error": error }).to_string(),
        ));
    }
    match event.kind.as_str() {
        "speech.audio.delta" => {
            let encoded = event.audio.unwrap_or_default();
            let audio = base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .map_err(|error| {
                    SpeechWireError::InvalidResponse(format!(
                        "{provider} sent speech audio that is not base64: {error}"
                    ))
                })?;
            Ok(Some(SseEvent::Audio(audio)))
        }
        "speech.audio.done" => Ok(Some(SseEvent::Done(event.usage))),
        _ => Ok(None),
    }
}

/// Classify an `{error: {message, type, code}}` reply by its status, keeping
/// the status in the message so a host can tell a rate limit from exhausted
/// credits. `status` 0 is an error event inside a 200 stream.
pub fn classify_error(provider: &str, status: u16, body: &str) -> SpeechWireError {
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let error = parsed
        .as_ref()
        .map(|parsed| parsed.get("error").unwrap_or(parsed));
    let field = |name: &str| {
        error
            .and_then(|error| error.get(name))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let message = Some(field("message"))
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| body.trim().to_string());
    let code = field("code");
    let kind = field("type");
    let label = if status == 0 {
        "stream error".to_string()
    } else {
        format!("HTTP {status}")
    };
    let tag = [code.as_str(), kind.as_str()]
        .into_iter()
        .find(|value| !value.is_empty())
        .map_or(label.clone(), |value| format!("{label}, {value}"));
    let said = format!("{provider} speech ({tag}): {message}");
    // OpenAI's speech reference documents no refusal shape; these are the codes
    // its other endpoints use for content it declines.
    let refusal = [code.as_str(), kind.as_str()].iter().any(|value| {
        value.contains("content_policy")
            || value.contains("moderation")
            || value.contains("content_filter")
            || value.contains("safety")
    });
    if refusal {
        return SpeechWireError::Refused(said);
    }
    match status {
        401 | 403 => SpeechWireError::Auth(said),
        429 if code == "insufficient_quota" || kind == "insufficient_quota" => {
            SpeechWireError::QuotaExceeded(said)
        }
        402 => SpeechWireError::QuotaExceeded(said),
        429 => SpeechWireError::RateLimited(said),
        400 | 404 | 413 | 422 => SpeechWireError::InvalidRequest(said),
        _ => SpeechWireError::Upstream {
            status,
            message: said,
        },
    }
}
