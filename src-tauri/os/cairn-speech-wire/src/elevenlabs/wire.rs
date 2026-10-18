//! ElevenLabs stream-input messages, server frames, and error classification.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::SpeechWireError;

/// ElevenLabs' expressive voice controls. These don't map across providers,
/// so they ride as extras rather than as typed style.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VoiceSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub similarity_boost: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_speaker_boost: Option<bool>,
}

/// Whether ElevenLabs spells out numbers and the like before speaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TextNormalization {
    Auto,
    On,
    Off,
}

impl TextNormalization {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

/// Everything ElevenLabs-specific a request can set. Typed, because these
/// controls split between the handshake query and the initial message.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ElevenLabsExtras {
    /// Initial message.
    pub voice_settings: Option<VoiceSettings>,
    /// Initial message, as `generation_config.chunk_length_schedule`. Each
    /// entry is 50 to 500 characters.
    pub chunk_length_schedule: Option<Vec<u16>>,
    /// Query: ISO 639-1, ignored by models that don't support it.
    pub language_code: Option<String>,
    /// Query.
    pub apply_text_normalization: Option<TextNormalization>,
    /// Query: best-effort determinism.
    pub seed: Option<u32>,
    /// Query: disables the chunk schedule and buffers. Only for whole sentences.
    pub auto_mode: Option<bool>,
    /// Query: seconds of silence before ElevenLabs closes the socket, at most
    /// 180. ElevenLabs' own default is 20.
    pub inactivity_timeout: Option<u16>,
    /// Query: alignment on every audio frame rather than only some.
    pub sync_alignment: Option<bool>,
}

impl ElevenLabsExtras {
    /// The handshake query parameters these extras set, in a fixed order.
    pub fn query(&self) -> Vec<(&'static str, String)> {
        let mut query = Vec::new();
        if let Some(seconds) = self.inactivity_timeout {
            query.push(("inactivity_timeout", seconds.to_string()));
        }
        if let Some(sync) = self.sync_alignment {
            query.push(("sync_alignment", sync.to_string()));
        }
        if let Some(code) = &self.language_code {
            query.push(("language_code", code.clone()));
        }
        if let Some(mode) = self.apply_text_normalization {
            query.push(("apply_text_normalization", mode.as_str().to_string()));
        }
        if let Some(seed) = self.seed {
            query.push(("seed", seed.to_string()));
        }
        if let Some(auto) = self.auto_mode {
            query.push(("auto_mode", auto.to_string()));
        }
        query
    }
}

/// The first message: a single space, plus voice settings (with speed) and the
/// chunk schedule when either is set.
pub fn init_message(extras: &ElevenLabsExtras, speed: Option<f32>) -> Value {
    let mut message = Map::new();
    message.insert("text".into(), json!(" "));
    let mut settings = match &extras.voice_settings {
        Some(settings) => match serde_json::to_value(settings) {
            Ok(Value::Object(map)) => map,
            _ => Map::new(),
        },
        None => Map::new(),
    };
    if let Some(speed) = speed {
        settings.insert("speed".into(), json!(speed));
    }
    if !settings.is_empty() {
        message.insert("voice_settings".into(), Value::Object(settings));
    }
    if let Some(schedule) = &extras.chunk_length_schedule {
        message.insert(
            "generation_config".into(),
            json!({ "chunk_length_schedule": schedule }),
        );
    }
    Value::Object(message)
}

/// A text chunk. ElevenLabs wants every chunk to end in a space and no word to
/// be split across chunks; the caller guarantees both.
pub fn text_message(text: &str) -> Value {
    json!({ "text": text })
}

/// `text` (possibly empty) with a forced generation of everything buffered.
/// An empty text would read as the close message, so it is sent as a space.
pub fn flush_message(text: &str) -> Value {
    let text = if text.is_empty() { " " } else { text };
    json!({ "text": text, "flush": true })
}

/// The keepalive ElevenLabs documents: a single space resets its inactivity
/// timer without closing anything.
pub fn keepalive_message() -> Value {
    json!({ "text": " " })
}

/// The end of input. ElevenLabs generates what remains, sends a final frame,
/// and closes.
pub fn close_message() -> Value {
    json!({ "text": "" })
}

/// Per-character timing for one audio frame, relative to that frame.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alignment {
    #[serde(default)]
    pub chars: Vec<String>,
    #[serde(default, alias = "char_start_times_ms")]
    pub char_start_times_ms: Vec<u32>,
    #[serde(default, alias = "char_durations_ms")]
    pub char_durations_ms: Vec<u32>,
}

/// One frame ElevenLabs sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerEvent {
    Audio {
        audio: Vec<u8>,
        /// Timing against the text as sent.
        alignment: Option<Alignment>,
        /// Timing against the text as ElevenLabs normalized it.
        normalized_alignment: Option<Alignment>,
    },
    /// Generation is complete; no more audio follows.
    Final,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawFrame {
    #[serde(default)]
    audio: Option<String>,
    #[serde(default, alias = "is_final")]
    is_final: Option<bool>,
    #[serde(default)]
    alignment: Option<Alignment>,
    #[serde(default, alias = "normalized_alignment")]
    normalized_alignment: Option<Alignment>,
    #[serde(default)]
    error: Option<Value>,
    #[serde(default)]
    detail: Option<Value>,
}

/// Decode one text frame. `Ok(None)` is a frame that carries nothing to act
/// on (no audio and not final). An error frame is classified like any other
/// ElevenLabs error; `format` names the output format the socket asked for.
pub fn decode_frame(text: &str, format: &str) -> Result<Option<ServerEvent>, SpeechWireError> {
    let frame: RawFrame = serde_json::from_str(text).map_err(|error| {
        SpeechWireError::InvalidResponse(format!(
            "ElevenLabs sent an unreadable frame: {error}: {}",
            preview(text)
        ))
    })?;
    if frame.error.is_some() || frame.detail.is_some() {
        return Err(classify_error(Source::Frame, text, format));
    }
    let audio = match frame.audio.as_deref().filter(|audio| !audio.is_empty()) {
        Some(encoded) => Some(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|error| {
                    SpeechWireError::InvalidResponse(format!(
                        "ElevenLabs sent audio that is not base64: {error}"
                    ))
                })?,
        ),
        None => None,
    };
    if let Some(audio) = audio {
        // A frame with audio is never also the final one (ElevenLabs sends
        // `audio: null` with `isFinal: true`), so audio is reported first.
        return Ok(Some(ServerEvent::Audio {
            audio,
            alignment: frame.alignment,
            normalized_alignment: frame.normalized_alignment,
        }));
    }
    if frame.is_final == Some(true) {
        return Ok(Some(ServerEvent::Final));
    }
    Ok(None)
}

/// Where an ElevenLabs error arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The handshake was answered with this HTTP status.
    Http(u16),
    /// The socket closed with this close code.
    Close(u16),
    /// An error frame on an open socket.
    Frame,
}

impl Source {
    fn label(self) -> String {
        match self {
            Self::Http(status) => format!("HTTP {status}"),
            Self::Close(code) => format!("close {code}"),
            Self::Frame => "error frame".to_string(),
        }
    }

    fn http(self) -> Option<u16> {
        match self {
            Self::Http(status) => Some(status),
            _ => None,
        }
    }
}

/// ElevenLabs' documented error fields, wherever they appeared.
#[derive(Debug, Default)]
struct ErrorFields {
    kind: Option<String>,
    code: Option<String>,
    message: String,
}

/// Read an error body: the documented `{"detail": {type, code, message}}`, a
/// plain `{"detail": "..."}`, an `{"error": ..., "message": ...}` frame, or
/// plain text.
fn error_fields(body: &str) -> ErrorFields {
    let text = |value: &Value| value.as_str().map(str::to_string);
    let Ok(parsed) = serde_json::from_str::<Value>(body) else {
        return ErrorFields {
            message: body.trim().to_string(),
            ..ErrorFields::default()
        };
    };
    let detail = parsed.get("detail").unwrap_or(&parsed);
    if let Some(message) = text(detail) {
        return ErrorFields {
            message,
            ..ErrorFields::default()
        };
    }
    let code = detail
        .get("code")
        .and_then(text)
        .or_else(|| detail.get("status").and_then(text))
        .or_else(|| detail.get("error").and_then(text));
    let message = detail
        .get("message")
        .and_then(text)
        .unwrap_or_else(|| body.trim().to_string());
    ErrorFields {
        kind: detail.get("type").and_then(text),
        code,
        message,
    }
}

/// Classify an ElevenLabs error from its documented `type` and `code`, falling
/// back to the HTTP status and then the message text.
pub fn classify_error(source: Source, body: &str, format: &str) -> SpeechWireError {
    let fields = error_fields(body);
    let code = fields.code.as_deref().unwrap_or_default();
    let kind = fields.kind.as_deref().unwrap_or_default();
    let lower = fields.message.to_ascii_lowercase();
    let status = source.http();
    let tag = match (fields.code.as_deref(), fields.kind.as_deref()) {
        (Some(code), _) | (None, Some(code)) => format!("{}, {code}", source.label()),
        (None, None) => source.label(),
    };
    let said = format!("ElevenLabs ({tag}): {}", fields.message);

    let plan_gated = matches!(
        code,
        "invalid_output_format" | "feature_not_available" | "subscription_required"
    ) || lower.contains("output_format")
        || lower.contains("output format");
    if plan_gated {
        return SpeechWireError::FormatNotOnPlan {
            format: format.to_string(),
            message: format!(
                "ElevenLabs refused output format {format}: the account's plan may not include \
                 it (ElevenLabs serves pcm_44100 only on Pro and above, and mp3 at 192 kbps only \
                 on Creator and above). Choose another format; Cairn does not switch formats on \
                 its own. {said}"
            ),
        };
    }
    if matches!(
        code,
        "invalid_api_key" | "missing_api_key" | "invalid_authorization_header" | "unauthorized"
    ) || matches!(kind, "authentication_error" | "authorization_error")
        || matches!(status, Some(401 | 403))
    {
        return SpeechWireError::Auth(said);
    }
    if matches!(
        code,
        "rate_limit_exceeded" | "concurrent_limit_exceeded" | "system_busy"
    ) || kind == "rate_limit_error"
        || status == Some(429)
    {
        return SpeechWireError::RateLimited(said);
    }
    if matches!(code, "insufficient_credits" | "quota_exceeded")
        || kind == "payment_required"
        || status == Some(402)
        || lower.contains("quota")
    {
        return SpeechWireError::QuotaExceeded(said);
    }
    if matches!(
        kind,
        "validation_error" | "invalid_request" | "not_found" | "conflict"
    ) || matches!(status, Some(400 | 404 | 409 | 422))
    {
        return SpeechWireError::InvalidRequest(said);
    }
    let status = match source {
        Source::Http(status) | Source::Close(status) => status,
        Source::Frame => 0,
    };
    SpeechWireError::Upstream {
        status,
        message: said,
    }
}

fn preview(text: &str) -> &str {
    let end = text
        .char_indices()
        .nth(200)
        .map_or(text.len(), |(index, _)| index);
    &text[..end]
}
