//! The facet's speech verb: a text stream in, an audio stream out.
//!
//! `speak` exists so that language-model output can be heard while it is
//! still being generated. Its input is a [`TextFeed`] the caller pushes text
//! into as it arrives; its output is [`Speech`], an iterator of audio. A
//! finished clip is the special case where the feed holds one string
//! ([`whole_text`]).
//!
//! The facet is blocking throughout, like `complete`: one worker thread per
//! utterance, an `Arc<AtomicBool>` for cancellation, and an async host wraps
//! the iterator in one `spawn_blocking`. Dropping [`Speech`] closes the
//! upstream connection (barge-in); dropping the feed, or calling
//! [`TextFeed::finish`], speaks what remains and ends the utterance.
//!
//! How text reaches the provider follows from the offer's
//! [`InputStreaming`]: a native provider (ElevenLabs) takes it over one
//! socket as it arrives, while a segmented one (OpenAI speech, Kokoro) gets
//! one whole request per sentence or clause, cut by the shared segmenter and
//! pipelined so audio for one segment plays while the next generates.
//!
//! A request is admitted against the offer's [`SpeechCapabilities`] before any
//! connection opens ([`admit`]), so the served [`AudioFormat`] is known before
//! the first byte and nothing is quietly degraded. Selection goes through the
//! catalog's one request-aware walk ([`super::Catalog::admitting_offer`]).
//! The facet streams audio and records usage; the consumer keeps the audio.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cairn_models_types::catalog::{Verb, VerbKind, Wire};
use cairn_models_types::usage::MeteredUsage;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use serde_json::{Map, Value};

pub use cairn_models_types::speech::{
    AudioEncoding, AudioFormat, InputStreaming, SpeechCapabilities, SpeedRange, StyleControl,
};

use super::{Catalog, NoOffer, OfferQuery, SelectedOffer};
use crate::speech_wire::elevenlabs::{ElevenLabsEndpoint, ElevenLabsExtras};
use crate::speech_wire::openai_speech::OpenAiSpeechEndpoint;
pub use crate::speech_wire::SpeechWireError;

/// The fixes every no-offer refusal of `speak` names.
pub const SPEECH_FIXES: &str = "To fix it, add a custom provider in Settings → Providers with \
the protocol set to ElevenLabs (an ElevenLabs API key) or OpenAI Speech (OpenAI at \
https://api.openai.com/v1, or a local OpenAI-compatible TTS server such as Kokoro-FastAPI).";

/// The deployment a speech request goes to, by the protocol it speaks.
#[derive(Debug, Clone)]
pub enum SpeechRoute {
    ElevenLabs(ElevenLabsEndpoint),
    OpenAiSpeech(OpenAiSpeechEndpoint),
}

impl SpeechRoute {
    /// The catalog's name for this route's protocol: the one mapping from a
    /// speech route onto [`Wire`].
    pub fn wire(&self) -> Wire {
        match self {
            Self::ElevenLabs(_) => Wire::ElevenLabsStreamInput,
            Self::OpenAiSpeech(_) => Wire::OpenAiSpeech,
        }
    }

    /// What to call the provider in an error a user will read.
    pub fn provider_name(&self) -> &str {
        match self {
            Self::ElevenLabs(_) => "ElevenLabs",
            Self::OpenAiSpeech(endpoint) => &endpoint.provider_name,
        }
    }
}

/// Who speaks: an ElevenLabs `voice_id`, or an OpenAI voice name or custom
/// voice id (`voice_...`), or whatever a compatible server names its voices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Voice {
    pub id: String,
}

impl Voice {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

/// Per-provider controls beyond the typed core.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum SpeechExtras {
    #[default]
    None,
    /// Voice settings, chunk schedule, language and the rest.
    ElevenLabs(ElevenLabsExtras),
    /// Fields merged over the `audio/speech` body, for a compatible server's
    /// own parameters.
    OpenAiSpeech(Map<String, Value>),
}

/// What a caller asks to be spoken with.
#[derive(Debug, Clone, PartialEq)]
pub struct SpeechRequest {
    pub voice: Voice,
    /// Acceptable formats in preference order. Empty takes the offer's
    /// default; an unset rate or bitrate accepts any.
    pub formats: Vec<AudioFormat>,
    /// 1.0 is normal speed.
    pub speed: Option<f32>,
    /// Free-text delivery instructions, for offers whose style is
    /// [`StyleControl::Instructions`].
    pub instructions: Option<String>,
    pub extras: SpeechExtras,
}

impl SpeechRequest {
    /// `voice` in the offer's default format, at normal speed.
    pub fn new(voice: impl Into<String>) -> Self {
        Self {
            voice: Voice::new(voice),
            formats: Vec::new(),
            speed: None,
            instructions: None,
            extras: SpeechExtras::None,
        }
    }
}

/// One utterance to speak.
pub struct SpeechCall {
    pub route: SpeechRoute,
    /// The id the provider is sent.
    pub model: String,
    /// What the chosen offer declared (from the catalog, or
    /// [`capabilities`] for a host that builds its own route).
    pub capabilities: SpeechCapabilities,
    pub request: SpeechRequest,
    pub text: SpeechText,
}

pub(crate) enum FeedItem {
    Text(String),
    Flush,
}

/// The input end: push text as it is produced. Dropping it, or calling
/// [`TextFeed::finish`], speaks what remains and ends the utterance.
pub struct TextFeed {
    sender: Sender<FeedItem>,
}

/// The text a [`TextFeed`] pushes, as [`SpeechCall::text`] carries it.
pub struct SpeechText {
    pub(crate) receiver: Receiver<FeedItem>,
}

/// A connected feed and the text it delivers.
pub fn text_feed() -> (TextFeed, SpeechText) {
    let (sender, receiver) = crossbeam_channel::unbounded();
    (TextFeed { sender }, SpeechText { receiver })
}

/// A finished feed holding `text`: the finished-clip case.
pub fn whole_text(text: &str) -> SpeechText {
    let (feed, text_in) = text_feed();
    let _ = feed.push(text);
    feed.finish();
    text_in
}

impl TextFeed {
    /// Queue `text` (any size, any boundary) to be spoken. Fails once the
    /// utterance has ended, for example because its [`Speech`] was dropped.
    pub fn push(&self, text: &str) -> Result<(), SpeakError> {
        if text.is_empty() {
            return Ok(());
        }
        self.sender
            .send(FeedItem::Text(text.to_string()))
            .map_err(|_| SpeakError::Closed)
    }

    /// Speak everything pushed so far now, without waiting for more context.
    /// A partial word is spoken as it stands.
    pub fn flush(&self) {
        let _ = self.sender.send(FeedItem::Flush);
    }

    /// End the input. Equivalent to dropping the feed.
    pub fn finish(self) {}
}

/// The output end: audio events in order, ending with [`SpeechEvent::End`]
/// or an error.
///
/// Dropping it cancels the utterance. The caller is released at once and
/// never waits on the network. The upstream connection then closes promptly
/// when the provider is responsive. Against a silent provider it closes within
/// a bound: the stall bound (30 s) for a segmented request, and the connect
/// and handshake timeout (10 s) for a native stream still being opened. The
/// detached worker exits at that bound.
pub struct Speech {
    events: Receiver<Result<SpeechEvent, SpeakError>>,
    cancel: Arc<AtomicBool>,
    format: AudioFormat,
    finished: bool,
}

impl Speech {
    pub(crate) fn new(
        events: Receiver<Result<SpeechEvent, SpeakError>>,
        cancel: Arc<AtomicBool>,
        format: AudioFormat,
    ) -> Self {
        Self {
            events,
            cancel,
            format,
            finished: false,
        }
    }

    /// The format every audio chunk is in, known before the first byte.
    pub fn format(&self) -> &AudioFormat {
        &self.format
    }

    /// The next event, waiting at most `timeout`. `None` means nothing
    /// arrived in time or the stream has finished; [`Self::is_finished`]
    /// tells which.
    pub fn next_timeout(&mut self, timeout: Duration) -> Option<Result<SpeechEvent, SpeakError>> {
        if self.finished {
            return None;
        }
        match self.events.recv_timeout(timeout) {
            Ok(event) => Some(self.observe(event)),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => self.vanished(),
        }
    }

    /// Whether the end (or an error) has been delivered.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn observe(
        &mut self,
        event: Result<SpeechEvent, SpeakError>,
    ) -> Result<SpeechEvent, SpeakError> {
        if matches!(event, Ok(SpeechEvent::End { .. }) | Err(_)) {
            self.finished = true;
        }
        event
    }

    fn vanished(&mut self) -> Option<Result<SpeechEvent, SpeakError>> {
        self.finished = true;
        Some(Err(SpeakError::Closed))
    }
}

impl Iterator for Speech {
    type Item = Result<SpeechEvent, SpeakError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.events.recv() {
            Ok(event) => Some(self.observe(event)),
            Err(_) => self.vanished(),
        }
    }
}

impl Drop for Speech {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

/// One thing the output end delivers.
#[derive(Debug, Clone, PartialEq)]
pub enum SpeechEvent {
    Audio(AudioChunk),
    /// Everything has been spoken. `usage` is what the facet measured.
    End {
        usage: MeteredUsage,
    },
}

/// A piece of audio in [`Speech::format`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioChunk {
    pub data: Vec<u8>,
    /// Which request produced this chunk: always 0 for a natively streaming
    /// offer, the segment index for a segmented one. PCM and MP3 concatenate
    /// across segments as they are; Opus does not, because each segment is its
    /// own complete Ogg stream (chained Ogg), so a consumer decoding Opus
    /// resets its decoder where the index changes.
    pub segment: u32,
    /// Per-character timing, relative to this chunk, when the provider sends
    /// it.
    pub timing: Option<Vec<CharTiming>>,
}

/// When one character is heard, relative to the start of its chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CharTiming {
    pub text: String,
    pub start_ms: u32,
    pub duration_ms: u32,
}

/// Why an utterance could not be spoken.
#[derive(Debug, Clone, PartialEq)]
pub enum SpeakError {
    NoOffer(NoOffer),
    /// The request asks for something the offer does not serve.
    Unsupported(String),
    Wire(SpeechWireError),
    /// Text was flushed but no audio came back in time.
    Stalled(String),
    /// The utterance has already ended.
    Closed,
}

impl fmt::Display for SpeakError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOffer(refusal) => refusal.fmt(f),
            Self::Unsupported(text) | Self::Stalled(text) => f.write_str(text),
            Self::Wire(error) => error.fmt(f),
            Self::Closed => f.write_str("the utterance has ended, so nothing more can be spoken"),
        }
    }
}

impl std::error::Error for SpeakError {}

impl From<SpeechWireError> for SpeakError {
    fn from(error: SpeechWireError) -> Self {
        Self::Wire(error)
    }
}

impl From<NoOffer> for SpeakError {
    fn from(refusal: NoOffer) -> Self {
        Self::NoOffer(refusal)
    }
}

/// Admit `request` against an offer's capabilities over `wire`, and choose
/// the format it is served in: the first requested format the offer lists, or
/// the offer's default when none is requested. Everything else the request
/// asks for must be served as asked, or it is refused naming what the offer
/// can do instead.
pub fn admit(
    wire: Wire,
    capabilities: &SpeechCapabilities,
    request: &SpeechRequest,
) -> Result<AudioFormat, SpeakError> {
    let unsupported = |text: String| Err(SpeakError::Unsupported(text));
    let served: Vec<String> = capabilities
        .formats
        .iter()
        .map(ToString::to_string)
        .collect();
    let format = if request.formats.is_empty() {
        match capabilities.formats.first() {
            Some(format) => *format,
            None => return unsupported("this speech offer declares no audio format".into()),
        }
    } else {
        let chosen = request.formats.iter().find_map(|requested| {
            capabilities
                .formats
                .iter()
                .find(|offered| offered.serves(requested))
        });
        match chosen {
            Some(format) => *format,
            None => {
                let asked: Vec<String> = request.formats.iter().map(ToString::to_string).collect();
                return unsupported(format!(
                    "this offer serves none of the requested formats ({}); it serves {}",
                    asked.join(", "),
                    served.join(", ")
                ));
            }
        }
    };
    if let Some(speed) = request.speed {
        match capabilities.speed {
            None => return unsupported("this offer's speed is not settable".into()),
            Some(range) if !range.admits(speed) => {
                return unsupported(format!(
                    "speed {speed} is outside this offer's {}–{}% range",
                    range.min_percent, range.max_percent
                ))
            }
            Some(_) => {}
        }
    }
    if request.instructions.is_some() && capabilities.style != StyleControl::Instructions {
        return unsupported(
            "this offer takes no delivery instructions; choose a model whose style is \
             instructions (OpenAI's gpt-4o-mini-tts, for one)"
                .into(),
        );
    }
    match (&request.extras, wire) {
        (SpeechExtras::None, _) => {}
        (SpeechExtras::ElevenLabs(extras), Wire::ElevenLabsStreamInput) => {
            if let Some(schedule) = &extras.chunk_length_schedule {
                if schedule.iter().any(|entry| !(50..=500).contains(entry)) {
                    return unsupported(format!(
                        "ElevenLabs takes chunk_length_schedule entries from 50 to 500, not {schedule:?}"
                    ));
                }
            }
            if extras
                .inactivity_timeout
                .is_some_and(|seconds| seconds > 180)
            {
                return unsupported(
                    "ElevenLabs allows an inactivity_timeout of at most 180 seconds".into(),
                );
            }
        }
        (SpeechExtras::OpenAiSpeech(extra), Wire::OpenAiSpeech) => {
            // The typed fields carry the contract this admission negotiates
            // (format, text, speed, delivery); an extra may add to the body
            // but never replace them.
            if let Some(field) = extra
                .keys()
                .find(|key| crate::speech_wire::openai_speech::TYPED_FIELDS.contains(&key.as_str()))
            {
                return unsupported(format!(
                    "OpenAI speech extras cannot set '{field}': the request's own voice, formats, speed and instructions set it"
                ));
            }
        }
        (SpeechExtras::ElevenLabs(_), other) => {
            return unsupported(format!("ElevenLabs extras cannot ride the {other} wire"))
        }
        (SpeechExtras::OpenAiSpeech(_), other) => {
            return unsupported(format!("OpenAI speech extras cannot ride the {other} wire"))
        }
    }
    Ok(format)
}

/// The speeds ElevenLabs accepts (0.7 to 1.2).
const ELEVENLABS_SPEED: SpeedRange = SpeedRange {
    min_percent: 70,
    max_percent: 120,
};

/// The speeds `audio/speech` accepts (0.25 to 4.0).
const OPENAI_SPEED: SpeedRange = SpeedRange {
    min_percent: 25,
    max_percent: 400,
};

/// Whether an OpenAI-speech model is OpenAI's instruction-following family,
/// the one that takes `instructions` and streams server-sent events with
/// usage. Everything else on the wire (`tts-1`, `tts-1-hd`, Kokoro and other
/// compatible servers) gets the base protocol: raw audio, no instructions.
fn openai_steerable(model: &str) -> bool {
    model.starts_with("gpt-4o-mini-tts")
}

/// Whether an `audio/speech` request for `model` streams server-sent events
/// (with usage) rather than raw audio. Decided by the same fact as its
/// capabilities.
pub fn openai_streams_events(model: &str) -> bool {
    openai_steerable(model)
}

/// What `model` can do over `wire`, or `None` for a wire that does not speak.
/// The one place speech capabilities are known: catalog declarations and
/// hosts that build their own routes both read it.
pub fn capabilities(wire: Wire, model: &str) -> Option<SpeechCapabilities> {
    use AudioEncoding::{Mp3, Opus};
    match wire {
        Wire::ElevenLabsStreamInput => {
            // ElevenLabs' documented output_format values, its own default
            // first. mu-law and A-law have no encoding here. pcm_44100 and
            // mp3 at 192 kbps are plan-gated: admission passes them, and a
            // refusal comes back as `SpeechWireError::FormatNotOnPlan`.
            let mut formats = vec![AudioFormat::fixed(Mp3, 44_100, 128)];
            formats.extend(
                [8_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000].map(AudioFormat::pcm),
            );
            formats.extend(
                [
                    (22_050, 32),
                    (24_000, 48),
                    (44_100, 32),
                    (44_100, 64),
                    (44_100, 96),
                    (44_100, 192),
                ]
                .map(|(rate, kbps)| AudioFormat::fixed(Mp3, rate, kbps)),
            );
            formats
                .extend([32, 64, 96, 128, 192].map(|kbps| AudioFormat::fixed(Opus, 48_000, kbps)));
            Some(SpeechCapabilities {
                input: InputStreaming::Native,
                formats,
                speed: Some(ELEVENLABS_SPEED),
                style: StyleControl::None,
            })
        }
        Wire::OpenAiSpeech => Some(SpeechCapabilities {
            input: InputStreaming::Segmented,
            formats: vec![
                AudioFormat::pcm(24_000),
                AudioFormat::any(Mp3),
                AudioFormat::any(Opus),
            ],
            speed: Some(OPENAI_SPEED),
            style: if openai_steerable(model) {
                StyleControl::Instructions
            } else {
                StyleControl::None
            },
        }),
        Wire::OpenAiChatCompletions
        | Wire::AnthropicMessages
        | Wire::OpenAiResponses
        | Wire::OpenAiDecisions
        | Wire::SystemOne
        | Wire::ClaudeStreamJson
        | Wire::CodexAppServer
        | Wire::OpenRouterImages
        | Wire::OpenAiImages
        | Wire::OpenAiEmbeddings
        | Wire::CairnEmbed => None,
    }
}

/// Whether a model a provider lists on `wire` is one discovery should offer
/// for `speak`. An OpenAI-compatible listing mixes chat and speech ids, so
/// only the speech ones count: OpenAI's (`tts-1`, `tts-1-hd`,
/// `gpt-4o-mini-tts...`) and any id naming TTS, plus Kokoro's. ElevenLabs'
/// listing marks its own speech models, so every id it passes counts. An id
/// this does not recognise can still be declared by hand; [`capabilities`]
/// answers for any id on a speech wire.
pub fn recognises(wire: Wire, model: &str) -> bool {
    match wire {
        Wire::ElevenLabsStreamInput => true,
        Wire::OpenAiSpeech => {
            let model = model.to_ascii_lowercase();
            model.contains("tts") || model.contains("kokoro")
        }
        _ => false,
    }
}

/// The capabilities an offer selected for `speak` declares.
pub fn offer_capabilities(offer: &SelectedOffer) -> Option<&SpeechCapabilities> {
    match &offer.served.verb {
        Verb::Speak(capabilities) => Some(capabilities),
        _ => None,
    }
}

impl Catalog {
    /// The offer that serves `speak` for what was asked and admits `request`,
    /// with the format it would be served in. A thin wrapper over the one
    /// request-aware walk, with [`admit`] as speech's admission.
    pub fn speech_offer(
        &self,
        query: OfferQuery<'_>,
        request: &SpeechRequest,
    ) -> Result<(SelectedOffer, AudioFormat), NoOffer> {
        let query = OfferQuery {
            verb: VerbKind::Speak,
            ..query
        };
        self.admitting_offer(query, |offer| match offer_capabilities(offer) {
            Some(capabilities) => admit(offer.served.wire, capabilities, request),
            None => Err(SpeakError::Unsupported("it does not speak".into())),
        })
    }
}

/// What a native stream is opened with.
#[derive(Debug, Clone)]
pub struct StreamOpen {
    pub model: String,
    pub voice: Voice,
    pub format: AudioFormat,
    pub speed: Option<f32>,
    pub extras: SpeechExtras,
}

/// One whole-text request of a segmented utterance.
#[derive(Debug, Clone)]
pub struct ClipRequest {
    pub model: String,
    pub voice: Voice,
    pub input: String,
    pub format: AudioFormat,
    pub speed: Option<f32>,
    pub instructions: Option<String>,
    pub extras: SpeechExtras,
}

/// What a native upstream delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamEvent {
    Audio {
        data: Vec<u8>,
        timing: Option<Vec<CharTiming>>,
    },
    /// No more audio follows.
    Final,
}

/// Usage a clip's provider reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClipUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// The seam every speech request crosses, as [`super::complete::Complete`]
/// is for completions: the real transport reaches providers, and a test
/// installs a scripted one.
pub trait SpeechTransport: Send + Sync {
    /// Open a stream that takes text as it arrives
    /// ([`InputStreaming::Native`]).
    fn open_stream(
        &self,
        route: &SpeechRoute,
        open: &StreamOpen,
    ) -> Result<Box<dyn StreamUpstream>, SpeechWireError>;

    /// Start one whole-text request ([`InputStreaming::Segmented`]). Its
    /// audio ends early once `cancel` is set.
    fn clip(
        &self,
        route: &SpeechRoute,
        clip: &ClipRequest,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Box<dyn ClipUpstream>, SpeechWireError>;
}

/// One open native stream, driven from a single worker thread. Dropping it
/// closes the connection without waiting.
///
/// Kyutai's streaming TTS server (which cairn-inference is to host) maps onto
/// this trait with no change to the verb, as a `Wire::KyutaiTts` and a
/// `SpeechRoute::Kyutai`. Its WebSocket takes the voice in a `voice=` query
/// parameter and the key in a `kyutai-api-key` header, and gates on a `Ready`
/// message after the handshake. `send` becomes a MessagePack
/// `{type: "Text"}`, `finish` an `{type: "Eos"}`, and `flush` is a no-op
/// because Kyutai synthesizes continuously. `recv` turns `Audio {pcm: [f32]}`
/// at 24 kHz into 16-bit PCM bytes and `Text {text, start_s, stop_s}` into
/// timing, and an `Error` into a [`SpeechWireError`]. Its capabilities would be
/// native input, PCM at 24 kHz only, and no style control.
pub trait StreamUpstream: Send {
    /// Send text that ends at a word boundary.
    fn send(&mut self, text: &str) -> Result<(), SpeechWireError>;
    /// Send `text` (possibly empty) and generate everything buffered now.
    fn flush(&mut self, text: &str) -> Result<(), SpeechWireError>;
    /// Keep an idle connection open, for providers that time out on silence.
    fn keepalive(&mut self) -> Result<(), SpeechWireError> {
        Ok(())
    }
    /// End the input; audio for what remains still arrives, then `Final`.
    fn finish(&mut self) -> Result<(), SpeechWireError>;
    /// The next event, waiting at most `timeout`.
    fn recv(&mut self, timeout: Duration) -> Result<Option<UpstreamEvent>, SpeechWireError>;
}

/// One whole-text request's audio as it arrives. Dropping it closes the
/// connection.
pub trait ClipUpstream: Send + Iterator<Item = Result<Vec<u8>, SpeechWireError>> {
    /// Usage, when the provider reported it (read after the audio ends).
    fn usage(&self) -> Option<ClipUsage>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::speech_wire::elevenlabs::ElevenLabsExtras;

    fn openai(model: &str) -> SpeechCapabilities {
        capabilities(Wire::OpenAiSpeech, model).unwrap()
    }

    #[test]
    fn capabilities_follow_the_wire_and_the_model() {
        let eleven = capabilities(Wire::ElevenLabsStreamInput, "eleven_flash_v2_5").unwrap();
        assert_eq!(eleven.input, InputStreaming::Native);
        assert_eq!(eleven.formats[0].to_string(), "mp3_44100_128");
        assert!(eleven.formats.contains(&AudioFormat::pcm(16_000)));
        assert_eq!(eleven.style, StyleControl::None);
        assert_eq!(openai("gpt-4o-mini-tts").style, StyleControl::Instructions);
        assert_eq!(
            openai("gpt-4o-mini-tts-2025-12-15").style,
            StyleControl::Instructions
        );
        assert_eq!(openai("tts-1").style, StyleControl::None);
        assert_eq!(openai("kokoro").style, StyleControl::None);
        assert_eq!(openai("kokoro").formats[0], AudioFormat::pcm(24_000));
        assert_eq!(openai("kokoro").input, InputStreaming::Segmented);
        assert!(openai_streams_events("gpt-4o-mini-tts"));
        assert!(!openai_streams_events("tts-1-hd") && !openai_streams_events("kokoro"));
        assert_eq!(capabilities(Wire::OpenAiChatCompletions, "gpt-5"), None);
    }

    #[test]
    fn discovery_recognises_speech_ids_in_a_mixed_listing() {
        for id in [
            "tts-1",
            "tts-1-hd",
            "gpt-4o-mini-tts",
            "gpt-4o-mini-tts-2025-12-15",
            "kokoro",
            "Orpheus-TTS",
        ] {
            assert!(recognises(Wire::OpenAiSpeech, id), "{id}");
        }
        for id in [
            "gpt-4o",
            "whisper-1",
            "text-embedding-3-small",
            "gpt-4o-transcribe",
        ] {
            assert!(!recognises(Wire::OpenAiSpeech, id), "{id}");
        }
        assert!(recognises(Wire::ElevenLabsStreamInput, "eleven_flash_v2_5"));
        assert!(!recognises(Wire::OpenAiChatCompletions, "tts-1"));
    }

    #[test]
    fn admission_takes_the_first_requested_format_the_offer_serves() {
        let caps = openai("kokoro");
        let mut request = SpeechRequest::new("af_bella");
        assert_eq!(
            admit(Wire::OpenAiSpeech, &caps, &request).unwrap(),
            AudioFormat::pcm(24_000)
        );
        request.formats = vec!["pcm_16000".parse().unwrap(), "mp3".parse().unwrap()];
        assert_eq!(
            admit(Wire::OpenAiSpeech, &caps, &request).unwrap(),
            AudioFormat::any(AudioEncoding::Mp3)
        );
        request.formats = vec!["pcm".parse().unwrap()];
        assert_eq!(
            admit(Wire::OpenAiSpeech, &caps, &request).unwrap(),
            AudioFormat::pcm(24_000)
        );

        let eleven = capabilities(Wire::ElevenLabsStreamInput, "eleven_flash_v2_5").unwrap();
        request.formats = vec!["mp3_44100".parse().unwrap()];
        assert_eq!(
            admit(Wire::ElevenLabsStreamInput, &eleven, &request)
                .unwrap()
                .to_string(),
            "mp3_44100_128"
        );
    }

    #[test]
    fn admission_refuses_rather_than_degrading() {
        let caps = openai("tts-1");
        let refuse = |request: &SpeechRequest| match admit(Wire::OpenAiSpeech, &caps, request) {
            Err(SpeakError::Unsupported(text)) => text,
            other => panic!("expected a refusal, got {other:?}"),
        };
        let mut request = SpeechRequest::new("alloy");
        request.formats = vec!["pcm_16000".parse().unwrap()];
        let text = refuse(&request);
        assert!(
            text.contains("pcm_16000") && text.contains("it serves pcm_24000, mp3, opus"),
            "{text}"
        );

        let mut request = SpeechRequest::new("alloy");
        request.speed = Some(5.0);
        assert!(refuse(&request).contains("outside this offer's 25–400% range"));

        let mut request = SpeechRequest::new("alloy");
        request.instructions = Some("whisper".into());
        assert!(refuse(&request).contains("no delivery instructions"));
        assert!(admit(Wire::OpenAiSpeech, &openai("gpt-4o-mini-tts"), &request).is_ok());

        let mut request = SpeechRequest::new("alloy");
        request.extras = SpeechExtras::ElevenLabs(ElevenLabsExtras::default());
        assert!(refuse(&request).contains("cannot ride the openai-speech wire"));

        for field in ["response_format", "stream_format", "input", "model"] {
            let mut request = SpeechRequest::new("alloy");
            let mut extra = Map::new();
            extra.insert(field.into(), serde_json::json!("mp3"));
            request.extras = SpeechExtras::OpenAiSpeech(extra);
            let text = refuse(&request);
            assert!(text.contains(&format!("cannot set '{field}'")), "{text}");
        }
        let mut request = SpeechRequest::new("af_bella");
        let mut extra = Map::new();
        extra.insert("lang_code".into(), serde_json::json!("a"));
        request.extras = SpeechExtras::OpenAiSpeech(extra);
        assert!(admit(Wire::OpenAiSpeech, &caps, &request).is_ok());

        let eleven = capabilities(Wire::ElevenLabsStreamInput, "m").unwrap();
        let mut request = SpeechRequest::new("v");
        request.extras = SpeechExtras::OpenAiSpeech(Map::new());
        assert!(admit(Wire::ElevenLabsStreamInput, &eleven, &request).is_err());
        request.extras = SpeechExtras::ElevenLabs(ElevenLabsExtras {
            chunk_length_schedule: Some(vec![20]),
            ..ElevenLabsExtras::default()
        });
        assert!(admit(Wire::ElevenLabsStreamInput, &eleven, &request).is_err());
        request.extras = SpeechExtras::None;
        request.speed = Some(1.5);
        assert!(admit(Wire::ElevenLabsStreamInput, &eleven, &request).is_err());
    }

    #[test]
    fn a_route_names_its_wire() {
        let route = SpeechRoute::OpenAiSpeech(OpenAiSpeechEndpoint {
            provider_name: "Kokoro".into(),
            url: "http://localhost:8880/v1/audio/speech".into(),
            headers: vec![],
        });
        assert_eq!(route.wire(), Wire::OpenAiSpeech);
        assert_eq!(route.provider_name(), "Kokoro");
        let route = SpeechRoute::ElevenLabs(ElevenLabsEndpoint {
            base: "wss://api.elevenlabs.io".into(),
            api_key: "k".into(),
        });
        assert_eq!(route.wire(), Wire::ElevenLabsStreamInput);
    }
}
