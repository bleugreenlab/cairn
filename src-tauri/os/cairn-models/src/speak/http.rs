//! The transport that reaches real speech providers.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::interface::speak::{
    openai_streams_events, AudioEncoding, CharTiming, ClipRequest, ClipUpstream, ClipUsage,
    SpeechExtras, SpeechRoute, SpeechTransport, SpeechWireError, StreamOpen, StreamUpstream,
    UpstreamEvent,
};
use crate::speech_wire::elevenlabs::{Alignment, ElevenLabsOpen, ElevenLabsSocket, ServerEvent};
use crate::speech_wire::openai_speech::{
    build_body, post_clip, ClipStream, SpeechBody, StreamFormat,
};

/// Bounds the TCP connect and WebSocket handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a clip may go without a byte before it is abandoned.
const CLIP_STALL: Duration = Duration::from_secs(30);
/// The longest silence ElevenLabs allows before closing, asked for by default
/// because a commander may think before speaking.
const ELEVENLABS_INACTIVITY_SECONDS: u16 = 180;

/// Speech over the providers' own protocols: ElevenLabs' WebSocket for native
/// streams, `audio/speech` for clips.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpSpeech;

impl SpeechTransport for HttpSpeech {
    fn open_stream(
        &self,
        route: &SpeechRoute,
        open: &StreamOpen,
    ) -> Result<Box<dyn StreamUpstream>, SpeechWireError> {
        let SpeechRoute::ElevenLabs(endpoint) = route else {
            return Err(SpeechWireError::InvalidRequest(format!(
                "{} takes whole text per request; it has no input stream",
                route.provider_name()
            )));
        };
        let mut extras = match &open.extras {
            SpeechExtras::ElevenLabs(extras) => extras.clone(),
            _ => Default::default(),
        };
        extras
            .inactivity_timeout
            .get_or_insert(ELEVENLABS_INACTIVITY_SECONDS);
        let output_format = open.format.to_string();
        let socket = ElevenLabsSocket::connect(
            endpoint,
            &ElevenLabsOpen {
                voice_id: &open.voice.id,
                model: &open.model,
                output_format: &output_format,
                speed: open.speed,
                extras: &extras,
            },
            CONNECT_TIMEOUT,
        )?;
        Ok(Box::new(ElevenLabsStream(socket)))
    }

    fn clip(
        &self,
        route: &SpeechRoute,
        clip: &ClipRequest,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Box<dyn ClipUpstream>, SpeechWireError> {
        let SpeechRoute::OpenAiSpeech(endpoint) = route else {
            return Err(SpeechWireError::InvalidRequest(format!(
                "{} streams input natively; it takes no whole-text request here",
                route.provider_name()
            )));
        };
        let stream_format = if openai_streams_events(&clip.model) {
            StreamFormat::Sse
        } else {
            StreamFormat::Audio
        };
        let extra_body = match &clip.extras {
            SpeechExtras::OpenAiSpeech(extra) => Some(extra),
            _ => None,
        };
        let body = build_body(&SpeechBody {
            model: &clip.model,
            input: &clip.input,
            voice: &clip.voice.id,
            response_format: match clip.format.encoding {
                AudioEncoding::PcmS16le => "pcm",
                AudioEncoding::Mp3 => "mp3",
                AudioEncoding::Opus => "opus",
            },
            speed: clip.speed,
            instructions: clip.instructions.as_deref(),
            stream_format,
            extra_body,
        });
        let stream = post_clip(endpoint, &body, stream_format, cancel.clone(), CLIP_STALL)?;
        Ok(Box::new(OpenAiClip(stream)))
    }
}

struct ElevenLabsStream(ElevenLabsSocket);

impl StreamUpstream for ElevenLabsStream {
    fn send(&mut self, text: &str) -> Result<(), SpeechWireError> {
        self.0.send_text(text)
    }

    fn flush(&mut self, text: &str) -> Result<(), SpeechWireError> {
        self.0.flush(text)
    }

    fn keepalive(&mut self) -> Result<(), SpeechWireError> {
        self.0.keepalive()
    }

    fn finish(&mut self) -> Result<(), SpeechWireError> {
        self.0.close_input()
    }

    fn recv(&mut self, timeout: Duration) -> Result<Option<UpstreamEvent>, SpeechWireError> {
        Ok(self.0.recv(timeout)?.map(|event| match event {
            ServerEvent::Audio {
                audio, alignment, ..
            } => UpstreamEvent::Audio {
                data: audio,
                timing: alignment.map(timing),
            },
            ServerEvent::Final => UpstreamEvent::Final,
        }))
    }
}

fn timing(alignment: Alignment) -> Vec<CharTiming> {
    alignment
        .chars
        .into_iter()
        .zip(alignment.char_start_times_ms)
        .zip(alignment.char_durations_ms)
        .map(|((text, start_ms), duration_ms)| CharTiming {
            text,
            start_ms,
            duration_ms,
        })
        .collect()
}

struct OpenAiClip(ClipStream);

impl Iterator for OpenAiClip {
    type Item = Result<Vec<u8>, SpeechWireError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl ClipUpstream for OpenAiClip {
    fn usage(&self) -> Option<ClipUsage> {
        self.0.usage().map(|usage| ClipUsage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
        })
    }
}
