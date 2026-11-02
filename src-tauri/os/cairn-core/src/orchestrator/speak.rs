//! The speech verb as agents and scripts call it (`run cairn://models/speak`).
//!
//! The facet streams: text in through a feed, audio events out. A `run` item
//! has one reply, so this door speaks the one-string case ([`whole_text`]) and
//! gathers the stream into a finished clip. The model is the caller's, else
//! the workspace's `speechModel` default, else a refusal naming that setting
//! ([`super::model_defaults`]). Selection is the facet's request-aware walk
//! over that model's offers only ([`cairn_models::interface::Catalog::speech_offer`]),
//! so a voice, format, speed or instruction an offer cannot serve is refused
//! naming the offers that could.
//!
//! Cancellation follows the facet's contract: dropping [`Speech`] closes the
//! upstream connection, so a clip that outruns its deadline, or the
//! attachment ceiling, is dropped at once rather than left to finish.

use std::time::Instant;

use cairn_models::interface::speak::{
    offer_capabilities, whole_text, AudioEncoding, AudioFormat, SpeechCall, SpeechEvent,
    SpeechRequest, SpeechRoute,
};
use cairn_models::interface::{ModelScope, Models, OfferQuery, SelectedOffer};
use cairn_models::types::catalog::{self, VerbKind};
use cairn_models::types::usage::MeteredUsage;

use super::model_defaults::DefaultedVerb;
use super::Orchestrator;

/// A finished clip: one playable file's bytes and how it was made.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Spoken {
    /// A complete file: MP3 frames, chained Ogg Opus, or PCM wrapped in WAV.
    pub audio: Vec<u8>,
    /// The format the provider served, before any WAV wrapping.
    pub format: AudioFormat,
    pub usage: MeteredUsage,
    pub offer: SelectedOffer,
}

impl Spoken {
    /// The file extension and media type the clip is stored under.
    pub(crate) fn file_type(&self) -> (&'static str, &'static str) {
        match self.format.encoding {
            AudioEncoding::Mp3 => ("mp3", "audio/mpeg"),
            AudioEncoding::Opus => ("ogg", "audio/ogg"),
            AudioEncoding::PcmS16le => ("wav", "audio/wav"),
        }
    }
}

/// How far one clip may go before it is dropped: past `deadline`, or past
/// `max_bytes` of audio (the most an attachment holds).
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClipLimits {
    pub deadline: Instant,
    pub max_bytes: usize,
}

impl Orchestrator {
    /// Speak `text` with `model` (or the workspace's `speechModel` when it
    /// names none), narrowed to `provider`'s offers when given, as one clip
    /// within `limits`. Blocking: it holds the provider's stream until the
    /// clip ends.
    pub(crate) fn speak_blocking(
        &self,
        project_id: Option<&str>,
        model: Option<&str>,
        provider: Option<&str>,
        request: SpeechRequest,
        text: &str,
        limits: ClipLimits,
    ) -> Result<Spoken, String> {
        let ClipLimits {
            deadline,
            max_bytes,
        } = limits;
        let model = self.explicit_model_for(DefaultedVerb::Speak, model)?;
        if text.trim().is_empty() {
            return Err("there is no text to speak".to_string());
        }
        if request.voice.id.trim().is_empty() {
            return Err(
                "name a voice: an OpenAI voice such as alloy, or an ElevenLabs voice_id"
                    .to_string(),
            );
        }
        let scope = ModelScope {
            project_id: project_id.map(str::to_string),
        };
        let catalog = self
            .catalog(&scope)
            .map_err(|error| format!("the model catalog is unavailable: {error}"))?;
        let provider = provider.map(str::trim).filter(|key| !key.is_empty());
        let (offer, _) = catalog
            .speech_offer(
                OfferQuery {
                    requested: Some(&model),
                    provider,
                    ..OfferQuery::new(VerbKind::Speak)
                },
                &request,
            )
            .map_err(|refusal| refusal.to_string())?;
        let capabilities = offer_capabilities(&offer)
            .cloned()
            .ok_or_else(|| format!("{} on {} does not speak", offer.model, offer.provider))?;
        let route = self.speech_route(project_id, &offer)?;
        let mut speech = cairn_models::speak::speak(
            self.models_runtime.speech.clone(),
            SpeechCall {
                route,
                model: offer.model.clone(),
                capabilities,
                request,
                text: whole_text(text),
            },
        )
        .map_err(|error| error.to_string())?;
        let format = *speech.format();
        let mut audio = Vec::new();
        let usage = loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err("the speech did not finish before the item's timeout".to_string());
            }
            match speech.next_timeout(left) {
                Some(Ok(SpeechEvent::Audio(chunk))) => {
                    audio.extend_from_slice(&chunk.data);
                    if audio.len() > max_bytes {
                        return Err(format!(
                            "the clip passed the {} MiB a stored clip may hold; speak shorter text, or ask for a compressed format such as mp3",
                            max_bytes / (1024 * 1024)
                        ));
                    }
                }
                Some(Ok(SpeechEvent::End { usage })) => break usage,
                Some(Err(error)) => return Err(error.to_string()),
                None if speech.is_finished() => {
                    return Err("the speech stream ended without finishing".to_string())
                }
                None => {}
            }
        };
        drop(speech);
        if audio.is_empty() {
            return Err(format!(
                "{} on {} returned no audio",
                offer.model, offer.provider
            ));
        }
        if let (AudioEncoding::PcmS16le, Some(rate)) = (format.encoding, format.sample_rate) {
            audio = wav(&audio, rate);
        }
        Ok(Spoken {
            audio,
            format,
            usage,
            offer,
        })
    }

    /// Where `offer` is spoken, with this workspace's credentials for it.
    fn speech_route(
        &self,
        project_id: Option<&str>,
        offer: &SelectedOffer,
    ) -> Result<SpeechRoute, String> {
        let provider = offer.provider.as_str();
        let account_id = catalog::custom_account_id(provider).ok_or_else(|| {
            format!("{provider} has no speech route in this build; speech is reached through a custom provider")
        })?;
        crate::backends::custom::CustomProviderConfig::resolve(self, account_id, project_id)
            .and_then(|config| config.speech_route())
            .map_err(|why| format!("{provider} {why}"))
    }
}

/// Mono 16-bit little-endian PCM at `sample_rate` Hz as a WAV file, which raw
/// PCM is not: without the header the bytes cannot be played.
fn wav(pcm: &[u8], sample_rate: u32) -> Vec<u8> {
    const CHANNELS: u16 = 1;
    const BITS: u16 = 16;
    let block_align = CHANNELS * BITS / 8;
    let byte_rate = sample_rate * u32::from(block_align);
    let data_len = u32::try_from(pcm.len()).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(44 + pcm.len());
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36u32.saturating_add(data_len)).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&CHANNELS.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&BITS.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(pcm);
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use cairn_models::interface::speak::{
        ClipRequest, ClipUpstream, ClipUsage, SpeechTransport, SpeechWireError, StreamOpen,
        StreamUpstream,
    };

    use crate::credentials::{CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAuth};
    use crate::models::UpdateSettings;
    use crate::orchestrator::models::ModelRuntime;

    /// An `audio/speech` server that answers every clip with the same chunks,
    /// each after `hold`, and records the model, text, format and voice-style
    /// instructions it was asked for.
    pub(crate) struct Voicebox {
        chunks: Vec<Vec<u8>>,
        hold: Duration,
        pub(crate) asked: Mutex<Vec<(String, String, String, Option<String>)>>,
        cancelled: Arc<AtomicBool>,
    }

    impl Voicebox {
        pub(crate) fn new(chunks: &[&[u8]], hold: Duration) -> Self {
            Self {
                chunks: chunks.iter().map(|chunk| chunk.to_vec()).collect(),
                hold,
                asked: Mutex::new(Vec::new()),
                cancelled: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    struct Clip {
        chunks: VecDeque<Vec<u8>>,
        hold: Duration,
        cancel: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
    }

    impl Iterator for Clip {
        type Item = Result<Vec<u8>, SpeechWireError>;

        fn next(&mut self) -> Option<Self::Item> {
            let started = Instant::now();
            while started.elapsed() < self.hold {
                if self.cancel.load(Ordering::SeqCst) {
                    self.cancelled.store(true, Ordering::SeqCst);
                    return None;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            self.chunks.pop_front().map(Ok)
        }
    }

    impl ClipUpstream for Clip {
        fn usage(&self) -> Option<ClipUsage> {
            Some(ClipUsage {
                input_tokens: 4,
                output_tokens: 0,
            })
        }
    }

    impl SpeechTransport for Voicebox {
        fn open_stream(
            &self,
            _route: &SpeechRoute,
            _open: &StreamOpen,
        ) -> Result<Box<dyn StreamUpstream>, SpeechWireError> {
            Err(SpeechWireError::InvalidRequest("no input stream".into()))
        }

        fn clip(
            &self,
            _route: &SpeechRoute,
            clip: &ClipRequest,
            cancel: &Arc<AtomicBool>,
        ) -> Result<Box<dyn ClipUpstream>, SpeechWireError> {
            self.asked.lock().unwrap().push((
                clip.model.clone(),
                clip.input.clone(),
                clip.format.to_string(),
                clip.instructions.clone(),
            ));
            Ok(Box::new(Clip {
                chunks: self.chunks.clone().into(),
                hold: self.hold,
                cancel: cancel.clone(),
                cancelled: self.cancelled.clone(),
            }))
        }
    }

    /// A workspace whose one speech provider is a custom OpenAI speech server
    /// offering `tts-1`, answered by `voicebox`.
    pub(crate) async fn workspace(voicebox: Voicebox) -> (Orchestrator, Arc<Voicebox>, String) {
        offering(voicebox, &["tts-1"]).await
    }

    /// [`workspace`], its speech server offering the models `ids` instead.
    pub(crate) async fn offering(
        voicebox: Voicebox,
        ids: &[&str],
    ) -> (Orchestrator, Arc<Voicebox>, String) {
        let mut orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let voicebox = Arc::new(voicebox);
        orch.models_runtime = ModelRuntime {
            speech: voicebox.clone(),
            ..orch.models_runtime.clone()
        };
        let account = orch
            .add_custom_provider(
                "Voicebox".into(),
                ProviderAuth::custom_endpoint(
                    "http://127.0.0.1:1/v1",
                    CustomProtocol::OpenAiSpeech,
                    None,
                    &[],
                    CustomModelPolicy::Explicit {
                        ids: ids.iter().map(|id| id.to_string()).collect(),
                    },
                    CustomSampling::default(),
                    1,
                )
                .unwrap(),
                None,
            )
            .unwrap();
        let key = catalog::custom_key(&account.id);
        crate::orchestrator::image::tests::refresh(&orch, &key).await;
        (orch, voicebox, key)
    }

    fn mp3() -> SpeechRequest {
        SpeechRequest {
            formats: vec![AudioFormat::any(AudioEncoding::Mp3)],
            ..SpeechRequest::new("alloy")
        }
    }

    async fn speak(
        orch: &Orchestrator,
        model: Option<&str>,
        request: SpeechRequest,
        text: &str,
        within: Duration,
        max_bytes: usize,
    ) -> Result<Spoken, String> {
        let orch = orch.clone();
        let model = model.map(str::to_string);
        let text = text.to_string();
        tokio::task::spawn_blocking(move || {
            orch.speak_blocking(
                None,
                model.as_deref(),
                None,
                request,
                &text,
                ClipLimits {
                    deadline: Instant::now() + within,
                    max_bytes,
                },
            )
        })
        .await
        .unwrap()
    }

    const SECOND: Duration = Duration::from_secs(1);

    #[tokio::test(flavor = "multi_thread")]
    async fn a_named_model_speaks_one_finished_clip_in_the_format_asked() {
        let (orch, voicebox, key) =
            workspace(Voicebox::new(&[b"ID3a", b"bc"], Duration::ZERO)).await;
        let spoken = speak(
            &orch,
            Some("tts-1"),
            mp3(),
            "Hello there.",
            5 * SECOND,
            1 << 20,
        )
        .await
        .unwrap();
        assert_eq!(spoken.audio, b"ID3abc");
        assert_eq!(spoken.format.encoding, AudioEncoding::Mp3);
        assert_eq!(spoken.file_type(), ("mp3", "audio/mpeg"));
        assert_eq!(spoken.offer.provider, key);
        let asked = voicebox.asked.lock().unwrap();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].0, "tts-1");
        assert_eq!(asked[0].1.trim(), "Hello there.");
        assert_eq!(asked[0].2, "mp3");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_offer_default_pcm_comes_back_as_a_wav_file() {
        let (orch, _, _) = workspace(Voicebox::new(&[b"\x01\x00\x02\x00"], Duration::ZERO)).await;
        let spoken = speak(
            &orch,
            Some("tts-1"),
            SpeechRequest::new("alloy"),
            "Hi.",
            5 * SECOND,
            1 << 20,
        )
        .await
        .unwrap();
        assert_eq!(spoken.format, AudioFormat::pcm(24_000));
        assert_eq!(spoken.file_type(), ("wav", "audio/wav"));
        assert_eq!(&spoken.audio[0..4], b"RIFF");
        assert_eq!(spoken.audio.len(), 44 + 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn what_cannot_be_spoken_is_refused_before_any_call() {
        let (orch, voicebox, _) = workspace(Voicebox::new(&[b"x"], Duration::ZERO)).await;
        let refused = speak(&orch, None, mp3(), "Hi.", SECOND, 1 << 20)
            .await
            .unwrap_err();
        assert!(refused.contains("`speechModel`"), "{refused}");
        let refused = speak(
            &orch,
            Some("tts-1"),
            SpeechRequest::new(" "),
            "Hi.",
            SECOND,
            1 << 20,
        )
        .await
        .unwrap_err();
        assert!(refused.contains("name a voice"), "{refused}");
        let refused = speak(&orch, Some("tts-1"), mp3(), "  ", SECOND, 1 << 20)
            .await
            .unwrap_err();
        assert!(refused.contains("no text"), "{refused}");
        assert!(voicebox.asked.lock().unwrap().is_empty());

        orch.update_settings(UpdateSettings {
            speech_model: Some(Some("tts-1".into())),
            ..Default::default()
        })
        .unwrap();
        assert!(speak(&orch, None, mp3(), "Hi.", 5 * SECOND, 1 << 20)
            .await
            .is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_clip_that_outruns_its_deadline_is_dropped_and_its_call_cancelled() {
        let (orch, voicebox, _) = workspace(Voicebox::new(&[b"x"], 3 * SECOND)).await;
        let started = Instant::now();
        let refused = speak(
            &orch,
            Some("tts-1"),
            mp3(),
            "Hi.",
            Duration::from_millis(200),
            1 << 20,
        )
        .await
        .unwrap_err();
        assert!(started.elapsed() < 2 * SECOND, "{:?}", started.elapsed());
        assert!(refused.contains("timeout"), "{refused}");
        let deadline = Instant::now() + 2 * SECOND;
        while !voicebox.cancelled.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            voicebox.cancelled.load(Ordering::SeqCst),
            "the upstream clip was cancelled"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_clip_past_the_attachment_ceiling_is_refused() {
        let (orch, _, _) = workspace(Voicebox::new(&[b"0123456789"], Duration::ZERO)).await;
        let refused = speak(&orch, Some("tts-1"), mp3(), "Hi.", 5 * SECOND, 4)
            .await
            .unwrap_err();
        assert!(refused.contains("a stored clip may hold"), "{refused}");
    }

    #[test]
    fn pcm_is_wrapped_as_a_playable_wav() {
        let pcm = [1u8, 0, 2, 0, 3, 0];
        let file = wav(&pcm, 24_000);
        assert_eq!(&file[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(file[4..8].try_into().unwrap()), 36 + 6);
        assert_eq!(&file[8..16], b"WAVEfmt ");
        assert_eq!(
            u16::from_le_bytes(file[22..24].try_into().unwrap()),
            1,
            "mono"
        );
        assert_eq!(u32::from_le_bytes(file[24..28].try_into().unwrap()), 24_000);
        assert_eq!(
            u32::from_le_bytes(file[28..32].try_into().unwrap()),
            48_000,
            "byte rate"
        );
        assert_eq!(&file[36..40], b"data");
        assert_eq!(u32::from_le_bytes(file[40..44].try_into().unwrap()), 6);
        assert_eq!(&file[44..], &pcm);
    }
}
