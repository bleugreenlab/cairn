//! The speech verb's streaming behaviour, against a scripted transport whose
//! fakes record what they were sent and when they were dropped.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use cairn_models_types::catalog::{Modality, Wire};
use cairn_models_types::usage::{BillingUnit, Charge};

use super::speak;
use crate::interface::speak::{
    capabilities, text_feed, whole_text, AudioFormat, ClipRequest, ClipUpstream, ClipUsage,
    SpeakError, Speech, SpeechCall, SpeechEvent, SpeechRequest, SpeechRoute, SpeechText,
    SpeechTransport, SpeechWireError, StreamOpen, StreamUpstream, UpstreamEvent,
};
use crate::speech_wire::elevenlabs::ElevenLabsEndpoint;
use crate::speech_wire::openai_speech::OpenAiSpeechEndpoint;

const WAIT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Sent {
    Text(String),
    Flush(String),
    Finish,
}

/// How one scripted clip answers.
#[derive(Clone)]
struct ClipPlan {
    chunks: Vec<Vec<u8>>,
    /// Waited before each chunk.
    delay: Duration,
    usage: Option<ClipUsage>,
}

#[derive(Default)]
struct Log {
    sent: Mutex<Vec<Sent>>,
    stream_opened: AtomicBool,
    stream_dropped: AtomicBool,
    clip_inputs: Mutex<Vec<String>>,
    /// Inputs whose audio ran out, in the order that happened.
    clips_completed: Mutex<Vec<String>>,
    live_clips: AtomicUsize,
    max_live: AtomicUsize,
}

impl Log {
    fn sent(&self) -> Vec<Sent> {
        self.sent.lock().unwrap().clone()
    }

    fn clip_inputs(&self) -> Vec<String> {
        self.clip_inputs.lock().unwrap().clone()
    }
}

/// Chooses a clip's answer from its input text. Clip threads race to make
/// their requests, so the call order is not the segment order.
type Planner = dyn Fn(&str) -> ClipPlan + Send + Sync;

struct ScriptedTransport {
    log: Arc<Log>,
    plan: Box<Planner>,
}

impl SpeechTransport for ScriptedTransport {
    fn open_stream(
        &self,
        _route: &SpeechRoute,
        _open: &StreamOpen,
    ) -> Result<Box<dyn StreamUpstream>, SpeechWireError> {
        self.log.stream_opened.store(true, Ordering::SeqCst);
        Ok(Box::new(FakeStream {
            log: self.log.clone(),
            queue: VecDeque::new(),
            finished: false,
        }))
    }

    fn clip(
        &self,
        _route: &SpeechRoute,
        clip: &ClipRequest,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Box<dyn ClipUpstream>, SpeechWireError> {
        self.log
            .clip_inputs
            .lock()
            .unwrap()
            .push(clip.input.clone());
        let live = self.log.live_clips.fetch_add(1, Ordering::SeqCst) + 1;
        self.log.max_live.fetch_max(live, Ordering::SeqCst);
        let plan = (self.plan)(&clip.input);
        Ok(Box::new(FakeClip {
            log: self.log.clone(),
            input: clip.input.clone(),
            chunks: plan.chunks.into(),
            delay: plan.delay,
            usage: plan.usage,
            cancel: cancel.clone(),
        }))
    }
}

/// Answers every text it is sent with that text's bytes as audio.
struct FakeStream {
    log: Arc<Log>,
    queue: VecDeque<UpstreamEvent>,
    finished: bool,
}

impl FakeStream {
    fn echo(&mut self, text: &str) {
        if !text.is_empty() {
            self.queue.push_back(UpstreamEvent::Audio {
                data: text.as_bytes().to_vec(),
                timing: None,
            });
        }
    }
}

impl StreamUpstream for FakeStream {
    fn send(&mut self, text: &str) -> Result<(), SpeechWireError> {
        self.log.sent.lock().unwrap().push(Sent::Text(text.into()));
        self.echo(text);
        Ok(())
    }

    fn flush(&mut self, text: &str) -> Result<(), SpeechWireError> {
        self.log.sent.lock().unwrap().push(Sent::Flush(text.into()));
        self.echo(text);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), SpeechWireError> {
        self.log.sent.lock().unwrap().push(Sent::Finish);
        self.finished = true;
        Ok(())
    }

    fn recv(&mut self, timeout: Duration) -> Result<Option<UpstreamEvent>, SpeechWireError> {
        if let Some(event) = self.queue.pop_front() {
            return Ok(Some(event));
        }
        if self.finished {
            return Ok(Some(UpstreamEvent::Final));
        }
        thread::sleep(timeout);
        Ok(None)
    }
}

impl Drop for FakeStream {
    fn drop(&mut self) {
        self.log.stream_dropped.store(true, Ordering::SeqCst);
    }
}

struct FakeClip {
    log: Arc<Log>,
    input: String,
    chunks: VecDeque<Vec<u8>>,
    delay: Duration,
    usage: Option<ClipUsage>,
    cancel: Arc<AtomicBool>,
}

impl Iterator for FakeClip {
    type Item = Result<Vec<u8>, SpeechWireError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.chunks.is_empty() {
            self.log
                .clips_completed
                .lock()
                .unwrap()
                .push(self.input.clone());
            return None;
        }
        // Wait like a provider generating, but notice a cancel at once.
        let started = Instant::now();
        while started.elapsed() < self.delay {
            if self.cancel.load(Ordering::SeqCst) {
                return None;
            }
            thread::sleep(Duration::from_millis(2));
        }
        self.chunks.pop_front().map(Ok)
    }
}

impl ClipUpstream for FakeClip {
    fn usage(&self) -> Option<ClipUsage> {
        self.usage
    }
}

impl Drop for FakeClip {
    fn drop(&mut self) {
        self.log.live_clips.fetch_sub(1, Ordering::SeqCst);
    }
}

fn scripted(
    plan: impl Fn(&str) -> ClipPlan + Send + Sync + 'static,
) -> (Arc<dyn SpeechTransport>, Arc<Log>) {
    let log = Arc::new(Log::default());
    let transport = ScriptedTransport {
        log: log.clone(),
        plan: Box::new(plan),
    };
    (Arc::new(transport), log)
}

/// Every clip answers instantly with its input's bytes.
fn echoing() -> (Arc<dyn SpeechTransport>, Arc<Log>) {
    scripted(|input| ClipPlan {
        chunks: vec![input.as_bytes().to_vec()],
        delay: Duration::ZERO,
        usage: None,
    })
}

fn native_call(text: SpeechText) -> SpeechCall {
    let mut request = SpeechRequest::new("voice1");
    request.formats = vec![AudioFormat::pcm(16_000)];
    SpeechCall {
        route: SpeechRoute::ElevenLabs(ElevenLabsEndpoint {
            base: "wss://example.invalid".into(),
            api_key: "k".into(),
        }),
        model: "eleven_flash_v2_5".into(),
        capabilities: capabilities(Wire::ElevenLabsStreamInput, "eleven_flash_v2_5").unwrap(),
        request,
        text,
    }
}

fn segmented_call(text: SpeechText) -> SpeechCall {
    SpeechCall {
        route: SpeechRoute::OpenAiSpeech(OpenAiSpeechEndpoint {
            provider_name: "Kokoro".into(),
            url: "http://example.invalid/v1/audio/speech".into(),
            headers: vec![],
        }),
        model: "kokoro".into(),
        capabilities: capabilities(Wire::OpenAiSpeech, "kokoro").unwrap(),
        request: SpeechRequest::new("af_bella"),
        text,
    }
}

/// The next event, failing the test if none arrives in time.
fn next(speech: &mut Speech) -> SpeechEvent {
    match speech.next_timeout(WAIT) {
        Some(Ok(event)) => event,
        Some(Err(error)) => panic!("speech failed: {error}"),
        None => panic!("no speech event within {WAIT:?}"),
    }
}

/// Audio as (segment, bytes) up to the end, and the end's usage.
fn drain(speech: &mut Speech) -> (Vec<(u32, Vec<u8>)>, cairn_models_types::usage::MeteredUsage) {
    let mut audio = Vec::new();
    loop {
        match next(speech) {
            SpeechEvent::Audio(chunk) => audio.push((chunk.segment, chunk.data)),
            SpeechEvent::End { usage } => return (audio, usage),
        }
    }
}

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < WAIT, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn streaming_first_audio_arrives_before_input_ends() {
    let (transport, _log) = echoing();
    let (feed, text) = text_feed();
    let mut speech = speak(transport, native_call(text)).unwrap();
    assert_eq!(*speech.format(), AudioFormat::pcm(16_000));
    feed.push("Hello ").unwrap();
    // The feed is still open: nothing has ended the input.
    let SpeechEvent::Audio(first) = next(&mut speech) else {
        panic!("expected audio first");
    };
    assert_eq!(first.data, b"Hello ");
    assert_eq!(first.segment, 0);
    feed.push("world").unwrap();
    drop(feed);
    let (rest, usage) = drain(&mut speech);
    assert_eq!(rest, vec![(0, b"world ".to_vec())]);
    assert_eq!(
        usage.quantity(Charge::Input, BillingUnit::Characters),
        Some(12)
    );
    assert!(speech.next().is_none());
}

#[test]
fn streaming_drop_cancels_upstream() {
    let (transport, log) = echoing();
    let (feed, text) = text_feed();
    let speech = speak(transport, native_call(text)).unwrap();
    wait_until("the stream opens", || {
        log.stream_opened.load(Ordering::SeqCst)
    });
    feed.push("Barge ").unwrap();
    let dropped = Instant::now();
    drop(speech);
    while !log.stream_dropped.load(Ordering::SeqCst) {
        assert!(
            dropped.elapsed() < Duration::from_millis(200),
            "upstream still open 200 ms after the speech was dropped"
        );
        thread::sleep(Duration::from_millis(1));
    }
    // The utterance is over, so the feed says so.
    wait_until("the feed is refused", || feed.push("more ").is_err());
    assert!(matches!(feed.push("x"), Err(SpeakError::Closed)));
}

#[test]
fn ending_the_feed_finishes_upstream_and_ends_with_usage() {
    let (transport, log) = echoing();
    let (feed, text) = text_feed();
    let mut speech = speak(transport, native_call(text)).unwrap();
    feed.push("Hi there").unwrap();
    feed.finish();
    let (audio, usage) = drain(&mut speech);
    assert_eq!(audio, vec![(0, b"Hi ".to_vec()), (0, b"there ".to_vec())]);
    assert_eq!(
        log.sent(),
        vec![
            Sent::Text("Hi ".into()),
            Sent::Text("there ".into()),
            Sent::Finish
        ]
    );
    assert_eq!(
        usage.quantity(Charge::Input, BillingUnit::Characters),
        Some(9)
    );
    // 9 bytes of 16 kHz PCM is a fraction of a second: one on the meter.
    let seconds = usage
        .measures
        .iter()
        .find(|measure| measure.meter.unit == BillingUnit::Seconds)
        .unwrap();
    assert_eq!(seconds.meter.modality, Some(Modality::Audio));
    assert_eq!(seconds.quantity, 1);
    assert!(speech.is_finished());
    assert!(speech.next().is_none());
}

#[test]
fn a_flush_reaches_upstream_with_the_partial_word() {
    let (transport, log) = echoing();
    let (feed, text) = text_feed();
    let _speech = speak(transport, native_call(text)).unwrap();
    feed.push("abc").unwrap();
    feed.flush();
    wait_until("the flush is sent", || {
        log.sent().contains(&Sent::Flush("abc ".into()))
    });
}

#[test]
fn a_partial_word_waits_for_its_whitespace() {
    let (transport, log) = echoing();
    let (feed, text) = text_feed();
    let _speech = speak(transport, native_call(text)).unwrap();
    feed.push("Hel").unwrap();
    thread::sleep(Duration::from_millis(100));
    assert!(log.sent().is_empty(), "{:?}", log.sent());
    feed.push("lo wor").unwrap();
    wait_until("the whole word is sent", || !log.sent().is_empty());
    thread::sleep(Duration::from_millis(60));
    assert_eq!(log.sent(), vec![Sent::Text("Hello ".into())]);
}

#[test]
fn segmented_first_sentence_is_heard_while_the_second_is_fed() {
    let (transport, log) = echoing();
    let (feed, text) = text_feed();
    let mut speech = speak(transport, segmented_call(text)).unwrap();
    assert_eq!(*speech.format(), AudioFormat::pcm(24_000));
    feed.push("First sentence here. Sec").unwrap();
    let SpeechEvent::Audio(first) = next(&mut speech) else {
        panic!("expected audio");
    };
    assert_eq!(first.data, b"First sentence here.");
    assert_eq!(first.segment, 0);
    feed.push("ond one.").unwrap();
    drop(feed);
    let (rest, _) = drain(&mut speech);
    assert_eq!(rest, vec![(1, b"Second one.".to_vec())]);
    assert_eq!(
        log.clip_inputs(),
        vec!["First sentence here.", "Second one."]
    );
}

#[test]
fn streaming_order_holds_across_segments() {
    // The first clip is slow and the second instant, so the second finishes
    // generating first; its audio must still come out second.
    let (transport, log) = scripted(|input| match input {
        "One." => ClipPlan {
            chunks: vec![b"a1".to_vec(), b"a2".to_vec()],
            delay: Duration::from_millis(150),
            usage: None,
        },
        _ => ClipPlan {
            chunks: vec![b"b1".to_vec()],
            delay: Duration::ZERO,
            usage: None,
        },
    });
    let mut speech = speak(transport, segmented_call(whole_text("One. Two."))).unwrap();
    let (audio, _) = drain(&mut speech);
    assert_eq!(
        audio,
        vec![
            (0, b"a1".to_vec()),
            (0, b"a2".to_vec()),
            (1, b"b1".to_vec())
        ]
    );
    assert_eq!(*log.clips_completed.lock().unwrap(), vec!["Two.", "One."]);
}

#[test]
fn never_more_than_two_requests_are_in_flight() {
    let (transport, log) = scripted(|input| ClipPlan {
        chunks: vec![input.as_bytes().to_vec(), b"!".to_vec()],
        delay: Duration::from_millis(30),
        usage: None,
    });
    let text = "Alpha one. Bravo two. Charlie three. Delta four. Echo five.";
    let mut speech = speak(transport, segmented_call(whole_text(text))).unwrap();
    let (audio, _) = drain(&mut speech);
    let segments: Vec<u32> = audio.iter().map(|(segment, _)| *segment).collect();
    assert_eq!(segments, vec![0, 0, 1, 1, 2, 2, 3, 3, 4, 4]);
    assert_eq!(log.clip_inputs().len(), 5);
    assert!(log.max_live.load(Ordering::SeqCst) <= 2);
    assert_eq!(
        log.max_live.load(Ordering::SeqCst),
        2,
        "the next clip generates while one streams"
    );
}

#[test]
fn dropping_segmented_speech_cancels_every_request_and_starts_no_more() {
    let (transport, log) = scripted(|_| ClipPlan {
        chunks: vec![vec![0; 8]; 3],
        delay: Duration::from_millis(500),
        usage: None,
    });
    let (feed, text) = text_feed();
    let speech = speak(transport, segmented_call(text)).unwrap();
    feed.push("One here. Two here. Three here. Four here. Five")
        .unwrap();
    wait_until("two clips are in flight", || {
        log.live_clips.load(Ordering::SeqCst) == 2
    });
    let dropped = Instant::now();
    drop(speech);
    while log.live_clips.load(Ordering::SeqCst) > 0 {
        assert!(
            dropped.elapsed() < Duration::from_millis(300),
            "clips still open 300 ms after the speech was dropped"
        );
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(150));
    assert_eq!(
        log.clip_inputs().len(),
        2,
        "no request started after the drop"
    );
}

#[test]
fn usage_sums_across_segments() {
    let (transport, _log) = scripted(|input| {
        let first = input.starts_with("Twelve");
        ClipPlan {
            // 2400 samples of 24 kHz PCM: a tenth of a second each.
            chunks: vec![vec![0; 4800]],
            delay: Duration::ZERO,
            usage: Some(ClipUsage {
                input_tokens: if first { 10 } else { 5 },
                output_tokens: if first { 100 } else { 50 },
            }),
        }
    });
    let mut speech = speak(
        transport,
        segmented_call(whole_text("Twelve chars. Eleven more.")),
    )
    .unwrap();
    let (_, usage) = drain(&mut speech);
    assert_eq!(
        usage.quantity(Charge::Input, BillingUnit::Characters),
        Some(25)
    );
    assert_eq!(usage.quantity(Charge::Input, BillingUnit::Tokens), Some(15));
    assert_eq!(
        usage.quantity(Charge::Output, BillingUnit::Tokens),
        Some(150)
    );
    assert_eq!(
        usage.quantity(Charge::Output, BillingUnit::Seconds),
        Some(1)
    );
}

#[test]
fn a_failed_request_ends_the_utterance_with_its_error() {
    struct Refusing;
    impl SpeechTransport for Refusing {
        fn open_stream(
            &self,
            _: &SpeechRoute,
            _: &StreamOpen,
        ) -> Result<Box<dyn StreamUpstream>, SpeechWireError> {
            Err(SpeechWireError::Auth(
                "ElevenLabs (HTTP 401): bad key".into(),
            ))
        }
        fn clip(
            &self,
            _: &SpeechRoute,
            _: &ClipRequest,
            _: &Arc<AtomicBool>,
        ) -> Result<Box<dyn ClipUpstream>, SpeechWireError> {
            Err(SpeechWireError::RateLimited(
                "Kokoro speech (HTTP 429): slow down".into(),
            ))
        }
    }
    let mut speech = speak(Arc::new(Refusing), native_call(whole_text("Hi."))).unwrap();
    assert!(matches!(
        speech.next(),
        Some(Err(SpeakError::Wire(SpeechWireError::Auth(_))))
    ));
    assert!(speech.next().is_none());
    let mut speech = speak(Arc::new(Refusing), segmented_call(whole_text("Hi."))).unwrap();
    assert!(matches!(
        speech.next(),
        Some(Err(SpeakError::Wire(SpeechWireError::RateLimited(_))))
    ));
}

#[test]
fn an_unadmitted_request_is_refused_before_anything_opens() {
    let (transport, log) = echoing();
    let mut call = segmented_call(whole_text("Hi."));
    call.request.instructions = Some("whisper".into());
    let refused = speak(transport, call).err().unwrap();
    assert!(matches!(refused, SpeakError::Unsupported(_)), "{refused}");
    assert!(log.clip_inputs().is_empty());
}
