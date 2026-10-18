//! The native driver: one upstream stream per utterance, text sent as words
//! complete.
//!
//! One worker thread both writes and reads. Each pass drains the feed, sends
//! everything up to the last whitespace (a provider that streams input wants
//! whole words, each chunk ending in a space) and holds the partial word,
//! waits briefly for audio, and checks the cancel flag. The stream is opened
//! before any text arrives, so its setup overlaps the caller's first tokens.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TryRecvError};

use super::usage::Tally;
use crate::interface::speak::{
    AudioChunk, AudioFormat, FeedItem, SpeakError, SpeechCall, SpeechEvent, SpeechRoute,
    SpeechText, SpeechTransport, StreamOpen, StreamUpstream, UpstreamEvent,
};

/// How long one receive waits before the worker looks at the feed again.
const TICK: Duration = Duration::from_millis(20);
/// How long flushed or finished text may go unanswered before the utterance
/// is reported stalled.
const STALL: Duration = Duration::from_secs(30);
/// How long the stream may sit without a send before a keepalive. Shorter
/// than any provider's idle timeout (ElevenLabs' is 20 s by default; Cairn
/// asks for 180 s, because a commander may think before speaking).
const KEEPALIVE: Duration = Duration::from_secs(15);

type Events = Sender<Result<SpeechEvent, SpeakError>>;

pub(super) fn start(
    transport: Arc<dyn SpeechTransport>,
    call: SpeechCall,
    format: AudioFormat,
    cancel: Arc<AtomicBool>,
    events: Events,
) -> std::io::Result<()> {
    let open = StreamOpen {
        model: call.model,
        voice: call.request.voice,
        format,
        speed: call.request.speed,
        extras: call.request.extras,
    };
    let route = call.route;
    let text = call.text;
    std::thread::Builder::new()
        .name("speak-native".into())
        .spawn(move || {
            let worker = Worker {
                format,
                cancel,
                events,
                tally: Tally::default(),
            };
            worker.run(transport.as_ref(), &route, &open, &text);
        })
        .map(|_| ())
}

struct Worker {
    format: AudioFormat,
    cancel: Arc<AtomicBool>,
    events: Events,
    tally: Tally,
}

impl Worker {
    fn run(
        mut self,
        transport: &dyn SpeechTransport,
        route: &SpeechRoute,
        open: &StreamOpen,
        text: &SpeechText,
    ) {
        let mut upstream = match transport.open_stream(route, open) {
            Ok(upstream) => upstream,
            Err(error) => {
                let _ = self.events.send(Err(error.into()));
                return;
            }
        };
        if let Err(error) = self.drive(upstream.as_mut(), route, text) {
            let _ = self.events.send(Err(error));
        }
        // Dropping `upstream` here closes the connection without waiting.
    }

    /// Run until the end, an error, or cancellation. `Ok` covers both a
    /// delivered end and a cancelled utterance (which delivers nothing).
    fn drive(
        &mut self,
        upstream: &mut dyn StreamUpstream,
        route: &SpeechRoute,
        text: &SpeechText,
    ) -> Result<(), SpeakError> {
        let mut pending = String::new();
        let mut input_done = false;
        let mut finished = false;
        let mut awaiting: Option<Instant> = None;
        let mut last_sent = Instant::now();
        loop {
            if self.cancel.load(Ordering::SeqCst) {
                return Ok(());
            }
            while !input_done {
                match text.receiver.try_recv() {
                    Ok(FeedItem::Text(more)) => pending.push_str(&more),
                    Ok(FeedItem::Flush) => {
                        let chunk = take_all(&mut pending);
                        self.tally.sent(&chunk);
                        upstream.flush(&chunk)?;
                        awaiting.get_or_insert_with(Instant::now);
                        last_sent = Instant::now();
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => input_done = true,
                }
            }
            if !finished {
                if let Some(words) = take_words(&mut pending) {
                    self.tally.sent(&words);
                    upstream.send(&words)?;
                    last_sent = Instant::now();
                }
                if input_done {
                    let rest = take_all(&mut pending);
                    if !rest.is_empty() {
                        self.tally.sent(&rest);
                        upstream.send(&rest)?;
                    }
                    upstream.finish()?;
                    finished = true;
                    awaiting = Some(Instant::now());
                } else if last_sent.elapsed() >= KEEPALIVE {
                    upstream.keepalive()?;
                    last_sent = Instant::now();
                }
            }
            match upstream.recv(TICK)? {
                Some(UpstreamEvent::Audio { data, timing }) => {
                    self.tally.received(data.len());
                    // After the input has ended, the clock keeps running
                    // until the final frame.
                    awaiting = finished.then(Instant::now);
                    let chunk = AudioChunk {
                        data,
                        segment: 0,
                        timing,
                    };
                    if self.events.send(Ok(SpeechEvent::Audio(chunk))).is_err() {
                        return Ok(());
                    }
                }
                Some(UpstreamEvent::Final) => {
                    let usage = self.tally.metered(&self.format);
                    let _ = self.events.send(Ok(SpeechEvent::End { usage }));
                    return Ok(());
                }
                None => {}
            }
            if awaiting.is_some_and(|since| since.elapsed() >= STALL) {
                return Err(SpeakError::Stalled(format!(
                    "{} sent no audio for {}s after text was flushed",
                    route.provider_name(),
                    STALL.as_secs()
                )));
            }
        }
    }
}

/// Everything in `pending` up to its last whitespace, as a chunk ending in
/// one space; the partial word after it stays. `None` when there is no whole
/// word yet.
fn take_words(pending: &mut String) -> Option<String> {
    let cut = pending
        .char_indices()
        .rev()
        .find(|(_, c)| c.is_whitespace())
        .map(|(index, c)| index + c.len_utf8())?;
    let words = pending[..cut].trim().to_string();
    pending.drain(..cut);
    (!words.is_empty()).then(|| format!("{words} "))
}

/// All of `pending`, partial word included, as a chunk ending in one space,
/// or empty when it holds no text.
fn take_all(pending: &mut String) -> String {
    let text = pending.trim().to_string();
    pending.clear();
    if text.is_empty() {
        text
    } else {
        format!("{text} ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_words_go_and_the_partial_word_stays() {
        let mut pending = "Hello wor".to_string();
        assert_eq!(take_words(&mut pending).as_deref(), Some("Hello "));
        assert_eq!(pending, "wor");
        assert_eq!(take_words(&mut pending), None);
        pending.push_str("ld,\nnext");
        assert_eq!(take_words(&mut pending).as_deref(), Some("world, "));
        assert_eq!(pending, "next");
        assert_eq!(take_all(&mut pending), "next ");
        assert!(pending.is_empty());
        let mut blank = "  ".to_string();
        assert_eq!(take_words(&mut blank), None);
        assert!(blank.is_empty());
        assert_eq!(take_all(&mut blank), "");
    }
}
