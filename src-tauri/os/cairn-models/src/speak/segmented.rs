//! The segmented driver: the shared segmenter over the feed, one whole-text
//! request per segment, at most [`MAX_IN_FLIGHT`] at once.
//!
//! A coordinator thread owns the order. Segment N streams to the caller while
//! N+1 generates on its own thread; N+1's audio waits in its channel, in
//! memory, and is released only once N has ended, so output order always
//! holds however the requests race. Every chunk carries its segment index.
//! Dropping the [`Speech`](crate::interface::speak::Speech) sets the shared
//! cancel flag: the coordinator stops issuing requests and each clip thread
//! drops its response at its next read, which closes its connection.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Select, Sender};

use super::segment::Segmenter;
use super::usage::Tally;
use crate::interface::speak::{
    AudioChunk, AudioFormat, ClipRequest, ClipUsage, FeedItem, SpeakError, SpeechCall, SpeechEvent,
    SpeechRoute, SpeechText, SpeechTransport, SpeechWireError,
};

/// Requests alive at once: the one streaming and the one generating next.
pub(super) const MAX_IN_FLIGHT: usize = 2;
/// How long the coordinator waits on its channels before rechecking cancel.
const TICK: Duration = Duration::from_millis(20);

type Events = Sender<Result<SpeechEvent, SpeakError>>;

enum ClipMessage {
    Audio(Vec<u8>),
    Done(Option<ClipUsage>),
    Failed(SpeechWireError),
}

struct InFlight {
    segment: u32,
    messages: Receiver<ClipMessage>,
}

pub(super) fn start(
    transport: Arc<dyn SpeechTransport>,
    call: SpeechCall,
    format: AudioFormat,
    cancel: Arc<AtomicBool>,
    events: Events,
) -> std::io::Result<()> {
    let template = ClipRequest {
        model: call.model,
        voice: call.request.voice,
        input: String::new(),
        format,
        speed: call.request.speed,
        instructions: call.request.instructions,
        extras: call.request.extras,
    };
    let coordinator = Coordinator {
        transport,
        route: call.route,
        template,
        cancel,
        events,
        tally: Tally::default(),
    };
    let text = call.text;
    std::thread::Builder::new()
        .name("speak-segmented".into())
        .spawn(move || coordinator.run(&text))
        .map(|_| ())
}

struct Coordinator {
    transport: Arc<dyn SpeechTransport>,
    route: SpeechRoute,
    template: ClipRequest,
    cancel: Arc<AtomicBool>,
    events: Events,
    tally: Tally,
}

impl Coordinator {
    fn run(mut self, text: &SpeechText) {
        let mut segmenter = Segmenter::new();
        let mut queued: VecDeque<String> = VecDeque::new();
        let mut in_flight: VecDeque<InFlight> = VecDeque::new();
        let mut next_segment: u32 = 0;
        let mut input_done = false;
        loop {
            if self.cancel.load(Ordering::SeqCst) {
                return;
            }
            while in_flight.len() < MAX_IN_FLIGHT {
                let Some(input) = queued.pop_front() else {
                    break;
                };
                self.tally.sent(&input);
                in_flight.push_back(self.issue(next_segment, input));
                next_segment += 1;
            }
            if input_done && queued.is_empty() && in_flight.is_empty() {
                let usage = self.tally.metered(&self.template.format);
                let _ = self.events.send(Ok(SpeechEvent::End { usage }));
                return;
            }

            // Wait on whichever is live: the feed, and the clip whose audio is
            // next in order. Later clips' audio waits in their channels.
            let mut select = Select::new();
            let feed = (!input_done).then(|| select.recv(&text.receiver));
            let head = in_flight.front().map(|clip| select.recv(&clip.messages));
            let Ok(operation) = select.select_timeout(TICK) else {
                continue;
            };
            let index = operation.index();
            if Some(index) == feed {
                match operation.recv(&text.receiver) {
                    Ok(FeedItem::Text(more)) => queued.extend(segmenter.push(&more)),
                    Ok(FeedItem::Flush) => queued.extend(segmenter.flush()),
                    Err(_) => {
                        queued.extend(segmenter.finish());
                        input_done = true;
                    }
                }
            } else if Some(index) == head {
                let clip = in_flight.front().expect("selected the head clip");
                let segment = clip.segment;
                match operation.recv(&clip.messages) {
                    Ok(ClipMessage::Audio(data)) => {
                        self.tally.received(data.len());
                        let chunk = AudioChunk {
                            data,
                            segment,
                            timing: None,
                        };
                        if self.events.send(Ok(SpeechEvent::Audio(chunk))).is_err() {
                            self.cancel.store(true, Ordering::SeqCst);
                            return;
                        }
                    }
                    Ok(ClipMessage::Done(usage)) => {
                        self.tally.reported(usage);
                        in_flight.pop_front();
                    }
                    Ok(ClipMessage::Failed(error)) => return self.fail(error.into()),
                    Err(_) => {
                        return self.fail(SpeakError::Wire(SpeechWireError::Transport(format!(
                            "the request for segment {segment} ended without finishing"
                        ))))
                    }
                }
            }
        }
    }

    /// Deliver `error` and stop every request still running.
    fn fail(&self, error: SpeakError) {
        self.cancel.store(true, Ordering::SeqCst);
        let _ = self.events.send(Err(error));
    }

    /// Start the request for one segment on its own thread.
    fn issue(&self, segment: u32, input: String) -> InFlight {
        let (sender, messages) = crossbeam_channel::unbounded();
        let transport = self.transport.clone();
        let route = self.route.clone();
        let cancel = self.cancel.clone();
        let request = ClipRequest {
            input,
            ..self.template.clone()
        };
        let spawned = std::thread::Builder::new()
            .name(format!("speak-clip-{segment}"))
            .spawn({
                let sender = sender.clone();
                move || run_clip(transport.as_ref(), &route, &request, &cancel, &sender)
            });
        if let Err(error) = spawned {
            let _ = sender.send(ClipMessage::Failed(SpeechWireError::Transport(format!(
                "could not start the request for segment {segment}: {error}"
            ))));
        }
        InFlight { segment, messages }
    }
}

fn run_clip(
    transport: &dyn SpeechTransport,
    route: &SpeechRoute,
    request: &ClipRequest,
    cancel: &Arc<AtomicBool>,
    sender: &Sender<ClipMessage>,
) {
    let mut upstream = match transport.clip(route, request, cancel) {
        Ok(upstream) => upstream,
        Err(error) => {
            let _ = sender.send(ClipMessage::Failed(error));
            return;
        }
    };
    for piece in upstream.by_ref() {
        if cancel.load(Ordering::SeqCst) {
            return;
        }
        let message = match piece {
            Ok(data) => ClipMessage::Audio(data),
            Err(error) => {
                let _ = sender.send(ClipMessage::Failed(error));
                return;
            }
        };
        if sender.send(message).is_err() {
            return;
        }
    }
    if !cancel.load(Ordering::SeqCst) {
        let _ = sender.send(ClipMessage::Done(upstream.usage()));
    }
}
