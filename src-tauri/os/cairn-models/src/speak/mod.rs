//! The speech verb's drivers: how an admitted [`SpeechCall`] becomes audio.
//!
//! [`speak`] admits the request, then starts one worker for the utterance and
//! returns at once, so the connection opens while the caller's language model
//! produces its first tokens. Which worker follows from the offer's input
//! streaming: [`native`] keeps one upstream connection and sends words as they
//! complete, [`segmented`] cuts the text with the shared [`segment::Segmenter`]
//! and pipelines one whole-text request per segment. [`HttpSpeech`] is the
//! transport that reaches real providers; [`openai`] and [`elevenlabs`] build
//! the routes it takes.

pub mod elevenlabs;
mod http;
mod native;
pub mod openai;
pub mod segment;
mod segmented;
mod usage;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub use http::HttpSpeech;

use crate::interface::speak::{
    admit, InputStreaming, SpeakError, Speech, SpeechCall, SpeechTransport, SpeechWireError,
};

/// Speak `call`: admit it against its offer's capabilities, then stream its
/// text to the provider as it arrives and its audio back. Admission refusals
/// return here; anything that fails after (connecting included) arrives as
/// the iterator's error.
pub fn speak(transport: Arc<dyn SpeechTransport>, call: SpeechCall) -> Result<Speech, SpeakError> {
    let format = admit(call.route.wire(), &call.capabilities, &call.request)?;
    let cancel = Arc::new(AtomicBool::new(false));
    let (events, events_out) = crossbeam_channel::unbounded();
    let started = match call.capabilities.input {
        InputStreaming::Native => native::start(transport, call, format, cancel.clone(), events),
        InputStreaming::Segmented => {
            segmented::start(transport, call, format, cancel.clone(), events)
        }
    };
    started.map_err(|error| {
        SpeakError::Wire(SpeechWireError::Transport(format!(
            "could not start the speech worker: {error}"
        )))
    })?;
    Ok(Speech::new(events_out, cancel, format))
}

#[cfg(test)]
mod tests;
