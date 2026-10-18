//! OpenAI-compatible `audio/speech`: one POST of whole text, audio streamed
//! back.
//!
//! Two delivery formats share the endpoint. `audio` (the base protocol, which
//! every compatible server such as Kokoro-FastAPI speaks) streams the encoded
//! bytes as the body. `sse` (OpenAI's `gpt-4o-mini-tts` family only; `tts-1`
//! refuses it) wraps base64 audio in `speech.audio.delta` events and ends with
//! `speech.audio.done`, which carries token usage.
//!
//! Input must be whole (at most 4096 characters on OpenAI), so a caller that
//! has a text stream segments it and issues one request per segment.

pub mod http;
pub mod wire;

pub use http::{post_clip, ClipStream, OpenAiSpeechEndpoint};
pub use wire::{build_body, SpeechBody, SpeechUsage, StreamFormat, TYPED_FIELDS};

#[cfg(test)]
mod tests;
