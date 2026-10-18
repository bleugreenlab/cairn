//! ElevenLabs' text-to-speech WebSocket: `GET /v1/text-to-speech/{voice_id}/stream-input`.
//!
//! The one provider protocol here that takes text as it is produced. The
//! client opens the socket with the voice, model and output format in the
//! query and the key in the `xi-api-key` handshake header (never in a message
//! body), sends an initial message carrying voice settings and the chunk
//! schedule, then text chunks that each end in a space, an optional `flush`,
//! and finally `{"text": ""}` to close input. Audio comes back as base64 in
//! JSON frames, optionally with per-character alignment, and a final frame
//! carries `isFinal: true`.
//!
//! ElevenLabs buffers text until its chunk schedule is met (by default 120,
//! 160, 250, then 290 characters) before generating, which keeps prosody
//! intact; `flush` forces generation of whatever is buffered.

pub mod wire;
pub mod ws;

pub use wire::{Alignment, ElevenLabsExtras, ServerEvent, TextNormalization, VoiceSettings};
pub use ws::{ElevenLabsEndpoint, ElevenLabsOpen, ElevenLabsSocket, DEFAULT_BASE};

#[cfg(test)]
mod tests;
