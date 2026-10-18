//! The `audio/speech` POST and its streamed read.
//!
//! Cancellation contract (the llm thread's ruling, amending
//! `speak-blocking-kokoro-first`): the caller is released at once on cancel
//! or drop and never waits on the network. A responsive server's connection
//! closes promptly, because the worker's next read sees the stream is gone and
//! drops the response. A silent server's closes within the stall bound, which
//! is reqwest blocking's per-operation timeout: it bounds the send up to the
//! headers and then each body read, never the whole stream.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use super::wire::{classify_error, decode_sse_data, SpeechUsage, SseEvent, StreamFormat};
use crate::SpeechWireError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How often a waiting stream rechecks the cancel flag.
const POLL: Duration = Duration::from_millis(20);
/// Raw audio is read in chunks of at most this many bytes.
const READ_CHUNK: usize = 16 * 1024;
/// How much of an error body is kept for the message.
const ERROR_BODY_LIMIT: u64 = 64 * 1024;

/// Where an `audio/speech` deployment lives and how it authenticates.
#[derive(Clone)]
pub struct OpenAiSpeechEndpoint {
    /// What to call this provider in an error a user will read.
    pub provider_name: String,
    /// The full `{base}/audio/speech` URL.
    pub url: String,
    pub headers: Vec<(String, String)>,
}

/// Header values carry bearer keys; only their names are printed.
impl std::fmt::Debug for OpenAiSpeechEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiSpeechEndpoint")
            .field("provider_name", &self.provider_name)
            .field("url", &self.url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| format!("{name}: <redacted>"))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl OpenAiSpeechEndpoint {
    fn header_map(&self) -> Result<HeaderMap, SpeechWireError> {
        let mut map = HeaderMap::new();
        for (name, value) in &self.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                SpeechWireError::InvalidRequest(format!(
                    "invalid {} header name: {error}",
                    self.provider_name
                ))
            })?;
            let value = HeaderValue::from_str(value).map_err(|error| {
                SpeechWireError::InvalidRequest(format!(
                    "invalid {} header value: {error}",
                    self.provider_name
                ))
            })?;
            map.insert(name, value);
        }
        Ok(map)
    }
}

enum Piece {
    Audio(Vec<u8>),
    Usage(Option<SpeechUsage>),
    End,
    Failed(SpeechWireError),
}

/// One clip's audio as it arrives.
///
/// A worker thread makes the request and reads the response, handing pieces
/// over a bounded channel; iteration waits on the channel, rechecking `cancel`
/// every few milliseconds, so the caller is never held by the network. Once
/// the stream ends (cancelled, stalled, finished or dropped) the worker is
/// detached: it sees that at its next read and drops the response, closing
/// the connection, and a read that never returns ends at the stall bound.
pub struct ClipStream {
    pieces: Receiver<Piece>,
    gone: Arc<AtomicBool>,
    cancel: Arc<AtomicBool>,
    stall: Duration,
    usage: Option<SpeechUsage>,
    finished: bool,
    provider: String,
}

impl ClipStream {
    /// Token usage, once `speech.audio.done` has been read (SSE only).
    pub fn usage(&self) -> Option<SpeechUsage> {
        self.usage
    }

    fn end(&mut self) {
        self.finished = true;
        self.gone.store(true, Ordering::SeqCst);
    }
}

impl Iterator for ClipStream {
    type Item = Result<Vec<u8>, SpeechWireError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let deadline = Instant::now() + self.stall;
        loop {
            if self.cancel.load(Ordering::SeqCst) {
                self.end();
                return None;
            }
            match self.pieces.recv_timeout(POLL) {
                Ok(Piece::Audio(audio)) => return Some(Ok(audio)),
                Ok(Piece::Usage(usage)) => self.usage = usage,
                Ok(Piece::End) => {
                    self.end();
                    return None;
                }
                Ok(Piece::Failed(error)) => {
                    self.end();
                    return Some(Err(error));
                }
                Err(RecvTimeoutError::Timeout) => {
                    if Instant::now() >= deadline {
                        self.end();
                        return Some(Err(SpeechWireError::Transport(format!(
                            "{} speech stalled: no audio for {}s",
                            self.provider,
                            self.stall.as_secs()
                        ))));
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.end();
                    return Some(Err(SpeechWireError::Transport(format!(
                        "{} speech stream ended without finishing",
                        self.provider
                    ))));
                }
            }
        }
    }
}

impl Drop for ClipStream {
    fn drop(&mut self) {
        self.gone.store(true, Ordering::SeqCst);
    }
}

/// The shared client for `stall`. Its timeout is per operation (the send up to
/// the headers, then each body read), so it is the bound a silent server is
/// held to; a long, steadily streaming clip is never cut off by it. Shared so
/// segments reuse connections instead of paying a TLS handshake each.
fn client(stall: Duration) -> Result<reqwest::blocking::Client, SpeechWireError> {
    static CLIENTS: OnceLock<Mutex<HashMap<Duration, reqwest::blocking::Client>>> = OnceLock::new();
    let mut clients = CLIENTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if let Some(client) = clients.get(&stall) {
        return Ok(client.clone());
    }
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT.min(stall))
        .timeout(stall)
        .build()
        .map_err(|error| SpeechWireError::Transport(error.to_string()))?;
    clients.insert(stall, client.clone());
    Ok(client)
}

/// Start one clip request and stream its audio. Returns at once: the request,
/// its headers (a refusal status included) and its audio all arrive through
/// the stream. `stall` bounds every wait for the next piece, and each network
/// operation the worker makes.
///
/// The caller's thread only parses the URL, headers and body. Even fetching
/// the shared client stays on the worker, because building one the first time
/// loads the platform's root certificates and proxy settings, which takes
/// hundreds of milliseconds on macOS.
pub fn post_clip(
    endpoint: &OpenAiSpeechEndpoint,
    body: &Value,
    stream_format: StreamFormat,
    cancel: Arc<AtomicBool>,
    stall: Duration,
) -> Result<ClipStream, SpeechWireError> {
    let provider = endpoint.provider_name.clone();
    let invalid = |error: &dyn std::fmt::Display| {
        SpeechWireError::InvalidRequest(format!("{provider} speech request: {error}"))
    };
    let url = reqwest::Url::parse(&endpoint.url).map_err(|error| invalid(&error))?;
    let payload = serde_json::to_vec(body).map_err(|error| invalid(&error))?;
    let mut request = reqwest::blocking::Request::new(reqwest::Method::POST, url);
    *request.headers_mut() = endpoint.header_map()?;
    request
        .headers_mut()
        .entry(reqwest::header::CONTENT_TYPE)
        .or_insert(HeaderValue::from_static("application/json"));
    *request.body_mut() = Some(payload.into());
    let gone = Arc::new(AtomicBool::new(false));
    let (sender, pieces) = mpsc::sync_channel(8);
    let worker = Worker {
        stall,
        provider: provider.clone(),
        gone: gone.clone(),
        sender,
    };
    std::thread::Builder::new()
        .name("speech-clip".into())
        .spawn(move || worker.run(request, stream_format))
        .map_err(|error| SpeechWireError::Transport(error.to_string()))?;
    Ok(ClipStream {
        pieces,
        gone,
        cancel,
        stall,
        usage: None,
        finished: false,
        provider,
    })
}

/// `error` with every cause it wraps, since reqwest's own message hides them.
fn with_causes(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    message
}

struct Worker {
    /// Selects the shared client, which the worker fetches itself.
    stall: Duration,
    provider: String,
    /// Set once the stream has ended: stop at the next read.
    gone: Arc<AtomicBool>,
    sender: SyncSender<Piece>,
}

impl Worker {
    /// The server went silent past the per-operation bound.
    fn stalled(&self) -> SpeechWireError {
        SpeechWireError::Transport(format!(
            "{} speech stalled: the server sent nothing within the stall bound",
            self.provider
        ))
    }

    fn read_failed(&self, error: &std::io::Error) -> SpeechWireError {
        if error.kind() == std::io::ErrorKind::TimedOut {
            return self.stalled();
        }
        SpeechWireError::Transport(format!(
            "reading {} speech audio failed: {}",
            self.provider,
            with_causes(error)
        ))
    }

    /// Hand `piece` over; `false` once the stream has ended.
    fn deliver(&self, piece: Piece) -> bool {
        !self.gone.load(Ordering::SeqCst) && self.sender.send(piece).is_ok()
    }

    fn run(self, request: reqwest::blocking::Request, stream_format: StreamFormat) {
        let provider = self.provider.clone();
        let client = match client(self.stall) {
            Ok(client) => client,
            Err(error) => {
                self.deliver(Piece::Failed(error));
                return;
            }
        };
        let response = match client.execute(request) {
            Ok(response) => response,
            Err(error) if error.is_timeout() => {
                self.deliver(Piece::Failed(self.stalled()));
                return;
            }
            Err(error) => {
                self.deliver(Piece::Failed(SpeechWireError::Transport(format!(
                    "{provider} speech request failed: {}",
                    with_causes(&error)
                ))));
                return;
            }
        };
        let status = response.status();
        if !status.is_success() {
            let mut text = String::new();
            let _ = response.take(ERROR_BODY_LIMIT).read_to_string(&mut text);
            self.deliver(Piece::Failed(classify_error(
                &provider,
                status.as_u16(),
                &text,
            )));
            return;
        }
        match stream_format {
            StreamFormat::Audio => self.read_audio(response),
            StreamFormat::Sse => self.read_sse(response),
        }
        // Dropping the response here closes a connection cut short.
    }

    fn read_audio(&self, mut body: impl Read) {
        let mut buffer = vec![0u8; READ_CHUNK];
        loop {
            match body.read(&mut buffer) {
                Ok(0) => {
                    self.deliver(Piece::End);
                    return;
                }
                Ok(read) => {
                    if !self.deliver(Piece::Audio(buffer[..read].to_vec())) {
                        return;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    self.deliver(Piece::Failed(self.read_failed(&error)));
                    return;
                }
            }
        }
    }

    fn read_sse(&self, body: impl Read) {
        let mut done = false;
        for line in BufReader::new(body).lines() {
            let line = match line {
                Ok(line) => line,
                Err(error) => {
                    self.deliver(Piece::Failed(self.read_failed(&error)));
                    return;
                }
            };
            if self.gone.load(Ordering::SeqCst) {
                return;
            }
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let piece = match decode_sse_data(&self.provider, data) {
                Ok(None) => continue,
                Ok(Some(SseEvent::Audio(audio))) => Piece::Audio(audio),
                Ok(Some(SseEvent::Done(usage))) => {
                    done = true;
                    Piece::Usage(usage)
                }
                Err(error) => Piece::Failed(error),
            };
            let failed = matches!(piece, Piece::Failed(_));
            if !self.deliver(piece) || failed {
                return;
            }
        }
        if done {
            self.deliver(Piece::End);
        } else {
            self.deliver(Piece::Failed(SpeechWireError::InvalidResponse(format!(
                "{} speech events ended without speech.audio.done",
                self.provider
            ))));
        }
    }
}
