//! The ElevenLabs socket: handshake, sends, and timed receives on one thread.

use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::HandshakeError;
use tungstenite::http::HeaderValue;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Connector, Message, WebSocket};

use super::wire::{self, classify_error, ElevenLabsExtras, ServerEvent, Source};
use crate::{percent_encode, SpeechWireError};

/// ElevenLabs' global endpoint. Regional and data-residency hosts
/// (`wss://api.us.elevenlabs.io`, `wss://api.eu.residency.elevenlabs.io`, ...)
/// are other bases for the same protocol.
pub const DEFAULT_BASE: &str = "wss://api.elevenlabs.io";

/// Where ElevenLabs lives and the key that authenticates to it.
#[derive(Clone)]
pub struct ElevenLabsEndpoint {
    /// `wss://api.elevenlabs.io`, a regional host, or (in tests) `ws://...`.
    pub base: String,
    pub api_key: String,
}

/// The key is a secret; only the base is printed.
impl std::fmt::Debug for ElevenLabsEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElevenLabsEndpoint")
            .field("base", &self.base)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

/// What one utterance is opened with.
#[derive(Debug, Clone, Copy)]
pub struct ElevenLabsOpen<'a> {
    pub voice_id: &'a str,
    pub model: &'a str,
    /// ElevenLabs' `codec_rate[_kbps]` spelling, for example `pcm_24000`.
    pub output_format: &'a str,
    /// 1.0 is normal; ElevenLabs accepts 0.7 to 1.2.
    pub speed: Option<f32>,
    pub extras: &'a ElevenLabsExtras,
}

impl ElevenLabsEndpoint {
    /// The stream-input URL for `open`: voice in the path, model, format and
    /// the query extras in the query.
    pub fn url(&self, open: &ElevenLabsOpen<'_>) -> String {
        let mut url = format!(
            "{}/v1/text-to-speech/{}/stream-input?model_id={}&output_format={}",
            self.base.trim_end_matches('/'),
            percent_encode(open.voice_id),
            percent_encode(open.model),
            percent_encode(open.output_format),
        );
        for (key, value) in open.extras.query() {
            url.push_str(&format!("&{key}={}", percent_encode(&value)));
        }
        url
    }
}

/// One open stream-input socket.
///
/// Writes and timed reads happen on the caller's thread: the read timeout is
/// set on the underlying TCP socket before each receive, so a single worker
/// can interleave sending text and draining audio. Dropping the socket sends a
/// close frame and shuts the connection down without waiting for a reply.
pub struct ElevenLabsSocket {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    /// The same OS socket as inside `socket`, kept to set timeouts and shut it
    /// down.
    tcp: TcpStream,
    output_format: String,
}

impl std::fmt::Debug for ElevenLabsSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElevenLabsSocket")
            .field("output_format", &self.output_format)
            .finish_non_exhaustive()
    }
}

impl ElevenLabsSocket {
    /// Open the socket and send the initial message. `timeout` bounds the TCP
    /// connect, the handshake, and every later write.
    pub fn connect(
        endpoint: &ElevenLabsEndpoint,
        open: &ElevenLabsOpen<'_>,
        timeout: Duration,
    ) -> Result<Self, SpeechWireError> {
        let url = endpoint.url(open);
        let mut request = url.as_str().into_client_request().map_err(|error| {
            SpeechWireError::InvalidRequest(format!(
                "ElevenLabs base '{}' is not a WebSocket URL: {error}",
                endpoint.base
            ))
        })?;
        let key = HeaderValue::from_str(&endpoint.api_key).map_err(|_| {
            SpeechWireError::Auth("the ElevenLabs API key is not a valid header value".into())
        })?;
        request.headers_mut().insert("xi-api-key", key);

        let uri = request.uri().clone();
        let secure = match uri.scheme_str() {
            Some("wss") => true,
            Some("ws") => false,
            _ => {
                return Err(SpeechWireError::InvalidRequest(format!(
                    "ElevenLabs base '{}' must start with wss:// (or ws:// for a local server)",
                    endpoint.base
                )))
            }
        };
        let host = uri
            .host()
            .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
            .ok_or_else(|| {
                SpeechWireError::InvalidRequest(format!(
                    "ElevenLabs base '{}' names no host",
                    endpoint.base
                ))
            })?;
        let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
        let tcp = dial(host, port, timeout)?;
        let handle = tcp.try_clone().map_err(transport)?;
        let connector = if secure {
            Some(Connector::Rustls(crate::tls::client_config()?))
        } else {
            None
        };
        let (socket, _response) = tungstenite::client_tls_with_config(
            request, tcp, None, connector,
        )
        .map_err(|error| match error {
            HandshakeError::Failure(tungstenite::Error::Http(response)) => {
                // tungstenite keeps only the body bytes that arrived with the
                // response head, so the body can be partial or absent; the
                // status still classifies, and its reason phrase stands in.
                let status = response.status();
                let body = response
                    .body()
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .filter(|body| !body.trim().is_empty())
                    .map(|body| body.into_owned())
                    .unwrap_or_else(|| status.canonical_reason().unwrap_or_default().to_string());
                classify_error(Source::Http(status.as_u16()), &body, open.output_format)
            }
            HandshakeError::Failure(error) => SpeechWireError::Transport(format!(
                "ElevenLabs WebSocket handshake failed: {error}"
            )),
            HandshakeError::Interrupted(_) => {
                SpeechWireError::Transport("ElevenLabs WebSocket handshake timed out".to_string())
            }
        })?;
        let mut socket = Self {
            socket,
            tcp: handle,
            output_format: open.output_format.to_string(),
        };
        socket.send(&wire::init_message(open.extras, open.speed))?;
        Ok(socket)
    }

    /// Send a text chunk. It must end in a space and split no word.
    pub fn send_text(&mut self, text: &str) -> Result<(), SpeechWireError> {
        self.send(&wire::text_message(text))
    }

    /// Send `text` (possibly empty) and force generation of everything
    /// buffered.
    pub fn flush(&mut self, text: &str) -> Result<(), SpeechWireError> {
        self.send(&wire::flush_message(text))
    }

    /// Reset ElevenLabs' inactivity timer.
    pub fn keepalive(&mut self) -> Result<(), SpeechWireError> {
        self.send(&wire::keepalive_message())
    }

    /// End input. Audio for what remains still arrives, then a final frame.
    pub fn close_input(&mut self) -> Result<(), SpeechWireError> {
        self.send(&wire::close_message())
    }

    /// The next frame, waiting at most `timeout`. `Ok(None)` means nothing
    /// arrived in time, or a frame carried nothing to act on.
    pub fn recv(&mut self, timeout: Duration) -> Result<Option<ServerEvent>, SpeechWireError> {
        self.tcp
            .set_read_timeout(Some(timeout.max(Duration::from_millis(1))))
            .map_err(transport)?;
        match self.socket.read() {
            Ok(Message::Text(text)) => wire::decode_frame(text.as_str(), &self.output_format),
            Ok(Message::Binary(_)) => Err(SpeechWireError::InvalidResponse(
                "ElevenLabs sent a binary frame; the stream-input protocol sends JSON".into(),
            )),
            Ok(Message::Close(frame)) => match frame {
                None => Ok(Some(ServerEvent::Final)),
                Some(frame) if frame.code == CloseCode::Normal => Ok(Some(ServerEvent::Final)),
                Some(frame) => Err(classify_error(
                    Source::Close(u16::from(frame.code)),
                    frame.reason.as_str(),
                    &self.output_format,
                )),
            },
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Err(tungstenite::Error::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                Ok(None)
            }
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => Err(
                SpeechWireError::Transport("the ElevenLabs socket is closed".to_string()),
            ),
            Err(error) => Err(SpeechWireError::Transport(format!(
                "reading from ElevenLabs failed: {error}"
            ))),
        }
    }

    fn send(&mut self, message: &serde_json::Value) -> Result<(), SpeechWireError> {
        self.socket
            .send(Message::text(message.to_string()))
            .map_err(|error| {
                SpeechWireError::Transport(format!("sending to ElevenLabs failed: {error}"))
            })
    }
}

impl Drop for ElevenLabsSocket {
    /// Barge-in: say goodbye and drop the connection at once. Nothing waits
    /// for ElevenLabs to answer the close.
    fn drop(&mut self) {
        let _ = self.tcp.set_write_timeout(Some(Duration::from_millis(500)));
        let _ = self.socket.close(None);
        let _ = self.socket.flush();
        let _ = self.tcp.shutdown(std::net::Shutdown::Both);
    }
}

fn dial(host: &str, port: u16, timeout: Duration) -> Result<TcpStream, SpeechWireError> {
    let addresses = (host, port).to_socket_addrs().map_err(|error| {
        SpeechWireError::Transport(format!("cannot resolve ElevenLabs host {host}: {error}"))
    })?;
    let mut last = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(tcp) => {
                tcp.set_read_timeout(Some(timeout)).map_err(transport)?;
                tcp.set_write_timeout(Some(timeout)).map_err(transport)?;
                let _ = tcp.set_nodelay(true);
                return Ok(tcp);
            }
            Err(error) => last = Some(error),
        }
    }
    Err(SpeechWireError::Transport(match last {
        Some(error) => format!("cannot connect to ElevenLabs at {host}:{port}: {error}"),
        None => format!("ElevenLabs host {host} resolved to no address"),
    }))
}

fn transport(error: std::io::Error) -> SpeechWireError {
    SpeechWireError::Transport(format!("ElevenLabs socket: {error}"))
}
