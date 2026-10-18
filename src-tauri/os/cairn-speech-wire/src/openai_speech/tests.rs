use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use super::wire::{classify_error, decode_sse_data, voice_value, SseEvent};
use super::*;
use crate::SpeechWireError;

const BODY_TTS1_RAW: &str = include_str!("testdata/body_tts1_raw.json");
const BODY_MINI_TTS_SSE: &str = include_str!("testdata/body_mini_tts_sse.json");
const STREAM_SSE: &str = include_str!("testdata/stream.sse");
const ERROR_400: &str = include_str!("testdata/error_400.json");
const ERROR_401: &str = include_str!("testdata/error_401.json");
const ERROR_429_RATE: &str = include_str!("testdata/error_429_rate.json");
const ERROR_429_QUOTA: &str = include_str!("testdata/error_429_quota.json");
const ERROR_500: &str = include_str!("testdata/error_500.json");
const ERROR_REFUSAL: &str = include_str!("testdata/error_refusal.json");

fn golden(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

fn tts1_body<'a>() -> SpeechBody<'a> {
    SpeechBody {
        model: "tts-1",
        input: "The commander casts Lightning Bolt.",
        voice: "alloy",
        response_format: "pcm",
        speed: None,
        instructions: None,
        stream_format: StreamFormat::Audio,
        extra_body: None,
    }
}

#[test]
fn a_raw_audio_body_omits_stream_format() {
    assert_eq!(build_body(&tts1_body()), golden(BODY_TTS1_RAW));
}

#[test]
fn an_sse_body_carries_instructions_speed_and_a_custom_voice() {
    let body = build_body(&SpeechBody {
        model: "gpt-4o-mini-tts",
        input: "Counterspell. Not today.",
        voice: "voice_1234",
        response_format: "mp3",
        speed: Some(1.25),
        instructions: Some("Dry, unhurried, faintly amused."),
        stream_format: StreamFormat::Sse,
        extra_body: None,
    });
    assert_eq!(body, golden(BODY_MINI_TTS_SSE));
    assert_eq!(voice_value("af_bella"), json!("af_bella"));
}

#[test]
fn extra_body_adds_fields_but_never_replaces_a_typed_one() {
    let mut extra = Map::new();
    extra.insert("lang_code".into(), json!("a"));
    extra.insert("normalization_options".into(), json!({"normalize": false}));
    for typed in TYPED_FIELDS {
        extra.insert(typed.to_string(), json!("replaced"));
    }
    let body = build_body(&SpeechBody {
        extra_body: Some(&extra),
        ..tts1_body()
    });
    assert_eq!(body["lang_code"], "a");
    assert_eq!(body["normalization_options"]["normalize"], false);
    // The admitted contract stands: PCM, raw audio, the segment's own text.
    assert_eq!(body["response_format"], "pcm");
    assert_eq!(body["input"], "The commander casts Lightning Bolt.");
    assert_eq!(body["model"], "tts-1");
    assert!(body.get("stream_format").is_none());
    assert!(body.get("speed").is_none() && body.get("instructions").is_none());
}

#[test]
fn sse_delta_and_done_decode_with_usage() {
    let mut audio = Vec::new();
    let mut usage = None;
    for line in STREAM_SSE.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        match decode_sse_data("OpenAI", data).unwrap() {
            Some(SseEvent::Audio(bytes)) => audio.extend(bytes),
            Some(SseEvent::Done(reported)) => usage = reported,
            None => {}
        }
    }
    assert_eq!(audio, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(
        usage,
        Some(SpeechUsage {
            input_tokens: 14,
            output_tokens: 101,
            total_tokens: Some(115)
        })
    );
    assert_eq!(decode_sse_data("OpenAI", " [DONE]").unwrap(), None);
    assert!(matches!(
        decode_sse_data("OpenAI", r#"{"type":"speech.audio.delta","audio":"***"}"#),
        Err(SpeechWireError::InvalidResponse(_))
    ));
}

#[test]
fn error_bodies_map_by_status_and_keep_it_in_the_message() {
    let bad = classify_error("OpenAI", 400, ERROR_400);
    assert!(
        matches!(&bad, SpeechWireError::InvalidRequest(m) if m.starts_with("OpenAI speech (HTTP 400, invalid_value): Invalid value")),
        "{bad:?}"
    );
    assert!(matches!(
        classify_error("OpenAI", 401, ERROR_401),
        SpeechWireError::Auth(_)
    ));
    let rate = classify_error("OpenAI", 429, ERROR_429_RATE);
    assert!(
        matches!(&rate, SpeechWireError::RateLimited(m) if m.contains("HTTP 429")),
        "{rate:?}"
    );
    let quota = classify_error("OpenAI", 429, ERROR_429_QUOTA);
    assert!(
        matches!(&quota, SpeechWireError::QuotaExceeded(m) if m.contains("HTTP 429")),
        "{quota:?}"
    );
    assert!(matches!(
        classify_error("OpenAI", 500, ERROR_500),
        SpeechWireError::Upstream { status: 500, .. }
    ));
    assert!(matches!(
        classify_error("OpenAI", 400, ERROR_REFUSAL),
        SpeechWireError::Refused(_)
    ));
    // A body that is not the documented shape is quoted as it came.
    let plain = classify_error("Kokoro", 502, "Bad Gateway");
    assert!(
        matches!(&plain, SpeechWireError::Upstream { status: 502, message } if message == "Kokoro speech (HTTP 502): Bad Gateway"),
        "{plain:?}"
    );
}

/// Read one HTTP request and return its JSON body.
fn read_request(stream: &mut TcpStream) -> (String, Value) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut head = String::new();
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().unwrap();
        }
        head.push_str(&line);
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).unwrap();
    (head, serde_json::from_slice(&body).unwrap())
}

fn chunk(stream: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    write!(stream, "{:x}\r\n", bytes.len())?;
    stream.write_all(bytes)?;
    stream.write_all(b"\r\n")?;
    stream.flush()
}

fn endpoint(address: std::net::SocketAddr) -> OpenAiSpeechEndpoint {
    OpenAiSpeechEndpoint {
        provider_name: "Kokoro".into(),
        url: format!("http://{address}/v1/audio/speech"),
        headers: vec![("authorization".into(), "Bearer local".into())],
    }
}

#[test]
fn a_raw_chunked_stream_is_read_as_it_arrives() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (seen, request) = mpsc::channel();
    let (go, release) = mpsc::channel::<()>();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        seen.send(read_request(&mut stream)).unwrap();
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: audio/pcm\r\ntransfer-encoding: chunked\r\n\r\n",
            )
            .unwrap();
        chunk(&mut stream, &[1, 2, 3]).unwrap();
        // The second chunk waits until the first has been read.
        release.recv_timeout(Duration::from_secs(5)).unwrap();
        chunk(&mut stream, &[4, 5]).unwrap();
        stream.write_all(b"0\r\n\r\n").unwrap();
    });
    let cancel = Arc::new(AtomicBool::new(false));
    let body = build_body(&tts1_body());
    let mut clip = post_clip(
        &endpoint(address),
        &body,
        StreamFormat::Audio,
        cancel,
        Duration::from_secs(5),
    )
    .unwrap();
    let (head, sent) = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(head.starts_with("POST /v1/audio/speech"), "{head}");
    assert!(
        head.to_ascii_lowercase()
            .contains("authorization: bearer local"),
        "{head}"
    );
    assert_eq!(sent, body);
    assert_eq!(clip.next().unwrap().unwrap(), vec![1, 2, 3]);
    go.send(()).unwrap();
    let rest: Vec<u8> = clip.by_ref().flat_map(Result::unwrap).collect();
    assert_eq!(rest, vec![4, 5]);
    assert_eq!(clip.usage(), None);
}

#[test]
fn an_sse_stream_yields_audio_then_usage() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n")
            .unwrap();
        chunk(&mut stream, STREAM_SSE.as_bytes()).unwrap();
        stream.write_all(b"0\r\n\r\n").unwrap();
    });
    let mut clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Sse,
        Arc::new(AtomicBool::new(false)),
        Duration::from_secs(5),
    )
    .unwrap();
    let audio: Vec<u8> = clip.by_ref().flat_map(Result::unwrap).collect();
    assert_eq!(audio, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(clip.usage().map(|usage| usage.output_tokens), Some(101));
}

#[test]
fn an_error_status_is_classified_as_the_streams_first_item() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        let body = ERROR_429_QUOTA.trim();
        let _ = write!(
            stream,
            "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
    });
    let mut clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Audio,
        Arc::new(AtomicBool::new(false)),
        Duration::from_secs(5),
    )
    .unwrap();
    let error = clip.next().unwrap().unwrap_err();
    assert!(
        matches!(error, SpeechWireError::QuotaExceeded(_)),
        "{error:?}"
    );
    assert!(clip.next().is_none());
}

/// A server that answers with `head` (nothing at all when empty), then sends
/// nothing more and reports when it sees the client close the connection.
fn silent_server(head: &'static [u8]) -> (std::net::SocketAddr, mpsc::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (closed, saw_closed) = mpsc::channel();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        if !head.is_empty() {
            stream.write_all(head).unwrap();
            stream.flush().unwrap();
        }
        // Block until the client goes away: a read returns 0 or fails then.
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut byte = [0u8; 1];
        if matches!(stream.read(&mut byte), Ok(0) | Err(_)) {
            let _ = closed.send(());
        }
    });
    (address, saw_closed)
}

/// The stall bound the silent-server tests run with: short, so the bound a
/// silent connection is held to can be observed.
const BOUND: Duration = Duration::from_millis(500);
/// Scheduling slack on top of [`BOUND`] before the server must see the close.
const SLACK: Duration = Duration::from_millis(1500);

/// The cancellation contract against a silent server: the caller is released
/// at once, and the connection is closed within the stall bound.
fn released_at_once_closed_within_the_bound(
    mut clip: ClipStream,
    cancel: Arc<AtomicBool>,
    saw_closed: mpsc::Receiver<()>,
) {
    let waiting = thread::spawn(move || {
        let ended = clip.next().is_none();
        (ended, Instant::now())
    });
    thread::sleep(Duration::from_millis(150));
    let cancelled = Instant::now();
    cancel.store(true, Ordering::SeqCst);
    let (ended, returned) = waiting.join().unwrap();
    assert!(ended, "a cancelled clip ends");
    assert!(
        returned.duration_since(cancelled) < Duration::from_millis(100),
        "the caller waited {:?} on the network",
        returned.duration_since(cancelled)
    );
    saw_closed
        .recv_timeout(BOUND + SLACK)
        .expect("the server saw the connection close within the stall bound");
}

#[test]
fn a_cancel_before_the_headers_releases_the_caller_and_closes_within_the_bound() {
    let (address, saw_closed) = silent_server(b"");
    let cancel = Arc::new(AtomicBool::new(false));
    let clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Audio,
        cancel.clone(),
        BOUND,
    )
    .unwrap();
    released_at_once_closed_within_the_bound(clip, cancel, saw_closed);
}

#[test]
fn a_cancel_while_the_body_is_silent_releases_the_caller_and_closes_within_the_bound() {
    let (address, saw_closed) = silent_server(
        b"HTTP/1.1 200 OK\r\ncontent-type: audio/pcm\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n",
    );
    let cancel = Arc::new(AtomicBool::new(false));
    let mut clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Audio,
        cancel.clone(),
        BOUND,
    )
    .unwrap();
    assert_eq!(clip.next().unwrap().unwrap(), b"abc");
    released_at_once_closed_within_the_bound(clip, cancel, saw_closed);
}

#[test]
fn dropping_a_silent_clip_closes_within_the_bound() {
    let (address, saw_closed) = silent_server(b"");
    let clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Sse,
        Arc::new(AtomicBool::new(false)),
        BOUND,
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let dropped = Instant::now();
    drop(clip);
    assert!(
        dropped.elapsed() < Duration::from_millis(50),
        "drop never waits"
    );
    saw_closed
        .recv_timeout(BOUND + SLACK)
        .expect("the server saw the connection close within the stall bound");
}

/// A server that takes a TLS ClientHello and never answers it, reporting when
/// the client closes the connection.
fn silent_tls_server() -> (OpenAiSpeechEndpoint, mpsc::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (closed, saw_closed) = mpsc::channel();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut hello = [0u8; 4096];
        // The ClientHello arrives; nothing is ever sent back.
        assert!(stream.read(&mut hello).unwrap() > 0, "a ClientHello");
        loop {
            match stream.read(&mut hello) {
                Ok(0) | Err(_) => {
                    let _ = closed.send(());
                    return;
                }
                Ok(_) => continue,
            }
        }
    });
    let endpoint = OpenAiSpeechEndpoint {
        provider_name: "Silent TLS".into(),
        url: format!("https://{address}/v1/audio/speech"),
        headers: vec![],
    };
    (endpoint, saw_closed)
}

#[test]
fn a_cancel_during_the_tls_handshake_releases_the_caller_and_closes_within_the_bound() {
    let (endpoint, saw_closed) = silent_tls_server();
    let cancel = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let clip = post_clip(
        &endpoint,
        &json!({}),
        StreamFormat::Audio,
        cancel.clone(),
        BOUND,
    )
    .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "post_clip does not wait on the server"
    );
    released_at_once_closed_within_the_bound(clip, cancel, saw_closed);
}

#[test]
fn dropping_a_clip_during_the_tls_handshake_closes_within_the_bound() {
    let (endpoint, saw_closed) = silent_tls_server();
    let clip = post_clip(
        &endpoint,
        &json!({}),
        StreamFormat::Audio,
        Arc::new(AtomicBool::new(false)),
        BOUND,
    )
    .unwrap();
    thread::sleep(Duration::from_millis(150));
    drop(clip);
    saw_closed
        .recv_timeout(BOUND + SLACK)
        .expect("the server saw the connection close within the stall bound");
}

/// A stalled clip fails with a transport error: the stream's own stall
/// deadline or the worker's per-operation timeout, whichever fires first.
fn assert_stalled(error: &SpeechWireError) {
    assert!(
        matches!(error, SpeechWireError::Transport(m) if m.contains("stalled") || m.contains("timed out")),
        "{error:?}"
    );
}

#[test]
fn the_stall_bound_covers_the_tls_handshake() {
    let (endpoint, saw_closed) = silent_tls_server();
    let started = Instant::now();
    let mut clip = post_clip(
        &endpoint,
        &json!({}),
        StreamFormat::Audio,
        Arc::new(AtomicBool::new(false)),
        BOUND,
    )
    .unwrap();
    assert_stalled(&clip.next().unwrap().unwrap_err());
    assert!(started.elapsed() < BOUND + SLACK);
    saw_closed
        .recv_timeout(BOUND + SLACK)
        .expect("the server saw the connection close within the stall bound");
}

#[test]
fn a_stalled_clip_reports_it_and_closes_within_the_bound() {
    let (address, saw_closed) = silent_server(b"");
    let mut clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Audio,
        Arc::new(AtomicBool::new(false)),
        BOUND,
    )
    .unwrap();
    assert_stalled(&clip.next().unwrap().unwrap_err());
    saw_closed
        .recv_timeout(BOUND + SLACK)
        .expect("the server saw the connection close within the stall bound");
}

#[test]
fn a_steady_stream_longer_than_the_bound_is_not_cut_off() {
    // The bound is per operation: a clip that keeps streaming for several
    // times the bound reads to its end.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: audio/pcm\r\ntransfer-encoding: chunked\r\n\r\n",
            )
            .unwrap();
        for _ in 0..12 {
            chunk(&mut stream, &[7u8; 64]).unwrap();
            thread::sleep(Duration::from_millis(150));
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
    });
    let clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Audio,
        Arc::new(AtomicBool::new(false)),
        BOUND,
    )
    .unwrap();
    let audio: Vec<u8> = clip.flat_map(Result::unwrap).collect();
    assert_eq!(audio.len(), 12 * 64);
}

#[test]
fn a_cancel_mid_stream_closes_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (closed, saw_closed) = mpsc::channel();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: audio/pcm\r\ntransfer-encoding: chunked\r\n\r\n",
            )
            .unwrap();
        // Keep streaming until the client goes away.
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(10) {
            if chunk(&mut stream, &[0u8; 512]).is_err() {
                closed.send(started.elapsed()).unwrap();
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
    let cancel = Arc::new(AtomicBool::new(false));
    let mut clip = post_clip(
        &endpoint(address),
        &json!({}),
        StreamFormat::Audio,
        cancel.clone(),
        Duration::from_secs(5),
    )
    .unwrap();
    assert!(clip.next().unwrap().is_ok());
    cancel.store(true, Ordering::SeqCst);
    assert!(clip.next().is_none(), "a cancelled clip ends");
    drop(clip);
    let after = saw_closed
        .recv_timeout(Duration::from_secs(5))
        .expect("the server saw the connection close");
    assert!(after < Duration::from_secs(5), "{after:?}");
}
