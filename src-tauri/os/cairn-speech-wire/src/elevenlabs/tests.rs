use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tungstenite::handshake::server::{Request, Response};
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::CloseFrame;
use tungstenite::Message;

use super::wire::{
    classify_error, close_message, decode_frame, flush_message, init_message, text_message, Source,
};
use super::*;
use crate::SpeechWireError;

const AUDIO_WITH_ALIGNMENT: &str = include_str!("testdata/audio_with_alignment.json");
const AUDIO_PLAIN: &str = include_str!("testdata/audio_plain.json");
const FINAL: &str = include_str!("testdata/final.json");
const ERROR_FRAME_QUOTA: &str = include_str!("testdata/error_frame_quota.json");
const HANDSHAKE_401: &str = include_str!("testdata/handshake_401.json");
const HANDSHAKE_FORMAT_403: &str = include_str!("testdata/handshake_format_403.json");
const HANDSHAKE_429: &str = include_str!("testdata/handshake_429.json");
const HANDSHAKE_VOICE_404: &str = include_str!("testdata/handshake_voice_404.json");

fn endpoint(base: &str) -> ElevenLabsEndpoint {
    ElevenLabsEndpoint {
        base: base.to_string(),
        api_key: "xi-secret".to_string(),
    }
}

#[test]
fn the_url_carries_voice_model_format_and_query_extras() {
    let extras = ElevenLabsExtras {
        language_code: Some("en".into()),
        apply_text_normalization: Some(TextNormalization::Off),
        seed: Some(7),
        auto_mode: Some(false),
        inactivity_timeout: Some(180),
        sync_alignment: Some(true),
        ..ElevenLabsExtras::default()
    };
    let open = ElevenLabsOpen {
        voice_id: "JBFqnCBsd6RMkjVDRZzb",
        model: "eleven_flash_v2_5",
        output_format: "pcm_24000",
        speed: None,
        extras: &extras,
    };
    assert_eq!(
        endpoint("wss://api.elevenlabs.io/").url(&open),
        "wss://api.elevenlabs.io/v1/text-to-speech/JBFqnCBsd6RMkjVDRZzb/stream-input\
         ?model_id=eleven_flash_v2_5&output_format=pcm_24000&inactivity_timeout=180\
         &sync_alignment=true&language_code=en&apply_text_normalization=off&seed=7\
         &auto_mode=false"
    );
    let bare = ElevenLabsExtras::default();
    assert_eq!(
        endpoint(DEFAULT_BASE).url(&ElevenLabsOpen {
            extras: &bare,
            voice_id: "a b",
            ..open
        }),
        "wss://api.elevenlabs.io/v1/text-to-speech/a%20b/stream-input?model_id=eleven_flash_v2_5&output_format=pcm_24000"
    );
}

#[test]
fn the_key_is_never_printed() {
    let printed = format!("{:?}", endpoint(DEFAULT_BASE));
    assert!(!printed.contains("xi-secret"), "{printed}");
}

#[test]
fn messages_match_the_documented_shapes() {
    let extras = ElevenLabsExtras {
        voice_settings: Some(VoiceSettings {
            stability: Some(0.5),
            similarity_boost: Some(0.75),
            style: None,
            use_speaker_boost: Some(true),
        }),
        chunk_length_schedule: Some(vec![50, 120, 160, 250]),
        ..ElevenLabsExtras::default()
    };
    assert_eq!(
        init_message(&extras, Some(1.1)),
        json!({
            "text": " ",
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.75,
                "use_speaker_boost": true,
                "speed": 1.100000023841858
            },
            "generation_config": {"chunk_length_schedule": [50, 120, 160, 250]}
        })
    );
    // Nothing set: only the opening space.
    assert_eq!(
        init_message(&ElevenLabsExtras::default(), None),
        json!({"text": " "})
    );
    assert_eq!(
        text_message("Hello there "),
        json!({"text": "Hello there "})
    );
    assert_eq!(
        flush_message("done. "),
        json!({"text": "done. ", "flush": true})
    );
    // An empty flush must not read as the close message.
    assert_eq!(flush_message(""), json!({"text": " ", "flush": true}));
    assert_eq!(close_message(), json!({"text": ""}));
}

#[test]
fn an_audio_frame_decodes_from_base64_with_and_without_alignment() {
    let Some(ServerEvent::Audio {
        audio,
        alignment,
        normalized_alignment,
    }) = decode_frame(AUDIO_WITH_ALIGNMENT, "pcm_24000").unwrap()
    else {
        panic!("expected audio");
    };
    assert_eq!(audio, vec![0x00, 0x01, 0x02, 0xff]);
    let alignment = alignment.expect("alignment");
    assert_eq!(alignment.chars, vec!["H", "i", " "]);
    assert_eq!(alignment.char_start_times_ms, vec![0, 70, 140]);
    assert_eq!(alignment.char_durations_ms, vec![70, 70, 46]);
    assert!(normalized_alignment.is_some());

    assert_eq!(
        decode_frame(AUDIO_PLAIN, "mp3_44100_128").unwrap(),
        Some(ServerEvent::Audio {
            audio: vec![0xff, 0xfe, 0xfd],
            alignment: None,
            normalized_alignment: None,
        })
    );
    assert_eq!(
        decode_frame(FINAL, "pcm_24000").unwrap(),
        Some(ServerEvent::Final)
    );
    // The snake_case spelling the reference's prose example shows is read too.
    let snake = r#"{"audio":"AA==","is_final":false,"alignment":{"chars":["a"],"char_start_times_ms":[3],"char_durations_ms":[4]}}"#;
    let Some(ServerEvent::Audio { alignment, .. }) = decode_frame(snake, "pcm_24000").unwrap()
    else {
        panic!("expected audio");
    };
    assert_eq!(alignment.unwrap().char_start_times_ms, vec![3]);
    // A frame carrying nothing is not an event.
    assert_eq!(decode_frame(r#"{"audio":""}"#, "pcm_24000").unwrap(), None);
    assert!(matches!(
        decode_frame("not json", "pcm_24000"),
        Err(SpeechWireError::InvalidResponse(_))
    ));
    assert!(matches!(
        decode_frame(r#"{"audio":"***"}"#, "pcm_24000"),
        Err(SpeechWireError::InvalidResponse(_))
    ));
}

#[test]
fn errors_map_from_their_documented_type_and_code() {
    let auth = classify_error(Source::Http(401), HANDSHAKE_401, "pcm_24000");
    assert!(
        matches!(&auth, SpeechWireError::Auth(m) if m.contains("HTTP 401, invalid_api_key") && m.contains("Invalid API key")),
        "{auth:?}"
    );
    let rate = classify_error(Source::Http(429), HANDSHAKE_429, "pcm_24000");
    assert!(matches!(rate, SpeechWireError::RateLimited(_)), "{rate:?}");
    let voice = classify_error(Source::Http(404), HANDSHAKE_VOICE_404, "pcm_24000");
    assert!(
        matches!(&voice, SpeechWireError::InvalidRequest(m) if m.contains("voice_not_found")),
        "{voice:?}"
    );
    let quota = classify_error(Source::Frame, ERROR_FRAME_QUOTA, "pcm_24000");
    assert!(
        matches!(quota, SpeechWireError::QuotaExceeded(_)),
        "{quota:?}"
    );
    let server = classify_error(Source::Http(503), "upstream connect error", "pcm_24000");
    assert!(
        matches!(&server, SpeechWireError::Upstream { status: 503, message } if message.contains("upstream connect error")),
        "{server:?}"
    );
    // A close frame with an unrecognised reason keeps its close code.
    let closed = classify_error(Source::Close(1011), "internal", "pcm_24000");
    assert!(
        matches!(closed, SpeechWireError::Upstream { status: 1011, .. }),
        "{closed:?}"
    );
    // A close reason that is itself the JSON error frame classifies the same.
    let quota = classify_error(Source::Close(1008), ERROR_FRAME_QUOTA, "pcm_24000");
    assert!(
        matches!(quota, SpeechWireError::QuotaExceeded(_)),
        "{quota:?}"
    );
}

#[test]
fn a_plan_gated_format_refusal_names_the_format_and_the_plan() {
    let gated = classify_error(Source::Http(403), HANDSHAKE_FORMAT_403, "pcm_44100");
    let SpeechWireError::FormatNotOnPlan { format, message } = &gated else {
        panic!("expected a plan refusal, got {gated:?}");
    };
    assert_eq!(format, "pcm_44100");
    assert!(message.contains("output format pcm_44100"), "{message}");
    assert!(message.contains("plan may not include it"), "{message}");
    assert!(message.contains("does not switch formats"), "{message}");
    // The documented validation code for a format is the same refusal.
    let invalid = classify_error(
        Source::Http(400),
        r#"{"detail":{"type":"validation_error","code":"invalid_output_format","message":"The requested output format is not supported."}}"#,
        "mp3_44100_192",
    );
    assert!(matches!(
        invalid,
        SpeechWireError::FormatNotOnPlan { ref format, .. } if format == "mp3_44100_192"
    ));
}

/// A one-connection stream-input server: records the handshake, then hands
/// the socket to `script`.
fn serve<F>(script: F) -> (String, mpsc::Receiver<(String, Option<String>)>)
where
    F: FnOnce(&mut tungstenite::WebSocket<std::net::TcpStream>) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("ws://{}", listener.local_addr().unwrap());
    let (seen, handshake) = mpsc::channel();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut socket =
            tungstenite::accept_hdr(stream, |request: &Request, response: Response| {
                let key = request
                    .headers()
                    .get("xi-api-key")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                seen.send((request.uri().to_string(), key)).unwrap();
                Ok(response)
            })
            .unwrap();
        script(&mut socket);
    });
    (base, handshake)
}

fn read_json(socket: &mut tungstenite::WebSocket<std::net::TcpStream>) -> Value {
    loop {
        match socket.read().unwrap() {
            Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}

#[test]
fn a_live_socket_sends_the_key_in_the_handshake_and_streams_audio_back() {
    let (sent, received) = mpsc::channel();
    let (base, handshake) = serve(move |socket| {
        for _ in 0..3 {
            let message = read_json(socket);
            sent.send(message).unwrap();
        }
        socket
            .send(Message::text(AUDIO_WITH_ALIGNMENT.trim()))
            .unwrap();
        let close = read_json(socket);
        sent.send(close).unwrap();
        socket.send(Message::text(FINAL.trim())).unwrap();
        let _ = socket.close(None);
        let _ = socket.flush();
    });
    let extras = ElevenLabsExtras {
        inactivity_timeout: Some(180),
        ..ElevenLabsExtras::default()
    };
    let mut socket = ElevenLabsSocket::connect(
        &endpoint(&base),
        &ElevenLabsOpen {
            voice_id: "voice1",
            model: "eleven_flash_v2_5",
            output_format: "pcm_16000",
            speed: Some(0.9),
            extras: &extras,
        },
        Duration::from_secs(5),
    )
    .unwrap();
    let (uri, key) = handshake.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        uri,
        "/v1/text-to-speech/voice1/stream-input?model_id=eleven_flash_v2_5&output_format=pcm_16000&inactivity_timeout=180"
    );
    assert_eq!(key.as_deref(), Some("xi-secret"));

    socket.send_text("Hello ").unwrap();
    socket.flush("").unwrap();
    let init = received.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(init["text"], " ");
    assert!(init.get("xi_api_key").is_none() && init.get("xi-api-key").is_none());
    assert!((init["voice_settings"]["speed"].as_f64().unwrap() - 0.9).abs() < 1e-6);
    assert_eq!(
        received.recv_timeout(Duration::from_secs(5)).unwrap(),
        json!({"text": "Hello "})
    );
    assert_eq!(
        received.recv_timeout(Duration::from_secs(5)).unwrap(),
        json!({"text": " ", "flush": true})
    );

    let audio = loop {
        if let Some(event) = socket.recv(Duration::from_millis(50)).unwrap() {
            break event;
        }
    };
    assert!(matches!(audio, ServerEvent::Audio { ref audio, .. } if audio == &[0, 1, 2, 255]));
    socket.close_input().unwrap();
    assert_eq!(
        received.recv_timeout(Duration::from_secs(5)).unwrap(),
        json!({"text": ""})
    );
    let last = loop {
        if let Some(event) = socket.recv(Duration::from_millis(50)).unwrap() {
            break event;
        }
    };
    assert_eq!(last, ServerEvent::Final);
}

#[test]
fn a_receive_with_nothing_to_read_times_out_quietly() {
    let (hold, release) = mpsc::channel::<()>();
    let (base, _handshake) = serve(move |socket| {
        let _ = read_json(socket);
        let _ = release.recv_timeout(Duration::from_secs(5));
    });
    let extras = ElevenLabsExtras::default();
    let mut socket = ElevenLabsSocket::connect(
        &endpoint(&base),
        &ElevenLabsOpen {
            voice_id: "v",
            model: "m",
            output_format: "pcm_24000",
            speed: None,
            extras: &extras,
        },
        Duration::from_secs(5),
    )
    .unwrap();
    let started = Instant::now();
    assert_eq!(socket.recv(Duration::from_millis(30)).unwrap(), None);
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(hold);
}

#[test]
fn dropping_the_socket_sends_a_close_frame() {
    let (closed, saw_close) = mpsc::channel();
    let (base, _handshake) = serve(move |socket| {
        let _ = read_json(socket);
        loop {
            match socket.read() {
                Ok(Message::Close(_)) => {
                    closed.send(true).unwrap();
                    return;
                }
                Ok(_) => continue,
                Err(_) => {
                    closed.send(false).unwrap();
                    return;
                }
            }
        }
    });
    let extras = ElevenLabsExtras::default();
    let socket = ElevenLabsSocket::connect(
        &endpoint(&base),
        &ElevenLabsOpen {
            voice_id: "v",
            model: "m",
            output_format: "pcm_24000",
            speed: None,
            extras: &extras,
        },
        Duration::from_secs(5),
    )
    .unwrap();
    let started = Instant::now();
    drop(socket);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "drop must not wait"
    );
    assert_eq!(saw_close.recv_timeout(Duration::from_secs(5)), Ok(true));
}

#[test]
fn a_close_frame_with_a_reason_is_classified() {
    let (base, _handshake) = serve(move |socket| {
        let _ = read_json(socket);
        // A close reason is at most 123 bytes, so ElevenLabs' are short text.
        let _ = socket.close(Some(CloseFrame {
            code: CloseCode::Policy,
            reason: "This request exceeds your quota.".into(),
        }));
        let _ = socket.flush();
        thread::sleep(Duration::from_millis(200));
    });
    let extras = ElevenLabsExtras::default();
    let mut socket = ElevenLabsSocket::connect(
        &endpoint(&base),
        &ElevenLabsOpen {
            voice_id: "v",
            model: "m",
            output_format: "pcm_24000",
            speed: None,
            extras: &extras,
        },
        Duration::from_secs(5),
    )
    .unwrap();
    let error = loop {
        match socket.recv(Duration::from_millis(50)) {
            Ok(None) => continue,
            Ok(Some(event)) => panic!("expected an error, got {event:?}"),
            Err(error) => break error,
        }
    };
    assert!(
        matches!(error, SpeechWireError::QuotaExceeded(_)),
        "{error:?}"
    );
}

#[test]
fn a_refused_handshake_is_classified_from_its_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("ws://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0u8; 4096];
        let _ = stream.read(&mut buffer);
        let body = HANDSHAKE_401.trim();
        let response = format!(
            "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
    });
    let extras = ElevenLabsExtras::default();
    let error = ElevenLabsSocket::connect(
        &endpoint(&base),
        &ElevenLabsOpen {
            voice_id: "v",
            model: "m",
            output_format: "pcm_24000",
            speed: None,
            extras: &extras,
        },
        Duration::from_secs(5),
    )
    .unwrap_err();
    assert!(
        matches!(&error, SpeechWireError::Auth(m) if m.contains("invalid_api_key")),
        "{error:?}"
    );
}

#[test]
fn a_refused_handshake_without_a_body_still_classifies_by_status() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("ws://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0u8; 4096];
        let _ = stream.read(&mut buffer);
        let _ = stream.write_all(b"HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\n\r\n");
    });
    let extras = ElevenLabsExtras::default();
    let error = ElevenLabsSocket::connect(
        &endpoint(&base),
        &ElevenLabsOpen {
            voice_id: "v",
            model: "m",
            output_format: "pcm_24000",
            speed: None,
            extras: &extras,
        },
        Duration::from_secs(5),
    )
    .unwrap_err();
    assert_eq!(
        error,
        SpeechWireError::RateLimited("ElevenLabs (HTTP 429): Too Many Requests".into())
    );
}
