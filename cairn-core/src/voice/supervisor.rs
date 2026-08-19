use super::{
    Segment, Transcript, VoiceError, VoiceResult, CHANNELS, CHUNK_ENCODING, PROTOCOL_VERSION,
    SAMPLE_RATE,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const TRANSCRIPTION_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_PROTOCOL_LINE_BYTES: usize = 8 * 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 16 * 1024;
const AUDIO_FEED_CAPACITY: usize = 8;

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ProtocolCommand {
    LoadModel {
        request_id: String,
        path: String,
    },
    TranscribeFile {
        request_id: String,
        path: String,
        options: Value,
    },
    BeginStream {
        request_id: String,
        stream_id: String,
        options: Value,
    },
    AudioChunk {
        request_id: String,
        stream_id: String,
        encoding: String,
        sample_rate: u32,
        channels: u32,
        data: String,
    },
    EndStream {
        request_id: String,
        stream_id: String,
    },
}

impl ProtocolCommand {
    pub fn audio_chunk(request_id: String, stream_id: String, data: String) -> Self {
        Self::AudioChunk {
            request_id,
            stream_id,
            encoding: CHUNK_ENCODING.into(),
            sample_rate: SAMPLE_RATE,
            channels: CHANNELS,
            data,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ProtocolEvent {
    Hello {
        protocol_version: u32,
        engine_version: String,
    },
    Ready {
        request_id: String,
        model: Value,
    },
    Accepted {
        request_id: String,
    },
    Progress {
        request_id: String,
        stage: String,
        progress: f32,
    },
    Segment {
        request_id: String,
        #[serde(default)]
        stream_id: Option<String>,
        index: u32,
        start_ms: i64,
        end_ms: i64,
        text: String,
    },
    Partial {
        request_id: String,
        stream_id: String,
        revision: u64,
        window_start_ms: i64,
        window_end_ms: i64,
        text: String,
    },
    Done {
        request_id: String,
        #[serde(default)]
        stream_id: Option<String>,
        text: String,
        segments: Vec<Segment>,
        audio_duration_ms: i64,
    },
    Error {
        #[serde(default)]
        request_id: Option<String>,
        code: String,
        message: String,
        #[serde(default)]
        details: Option<Value>,
    },
}

type EventSink = Arc<dyn Fn(ProtocolEvent) + Send + Sync>;

/// One verified child whose stdout is drained for its entire lifetime. Commands
/// subscribe before writing, so responses cannot race their waiter.
pub struct VoiceProcess {
    child: Arc<tokio::sync::Mutex<Child>>,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    events_tx: tokio::sync::broadcast::Sender<ProtocolEvent>,
    reader_task: JoinHandle<()>,
    stderr_task: JoinHandle<Vec<u8>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    audio_feed_admission: Arc<Semaphore>,
}

impl VoiceProcess {
    pub async fn spawn(
        executable: &Path,
        expected_version: impl Into<String>,
        events: impl Fn(ProtocolEvent) + Send + Sync + 'static,
    ) -> VoiceResult<Self> {
        let expected_version = expected_version.into();
        let mut command = Command::new(executable);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env_clear()
            .env("RUST_BACKTRACE", "0");
        #[cfg(unix)]
        unsafe {
            command.pre_exec(|| {
                nix::unistd::setsid()
                    .map(|_| ())
                    .map_err(std::io::Error::other)
            });
        }
        let mut child = command
            .spawn()
            .map_err(|e| VoiceError::Transport(format!("engine spawn failed: {e}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| VoiceError::Transport("engine stdin was not piped".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| VoiceError::Transport("engine stdout was not piped".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| VoiceError::Transport("engine stderr was not piped".into()))?;
        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut tail = Vec::new();
            loop {
                let mut chunk = vec![0; 1024];
                match tokio::io::AsyncReadExt::read(&mut reader, &mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        tail.extend_from_slice(&chunk[..n]);
                        if tail.len() > STDERR_TAIL_BYTES {
                            tail.drain(..tail.len() - STDERR_TAIL_BYTES);
                        }
                    }
                }
            }
            tail
        });
        let mut stdout = BufReader::new(stdout);
        let hello = tokio::time::timeout(READY_TIMEOUT, read_event(&mut stdout))
            .await
            .map_err(|_| {
                VoiceError::Transport("engine did not send hello before readiness timeout".into())
            })??;
        match hello {
            ProtocolEvent::Hello { protocol_version, engine_version }
                if protocol_version == PROTOCOL_VERSION && engine_version == expected_version => {}
            ProtocolEvent::Hello { protocol_version, engine_version } => return Err(VoiceError::Incompatible(format!(
                "expected protocol {PROTOCOL_VERSION} engine {expected_version}, got protocol {protocol_version} engine {engine_version}"
            ))),
            _ => return Err(VoiceError::Transport("engine's first event was not hello".into())),
        }
        let (events_tx, _) = tokio::sync::broadcast::channel(512);
        let reader_tx = events_tx.clone();
        let event_sink: EventSink = Arc::new(events);
        let reader_sink = event_sink.clone();
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let reader_running = running.clone();
        let reader_task = tokio::spawn(async move {
            let mut revisions = HashMap::<String, u64>::new();
            while let Ok(event) = read_event(&mut stdout).await {
                if let ProtocolEvent::Partial {
                    stream_id,
                    revision,
                    ..
                } = &event
                {
                    let previous = revisions.entry(stream_id.clone()).or_default();
                    if revision <= previous {
                        continue;
                    }
                    *previous = *revision;
                }
                let _ = reader_tx.send(event.clone());
                (reader_sink)(event);
            }
            reader_running.store(false, std::sync::atomic::Ordering::Release);
        });
        Ok(Self {
            child: Arc::new(tokio::sync::Mutex::new(child)),
            stdin: tokio::sync::Mutex::new(Some(stdin)),
            events_tx,
            reader_task,
            stderr_task,
            running,
            audio_feed_admission: Arc::new(Semaphore::new(AUDIO_FEED_CAPACITY)),
        })
    }

    pub async fn load_model(&self, request_id: String, model: &Path) -> VoiceResult<()> {
        let command = ProtocolCommand::LoadModel {
            request_id: request_id.clone(),
            path: model.to_string_lossy().into_owned(),
        };
        self.wait(&command, &request_id, REQUEST_TIMEOUT, |e| {
            matches!(e, ProtocolEvent::Ready { .. })
        })
        .await
        .map(|_| ())
    }

    pub async fn feed_audio(
        &self,
        request_id: String,
        stream_id: String,
        data: String,
    ) -> VoiceResult<()> {
        let _permit = self
            .audio_feed_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| VoiceError::Backpressure)?;
        self.command(
            &ProtocolCommand::audio_chunk(request_id.clone(), stream_id, data),
            &request_id,
        )
        .await
    }

    pub async fn request(
        &self,
        command: &ProtocolCommand,
        request_id: &str,
    ) -> VoiceResult<Transcript> {
        match self
            .wait(command, request_id, TRANSCRIPTION_IDLE_TIMEOUT, |e| {
                matches!(e, ProtocolEvent::Done { .. })
            })
            .await?
        {
            ProtocolEvent::Done {
                text,
                segments,
                audio_duration_ms,
                ..
            } => Ok(Transcript {
                text,
                segments,
                audio_duration_ms,
            }),
            _ => unreachable!(),
        }
    }

    pub async fn command(&self, command: &ProtocolCommand, request_id: &str) -> VoiceResult<()> {
        self.wait(command, request_id, REQUEST_TIMEOUT, |e| {
            matches!(e, ProtocolEvent::Accepted { .. })
        })
        .await
        .map(|_| ())
    }

    pub async fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Acquire)
            && matches!(self.child.lock().await.try_wait(), Ok(None))
    }

    async fn wait(
        &self,
        command: &ProtocolCommand,
        request_id: &str,
        timeout: Duration,
        done: impl Fn(&ProtocolEvent) -> bool,
    ) -> VoiceResult<ProtocolEvent> {
        let mut receiver = self.events_tx.subscribe();
        self.write(command).await?;
        loop {
            let event = tokio::time::timeout(timeout, receiver.recv())
                .await
                .map_err(|_| VoiceError::Transport("engine request timed out".into()))?
                .map_err(|e| {
                    VoiceError::Transport(format!("engine event dispatcher stopped: {e}"))
                })?;
            if event_request_id(&event) != Some(request_id) {
                continue;
            }
            match event {
                ProtocolEvent::Error { code, message, .. } => {
                    return Err(VoiceError::Request { code, message })
                }
                event if done(&event) => return Ok(event),
                _ => {}
            }
        }
    }

    async fn write(&self, command: &ProtocolCommand) -> VoiceResult<()> {
        if !self.is_running().await {
            return Err(VoiceError::Transport("voice engine is not running".into()));
        }
        let mut guard = self.stdin.lock().await;
        let stdin = guard
            .as_mut()
            .ok_or_else(|| VoiceError::Transport("engine stdin is closed".into()))?;
        let mut bytes =
            serde_json::to_vec(command).map_err(|e| VoiceError::Transport(e.to_string()))?;
        bytes.push(b'\n');
        stdin
            .write_all(&bytes)
            .await
            .map_err(|e| VoiceError::Transport(format!("writing engine command failed: {e}")))?;
        stdin
            .flush()
            .await
            .map_err(|e| VoiceError::Transport(format!("flushing engine command failed: {e}")))
    }

    pub async fn shutdown(self) -> VoiceResult<()> {
        self.shutdown_until(tokio::time::Instant::now() + SHUTDOWN_TIMEOUT)
            .await
    }

    pub async fn shutdown_until(self, deadline: tokio::time::Instant) -> VoiceResult<()> {
        let graceful_deadline = deadline
            .checked_sub(Duration::from_secs(1))
            .unwrap_or(deadline);
        let result = async {
            let mut stdin = tokio::time::timeout_at(graceful_deadline, self.stdin.lock())
                .await
                .map_err(|_| {
                    VoiceError::Transport("timed out acquiring engine stdin for shutdown".into())
                })?;
            drop(stdin.take());
            drop(stdin);
            let mut child = tokio::time::timeout_at(graceful_deadline, self.child.lock())
                .await
                .map_err(|_| {
                    VoiceError::Transport("timed out acquiring engine process for shutdown".into())
                })?;
            match tokio::time::timeout_at(graceful_deadline, child.wait()).await {
                Ok(Ok(status)) if status.success() => Ok(()),
                Ok(Ok(status)) => Err(VoiceError::Transport(format!(
                    "engine exited with {status}"
                ))),
                Ok(Err(e)) => Err(VoiceError::Transport(format!(
                    "waiting for engine failed: {e}"
                ))),
                Err(_) => {
                    terminate_process_tree(&mut child).await;
                    let _ = tokio::time::timeout_at(deadline, child.wait()).await;
                    Err(VoiceError::Transport(
                        "engine did not exit after stdin closed".into(),
                    ))
                }
            }
        }
        .await;
        self.reader_task.abort();
        self.stderr_task.abort();
        result
    }
}

fn event_request_id(event: &ProtocolEvent) -> Option<&str> {
    match event {
        ProtocolEvent::Ready { request_id, .. }
        | ProtocolEvent::Accepted { request_id }
        | ProtocolEvent::Progress { request_id, .. }
        | ProtocolEvent::Segment { request_id, .. }
        | ProtocolEvent::Partial { request_id, .. }
        | ProtocolEvent::Done { request_id, .. } => Some(request_id),
        ProtocolEvent::Error { request_id, .. } => request_id.as_deref(),
        ProtocolEvent::Hello { .. } => None,
    }
}

async fn read_event(stdout: &mut BufReader<ChildStdout>) -> VoiceResult<ProtocolEvent> {
    let mut line = Vec::new();
    loop {
        let buffer = stdout
            .fill_buf()
            .await
            .map_err(|e| VoiceError::Transport(format!("reading engine stdout failed: {e}")))?;
        if buffer.is_empty() {
            return Err(VoiceError::Transport(
                "engine stdout closed unexpectedly".into(),
            ));
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(buffer.len(), |i| i + 1);
        if line.len().saturating_add(take) > MAX_PROTOCOL_LINE_BYTES {
            return Err(VoiceError::Transport(
                "engine emitted an oversized protocol line".into(),
            ));
        }
        line.extend_from_slice(&buffer[..take]);
        stdout.consume(take);
        if newline.is_some() {
            break;
        }
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    serde_json::from_slice(&line)
        .map_err(|e| VoiceError::Transport(format!("engine emitted malformed protocol: {e}")))
}

#[cfg(unix)]
async fn terminate_process_tree(child: &mut Child) {
    if let Some(id) = child.id() {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(-(id as i32)),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}
#[cfg(not(unix))]
async fn terminate_process_tree(child: &mut Child) {
    let _ = child.kill().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    fn fake_engine(script_body: &str) -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("fake-voice-engine");
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf '%s\\n' '{{\"type\":\"hello\",\"protocolVersion\":{PROTOCOL_VERSION},\"engineVersion\":\"test\"}}'\n{script_body}\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }

    #[test]
    fn protocol_is_strict_and_rejects_unknown_fields() {
        let hello = format!(
            r#"{{"type":"hello","protocolVersion":{},"engineVersion":"1","extra":true}}"#,
            PROTOCOL_VERSION
        );
        assert!(serde_json::from_str::<ProtocolEvent>(&hello).is_err());
    }

    #[test]
    fn audio_command_pins_the_wire_format() {
        let value = serde_json::to_value(ProtocolCommand::audio_chunk(
            "r".into(),
            "s".into(),
            "AA==".into(),
        ))
        .unwrap();
        assert_eq!(value["encoding"], CHUNK_ENCODING);
        assert_eq!(value["sampleRate"], SAMPLE_RATE);
        assert_eq!(value["channels"], CHANNELS);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continuous_reader_delivers_partial_before_stream_finishes() {
        let engine = fake_engine(
            r#"while IFS= read -r command; do
  case "$command" in
    *'"type":"beginStream"'*)
      printf '%s\n' '{"type":"accepted","requestId":"begin"}'
      printf '%s\n' '{"type":"partial","requestId":"begin","streamId":"stream","revision":1,"windowStartMs":0,"windowEndMs":40,"text":"live"}'
      ;;
    *'"type":"endStream"'*)
      printf '%s\n' '{"type":"done","requestId":"end","streamId":"stream","text":"finished","segments":[],"audioDurationMs":40}'
      ;;
  esac
done"#,
        );
        let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
        let process = VoiceProcess::spawn(
            engine.path().join("fake-voice-engine").as_path(),
            "test",
            move |event| {
                let _ = events_tx.send(event);
            },
        )
        .await
        .unwrap();

        process
            .command(
                &ProtocolCommand::BeginStream {
                    request_id: "begin".into(),
                    stream_id: "stream".into(),
                    options: Value::Null,
                },
                "begin",
            )
            .await
            .unwrap();

        let partial = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let ProtocolEvent::Partial { text, .. } = events_rx.recv().await.unwrap() {
                    break text;
                }
            }
        })
        .await
        .expect("partial must arrive without waiting for endStream");
        assert_eq!(partial, "live");
        assert!(process.is_running().await);

        let transcript = process
            .request(
                &ProtocolCommand::EndStream {
                    request_id: "end".into(),
                    stream_id: "stream".into(),
                },
                "end",
            )
            .await
            .unwrap();
        assert_eq!(transcript.text, "finished");
        process.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn correlated_recoverable_error_leaves_process_usable() {
        let engine = fake_engine(
            r#"while IFS= read -r command; do
  case "$command" in
    *'"type":"audioChunk"'*)
      printf '%s\n' '{"type":"error","requestId":"chunk","code":"badAudio","message":"recoverable"}'
      ;;
    *'"requestId":"bad-begin"'*)
      printf '%s\n' '{"type":"error","requestId":"bad-begin","code":"badOptions","message":"recoverable begin error"}'
      ;;
    *'"requestId":"retry"'*)
      printf '%s\n' '{"type":"accepted","requestId":"retry"}'
      ;;
  esac
done"#,
        );
        let process = VoiceProcess::spawn(
            engine.path().join("fake-voice-engine").as_path(),
            "test",
            |_| {},
        )
        .await
        .unwrap();

        let error = process
            .command(
                &ProtocolCommand::audio_chunk("chunk".into(), "stream".into(), "AA==".into()),
                "chunk",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            VoiceError::Request { ref code, ref message }
                if code == "badAudio" && message == "recoverable"
        ));
        assert!(process.is_running().await);

        let begin_error = process
            .command(
                &ProtocolCommand::BeginStream {
                    request_id: "bad-begin".into(),
                    stream_id: "stream-2".into(),
                    options: Value::Null,
                },
                "bad-begin",
            )
            .await
            .unwrap_err();
        assert!(matches!(
            begin_error,
            VoiceError::Request { ref code, ref message }
                if code == "badOptions" && message == "recoverable begin error"
        ));
        assert!(process.is_running().await);

        process
            .command(
                &ProtocolCommand::BeginStream {
                    request_id: "retry".into(),
                    stream_id: "stream-2".into(),
                    options: Value::Null,
                },
                "retry",
            )
            .await
            .unwrap();
        assert!(process.is_running().await);
        process.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delayed_audio_acknowledgements_apply_bounded_backpressure() {
        let engine = fake_engine(
            r#"while IFS= read -r command; do
  case "$command" in
    *'"type":"audioChunk"'*)
      request_id=$(printf '%s' "$command" | sed -n 's/.*"requestId":"\([^"]*\)".*/\1/p')
      sleep 0.1
      printf '{"type":"accepted","requestId":"%s"}\n' "$request_id"
      ;;
  esac
done"#,
        );
        let process = VoiceProcess::spawn(
            engine.path().join("fake-voice-engine").as_path(),
            "test",
            |_| {},
        )
        .await
        .unwrap();

        let feeds = (0..=AUDIO_FEED_CAPACITY).map(|index| {
            process.feed_audio(format!("chunk-{index}"), "stream".into(), "AA==".into())
        });
        let results = futures_util::future::join_all(feeds).await;
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(VoiceError::Backpressure)))
                .count(),
            1
        );
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            AUDIO_FEED_CAPACITY
        );

        process.shutdown().await.unwrap();
    }
}
