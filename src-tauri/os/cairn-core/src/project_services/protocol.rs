//! The wire between the runner and a service process: NDJSON over the
//! resident process's own pipes. Runner frames go to stdin; service frames come
//! back on stdout; stderr is the service's log.
//!
//! A pipe is the transport because it is private to the process. There is no
//! listening endpoint for a page, another local process, or another machine to
//! reach, and the process's life is the pipe's life, so a crash fails every
//! in-flight call at once rather than leaving it to a timeout.

use cairn_common::identity::PrincipalRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub(crate) const PROTOCOL_VERSION: u32 = 1;
/// The longest line either side may send. A longer one is reported and
/// discarded rather than buffered.
pub(crate) const MAX_LINE_BYTES: usize = 1024 * 1024;
/// How many protocol violations within [`VIOLATION_WINDOW`] kill an
/// incarnation.
pub(crate) const VIOLATION_LIMIT: usize = 3;
pub(crate) const VIOLATION_WINDOW: Duration = Duration::from_secs(10);

/// Who a call is from, as data the service may use for its own further
/// decisions. None of it is a grant: the runner has already decided what the
/// call may do, and the authority it carries is [`CallAuthority`].
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CallerFrame {
    pub kind: CallerKind,
    /// The home whose authority the call was made in the context of: a page's
    /// owning thread, the calling agent's home, or for a scheduled call the
    /// service's owner (its thread, or its project). Attributed context, not
    /// proof of who acted.
    pub acting_for: String,
    /// For an agent call, the authenticated acting incarnation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<PrincipalRef>,
    /// For a page call, the runner-witnessed viewer classification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub viewer: Option<Value>,
    /// For a page call, the page and version that made it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument: Option<Instrument>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CallerKind {
    Page,
    Agent,
    /// The supervisor, calling a `schedule` entry on the owner's behalf.
    Schedule,
}

impl CallerKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CallerKind::Page => "page",
            CallerKind::Agent => "agent",
            CallerKind::Schedule => "schedule",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Instrument {
    pub page: String,
    pub version: i64,
}

/// The credential a call executes under: a bearer for a delegation run whose
/// authority is the service's intersected with the caller's. A call without
/// one can compute and answer, and every Cairn request it makes is refused.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CallAuthority {
    pub run_id: String,
    pub token: String,
}

/// Never prints the token.
impl std::fmt::Debug for CallAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallAuthority")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

/// Runner to service.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum RunnerFrame {
    Hello {
        protocol: u32,
        project: String,
        service: String,
        source: String,
    },
    #[serde(rename_all = "camelCase")]
    Call {
        id: String,
        method: String,
        params: Value,
        caller: Box<CallerFrame>,
        #[serde(skip_serializing_if = "Option::is_none")]
        authority: Option<CallAuthority>,
        /// Unix milliseconds.
        deadline_ms: u64,
    },
    Cancel {
        id: String,
    },
    Drain,
}

impl RunnerFrame {
    /// One line, newline-terminated.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut line = serde_json::to_vec(self).expect("runner frames always serialize");
        line.push(b'\n');
        line
    }
}

/// A service's error answer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct ServiceError {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

/// Service to runner.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServiceFrame {
    Ready {
        #[serde(default)]
        methods: Vec<String>,
    },
    Result {
        id: String,
        ok: bool,
        #[serde(default)]
        value: Option<Value>,
        #[serde(default)]
        error: Option<ServiceError>,
    },
    Drained,
}

impl ServiceFrame {
    pub(crate) fn decode(line: &[u8]) -> Result<Self, String> {
        let frame: ServiceFrame = serde_json::from_slice(line)
            .map_err(|error| format!("not a service frame ({error}): {}", preview(line, 200)))?;
        if let ServiceFrame::Result {
            ok: false,
            error: None,
            ..
        } = &frame
        {
            return Err("a failed result must carry an error".into());
        }
        Ok(frame)
    }
}

/// The start of `bytes` as text, for a log line that names what was wrong.
pub(crate) fn preview(bytes: &[u8], max: usize) -> String {
    let cut = bytes.len().min(max);
    let mut text = String::from_utf8_lossy(&bytes[..cut]).into_owned();
    if bytes.len() > max {
        text.push('…');
    }
    text
}

/// One line out of a byte stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Line {
    Complete(Vec<u8>),
    /// A line longer than the assembler's cap: its first `cap` bytes and how
    /// long it was in total. The rest was never buffered.
    Oversized {
        prefix: Vec<u8>,
        total: usize,
    },
}

/// Reassembles lines from the byte chunks a resident process's output events
/// carry, which split anywhere.
#[derive(Debug)]
pub(crate) struct LineAssembler {
    cap: usize,
    buffer: Vec<u8>,
    /// Bytes of the current line beyond `cap`, counted but not kept.
    overflow: usize,
}

impl LineAssembler {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            buffer: Vec::new(),
            overflow: 0,
        }
    }

    pub(crate) fn push(&mut self, mut chunk: &[u8]) -> Vec<Line> {
        let mut lines = Vec::new();
        while !chunk.is_empty() {
            let (piece, ended) = match chunk.iter().position(|b| *b == b'\n') {
                Some(at) => (&chunk[..at], true),
                None => (chunk, false),
            };
            let room = self.cap.saturating_sub(self.buffer.len());
            let kept = piece.len().min(room);
            self.buffer.extend_from_slice(&piece[..kept]);
            self.overflow += piece.len() - kept;
            if ended {
                lines.push(self.take());
                chunk = &chunk[piece.len() + 1..];
            } else {
                chunk = &[];
            }
        }
        lines
    }

    /// What is left when the stream ends without a final newline.
    pub(crate) fn finish(&mut self) -> Option<Line> {
        (!self.buffer.is_empty() || self.overflow > 0).then(|| self.take())
    }

    fn take(&mut self) -> Line {
        let mut buffer = std::mem::take(&mut self.buffer);
        if buffer.last() == Some(&b'\r') && self.overflow == 0 {
            buffer.pop();
        }
        let overflow = std::mem::take(&mut self.overflow);
        if overflow > 0 {
            Line::Oversized {
                total: buffer.len() + overflow,
                prefix: buffer,
            }
        } else {
            Line::Complete(buffer)
        }
    }
}

/// Counts protocol violations in a sliding window.
#[derive(Debug, Default)]
pub(crate) struct ViolationWindow {
    at: VecDeque<Instant>,
}

impl ViolationWindow {
    /// Record one violation; true when the incarnation must be killed.
    pub(crate) fn record(&mut self, now: Instant) -> bool {
        while self
            .at
            .front()
            .is_some_and(|first| now.duration_since(*first) > VIOLATION_WINDOW)
        {
            self.at.pop_front();
        }
        self.at.push_back(now);
        self.at.len() >= VIOLATION_LIMIT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_line_split_across_chunks_is_reassembled() {
        let mut lines = LineAssembler::new(64);
        assert!(lines.push(b"{\"type\":").is_empty());
        assert!(lines.push(b"\"drained\"").is_empty());
        assert_eq!(
            lines.push(b"}\r\n{\"type\":\"rea"),
            vec![Line::Complete(b"{\"type\":\"drained\"}".to_vec())]
        );
        assert_eq!(
            lines.push(b"dy\"}\n\n"),
            vec![
                Line::Complete(b"{\"type\":\"ready\"}".to_vec()),
                Line::Complete(Vec::new())
            ]
        );
        assert_eq!(lines.finish(), None);
    }

    #[test]
    fn an_oversized_line_is_capped_reported_and_followed_by_the_next() {
        let mut lines = LineAssembler::new(4);
        assert!(lines.push(b"abcdefgh").is_empty());
        assert_eq!(
            lines.push(b"ij\nok\n"),
            vec![
                Line::Oversized {
                    prefix: b"abcd".to_vec(),
                    total: 10
                },
                Line::Complete(b"ok".to_vec())
            ]
        );
        lines.push(b"tail");
        assert_eq!(lines.finish(), Some(Line::Complete(b"tail".to_vec())));
    }

    #[test]
    fn runner_frames_encode_as_one_line() {
        let call = RunnerFrame::Call {
            id: "c-1".into(),
            method: "echo".into(),
            params: json!({"hi": 1}),
            caller: Box::new(CallerFrame {
                kind: CallerKind::Agent,
                acting_for: "cairn://p/cairn/12/1/builder".into(),
                agent: Some(PrincipalRef::Agent {
                    node: "cairn://p/cairn/12/1/builder".into(),
                    run_id: Some("run-1".into()),
                }),
                viewer: None,
                instrument: None,
            }),
            authority: None,
            deadline_ms: 42,
        };
        let line = call.encode();
        assert_eq!(line.last(), Some(&b'\n'));
        assert_eq!(line.iter().filter(|b| **b == b'\n').count(), 1);
        let value: Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(value["type"], "call");
        assert_eq!(value["deadlineMs"], 42);
        assert_eq!(value["caller"]["actingFor"], "cairn://p/cairn/12/1/builder");
        assert_eq!(value["caller"]["agent"]["kind"], "agent");
        assert!(value.get("authority").is_none());
        assert_eq!(
            serde_json::from_slice::<Value>(&RunnerFrame::Drain.encode()).unwrap(),
            json!({"type": "drain"})
        );
    }

    #[test]
    fn a_call_authority_never_debug_prints_its_token() {
        let authority = CallAuthority {
            run_id: "run-d".into(),
            token: "secret-token-Zx91".into(),
        };
        assert!(!format!("{authority:?}").contains("secret-token"));
    }

    #[test]
    fn service_frames_decode_and_malformed_ones_are_violations() {
        assert_eq!(
            ServiceFrame::decode(br#"{"type":"ready","methods":["echo"]}"#).unwrap(),
            ServiceFrame::Ready {
                methods: vec!["echo".into()]
            }
        );
        assert!(matches!(
            ServiceFrame::decode(
                br#"{"type":"result","id":"c-2","ok":false,"error":{"message":"no","code":"bad"}}"#
            )
            .unwrap(),
            ServiceFrame::Result {
                ok: false,
                error: Some(ServiceError { code: Some(_), .. }),
                ..
            }
        ));
        for bad in [
            &b"hello world"[..],
            br#"{"type":"progress"}"#,
            br#"{"type":"result","id":"c-3","ok":false}"#,
        ] {
            assert!(ServiceFrame::decode(bad).is_err(), "{}", preview(bad, 40));
        }
    }

    #[test]
    fn three_violations_inside_the_window_kill() {
        let mut window = ViolationWindow::default();
        let start = Instant::now();
        assert!(!window.record(start));
        assert!(!window.record(start + Duration::from_secs(1)));
        // Both have aged out by the third, so the count starts over.
        assert!(!window.record(start + Duration::from_secs(12)));
        assert!(!window.record(start + Duration::from_secs(13)));
        assert!(window.record(start + Duration::from_secs(14)));
    }
}
