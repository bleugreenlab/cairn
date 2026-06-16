//! Stateful OSC 133 semantic-prompt marker parser for interactive PTYs.
//!
//! Shell integration scripts emit OSC 133 markers (`ESC ] 133 ; <field> ST`)
//! around the prompt and command execution. This parser scans a PTY output
//! stream, strips those markers so they never reach the frontend terminal, and
//! yields semantic events that drive the per-session command-busy signal.
//!
//! Only the `C` (command start) and `D` (command end, with exit code) markers
//! produce events. The `A` (prompt start) marker is passed through unchanged so
//! it reaches xterm, where the frontend places a buffer marker at the exact
//! prompt row for prompt-aware scrollback navigation; `B` and any unrecognized
//! field are stripped silently. A marker split across two reads is held back and
//! completed on the next `feed`.

/// A semantic event extracted from the OSC 133 stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Osc133Event {
    /// `C` — command execution begins (output starts). Busy → true.
    CommandStart,
    /// `D;<exit>` — command finished. Busy → false, carrying the exit status.
    CommandEnd { exit: i32 },
}

/// The fixed prefix of every OSC 133 marker: `ESC ] 133 ;`.
const PREFIX: &[u8] = b"\x1b]133;";

/// What to do with a fully-parsed OSC 133 marker.
enum MarkerAction {
    /// Strip the marker bytes from the output; optionally emit a semantic event.
    Strip(Option<Osc133Event>),
    /// Keep the marker bytes in the output. Used for `A` (prompt start) so the
    /// frontend's OSC 133 handler can anchor a buffer marker at the prompt row.
    Passthrough,
}

enum ParseResult {
    /// A complete marker: consume `consumed` bytes and apply `action`.
    Match {
        consumed: usize,
        action: MarkerAction,
    },
    /// A 133-marker prefix that may complete on the next chunk — hold it back.
    Incomplete,
    /// Not an OSC 133 marker; the leading ESC passes through unchanged.
    NotOsc133,
}

/// Stateful scanner that strips OSC 133 markers and yields semantic events.
#[derive(Default)]
pub struct Osc133Parser {
    /// A partial escape sequence carried over from the end of the previous chunk.
    pending: String,
}

impl Osc133Parser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a chunk of PTY output; return the marker-stripped text plus any
    /// semantic events in order. A marker split across this and the next chunk
    /// is held back internally and emitted once it completes.
    pub fn feed(&mut self, data: &str) -> (String, Vec<Osc133Event>) {
        let combined = if self.pending.is_empty() {
            data.to_string()
        } else {
            let mut s = std::mem::take(&mut self.pending);
            s.push_str(data);
            s
        };
        let bytes = combined.as_bytes();
        let mut out = String::with_capacity(bytes.len());
        let mut events = Vec::new();
        let mut last = 0;
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] != 0x1b {
                i += 1;
                continue;
            }
            match parse_osc133(&bytes[i..]) {
                ParseResult::Match { consumed, action } => match action {
                    MarkerAction::Strip(event) => {
                        out.push_str(&combined[last..i]);
                        if let Some(ev) = event {
                            events.push(ev);
                        }
                        i += consumed;
                        last = i;
                    }
                    MarkerAction::Passthrough => {
                        // Keep the marker bytes in the output: advance past it
                        // without flushing so the unflushed region stays
                        // contiguous and the marker survives into `out`.
                        i += consumed;
                    }
                },
                ParseResult::Incomplete => {
                    out.push_str(&combined[last..i]);
                    self.pending = combined[i..].to_string();
                    return (out, events);
                }
                ParseResult::NotOsc133 => {
                    // The ESC is ordinary output; leave it in place and keep scanning.
                    i += 1;
                }
            }
        }
        out.push_str(&combined[last..]);
        (out, events)
    }
}

/// Try to parse an OSC 133 marker at the start of `slice` (which begins with ESC).
fn parse_osc133(slice: &[u8]) -> ParseResult {
    let n = slice.len().min(PREFIX.len());
    if slice[..n] != PREFIX[..n] {
        return ParseResult::NotOsc133;
    }
    if n < PREFIX.len() {
        // Matches the 133 prefix so far but is truncated — may complete next chunk.
        return ParseResult::Incomplete;
    }
    let mut j = PREFIX.len();
    while j < slice.len() {
        match slice[j] {
            0x07 => {
                // BEL terminator.
                return ParseResult::Match {
                    consumed: j + 1,
                    action: classify(&slice[PREFIX.len()..j]),
                };
            }
            0x1b => {
                // Possible ST terminator (ESC \).
                match slice.get(j + 1) {
                    Some(b'\\') => {
                        return ParseResult::Match {
                            consumed: j + 2,
                            action: classify(&slice[PREFIX.len()..j]),
                        };
                    }
                    // ESC mid-field with no ST: malformed — pass the leading ESC through.
                    Some(_) => return ParseResult::NotOsc133,
                    None => return ParseResult::Incomplete,
                }
            }
            _ => j += 1,
        }
    }
    ParseResult::Incomplete
}

/// Map an OSC 133 field to a marker action. `C` and `D` carry busy-state meaning
/// and are stripped with an event; `A` (prompt start) passes through so the
/// frontend can mark the prompt row; `B` and anything unrecognized are stripped
/// without an event.
fn classify(field: &[u8]) -> MarkerAction {
    match field.first() {
        Some(b'C') => MarkerAction::Strip(Some(Osc133Event::CommandStart)),
        Some(b'D') => MarkerAction::Strip(Some(Osc133Event::CommandEnd {
            exit: parse_exit(field),
        })),
        Some(b'A') => MarkerAction::Passthrough,
        _ => MarkerAction::Strip(None),
    }
}

/// Parse the exit code from a `D` field of the form `D` or `D;<int>`; defaults to 0.
fn parse_exit(field: &[u8]) -> i32 {
    // field[0] is b'D'.
    let digits = match field[1..].split_first() {
        Some((b';', tail)) => tail,
        _ => return 0,
    };
    std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed_all(parser: &mut Osc133Parser, chunks: &[&str]) -> (String, Vec<Osc133Event>) {
        let mut out = String::new();
        let mut events = Vec::new();
        for chunk in chunks {
            let (clean, evs) = parser.feed(chunk);
            out.push_str(&clean);
            events.extend(evs);
        }
        (out, events)
    }

    #[test]
    fn passes_plain_output_through_unchanged() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("hello world\n");
        assert_eq!(out, "hello world\n");
        assert!(events.is_empty());
    }

    #[test]
    fn strips_command_start_marker_and_emits_event() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("\x1b]133;C\x07ls\n");
        assert_eq!(out, "ls\n");
        assert_eq!(events, vec![Osc133Event::CommandStart]);
    }

    #[test]
    fn strips_command_end_marker_with_exit() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("done\n\x1b]133;D;7\x07");
        assert_eq!(out, "done\n");
        assert_eq!(events, vec![Osc133Event::CommandEnd { exit: 7 }]);
    }

    #[test]
    fn command_end_without_exit_defaults_to_zero() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("\x1b]133;D\x07");
        assert_eq!(out, "");
        assert_eq!(events, vec![Osc133Event::CommandEnd { exit: 0 }]);
    }

    #[test]
    fn passes_prompt_start_through_but_strips_prompt_end() {
        // `A` (prompt start) survives so the frontend can anchor a buffer marker;
        // `B` (prompt end) is still stripped. Neither produces an event.
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("\x1b]133;A\x07$ \x1b]133;B\x07");
        assert_eq!(out, "\x1b]133;A\x07$ ");
        assert!(events.is_empty());
    }

    #[test]
    fn prompt_start_marker_passes_through_verbatim_with_no_event() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("\x1b]133;A\x07");
        assert_eq!(out, "\x1b]133;A\x07");
        assert!(events.is_empty());
    }

    #[test]
    fn handles_st_terminator() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("\x1b]133;C\x1b\\echo");
        assert_eq!(out, "echo");
        assert_eq!(events, vec![Osc133Event::CommandStart]);
    }

    #[test]
    fn handles_multiple_markers_in_one_chunk() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("\x1b]133;D;0\x07\x1b]133;A\x07$ \x1b]133;C\x07output");
        // `D` and `C` are stripped; `A` passes through so the prompt row is marked.
        assert_eq!(out, "\x1b]133;A\x07$ output");
        assert_eq!(
            events,
            vec![
                Osc133Event::CommandEnd { exit: 0 },
                Osc133Event::CommandStart
            ]
        );
    }

    #[test]
    fn marker_split_across_two_feeds_is_held_back() {
        let mut p = Osc133Parser::new();
        // Split right in the middle of the escape sequence.
        let (out, events) = feed_all(&mut p, &["before\x1b]13", "3;C\x07after"]);
        assert_eq!(out, "beforeafter");
        assert_eq!(events, vec![Osc133Event::CommandStart]);
    }

    #[test]
    fn marker_split_at_terminator_is_held_back() {
        let mut p = Osc133Parser::new();
        let (out, events) = feed_all(&mut p, &["\x1b]133;D;42", "\x07tail"]);
        assert_eq!(out, "tail");
        assert_eq!(events, vec![Osc133Event::CommandEnd { exit: 42 }]);
    }

    #[test]
    fn esc_split_at_chunk_boundary_is_held_back() {
        let mut p = Osc133Parser::new();
        // A lone ESC at the end could begin a marker; it must not leak early.
        let (out, events) = feed_all(&mut p, &["x\x1b", "]133;C\x07y"]);
        assert_eq!(out, "xy");
        assert_eq!(events, vec![Osc133Event::CommandStart]);
    }

    #[test]
    fn passes_through_unrelated_osc_sequences() {
        let mut p = Osc133Parser::new();
        // OSC 0 (window title) must survive untouched.
        let (out, events) = p.feed("\x1b]0;my title\x07hello");
        assert_eq!(out, "\x1b]0;my title\x07hello");
        assert!(events.is_empty());
    }

    #[test]
    fn preserves_multibyte_utf8_around_markers() {
        let mut p = Osc133Parser::new();
        let (out, events) = p.feed("héllo\x1b]133;C\x07wörld");
        assert_eq!(out, "héllowörld");
        assert_eq!(events, vec![Osc133Event::CommandStart]);
    }

    #[test]
    fn interleaves_markers_with_normal_output() {
        let mut p = Osc133Parser::new();
        let (out, events) = feed_all(
            &mut p,
            &[
                "\x1b]133;A\x07user@host $ ",
                "\x1b]133;C\x07",
                "command output line 1\n",
                "line 2\n\x1b]133;D;0\x07",
            ],
        );
        // `A` passes through verbatim; `C`/`D` are stripped and evented.
        assert_eq!(
            out,
            "\x1b]133;A\x07user@host $ command output line 1\nline 2\n"
        );
        assert_eq!(
            events,
            vec![
                Osc133Event::CommandStart,
                Osc133Event::CommandEnd { exit: 0 }
            ]
        );
    }
}
