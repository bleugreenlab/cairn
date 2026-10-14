//! Classifying the tool inputs the agent runtime refused to parse.

use serde_json::Value;

/// Why the agent runtime refused a tool call's input JSON.
///
/// With native tool use the runtime accumulates the streamed `tool_use` input
/// and parses it itself. When that parse fails the call is never dispatched —
/// Cairn's MCP server never sees it — and the runtime hands the block back with
/// `input` replaced by a `__unparsedToolInput` marker carrying the byte length
/// and a clipped prefix of the text it could not read. Classifying that prefix
/// is the only account anyone gets of why the call was lost, so the variants
/// here name causes observed in real rejections rather than a generic failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputDefect {
    /// A backslash escape JSON does not define, such as a regex `\.` or `\|`
    /// that reached the wire with only one level of escaping.
    InvalidEscape,
    /// `\u` not followed by four hex digits — Rust's `\u{2717}` form leaking
    /// into a JSON string.
    UnicodeEscape,
    /// A raw control character inside a string, which JSON requires escaped.
    /// Most often a literal newline carried in from a shell heredoc.
    ControlCharacter,
    /// A complete value followed by more text. The observed shape is a
    /// duplicated object tail, where a trailing key is emitted twice.
    TrailingGarbage,
    /// A string that never closes in a prefix known to be the whole payload.
    UnterminatedString,
    /// The retained prefix holds no defect and stops short of the full length,
    /// so the cause lies in the part the runtime did not keep.
    BeyondCapture,
    /// Well-formed JSON by our reading, so the rejection was not a syntax
    /// problem — a schema mismatch reported through the same error.
    NotSyntax,
    /// Invalid some other way, typically a string closed early by an unescaped
    /// quote, leaving the remainder of the object desynchronized.
    Structure,
}

impl InputDefect {
    pub fn as_str(self) -> &'static str {
        match self {
            InputDefect::InvalidEscape => "invalid-escape",
            InputDefect::UnicodeEscape => "unicode-escape",
            InputDefect::ControlCharacter => "control-character",
            InputDefect::TrailingGarbage => "trailing-garbage",
            InputDefect::UnterminatedString => "unterminated-string",
            InputDefect::BeyondCapture => "beyond-capture",
            InputDefect::NotSyntax => "not-syntax",
            InputDefect::Structure => "structure",
        }
    }
}

/// A tool call the runtime rejected before Cairn could run it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedToolInput {
    /// Byte length of the input the runtime tried to parse.
    pub len: usize,
    /// Bytes actually retained for inspection; the runtime clips its sample.
    pub captured: usize,
    pub defect: InputDefect,
}

impl RejectedToolInput {
    /// Recognize the marker the runtime substitutes for an unparsable input.
    pub fn detect(input: &Value) -> Option<Self> {
        let marker = input.get("__unparsedToolInput")?;
        let raw = marker
            .get("raw")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let captured = raw.len();
        let len = marker
            .get("len")
            .and_then(Value::as_u64)
            .map_or(captured, |len| len as usize);
        Some(Self {
            len,
            captured,
            defect: classify_input_defect(raw, captured < len),
        })
    }
}

pub(crate) fn classify_input_defect(raw: &str, clipped: bool) -> InputDefect {
    match scan_json_strings(raw) {
        StringScan::Defect(defect) => return defect,
        StringScan::EndedInString if clipped => return InputDefect::BeyondCapture,
        StringScan::EndedInString => return InputDefect::UnterminatedString,
        StringScan::Balanced => {}
    }
    if clipped {
        return InputDefect::BeyondCapture;
    }
    // The payload is whole and its strings are sound, so the fault is in the
    // structure around them. Decoding one value and asking where it stopped
    // separates a duplicated tail from an object that never made sense.
    let mut values = serde_json::Deserializer::from_str(raw).into_iter::<Value>();
    match values.next() {
        Some(Ok(_)) if values.byte_offset() >= raw.trim_end().len() => InputDefect::NotSyntax,
        Some(Ok(_)) => InputDefect::TrailingGarbage,
        _ => InputDefect::Structure,
    }
}

enum StringScan {
    Balanced,
    EndedInString,
    Defect(InputDefect),
}

/// Walk the text as JSON would, reporting the first defect inside a string
/// literal. Escaping mistakes are the dominant cause of a rejected input, and
/// they are decidable from a prefix, so this runs before any whole-value parse.
fn scan_json_strings(raw: &str) -> StringScan {
    let mut chars = raw.chars();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if !in_string {
            in_string = c == '"';
            continue;
        }
        match c {
            '"' => in_string = false,
            '\\' => match chars.next() {
                None => return StringScan::EndedInString,
                Some('u') => {
                    for _ in 0..4 {
                        match chars.next() {
                            Some(hex) if hex.is_ascii_hexdigit() => {}
                            Some(_) => return StringScan::Defect(InputDefect::UnicodeEscape),
                            None => return StringScan::EndedInString,
                        }
                    }
                }
                Some('"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't') => {}
                Some(_) => return StringScan::Defect(InputDefect::InvalidEscape),
            },
            c if (c as u32) < 0x20 => return StringScan::Defect(InputDefect::ControlCharacter),
            _ => {}
        }
    }
    if in_string {
        StringScan::EndedInString
    } else {
        StringScan::Balanced
    }
}
