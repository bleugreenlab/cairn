//! Deterministic, model-free repair of tool calls emitted by the in-process
//! HTTP chat-completions turn loop.
//!
//! HTTP-loop backends are the only ones where Cairn drives the turn loop and
//! parses raw tool-call JSON itself (Claude and Codex own their own model
//! conversations through their CLIs), so smaller models occasionally emit a
//! mangled tool name or truncated/garbled argument JSON. These pure functions
//! repair what is cheaply repairable and degrade the rest gracefully: an
//! unrepairable name yields `None` (the caller surfaces "unknown tool") and
//! unrepairable arguments yield `None` (the caller surfaces a retry message).
//! Nothing here panics or re-invokes the model, so a single bad tool call can
//! never crash the run. OpenRouter is the only adapter using this today.

use serde_json::Value;

/// The three verbs the HTTP-loop backend dispatches.
const VERBS: [&str; 3] = ["read", "write", "run"];

/// Cutoff for the fuzzy (normalized edit-distance) tool-name match. Mirrors
/// Hermes Agent's `difflib` ratio of 0.7: confident enough to fix a typo
/// (`reat` -> `read`) without misrouting an unrelated name.
const FUZZY_CUTOFF: f64 = 0.7;

/// Normalize a model-emitted tool name to one of Cairn's three verbs.
///
/// Order: strip the `mcp__<server>__` gateway prefix, lowercase, drop all
/// non-alphanumeric separators, strip a trailing `tool` suffix, then try an
/// exact match, an alias map, and finally a fuzzy edit-distance match. Returns
/// `None` when nothing matches confidently so the caller can tell the model the
/// valid names.
pub(in crate::backends) fn normalize_tool_name(raw: &str) -> Option<&'static str> {
    let cleaned = clean_name(raw);
    if cleaned.is_empty() {
        return None;
    }

    if let Some(verb) = VERBS.iter().find(|verb| **verb == cleaned).copied() {
        return Some(verb);
    }

    // Common aliases other agents/models reach for. Kept conservative: only
    // names that unambiguously map to one verb.
    let aliased = match cleaned.as_str() {
        "edit" | "writefile" | "strreplace" | "applypatch" | "apply" | "patch" | "multiedit" => {
            Some("write")
        }
        "bash" | "shell" | "exec" | "execute" | "command" | "cmd" | "sh" | "terminal" => {
            Some("run")
        }
        "cat" | "view" | "open" | "readfile" | "fetch" | "get" | "ls" | "list" => Some("read"),
        _ => None,
    };
    if let Some(verb) = aliased {
        return VERBS.iter().find(|v| **v == verb).copied();
    }

    // Fuzzy fallback for typos: pick the closest verb above the cutoff.
    VERBS
        .iter()
        .filter_map(|verb| {
            let max = cleaned.chars().count().max(verb.chars().count());
            if max == 0 {
                return None;
            }
            let ratio = 1.0 - (levenshtein(&cleaned, verb) as f64) / (max as f64);
            (ratio >= FUZZY_CUTOFF).then_some((*verb, ratio))
        })
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(verb, _)| verb)
}

/// Lowercase, strip the `mcp__<server>__` gateway prefix, drop non-alphanumeric
/// separators, and strip a trailing `tool` suffix. The prefix strip runs while
/// the `__` separators are still present; separator removal runs after.
fn clean_name(raw: &str) -> String {
    let mut s = raw.trim().to_lowercase();
    // `mcp__cairn__run` / `mcp__server__write` -> the segment after the last `__`.
    if s.starts_with("mcp__") {
        if let Some(idx) = s.rfind("__") {
            s = s[idx + 2..].to_string();
        }
    }
    let mut s: String = s.chars().filter(|c| c.is_alphanumeric()).collect();
    // Strip a trailing `tool` suffix (`writetool`, `read_tool` -> `readtool`).
    if s.len() > 4 && s.ends_with("tool") {
        s.truncate(s.len() - 4);
    }
    s
}

/// Classic two-row Levenshtein edit distance over chars.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

/// How a tool-argument string parsed, so callers can treat a truncation-balanced
/// payload differently from a cosmetically-repaired one. The distinction is
/// load-bearing: a truncated `write`/`run` must NOT be dispatched, because
/// synthesizing the missing closers fabricates a payload the model never
/// finished emitting (a file whose content is cut off, a half-formed command).
pub(in crate::backends) enum ParsedArguments {
    /// Parsed as-is or after cosmetic-only repair (Markdown code fences, trailing
    /// commas). The JSON was complete; safe to dispatch. `repaired` records
    /// whether any cosmetic fix was applied.
    Ready { value: Value, repaired: bool },
    /// Only parseable after synthesizing closing quotes/brackets: the input was
    /// truncated, so `value` is a reconstruction, not what the model finished.
    /// Non-destructive verbs may still use it; side-effecting verbs must not.
    Truncated { value: Value },
    /// Unparseable even after repair.
    Unrecoverable,
}

impl ParsedArguments {
    /// Best-effort value for the stored transcript (never panics). Unrecoverable
    /// arguments surface as JSON null, matching the prior transcript fallback.
    pub(in crate::backends) fn value(&self) -> Value {
        match self {
            ParsedArguments::Ready { value, .. } | ParsedArguments::Truncated { value } => {
                value.clone()
            }
            ParsedArguments::Unrecoverable => Value::Null,
        }
    }
}

/// Parse model-emitted tool arguments, repairing common malformations and
/// classifying the outcome. Empty/whitespace becomes a `Ready` empty object.
/// Cosmetic repairs (fences, trailing commas) keep the result `Ready` because
/// the JSON was complete; only synthesizing closers (the truncation case) yields
/// `Truncated`, so the caller can refuse to dispatch a side-effecting partial.
pub(in crate::backends) fn parse_tool_arguments(raw: &str) -> ParsedArguments {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return ParsedArguments::Ready {
            value: Value::Object(Default::default()),
            repaired: false,
        };
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return ParsedArguments::Ready {
            value,
            repaired: false,
        };
    }
    // Cosmetic-only: complete JSON merely dressed up. No closers are synthesized,
    // so a success here means the payload was whole.
    let cosmetic = strip_trailing_commas(&strip_code_fences(trimmed));
    if let Ok(value) = serde_json::from_str::<Value>(&cosmetic) {
        return ParsedArguments::Ready {
            value,
            repaired: true,
        };
    }
    // Structural: required balancing unclosed strings/brackets => truncation.
    let structural = repair_json_arguments(trimmed);
    if let Ok(value) = serde_json::from_str::<Value>(&structural) {
        return ParsedArguments::Truncated { value };
    }
    ParsedArguments::Unrecoverable
}

/// Apply deterministic repair passes to a JSON argument string: strip Markdown
/// code fences, balance unclosed strings/brackets (the observed truncation
/// case), and drop trailing commas. The result is best-effort and may still be
/// invalid JSON; callers re-parse and fall back when it is.
pub(super) fn repair_json_arguments(raw: &str) -> String {
    let s = strip_code_fences(raw);
    let s = balance_brackets(&s);
    strip_trailing_commas(&s)
}

/// Strip a leading/trailing ```` ``` ```` fence and an optional `json` language
/// tag. Only triggers when the whole string is fenced, so JSON strings that
/// merely contain backticks are untouched.
fn strip_code_fences(s: &str) -> String {
    let t = s.trim();
    if !t.starts_with("```") {
        return s.to_string();
    }
    let t = t.trim_matches('`').trim();
    let t = t.strip_prefix("json").map(str::trim_start).unwrap_or(t);
    t.trim().to_string()
}

/// Close any strings and brackets left open by truncation. Scans once tracking
/// string/escape state and a stack of expected closers, then appends the
/// missing closers in reverse order. A trailing lone backslash inside an open
/// string is dropped so the synthesized closing quote is not escaped.
fn balance_brackets(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 8);
    let mut stack: Vec<char> = Vec::new();
    let mut in_string = false;
    let mut escaped = false;
    for c in s.chars() {
        result.push(c);
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
        } else {
            match c {
                '"' => in_string = true,
                '{' => stack.push('}'),
                '[' => stack.push(']'),
                '}' | ']' => {
                    if stack.last() == Some(&c) {
                        stack.pop();
                    }
                }
                _ => {}
            }
        }
    }
    if in_string {
        if escaped {
            // Drop the dangling backslash so it does not escape our quote.
            result.pop();
        }
        result.push('"');
    }
    while let Some(closer) = stack.pop() {
        result.push(closer);
    }
    result
}

/// Remove commas that sit (ignoring whitespace) immediately before a `}` or `]`,
/// outside of strings. Runs after balancing so a comma exposed by a synthesized
/// closer is also dropped.
fn strip_trailing_commas(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::with_capacity(s.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            result.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if c == '"' {
            in_string = true;
            result.push(c);
            i += 1;
            continue;
        }
        if c == ',' {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && (chars[j] == '}' || chars[j] == ']') {
                i += 1; // drop the comma
                continue;
            }
        }
        result.push(c);
        i += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalize_tool_name_exact_and_casing() {
        assert_eq!(normalize_tool_name("read"), Some("read"));
        assert_eq!(normalize_tool_name("Read"), Some("read"));
        assert_eq!(normalize_tool_name("WRITE"), Some("write"));
        assert_eq!(normalize_tool_name("  run  "), Some("run"));
    }

    #[test]
    fn normalize_tool_name_prefix_and_aliases() {
        assert_eq!(normalize_tool_name("mcp__cairn__run"), Some("run"));
        assert_eq!(normalize_tool_name("mcp__other__write"), Some("write"));
        assert_eq!(normalize_tool_name("write_file"), Some("write"));
        assert_eq!(normalize_tool_name("str_replace"), Some("write"));
        assert_eq!(normalize_tool_name("bash"), Some("run"));
        assert_eq!(normalize_tool_name("shell"), Some("run"));
        assert_eq!(normalize_tool_name("cat"), Some("read"));
        assert_eq!(normalize_tool_name("read_tool"), Some("read"));
    }

    #[test]
    fn normalize_tool_name_fuzzy_typos() {
        assert_eq!(normalize_tool_name("reat"), Some("read"));
        assert_eq!(normalize_tool_name("wrte"), Some("write"));
    }

    #[test]
    fn normalize_tool_name_rejects_unknown() {
        assert_eq!(normalize_tool_name("frobnicate"), None);
        assert_eq!(normalize_tool_name(""), None);
        assert_eq!(normalize_tool_name("   "), None);
    }

    fn ready(raw: &str) -> Value {
        match parse_tool_arguments(raw) {
            ParsedArguments::Ready { value, .. } => value,
            other => panic!("expected Ready, got {:?}", other.value()),
        }
    }

    #[test]
    fn parse_tool_arguments_empty_is_ready_object() {
        assert_eq!(ready(""), json!({}));
        assert_eq!(ready("   "), json!({}));
        assert!(matches!(
            parse_tool_arguments(""),
            ParsedArguments::Ready {
                repaired: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_tool_arguments_valid_passthrough_not_repaired() {
        assert_eq!(ready(r#"{"command":"ls"}"#), json!({"command": "ls"}));
        assert!(matches!(
            parse_tool_arguments(r#"{"command":"ls"}"#),
            ParsedArguments::Ready {
                repaired: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_tool_arguments_code_fence_is_cosmetic_ready() {
        let fenced = "```json\n{\"command\": \"ls\"}\n```";
        assert_eq!(ready(fenced), json!({"command": "ls"}));
        assert!(matches!(
            parse_tool_arguments(fenced),
            ParsedArguments::Ready { repaired: true, .. }
        ));
    }

    #[test]
    fn parse_tool_arguments_trailing_comma_is_cosmetic_ready() {
        // Trailing commas mean the JSON was complete, just sloppy: safe to
        // dispatch, so these stay Ready (not Truncated).
        assert_eq!(ready(r#"{"command": "ls",}"#), json!({"command": "ls"}));
        assert_eq!(
            ready(r#"{"paths": ["a", "b",]}"#),
            json!({"paths": ["a", "b"]})
        );
        assert!(matches!(
            parse_tool_arguments(r#"{"command": "ls",}"#),
            ParsedArguments::Ready { repaired: true, .. }
        ));
    }

    #[test]
    fn parse_tool_arguments_truncated_string_is_truncated() {
        // The CAIRN-1784 `column 13388` shape: a write cut off mid-string value.
        // Balancing recovers a parseable object, but it must be flagged Truncated
        // so a side-effecting call is not dispatched with shortened content.
        let truncated = r#"{"changes": [{"target": "file:x", "content": "abc def ghi"#;
        match parse_tool_arguments(truncated) {
            ParsedArguments::Truncated { value } => {
                assert_eq!(value["changes"][0]["target"], json!("file:x"));
            }
            other => panic!("expected Truncated, got {:?}", other.value()),
        }
    }

    #[test]
    fn parse_tool_arguments_unclosed_object_is_truncated() {
        match parse_tool_arguments(r#"{"command": "ls""#) {
            ParsedArguments::Truncated { value } => {
                assert_eq!(value, json!({"command": "ls"}));
            }
            other => panic!("expected Truncated, got {:?}", other.value()),
        }
    }

    #[test]
    fn parse_tool_arguments_garbage_is_unrecoverable() {
        assert!(matches!(
            parse_tool_arguments("not json at all <<<"),
            ParsedArguments::Unrecoverable
        ));
        // Truncation right after a key/colon is unrecoverable, not a partial
        // dispatch.
        assert!(matches!(
            parse_tool_arguments("{\"a\":"),
            ParsedArguments::Unrecoverable
        ));
    }

    #[test]
    fn parse_tool_arguments_never_panics_on_random_bytes() {
        for sample in ["{[}", "\"\\", "[[[[[", "}}}}}", "{,,,}", "\\\\\\"] {
            let _ = parse_tool_arguments(sample).value();
        }
    }
}
