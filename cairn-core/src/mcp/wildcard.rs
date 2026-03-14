//! Wildcard-anchored text matching and replacement.
//!
//! When `old_string` contains `\n~~~~~\n`, the text is split into a "head"
//! anchor and a "tail" anchor. The edit replaces everything from head through
//! tail in the target content, using delimiter-balanced matching to find the
//! correct tail position.

/// The wildcard separator used in old_string for anchored matching.
/// When old_string contains this on its own line, the text before it is the
/// "head anchor" and text after is the "tail anchor". Everything from the
/// start of the head match through the end of the tail match gets replaced.
///
/// Tail matching is delimiter-balanced: depth is tracked across the gap
/// (wildcard region) only. The tail matches at the first position where the
/// gap's accumulated delimiter depth returns to zero for all pairs.
pub const WILDCARD_SEP: &str = "\n~~~~~\n";

/// Scan mode for lightweight tokenization during balanced matching.
/// Delimiters inside strings and comments are ignored.
#[derive(Clone, Copy, PartialEq)]
enum ScanMode {
    /// Normal code — track delimiters, check for tail match
    Code,
    /// Inside "..." (skip until unescaped ")
    DoubleQuote,
    /// Inside '...' (skip until unescaped ')
    SingleQuote,
    /// Inside // comment (skip until newline)
    LineComment,
    /// Inside /* comment (skip until */)
    BlockComment,
}

/// Track net depth of paired delimiters for balanced matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DelimDepth {
    pub brace: i32,   // { }
    pub bracket: i32, // [ ]
    pub paren: i32,   // ( )
}

impl DelimDepth {
    fn zero() -> Self {
        Self {
            brace: 0,
            bracket: 0,
            paren: 0,
        }
    }

    pub(crate) fn from_text(text: &str) -> Self {
        let mut d = Self::zero();
        let bytes = text.as_bytes();
        let mut mode = ScanMode::Code;
        let mut i = 0;
        while i < bytes.len() {
            match mode {
                ScanMode::Code => {
                    if i + 1 < bytes.len() {
                        if bytes[i] == b'/' && bytes[i + 1] == b'/' {
                            mode = ScanMode::LineComment;
                            i += 2;
                            continue;
                        }
                        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
                            mode = ScanMode::BlockComment;
                            i += 2;
                            continue;
                        }
                    }
                    if bytes[i] == b'"' {
                        mode = ScanMode::DoubleQuote;
                    } else if bytes[i] == b'\'' {
                        mode = ScanMode::SingleQuote;
                    } else {
                        d.track(bytes[i]);
                    }
                }
                ScanMode::DoubleQuote => {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        i += 2;
                        continue;
                    } else if bytes[i] == b'"' || bytes[i] == b'\n' {
                        mode = ScanMode::Code;
                    }
                }
                ScanMode::SingleQuote => {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        i += 2;
                        continue;
                    } else if bytes[i] == b'\'' || bytes[i] == b'\n' {
                        mode = ScanMode::Code;
                    }
                }
                ScanMode::LineComment => {
                    if bytes[i] == b'\n' {
                        mode = ScanMode::Code;
                    }
                }
                ScanMode::BlockComment => {
                    if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                        mode = ScanMode::Code;
                        i += 2;
                        continue;
                    }
                }
            }
            i += 1;
        }
        d
    }

    fn track(&mut self, byte: u8) {
        match byte {
            b'{' => self.brace += 1,
            b'}' => self.brace -= 1,
            b'[' => self.bracket += 1,
            b']' => self.bracket -= 1,
            b'(' => self.paren += 1,
            b')' => self.paren -= 1,
            _ => {}
        }
    }

    fn negated(&self) -> Self {
        Self {
            brace: -self.brace,
            bracket: -self.bracket,
            paren: -self.paren,
        }
    }
}

/// Split old_string into anchors around wildcard separators.
/// Returns None if no separator is present or any segment is empty.
/// A single separator yields 2 anchors; N separators yield N+1.
pub fn parse_wildcard(old_string: &str) -> Option<Vec<&str>> {
    if !old_string.contains(WILDCARD_SEP) {
        return None;
    }
    let segments: Vec<&str> = old_string.split(WILDCARD_SEP).collect();
    if segments.len() < 2 || segments.iter().any(|s| s.is_empty()) {
        return None;
    }
    Some(segments)
}

/// Scan content from `start` for an anchor using delimiter-balanced matching.
///
/// Depth is tracked across the gap only. Returns the absolute byte position
/// where the anchor starts, or an error if not found.
fn scan_for_anchor(content: &str, start: usize, anchor: &str) -> Result<usize, ()> {
    let target = DelimDepth::from_text(anchor).negated();
    let search = &content[start..];
    let search_bytes = search.as_bytes();
    let mut depth = target.clone();
    let mut mode = ScanMode::Code;

    let mut i = 0;
    while i < search_bytes.len() {
        match mode {
            ScanMode::Code => {
                if depth == target && search.is_char_boundary(i) && search[i..].starts_with(anchor)
                {
                    return Ok(start + i);
                }

                if i + 1 < search_bytes.len() {
                    if search_bytes[i] == b'/' && search_bytes[i + 1] == b'/' {
                        mode = ScanMode::LineComment;
                        i += 2;
                        continue;
                    }
                    if search_bytes[i] == b'/' && search_bytes[i + 1] == b'*' {
                        mode = ScanMode::BlockComment;
                        i += 2;
                        continue;
                    }
                }
                if search_bytes[i] == b'"' {
                    mode = ScanMode::DoubleQuote;
                } else if search_bytes[i] == b'\'' {
                    mode = ScanMode::SingleQuote;
                } else {
                    depth.track(search_bytes[i]);
                }
            }
            ScanMode::DoubleQuote => {
                if search_bytes[i] == b'\\' && i + 1 < search_bytes.len() {
                    i += 2;
                    continue;
                } else if search_bytes[i] == b'"' || search_bytes[i] == b'\n' {
                    mode = ScanMode::Code;
                }
            }
            ScanMode::SingleQuote => {
                if search_bytes[i] == b'\\' && i + 1 < search_bytes.len() {
                    i += 2;
                    continue;
                } else if search_bytes[i] == b'\'' || search_bytes[i] == b'\n' {
                    mode = ScanMode::Code;
                }
            }
            ScanMode::LineComment => {
                if search_bytes[i] == b'\n' {
                    mode = ScanMode::Code;
                }
            }
            ScanMode::BlockComment => {
                if search_bytes[i] == b'*'
                    && i + 1 < search_bytes.len()
                    && search_bytes[i + 1] == b'/'
                {
                    mode = ScanMode::Code;
                    i += 2;
                    continue;
                }
            }
        }
        i += 1;
    }

    // Check at end of search — anchor could match after last byte
    if mode == ScanMode::Code && depth == target && search.ends_with(anchor) {
        return Ok(content.len() - anchor.len());
    }

    Err(())
}

/// Find the anchored region in content using balanced delimiter matching
/// with lightweight string/comment awareness.
///
/// Supports multiple gaps: `anchors` contains N segments (N >= 2) separated
/// by wildcards. Each gap between consecutive anchors is scanned independently
/// with delimiter-aware depth tracking.
///
/// Delimiters inside `"..."`, `'...'`, `// ...`, and `/* ... */` are not
/// counted toward depth.
pub fn apply_wildcard_edit(
    content: &str,
    anchors: &[&str],
    new_string: &str,
) -> Result<(String, String), String> {
    assert!(anchors.len() >= 2, "need at least 2 anchors");
    let n = anchors.len();
    let is_single_gap = n == 2;

    // Find head anchor via simple substring search
    let head_start = content.find(anchors[0]).ok_or_else(|| {
        format!(
            "Head anchor not found in file. Make sure the text before ~~~~~ matches exactly.\nSearching for:\n{}",
            anchors[0]
        )
    })?;
    let mut cursor = head_start + anchors[0].len();

    // Scan for each subsequent anchor
    for (i, anchor) in anchors.iter().enumerate().skip(1) {
        let anchor_start = scan_for_anchor(content, cursor, anchor).map_err(|_| {
            if is_single_gap {
                format!(
                    "Tail anchor not found after head anchor. Make sure the text after ~~~~~ matches exactly.\nSearching for:\n{}",
                    anchor
                )
            } else {
                format!(
                    "Anchor {} of {} not found after previous anchor.\nSearching for:\n{}",
                    i + 1, n, anchor
                )
            }
        })?;
        cursor = anchor_start + anchor.len();
    }

    let replaced = content[head_start..cursor].to_string();
    let new_content = format!(
        "{}{}{}",
        &content[..head_start],
        new_string,
        &content[cursor..]
    );

    Ok((new_content, replaced))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- DelimDepth tests ---

    #[test]
    fn delim_depth_from_balanced_text() {
        let d = DelimDepth::from_text("fn foo(x: i32) { bar() }");
        assert_eq!(d, DelimDepth::zero());
    }

    #[test]
    fn delim_depth_from_unbalanced_opener() {
        let d = DelimDepth::from_text("fn main() {");
        assert_eq!(
            d,
            DelimDepth {
                brace: 1,
                bracket: 0,
                paren: 0
            }
        );
    }

    #[test]
    fn delim_depth_mixed_delimiters() {
        let d = DelimDepth::from_text("fn foo(items: &[");
        assert_eq!(
            d,
            DelimDepth {
                brace: 0,
                bracket: 1,
                paren: 1
            }
        );
    }

    #[test]
    fn delim_depth_negated() {
        let d = DelimDepth::from_text("])");
        assert_eq!(
            d.negated(),
            DelimDepth {
                brace: 0,
                bracket: 1,
                paren: 1
            }
        );
    }

    // --- parse_wildcard tests ---

    #[test]
    fn parse_wildcard_basic() {
        let input = "head line\n~~~~~\ntail line";
        let anchors = parse_wildcard(input).unwrap();
        assert_eq!(anchors, vec!["head line", "tail line"]);
    }

    #[test]
    fn parse_wildcard_multiline_anchors() {
        let input = "line1\nline2\n~~~~~\nline4\nline5";
        let anchors = parse_wildcard(input).unwrap();
        assert_eq!(anchors, vec!["line1\nline2", "line4\nline5"]);
    }

    #[test]
    fn parse_wildcard_no_separator() {
        assert!(parse_wildcard("just normal text").is_none());
    }

    #[test]
    fn parse_wildcard_separator_at_start_returns_none() {
        assert!(parse_wildcard("\n~~~~~\ntail").is_none());
    }

    #[test]
    fn parse_wildcard_separator_at_end_returns_none() {
        assert!(parse_wildcard("head\n~~~~~\n").is_none());
    }

    #[test]
    fn parse_wildcard_tildes_inline_not_matched() {
        assert!(parse_wildcard("foo~~~~~ bar").is_none());
    }

    // --- apply_wildcard_edit: basic behavior ---

    #[test]
    fn wildcard_replace_body() {
        let content = "fn main() {\n    let x = 1;\n    let y = 2;\n}";
        let (result, replaced) = apply_wildcard_edit(
            content,
            &["fn main() {", "}"],
            "fn main() {\n    let z = 42;\n}",
        )
        .unwrap();
        assert_eq!(result, "fn main() {\n    let z = 42;\n}");
        assert_eq!(replaced, content);
    }

    #[test]
    fn wildcard_preserves_surrounding_content() {
        let content = "// header\nfn foo() {\n    old_code();\n}\n// footer";
        let (result, replaced) = apply_wildcard_edit(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new_code();\n}",
        )
        .unwrap();
        assert_eq!(
            result,
            "// header\nfn foo() {\n    new_code();\n}\n// footer"
        );
        assert_eq!(replaced, "fn foo() {\n    old_code();\n}");
    }

    #[test]
    fn wildcard_multiline_anchors() {
        let content = "a\nb\nc\nd\ne\nf\ng";
        let (result, replaced) =
            apply_wildcard_edit(content, &["b\nc", "f\ng"], "B\nC\nD\nE\nF\nG").unwrap();
        assert_eq!(result, "a\nB\nC\nD\nE\nF\nG");
        assert_eq!(replaced, "b\nc\nd\ne\nf\ng");
    }

    #[test]
    fn wildcard_delete_region() {
        let content = "keep\ndelete_start\nmiddle\ndelete_end\nkeep";
        let (result, replaced) =
            apply_wildcard_edit(content, &["delete_start", "delete_end"], "").unwrap();
        assert_eq!(result, "keep\n\nkeep");
        assert_eq!(replaced, "delete_start\nmiddle\ndelete_end");
    }

    #[test]
    fn wildcard_head_not_found() {
        let result = apply_wildcard_edit("some content", &["nonexistent", "also missing"], "new");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Head anchor not found"));
    }

    #[test]
    fn wildcard_tail_not_found() {
        let result = apply_wildcard_edit("some content here", &["some", "nonexistent"], "new");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Tail anchor not found"));
    }

    #[test]
    fn wildcard_uses_first_head_match() {
        let content = "fn a() {\n    x\n}\nfn a() {\n    y\n}";
        let (result, replaced) =
            apply_wildcard_edit(content, &["fn a() {", "}"], "fn a() {\n    z\n}").unwrap();
        assert_eq!(result, "fn a() {\n    z\n}\nfn a() {\n    y\n}");
        assert_eq!(replaced, "fn a() {\n    x\n}");
    }

    #[test]
    fn wildcard_tail_after_head() {
        let content = "} early\nfn start() {\n    body\n} end";
        let (result, _) = apply_wildcard_edit(
            content,
            &["fn start() {", "} end"],
            "fn start() {\n    new\n} end",
        )
        .unwrap();
        assert_eq!(result, "} early\nfn start() {\n    new\n} end");
    }

    #[test]
    fn wildcard_adjacent_anchors() {
        let content = "aXb";
        let (result, replaced) = apply_wildcard_edit(content, &["a", "b"], "a_new_b").unwrap();
        assert_eq!(result, "a_new_b");
        assert_eq!(replaced, "aXb");
    }

    // --- apply_wildcard_edit: balanced delimiter matching ---

    #[test]
    fn wildcard_balanced_braces_skips_inner() {
        // Inner `}` should be skipped, outer `}` matched
        let content = "fn main() {\n    if true {\n        x\n    }\n    y\n}";
        let (result, replaced) =
            apply_wildcard_edit(content, &["fn main() {", "}"], "fn main() {\n    z\n}").unwrap();
        assert_eq!(result, "fn main() {\n    z\n}");
        assert_eq!(replaced, content);
    }

    #[test]
    fn wildcard_balanced_brackets_skips_inner() {
        // Inner `]` in nested arrays should be skipped
        let content = "const arr = [\n    [1, 2],\n    [3, 4],\n]";
        let (result, replaced) = apply_wildcard_edit(
            content,
            &["const arr = [", "]"],
            "const arr = [\n    [5, 6],\n]",
        )
        .unwrap();
        assert_eq!(result, "const arr = [\n    [5, 6],\n]");
        assert_eq!(replaced, content);
    }

    #[test]
    fn wildcard_balanced_deeply_nested() {
        // Three levels of nesting
        let content = "a {\n  b {\n    c {\n      x\n    }\n  }\n}";
        let (result, _) = apply_wildcard_edit(content, &["a {", "}"], "a {\n  new\n}").unwrap();
        assert_eq!(result, "a {\n  new\n}");
    }

    #[test]
    fn wildcard_balanced_multiple_same_level() {
        // Multiple blocks at same nesting level — should match after last inner close
        let content = "fn main() {\n    if a {\n        x\n    }\n    if b {\n        y\n    }\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn main() {", "}"], "fn main() {\n    z\n}").unwrap();
        assert_eq!(result, "fn main() {\n    z\n}");
    }

    #[test]
    fn wildcard_balanced_mixed_delimiters() {
        // Head opens `{` and middle contains `[]` and `()` — only `{}` depth matters for tail `}`
        let content = "fn foo() {\n    let v = vec![1, 2];\n    bar(v);\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    baz();\n}")
                .unwrap();
        assert_eq!(result, "fn foo() {\n    baz();\n}");
    }

    #[test]
    fn wildcard_no_delimiters_uses_first_match() {
        // No delimiters in head/tail — degrades to first-match
        let content = "// START\nfoo\n// END\nbar\n// END";
        let (result, replaced) =
            apply_wildcard_edit(content, &["// START", "// END"], "// START\nnew\n// END").unwrap();
        assert_eq!(result, "// START\nnew\n// END\nbar\n// END");
        assert_eq!(replaced, "// START\nfoo\n// END");
    }

    #[test]
    fn wildcard_balanced_real_world_navigation_array() {
        // The original bug: `]` matching `],` inside nested objects
        let content = r#"const nav = [
    { label: "Home", items: ["a", "b"] },
    { label: "About", items: ["c"] },
]"#;
        let (result, replaced) = apply_wildcard_edit(
            content,
            &["const nav = [", "]"],
            "const nav = [\n    { label: \"New\" },\n]",
        )
        .unwrap();
        assert_eq!(result, "const nav = [\n    { label: \"New\" },\n]");
        assert_eq!(replaced, content);
    }

    #[test]
    fn wildcard_balanced_empty_body() {
        // Tail immediately after head — empty middle
        let content = "fn empty() {}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn empty() {", "}"], "fn empty() { 42 }").unwrap();
        assert_eq!(result, "fn empty() { 42 }");
    }

    #[test]
    fn wildcard_balanced_parens() {
        // Paren balancing for function call arguments
        let content = "call(\n    inner(1, 2),\n    inner(3, 4),\n)";
        let (result, _) =
            apply_wildcard_edit(content, &["call(", ")"], "call(\n    inner(5),\n)").unwrap();
        assert_eq!(result, "call(\n    inner(5),\n)");
    }

    #[test]
    fn wildcard_balanced_preserves_after_match() {
        // Content after the matched region should be preserved
        let content = "fn a() {\n    if x {\n        y\n    }\n}\nfn b() {\n    z\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn a() {", "}"], "fn a() {\n    new\n}").unwrap();
        assert_eq!(result, "fn a() {\n    new\n}\nfn b() {\n    z\n}");
    }

    // --- apply_wildcard_edit: string/comment awareness ---

    #[test]
    fn wildcard_ignores_brace_in_double_quote_string() {
        let content = "fn foo() {\n    let x = \"}\";\n    real_code();\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    new();\n}")
                .unwrap();
        // Should match the outer }, not the } inside the string
        assert_eq!(result, "fn foo() {\n    new();\n}");
    }

    #[test]
    fn wildcard_ignores_bracket_in_string() {
        let content = "const arr = [\n    \"]not a bracket\",\n    real,\n]";
        let (result, _) = apply_wildcard_edit(
            content,
            &["const arr = [", "]"],
            "const arr = [\n    new,\n]",
        )
        .unwrap();
        assert_eq!(result, "const arr = [\n    new,\n]");
    }

    #[test]
    fn wildcard_ignores_brace_in_single_quote_string() {
        let content = "fn foo() {\n    let x = '}';\n    code();\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    new();\n}")
                .unwrap();
        assert_eq!(result, "fn foo() {\n    new();\n}");
    }

    #[test]
    fn wildcard_ignores_brace_in_line_comment() {
        let content = "fn foo() {\n    // } this is a comment\n    code();\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    new();\n}")
                .unwrap();
        assert_eq!(result, "fn foo() {\n    new();\n}");
    }

    #[test]
    fn wildcard_ignores_brace_in_block_comment() {
        let content = "fn foo() {\n    /* } not real */\n    code();\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    new();\n}")
                .unwrap();
        assert_eq!(result, "fn foo() {\n    new();\n}");
    }

    #[test]
    fn wildcard_handles_escaped_quote_in_string() {
        // The \" inside the string should not end the string
        let content = "fn foo() {\n    let x = \"\\\"}\";\n    code();\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    new();\n}")
                .unwrap();
        assert_eq!(result, "fn foo() {\n    new();\n}");
    }

    #[test]
    fn wildcard_multiple_strings_with_braces() {
        let content = "fn foo() {\n    log(\"{}\");\n    fmt(\"{}\");\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    new();\n}")
                .unwrap();
        assert_eq!(result, "fn foo() {\n    new();\n}");
    }

    #[test]
    fn wildcard_block_comment_with_nested_delimiters() {
        let content = "fn foo() {\n    /* { [ ( } ] ) */\n    code();\n}";
        let (result, _) =
            apply_wildcard_edit(content, &["fn foo() {", "}"], "fn foo() {\n    new();\n}")
                .unwrap();
        assert_eq!(result, "fn foo() {\n    new();\n}");
    }

    #[test]
    fn wildcard_no_false_tail_match_in_string() {
        // Tail text appears inside a string — should not match there
        let content = "fn start() {\n    let s = \"} end\";\n    code();\n} end";
        let (result, _) = apply_wildcard_edit(
            content,
            &["fn start() {", "} end"],
            "fn start() {\n    new();\n} end",
        )
        .unwrap();
        assert_eq!(result, "fn start() {\n    new();\n} end");
    }

    // --- Bug 1: unbalanced head/tail (gap-only depth tracking) ---

    #[test]
    fn wildcard_unbalanced_head_opens_more_than_tail_closes() {
        // Head opens 2 braces (function + if-let), tail closes 1.
        // This is the agent's exact failure case that motivated the fix.
        let content = "\
fn foo() {
    if x {
        body();
    }

    // next
    rest();
}";
        let (result, _) = apply_wildcard_edit(
            content,
            &["fn foo() {\n    if x {", "    }\n\n    // next"],
            "fn foo() {\n    if x {\n        new_body();\n    }\n\n    // next",
        )
        .unwrap();
        assert_eq!(
            result,
            "\
fn foo() {
    if x {
        new_body();
    }

    // next
    rest();
}"
        );
    }

    #[test]
    fn wildcard_unbalanced_head_in_function_tail_is_comment() {
        // Head opens { but tail has no delimiters — editing inside a scope
        let content = "fn main() {\n    // START\n    code();\n    // END\n}";
        let (result, _) = apply_wildcard_edit(
            content,
            &["fn main() {\n    // START", "    // END"],
            "fn main() {\n    // START\n    new_code();\n    // END",
        )
        .unwrap();
        assert_eq!(
            result,
            "fn main() {\n    // START\n    new_code();\n    // END\n}"
        );
    }

    #[test]
    fn wildcard_unbalanced_nested_three_levels() {
        // Head opens 3 levels, tail closes 1. Gap has balanced inner braces.
        let content = "mod m {\nfn f() {\nif true {\n    old();\n}\n}\n}";
        let (result, _) = apply_wildcard_edit(
            content,
            &["mod m {\nfn f() {\nif true {", "}"],
            "mod m {\nfn f() {\nif true {\n    new();\n}",
        )
        .unwrap();
        // Should match the first } after the gap returns to depth 0
        assert_eq!(result, "mod m {\nfn f() {\nif true {\n    new();\n}\n}\n}");
    }

    #[test]
    fn wildcard_head_opens_paren_tail_has_no_delimiters() {
        // Unbalanced parens: head opens (, tail is plain text
        let content = "call(\n    arg1,\n    arg2,\n    // done\n)";
        let (result, _) = apply_wildcard_edit(
            content,
            &["call(", "    // done"],
            "call(\n    new_arg,\n    // done",
        )
        .unwrap();
        assert_eq!(result, "call(\n    new_arg,\n    // done\n)");
    }

    // --- Bug 2: from_text string/comment awareness ---

    #[test]
    fn wildcard_from_text_ignores_brace_in_line_comment() {
        let d = DelimDepth::from_text("fn foo() { // {");
        assert_eq!(d.brace, 1);
    }

    #[test]
    fn wildcard_from_text_ignores_brace_in_string() {
        let d = DelimDepth::from_text(r#"let x = "{[";"#);
        assert_eq!(d.brace, 0);
        assert_eq!(d.bracket, 0);
    }

    #[test]
    fn wildcard_from_text_ignores_brace_in_block_comment() {
        let d = DelimDepth::from_text("start /* { [ ( */ end");
        assert_eq!(d.brace, 0);
        assert_eq!(d.bracket, 0);
        assert_eq!(d.paren, 0);
    }

    #[test]
    fn wildcard_from_text_ignores_escaped_quote() {
        // Escaped quote inside string should not end the string early
        let d = DelimDepth::from_text(r#"let x = "hello\"{";"#);
        assert_eq!(d.brace, 0);
    }

    #[test]
    fn wildcard_from_text_handles_single_quote_with_brace() {
        let d = DelimDepth::from_text("let c = '{';");
        assert_eq!(d.brace, 0);
    }

    #[test]
    fn wildcard_from_text_mixed_comments_and_strings() {
        // Multiple comment/string types in one line
        let d = DelimDepth::from_text(r#"fn f() { let s = "{"; /* } */ }"#);
        // Real braces: { at fn body, } at end = net 0
        // "{" brace and /* } */ brace are ignored
        assert_eq!(d.brace, 0);
    }

    #[test]
    fn wildcard_head_with_brace_in_comment() {
        // Head has { in a line comment — should not affect depth matching
        let content = "fn foo() { // {\n    body();\n}";
        let (result, _) = apply_wildcard_edit(
            content,
            &["fn foo() { // {", "}"],
            "fn foo() { // {\n    new_body();\n}",
        )
        .unwrap();
        assert_eq!(result, "fn foo() { // {\n    new_body();\n}");
    }

    #[test]
    fn wildcard_tail_with_brace_in_string() {
        // Tail contains a brace in a string literal — from_text should ignore it
        let content = "fn f() {\n    old();\n    let end = \"}\";\n}";
        let (result, _) = apply_wildcard_edit(
            content,
            &["fn f() {", "    let end = \"}\";\n}"],
            "fn f() {\n    new();\n    let end = \"}\";\n}",
        )
        .unwrap();
        assert_eq!(result, "fn f() {\n    new();\n    let end = \"}\";\n}");
    }

    #[test]
    fn wildcard_gap_only_depth_with_multiple_inner_scopes() {
        // Head opens 1 brace, gap has multiple balanced inner scopes
        let content = "\
impl Foo {
    fn a() {
        a_body();
    }
    fn b() {
        b_body();
    }
    fn c() {
        c_body();
    }
}";
        let (result, _) = apply_wildcard_edit(
            content,
            &["impl Foo {", "}"],
            "impl Foo {\n    fn new_method() {}\n}",
        )
        .unwrap();
        assert_eq!(result, "impl Foo {\n    fn new_method() {}\n}");
    }

    // --- Multi-gap wildcard tests ---

    #[test]
    fn parse_wildcard_multiple_separators() {
        let input = "head\n~~~~~\nmiddle\n~~~~~\ntail";
        let anchors = parse_wildcard(input).unwrap();
        assert_eq!(anchors, vec!["head", "middle", "tail"]);
    }

    #[test]
    fn parse_wildcard_empty_middle_anchor_returns_none() {
        // Two adjacent separators produce an empty middle segment
        assert!(parse_wildcard("head\n~~~~~\n\n~~~~~\ntail").is_none());
    }

    #[test]
    fn wildcard_multi_gap_basic() {
        let content = "aaa\nbbb\nccc\nddd\neee";
        let (result, replaced) =
            apply_wildcard_edit(content, &["aaa", "ccc", "eee"], "XXX").unwrap();
        assert_eq!(result, "XXX");
        assert_eq!(replaced, content);
    }

    #[test]
    fn wildcard_multi_gap_three_gaps() {
        let content = "A\n1\nB\n2\nC\n3\nD\n4\nE";
        let (result, replaced) =
            apply_wildcard_edit(content, &["A", "B", "C", "D", "E"], "replaced").unwrap();
        assert_eq!(result, "replaced");
        assert_eq!(replaced, content);
    }

    #[test]
    fn wildcard_multi_gap_balanced_braces() {
        // Each gap has nested delimiters
        let content = "\
fn a() {
    inner_a();
}
fn b() {
    if true {
        inner_b();
    }
}
fn c() {
    inner_c();
}";
        let (result, _) = apply_wildcard_edit(
            content,
            &["fn a() {", "}\nfn b() {", "}\nfn c() {", "}"],
            "fn combined() {\n    all();\n}",
        )
        .unwrap();
        assert_eq!(result, "fn combined() {\n    all();\n}");
    }

    #[test]
    fn wildcard_multi_gap_match_arms() {
        // The motivating use case: removing multiple match arms
        let content = "\
match cmd {
    \"foo\" => {
        do_foo();
    }
    \"bar\" => {
        do_bar();
    }
    \"baz\" => {
        do_baz();
    }
    _ => {}
}";
        let (result, replaced) = apply_wildcard_edit(
            content,
            &[
                "\"foo\" => {",
                "    }\n    \"bar\" => {",
                "    }\n    \"baz\" => {",
            ],
            "\"combined\" => {",
        )
        .unwrap();
        assert_eq!(
            result,
            "\
match cmd {
    \"combined\" => {
        do_baz();
    }
    _ => {}
}"
        );
        // replaced should span from "foo" through end of "baz" opening
        assert!(replaced.starts_with("\"foo\" => {"));
        assert!(replaced.ends_with("\"baz\" => {"));
    }

    #[test]
    fn wildcard_multi_gap_middle_anchor_not_found() {
        let content = "aaa\nbbb\nccc";
        let result = apply_wildcard_edit(content, &["aaa", "MISSING", "ccc"], "new");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Anchor 2 of 3"), "got: {}", err);
        assert!(err.contains("MISSING"));
    }

    #[test]
    fn wildcard_multi_gap_preserves_surrounding() {
        let content = "before\nA\n1\nB\n2\nC\nafter";
        let (result, replaced) = apply_wildcard_edit(content, &["A", "B", "C"], "X").unwrap();
        assert_eq!(result, "before\nX\nafter");
        assert_eq!(replaced, "A\n1\nB\n2\nC");
    }

    #[test]
    fn wildcard_single_gap_unchanged() {
        // Regression: existing 2-anchor behavior is preserved
        let content = "fn main() {\n    let x = 1;\n}";
        let (result, replaced) = apply_wildcard_edit(
            content,
            &["fn main() {", "}"],
            "fn main() {\n    let y = 2;\n}",
        )
        .unwrap();
        assert_eq!(result, "fn main() {\n    let y = 2;\n}");
        assert_eq!(replaced, content);
    }
}
