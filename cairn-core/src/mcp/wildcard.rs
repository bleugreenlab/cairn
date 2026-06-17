//! Wildcard-anchored text matching and replacement.
//!
//! `old_string` may contain one or more `~~*~~` markers. Each marker punches a
//! "hole" between two literal anchor segments: the text is split on unescaped
//! markers into N+1 anchors, and the edit replaces everything from the start of
//! the first anchor's match through the end of the last anchor's match.
//!
//! For each gap (between an already-matched anchor and the next anchor to find)
//! the marker-facing edges are whitespace-trimmed for matching, and the gap is
//! resolved one of two ways:
//!
//! - **Balanced** — opt-in, only when the marker is written as the contiguous
//!   `{~~*~~}` token: an opener (`{`/`[`/`(`) sits immediately before the marker
//!   and the matching closer immediately after it, with no intervening newline
//!   or whitespace. The closer is located by local single-pair depth matching
//!   (string/comment aware), so nested delimiters inside the hole are skipped.
//!   `"const arr = [~~*~~]"` matches the outer `]`. (An empty trailing anchor —
//!   `"fn f() {~~*~~"` — has no literal tail to span to, so it always balances.)
//! - **Span** — every other form, including the own-line `"fn f() {\n~~*~~\n}"`.
//!   The next anchor is found at its first literal occurrence after the cursor.
//!   No delimiter counting; balanced mode is never inferred from the characters
//!   the anchors happen to end and begin with.
//!
//! Whitespace at marker-facing edges is absorbed into the replaced region (and
//! reproduced via `new_string`), so the own-line (`"fn f() {\n~~*~~\n}"`) and
//! tight (`"fn f() {~~*~~}"`) forms match the same anchor text — but the marker
//! form selects the gap strategy: only the contiguous `{~~*~~}` balances.
//!
//! A `~~*~~` immediately preceded by a backslash (`\~~*~~`) is treated as
//! literal content, not a marker; the backslash is stripped from the anchor.

/// The wildcard marker used in `old_string` for anchored matching.
pub const WILDCARD_TOKEN: &str = "~~*~~";

/// Scan mode for lightweight tokenization during balanced matching.
/// Delimiters inside strings and comments are ignored.
#[derive(Clone, Copy, PartialEq)]
enum ScanMode {
    /// Normal code — track delimiters, check for closer
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

/// Split `old_string` into anchors around unescaped `~~*~~` markers.
///
/// Returns `None` when no unescaped marker is present (the caller falls back to
/// literal matching). Escaped markers (`\~~*~~`) are folded into the surrounding
/// anchor as literal `~~*~~` with the backslash removed.
///
/// Empty-anchor rules:
/// - A truly-empty *leading* anchor is rejected (no starting anchor).
/// - Empty *middle* anchors are rejected.
/// - An empty *trailing* anchor is permitted only when the preceding anchor's
///   trimmed right edge ends with an opener (`{`/`[`/`(`) — balance supplies the
///   matching closer.
pub fn parse_wildcard(old_string: &str) -> Option<Vec<String>> {
    let mut segments: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut found_separator = false;
    let mut rest = old_string;

    while let Some(pos) = rest.find(WILDCARD_TOKEN) {
        current.push_str(&rest[..pos]);
        if current.ends_with('\\') {
            // Escaped marker: literal `~~*~~`, drop the backslash.
            current.pop();
            current.push_str(WILDCARD_TOKEN);
        } else {
            found_separator = true;
            segments.push(std::mem::take(&mut current));
        }
        rest = &rest[pos + WILDCARD_TOKEN.len()..];
    }
    current.push_str(rest);
    segments.push(current);

    if !found_separator {
        return None;
    }

    let n = segments.len();
    for (idx, seg) in segments.iter().enumerate() {
        if seg.is_empty() {
            let is_trailing = idx == n - 1;
            let prev_opener = idx > 0 && segments[idx - 1].trim_end().ends_with(['{', '[', '(']);
            if !(is_trailing && prev_opener) {
                return None;
            }
        }
    }

    Some(segments)
}

/// Strip escaping backslashes from `\~~*~~` sequences, yielding the literal text
/// to match when `old_string` is not a wildcard edit (so an escaped marker still
/// targets a literal `~~*~~` in the file).
pub fn unescape_literal(old: &str) -> String {
    old.replace("\\~~*~~", WILDCARD_TOKEN)
}

/// Locate the closer that balances an opener at depth 1 starting just past the
/// opener (`start`). Tracks only the single flanking pair, skipping delimiters
/// inside strings and comments. Returns the byte position of the matching
/// closer, or `None` if depth never returns to zero.
fn find_balanced_closer(content: &str, start: usize, opener: u8, closer: u8) -> Option<usize> {
    let bytes = content.as_bytes();
    let mut depth: i32 = 1;
    let mut mode = ScanMode::Code;
    let mut i = start;

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
                } else if bytes[i] == opener {
                    depth += 1;
                } else if bytes[i] == closer {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
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
    None
}

/// Collapse all runs of whitespace to a single space (and trim ends).
fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Whether `anchor` is absent literally but present after whitespace
/// normalization — i.e. the only difference is indentation/spacing.
fn is_whitespace_near_miss(haystack: &str, anchor: &str) -> bool {
    let needle = collapse_ws(anchor);
    !needle.is_empty() && !haystack.contains(anchor) && collapse_ws(haystack).contains(&needle)
}

/// The closer byte that balances an opener, if it is one of `{`/`[`/`(`.
fn matching_closer(opener: u8) -> Option<u8> {
    match opener {
        b'{' => Some(b'}'),
        b'[' => Some(b']'),
        b'(' => Some(b')'),
        _ => None,
    }
}

/// Find the anchored region in `content` and produce the edited content along
/// with the replaced span.
///
/// `anchors` holds N segments (N >= 2) produced by [`parse_wildcard`]. The first
/// anchor's right edge and the last anchor's left edge each face one marker;
/// middle anchors face a marker on both sides. Those marker-facing edges are
/// whitespace-trimmed before matching. Each gap is resolved as balanced or span
/// per the module docs.
pub fn apply_wildcard_edit<S: AsRef<str>>(
    content: &str,
    anchors: &[S],
    new_string: &str,
) -> Result<(String, String), String> {
    let n = anchors.len();
    assert!(n >= 2, "need at least 2 anchors");
    let is_single_gap = n == 2;

    // Trim each anchor's marker-facing edge(s).
    let eff: Vec<&str> = anchors
        .iter()
        .enumerate()
        .map(|(i, a)| {
            let s = a.as_ref();
            if i == 0 {
                s.trim_end()
            } else if i == n - 1 {
                s.trim_start()
            } else {
                s.trim()
            }
        })
        .collect();

    let head = eff[0];
    let head_start = content.find(head).ok_or_else(|| {
        if is_whitespace_near_miss(content, head) {
            format!(
                "Head anchor not found exactly, but a match exists after whitespace normalization — check indentation and spacing.\nSearching for:\n{head}"
            )
        } else {
            format!(
                "Head anchor not found in file. Make sure the text before the first `~~*~~` matches exactly.\nSearching for:\n{head}"
            )
        }
    })?;
    let mut cursor = head_start + head.len();

    for i in 1..n {
        let next = eff[i];

        // Balanced mode is opt-in via the contiguous `{~~*~~}` form: the raw
        // (untrimmed) anchors must place an opener immediately before the marker
        // and the matching closer immediately after it. A plain marker — notably
        // the own-line `{\n~~*~~\n}` form, where a newline separates the marker
        // from the delimiters — always spans, regardless of what characters the
        // anchors happen to end and begin with. (An empty trailing anchor has no
        // literal tail to span to, so it always balances.)
        let raw_prev = anchors[i - 1].as_ref();
        let raw_next = anchors[i].as_ref();
        let opener = raw_prev.as_bytes().last().copied();
        let closer = opener.and_then(matching_closer);
        let is_balanced = match closer {
            Some(c) => next.is_empty() || raw_next.as_bytes().first() == Some(&c),
            None => false,
        };

        let anchor_start = if is_balanced {
            let opener_b = opener.expect("opener present when balanced");
            let closer_b = closer.expect("closer present when balanced");
            match find_balanced_closer(content, cursor, opener_b, closer_b) {
                Some(p) if content[p..].starts_with(next) => p,
                _ => {
                    return Err(format!(
                        "Could not balance the `{}` ending the preceding anchor to a matching `{}`{}.\nSearching for:\n{}",
                        opener_b as char,
                        closer_b as char,
                        if next.is_empty() {
                            String::new()
                        } else {
                            " followed by the next anchor text".to_string()
                        },
                        next
                    ));
                }
            }
        } else {
            match content[cursor..].find(next) {
                Some(off) => cursor + off,
                None => {
                    let base = if is_single_gap {
                        format!(
                            "Tail anchor not found after head anchor. Make sure the text after `~~*~~` matches exactly.\nSearching for:\n{next}"
                        )
                    } else {
                        format!(
                            "Anchor {} of {} not found after previous anchor.\nSearching for:\n{}",
                            i + 1,
                            n,
                            next
                        )
                    };
                    return Err(if is_whitespace_near_miss(&content[cursor..], next) {
                        format!(
                            "{base}\n(Note: a match exists after whitespace normalization — check indentation and spacing.)"
                        )
                    } else {
                        base
                    });
                }
            }
        };

        let matched_len = if is_balanced && next.is_empty() {
            1 // consume the supplied closer
        } else {
            next.len()
        };
        cursor = anchor_start + matched_len;
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

    fn assert_wildcard_result<S: AsRef<str>>(
        content: &str,
        anchors: &[S],
        replacement: &str,
        expected: &str,
    ) {
        let (result, _) = apply_wildcard_edit(content, anchors, replacement).unwrap();
        assert_eq!(result, expected);
    }

    // --- parse_wildcard tests ---

    #[test]
    fn parse_wildcard_basic() {
        let anchors = parse_wildcard("head line~~*~~tail line").unwrap();
        assert_eq!(anchors, vec!["head line", "tail line"]);
    }

    #[test]
    fn parse_wildcard_own_line_form() {
        let anchors = parse_wildcard("head\n~~*~~\ntail").unwrap();
        assert_eq!(anchors, vec!["head\n", "\ntail"]);
    }

    #[test]
    fn parse_wildcard_multiline_anchors() {
        let anchors = parse_wildcard("line1\nline2~~*~~line4\nline5").unwrap();
        assert_eq!(anchors, vec!["line1\nline2", "line4\nline5"]);
    }

    #[test]
    fn parse_wildcard_no_marker() {
        assert!(parse_wildcard("just normal text").is_none());
    }

    #[test]
    fn parse_wildcard_old_token_not_matched() {
        // The retired `~~~~~` token is no longer recognized.
        assert!(parse_wildcard("head\n~~~~~\ntail").is_none());
    }

    #[test]
    fn parse_wildcard_leading_empty_rejected() {
        assert!(parse_wildcard("~~*~~tail").is_none());
    }

    #[test]
    fn parse_wildcard_trailing_empty_without_opener_rejected() {
        assert!(parse_wildcard("head~~*~~").is_none());
    }

    #[test]
    fn parse_wildcard_trailing_empty_with_opener_allowed() {
        let anchors = parse_wildcard("HEAD {~~*~~").unwrap();
        assert_eq!(anchors, vec!["HEAD {", ""]);
    }

    #[test]
    fn parse_wildcard_trailing_empty_with_trailing_ws_opener_allowed() {
        // Trimmed right edge ends with an opener even though raw ends in newline.
        let anchors = parse_wildcard("HEAD {\n~~*~~").unwrap();
        assert_eq!(anchors, vec!["HEAD {\n", ""]);
    }

    #[test]
    fn parse_wildcard_empty_middle_anchor_rejected() {
        assert!(parse_wildcard("head~~*~~~~*~~tail").is_none());
    }

    #[test]
    fn parse_wildcard_multiple_markers() {
        let anchors = parse_wildcard("head~~*~~middle~~*~~tail").unwrap();
        assert_eq!(anchors, vec!["head", "middle", "tail"]);
    }

    #[test]
    fn parse_wildcard_escaped_marker_is_literal() {
        // A single escaped marker, no real separator → not a wildcard edit.
        let result = parse_wildcard("keep \\~~*~~ literal");
        assert!(result.is_none());
    }

    #[test]
    fn parse_wildcard_escaped_and_real_marker() {
        let anchors = parse_wildcard("a\\~~*~~b~~*~~c").unwrap();
        assert_eq!(anchors, vec!["a~~*~~b", "c"]);
    }

    #[test]
    fn unescape_literal_strips_backslash() {
        assert_eq!(unescape_literal("keep \\~~*~~ here"), "keep ~~*~~ here");
        // No escape → unchanged.
        assert_eq!(unescape_literal("plain text"), "plain text");
        // A bare marker is left as-is (the dispatch handles it separately).
        assert_eq!(unescape_literal("a ~~*~~ b"), "a ~~*~~ b");
    }

    // --- apply_wildcard_edit: span (default) behavior ---

    #[test]
    fn wildcard_specimen_span_delete_and_replace() {
        // The motivating specimen: head ends in `{`, tail begins with non-closer
        // (`pub`). Global balance produced an unsatisfiable brace constraint;
        // span finds the first literal occurrence of the tail and succeeds.
        let content = "\
fn lookup_project_by_cwd() {
    old_impl();
}

pub async fn handle_read_resource() {
    body();
}";
        let (result, replaced) = apply_wildcard_edit(
            content,
            &["fn lookup_project_by_cwd() {", "pub async fn handle_read_resource("],
            "fn lookup_project_by_cwd() {\n    new_impl();\n}\n\npub async fn handle_read_resource(",
        )
        .unwrap();
        assert_eq!(
            result,
            "\
fn lookup_project_by_cwd() {
    new_impl();
}

pub async fn handle_read_resource() {
    body();
}"
        );
        assert!(replaced.starts_with("fn lookup_project_by_cwd() {"));
        assert!(replaced.ends_with("pub async fn handle_read_resource("));
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
    fn wildcard_uses_first_head_match() {
        let content = "fn a() {X}\nfn a() {Y}";
        let (result, replaced) =
            apply_wildcard_edit(content, &["fn a() {", "}"], "fn a() {Z}").unwrap();
        assert_eq!(result, "fn a() {Z}\nfn a() {Y}");
        assert_eq!(replaced, "fn a() {X}");
    }

    #[test]
    fn wildcard_no_delimiters_uses_first_match() {
        let content = "// START\nfoo\n// END\nbar\n// END";
        let (result, replaced) =
            apply_wildcard_edit(content, &["// START", "// END"], "// START\nnew\n// END").unwrap();
        assert_eq!(result, "// START\nnew\n// END\nbar\n// END");
        assert_eq!(replaced, "// START\nfoo\n// END");
    }

    #[test]
    fn wildcard_adjacent_anchors() {
        let content = "aXb";
        let (result, replaced) = apply_wildcard_edit(content, &["a", "b"], "a_new_b").unwrap();
        assert_eq!(result, "a_new_b");
        assert_eq!(replaced, "aXb");
    }

    // --- apply_wildcard_edit: whitespace absorption ---

    #[test]
    fn wildcard_whitespace_own_line_and_tight_forms_match() {
        // No nested delimiters here, so the own-line (span) and tight (balanced)
        // forms land on the same single `}` and produce the same result; they
        // diverge only when the gap contains nested delimiters (see the own-line
        // span tests below).
        let content = "fn main() {\n    let x = 1;\n}";
        let new = "fn main() {\n    let z = 42;\n}";

        // own-line marker form
        let own = parse_wildcard("fn main() {\n~~*~~\n}").unwrap();
        assert_wildcard_result(content, &own, new, new);

        // tight marker form
        let tight = parse_wildcard("fn main() {~~*~~}").unwrap();
        assert_wildcard_result(content, &tight, new, new);
    }

    // --- apply_wildcard_edit: balanced delimiter matching ---

    #[test]
    fn wildcard_balanced_braces_skips_inner() {
        let content = "fn main() {\n    if true {\n        x\n    }\n    y\n}";
        let (result, replaced) =
            apply_wildcard_edit(content, &["fn main() {", "}"], "fn main() {\n    z\n}").unwrap();
        assert_eq!(result, "fn main() {\n    z\n}");
        assert_eq!(replaced, content);
    }

    #[test]
    fn wildcard_balanced_brackets_skips_inner() {
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
        let content = "a {\n  b {\n    c {\n      x\n    }\n  }\n}";
        assert_wildcard_result(content, &["a {", "}"], "a {\n  new\n}", "a {\n  new\n}");
    }

    #[test]
    fn wildcard_balanced_multiple_same_level() {
        let content = "fn main() {\n    if a {\n        x\n    }\n    if b {\n        y\n    }\n}";
        assert_wildcard_result(
            content,
            &["fn main() {", "}"],
            "fn main() {\n    z\n}",
            "fn main() {\n    z\n}",
        );
    }

    #[test]
    fn wildcard_balanced_mixed_delimiters() {
        let content = "fn foo() {\n    let v = vec![1, 2];\n    bar(v);\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    baz();\n}",
            "fn foo() {\n    baz();\n}",
        );
    }

    #[test]
    fn wildcard_balanced_real_world_navigation_array() {
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
        let content = "fn empty() {}";
        assert_wildcard_result(
            content,
            &["fn empty() {", "}"],
            "fn empty() { 42 }",
            "fn empty() { 42 }",
        );
    }

    #[test]
    fn wildcard_balanced_parens() {
        let content = "call(\n    inner(1, 2),\n    inner(3, 4),\n)";
        assert_wildcard_result(
            content,
            &["call(", ")"],
            "call(\n    inner(5),\n)",
            "call(\n    inner(5),\n)",
        );
    }

    #[test]
    fn wildcard_balanced_preserves_after_match() {
        let content = "fn a() {\n    if x {\n        y\n    }\n}\nfn b() {\n    z\n}";
        assert_wildcard_result(
            content,
            &["fn a() {", "}"],
            "fn a() {\n    new\n}",
            "fn a() {\n    new\n}\nfn b() {\n    z\n}",
        );
    }

    #[test]
    fn wildcard_balanced_tail_after_head() {
        let content = "} early\nfn start() {\n    body\n} end";
        assert_wildcard_result(
            content,
            &["fn start() {", "} end"],
            "fn start() {\n    new\n} end",
            "} early\nfn start() {\n    new\n} end",
        );
    }

    // --- apply_wildcard_edit: empty trailing anchor with head opener ---

    #[test]
    fn wildcard_trailing_empty_anchor_consumes_closer() {
        let content = "fn main() {\n    body();\n}\nafter";
        let anchors = parse_wildcard("fn main() {~~*~~").unwrap();
        let (result, replaced) =
            apply_wildcard_edit(content, &anchors, "fn main() {\n    new();\n}").unwrap();
        assert_eq!(result, "fn main() {\n    new();\n}\nafter");
        assert_eq!(replaced, "fn main() {\n    body();\n}");
    }

    // --- apply_wildcard_edit: leading bare-opener form ---

    #[test]
    fn wildcard_leading_bare_opener() {
        let content = "prefix {\n    inner();\n} suffix";
        let anchors = parse_wildcard("{~~*~~}").unwrap();
        assert_eq!(anchors, vec!["{", "}"]);
        let (result, replaced) = apply_wildcard_edit(content, &anchors, "{ new }").unwrap();
        assert_eq!(result, "prefix { new } suffix");
        assert_eq!(replaced, "{\n    inner();\n}");
    }

    // --- apply_wildcard_edit: string/comment awareness (balanced) ---

    #[test]
    fn wildcard_ignores_brace_in_double_quote_string() {
        let content = "fn foo() {\n    let x = \"}\";\n    real_code();\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new();\n}",
            "fn foo() {\n    new();\n}",
        );
    }

    #[test]
    fn wildcard_ignores_bracket_in_string() {
        let content = "const arr = [\n    \"]not a bracket\",\n    real,\n]";
        assert_wildcard_result(
            content,
            &["const arr = [", "]"],
            "const arr = [\n    new,\n]",
            "const arr = [\n    new,\n]",
        );
    }

    #[test]
    fn wildcard_ignores_brace_in_single_quote_string() {
        let content = "fn foo() {\n    let x = '}';\n    code();\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new();\n}",
            "fn foo() {\n    new();\n}",
        );
    }

    #[test]
    fn wildcard_ignores_brace_in_line_comment() {
        let content = "fn foo() {\n    // } this is a comment\n    code();\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new();\n}",
            "fn foo() {\n    new();\n}",
        );
    }

    #[test]
    fn wildcard_ignores_brace_in_block_comment() {
        let content = "fn foo() {\n    /* } not real */\n    code();\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new();\n}",
            "fn foo() {\n    new();\n}",
        );
    }

    #[test]
    fn wildcard_handles_escaped_quote_in_string() {
        let content = "fn foo() {\n    let x = \"\\\"}\";\n    code();\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new();\n}",
            "fn foo() {\n    new();\n}",
        );
    }

    #[test]
    fn wildcard_multiple_strings_with_braces() {
        let content = "fn foo() {\n    log(\"{}\");\n    fmt(\"{}\");\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new();\n}",
            "fn foo() {\n    new();\n}",
        );
    }

    #[test]
    fn wildcard_block_comment_with_nested_delimiters() {
        let content = "fn foo() {\n    /* { [ ( } ] ) */\n    code();\n}";
        assert_wildcard_result(
            content,
            &["fn foo() {", "}"],
            "fn foo() {\n    new();\n}",
            "fn foo() {\n    new();\n}",
        );
    }

    #[test]
    fn wildcard_no_false_closer_match_in_string() {
        let content = "fn start() {\n    let s = \"} end\";\n    code();\n} end";
        assert_wildcard_result(
            content,
            &["fn start() {", "} end"],
            "fn start() {\n    new();\n} end",
            "fn start() {\n    new();\n} end",
        );
    }

    #[test]
    fn wildcard_head_with_brace_in_comment() {
        let content = "fn foo() { // {\n    body();\n}";
        assert_wildcard_result(
            content,
            &["fn foo() { // {", "}"],
            "fn foo() { // {\n    new_body();\n}",
            "fn foo() { // {\n    new_body();\n}",
        );
    }

    #[test]
    fn wildcard_gap_with_multiple_inner_scopes() {
        let content = "\
impl Foo {
    fn a() {
        a_body();
    }
    fn b() {
        b_body();
    }
}";
        assert_wildcard_result(
            content,
            &["impl Foo {", "}"],
            "impl Foo {\n    fn new_method() {}\n}",
            "impl Foo {\n    fn new_method() {}\n}",
        );
    }

    // --- multi-gap tests ---

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
        assert_wildcard_result(
            content,
            &["fn a() {", "}\nfn b() {", "}\nfn c() {", "}"],
            "fn combined() {\n    all();\n}",
            "fn combined() {\n    all();\n}",
        );
    }

    #[test]
    fn wildcard_multi_gap_match_arms() {
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
        assert!(replaced.starts_with("\"foo\" => {"));
        assert!(replaced.ends_with("\"baz\" => {"));
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

    // --- error messages ---

    #[test]
    fn wildcard_head_not_found() {
        let result = apply_wildcard_edit("some content", &["nonexistent", "also missing"], "new");
        let err = result.unwrap_err();
        assert!(err.contains("Head anchor not found"), "got: {err}");
    }

    #[test]
    fn wildcard_head_whitespace_near_miss() {
        // Head differs only by internal spacing.
        let result = apply_wildcard_edit("fn  main() {\n  x\n}", &["fn main() {", "}"], "new");
        let err = result.unwrap_err();
        assert!(err.contains("whitespace"), "got: {err}");
    }

    #[test]
    fn wildcard_tail_not_found() {
        let result = apply_wildcard_edit("some content here", &["some", "nonexistent"], "new");
        let err = result.unwrap_err();
        assert!(err.contains("Tail anchor not found"), "got: {err}");
    }

    #[test]
    fn wildcard_tail_whitespace_near_miss() {
        // Tail (span) differs only by internal spacing.
        let result = apply_wildcard_edit("head then  tail here", &["head", "then tail"], "new");
        let err = result.unwrap_err();
        assert!(err.contains("whitespace"), "got: {err}");
    }

    #[test]
    fn wildcard_multi_gap_middle_anchor_not_found() {
        let result = apply_wildcard_edit("aaa\nbbb\nccc", &["aaa", "MISSING", "ccc"], "new");
        let err = result.unwrap_err();
        assert!(err.contains("Anchor 2 of 3"), "got: {err}");
        assert!(err.contains("MISSING"), "got: {err}");
    }

    #[test]
    fn wildcard_balanced_closer_not_found() {
        // Tight `{~~*~~}` opts into balance, but there is no balancing `}` in the
        // content — a hard balance error (balance was explicitly requested).
        let result = apply_wildcard_edit("fn main() {\n    body();", &["fn main() {", "}"], "new");
        let err = result.unwrap_err();
        assert!(err.contains("Could not balance"), "got: {err}");
    }

    #[test]
    fn wildcard_balanced_empty_trailing_no_closer_errors() {
        // With an empty trailing anchor there is no literal tail to span to, so a
        // missing balancing closer is a hard balance error.
        let anchors = parse_wildcard("fn main() {~~*~~").unwrap();
        let result = apply_wildcard_edit("fn main() {\n    body();", &anchors, "new");
        let err = result.unwrap_err();
        assert!(err.contains("Could not balance"), "got: {err}");
    }

    #[test]
    fn wildcard_tight_form_opts_into_balanced() {
        // The contiguous `{~~*~~}` token balances and skips nested delimiters.
        let content = "fn main() {\n    if true {\n        x\n    }\n    y\n}";
        let anchors = parse_wildcard("fn main() {~~*~~}").unwrap();
        assert_wildcard_result(content, &anchors, "fn main() {\n    z\n}", "fn main() {\n    z\n}");
    }

    #[test]
    fn wildcard_own_line_form_spans_not_balances() {
        // The own-line `{\n~~*~~\n}` form is a plain span: it stops at the FIRST
        // `}` after the head, never inferring balance from the anchor edges. (Use
        // the contiguous `{~~*~~}` to balance and skip nested delimiters.)
        let content = "fn main() {\n    if true {\n        x\n    }\n    y\n}";
        let anchors = parse_wildcard("fn main() {\n~~*~~\n}").unwrap();
        let (result, replaced) = apply_wildcard_edit(content, &anchors, "REPL").unwrap();
        assert_eq!(result, "REPL\n    y\n}");
        assert_eq!(replaced, "fn main() {\n    if true {\n        x\n    }");
    }

    #[test]
    fn wildcard_own_line_form_spans_past_unmodeled_braces() {
        // The motivating repro: an own-line marker whose head ends in `{` and tail
        // begins with `}` only incidentally. Because it is a plain span, the brace
        // scanner (which does not model the `}` inside the template literal) never
        // runs, and the edit resolves against the literal tail instead of failing
        // with a balance error.
        let content = "\
const f = useCallback(() => {
    const s = `pre } post`;
    doThing();
}, [dep]);
after();";
        let anchors =
            parse_wildcard("const f = useCallback(() => {\n~~*~~\n}, [dep]);").unwrap();
        let (result, replaced) = apply_wildcard_edit(
            content,
            &anchors,
            "const f = useCallback(() => {\n    newBody();\n}, [dep]);",
        )
        .unwrap();
        assert_eq!(
            result,
            "\
const f = useCallback(() => {
    newBody();
}, [dep]);
after();"
        );
        assert!(replaced.starts_with("const f = useCallback(() => {"));
        assert!(replaced.ends_with("}, [dep]);"));
    }
}
