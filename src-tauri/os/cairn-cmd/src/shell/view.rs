//! Pure terminal presentation for shell reads.

use std::io::IsTerminal;

use cairn_common::read::{ReadBatchEnvelope, SegmentKind, SegmentMeta};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const DIM: &str = "\x1b[38;2;107;105;96m";
const ACCENT: &str = "\x1b[38;2;125;143;122m";
const ERROR: &str = "\x1b[38;2;248;113;113m";
const RESET: &str = "\x1b[0m";

pub(crate) fn terminal_width() -> usize {
    width_from(
        std::env::var("COLUMNS")
            .ok()
            .and_then(|value| value.parse().ok()),
        || {
            crossterm::terminal::size()
                .ok()
                .map(|(columns, _)| columns as usize)
        },
    )
}

fn width_from(override_width: Option<usize>, live: impl FnOnce() -> Option<usize>) -> usize {
    override_width
        .filter(|width| *width >= 40)
        .or_else(live)
        .filter(|width| *width >= 40)
        .unwrap_or(80)
}

pub(crate) fn render(envelope: &ReadBatchEnvelope, width: usize, color: bool) -> String {
    let bodies = envelope
        .bodies
        .as_ref()
        .filter(|bodies| bodies.len() == envelope.segments.len());
    let mut out = Vec::new();
    for (index, meta) in envelope.segments.iter().enumerate() {
        if index > 0 {
            out.push(String::new());
        }
        out.push(frame(meta, width, color));
        let fallback;
        let body = match bodies.and_then(|bodies| bodies.get(index)) {
            Some(body) => body.as_str(),
            None => {
                fallback = body_without_chrome(&envelope.text, &envelope.affordances);
                &fallback
            }
        };
        out.extend(render_body(body, meta.kind, width, color));
    }
    if let Some(spec) = envelope
        .affordances
        .iter()
        .find_map(|item| item.spec.as_ref())
    {
        out.push(String::new());
        let hint = format!(
            "? {} action{}   ⇥ complete{}",
            spec.actions.len(),
            if spec.actions.len() == 1 { "" } else { "s" },
            if envelope.segments.iter().any(|segment| segment.truncated) {
                "   ↵ next"
            } else {
                ""
            }
        );
        out.extend(
            wrap(&hint, width, 0)
                .into_iter()
                .map(|line| paint(&line, DIM, color)),
        );
    }
    out.join("\n").trim_end().to_string()
}

pub(crate) fn color_enabled() -> bool {
    std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
}

fn frame(meta: &SegmentMeta, width: usize, color: bool) -> String {
    let location = meta.uri.strip_prefix("cairn://").unwrap_or(&meta.uri);
    let count = match meta.total_units {
        Some(total) if meta.shown_units < total => format!("{} of {total}", meta.shown_units),
        Some(total) => total.to_string(),
        None => String::new(),
    };
    let reserved = UnicodeWidthStr::width(count.as_str()) + 3;
    let left = truncate(location, width.saturating_sub(reserved).max(10));
    let used = UnicodeWidthStr::width(left.as_str()) + UnicodeWidthStr::width(count.as_str());
    let rule = "─".repeat(width.saturating_sub(used + 2));
    paint(
        format!("{left} {rule} {count}").trim_end(),
        if meta.kind == SegmentKind::Error {
            ERROR
        } else {
            DIM
        },
        color,
    )
}

fn render_body(body: &str, kind: SegmentKind, width: usize, color: bool) -> Vec<String> {
    let mut out = Vec::new();
    for raw in body.lines() {
        if raw.starts_with("=== ")
            || raw.starts_with("continue:")
            || raw.starts_with('[') && raw.contains("continue:")
        {
            continue;
        }
        if raw.starts_with("### filters") || raw.starts_with("### actions") {
            break;
        }
        if matches!(kind, SegmentKind::File | SegmentKind::Grep) {
            // Source and grep output are preformatted data. Preserve indentation,
            // tabs, and columns; clipping is preferable to changing their meaning.
            out.push(truncate(&expand_tabs(raw, 8), width));
            continue;
        }
        let (indent, text, style) = if let Some(heading) = raw
            .strip_prefix("# ")
            .or_else(|| raw.strip_prefix("## "))
            .or_else(|| raw.strip_prefix("### "))
        {
            (0, clean_inline(heading), ACCENT)
        } else if let Some(item) = raw.strip_prefix("- ") {
            (2, format!("• {}", clean_inline(item)), "")
        } else {
            (0, clean_inline(raw), "")
        };
        for line in wrap(&text, width, indent) {
            out.push(paint(&line, style, color));
        }
    }
    while out.last().is_some_and(String::is_empty) {
        out.pop();
    }
    out
}

fn body_without_chrome(text: &str, affordances: &[cairn_common::read::Affordance]) -> String {
    let mut body = text;
    for block in affordances.iter().map(|item| item.block.as_str()) {
        let suffix = format!("\n\n{block}");
        if let Some(stripped) = body.strip_suffix(&suffix) {
            body = stripped;
        }
    }
    body.lines().skip(1).collect::<Vec<_>>().join("\n")
}

fn clean_inline(input: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '[' {
            if let Some(close) = chars[i + 1..].iter().position(|c| *c == ']') {
                let close = i + 1 + close;
                if chars.get(close + 1) == Some(&'(') {
                    if let Some(end) = chars[close + 2..].iter().position(|c| *c == ')') {
                        out.extend(chars[i + 1..close].iter());
                        i = close + 3 + end;
                        continue;
                    }
                }
            }
        }
        if chars[i] != '`' && chars[i] != '*' {
            out.push(chars[i]);
        }
        i += 1;
    }
    out
}

fn wrap(text: &str, width: usize, indent: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let available = width.saturating_sub(indent).max(1);
    let prefix = " ".repeat(indent);
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        let next = UnicodeWidthStr::width(current.as_str())
            + usize::from(!current.is_empty())
            + UnicodeWidthStr::width(word);
        if next > available && !current.is_empty() {
            lines.push(format!("{prefix}{current}"));
            current.clear();
        }
        if UnicodeWidthStr::width(word) > available {
            for ch in word.chars() {
                if UnicodeWidthStr::width(current.as_str()) + ch.width().unwrap_or(0) > available {
                    lines.push(format!("{prefix}{current}"));
                    current.clear();
                }
                current.push(ch);
            }
        } else {
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(word);
        }
    }
    if !current.is_empty() {
        lines.push(format!("{prefix}{current}"));
    }
    lines
}

fn expand_tabs(text: &str, tab_stop: usize) -> String {
    let mut out = String::new();
    let mut column = 0;
    for ch in text.chars() {
        if ch == '\t' {
            let spaces = tab_stop - column % tab_stop;
            out.extend(std::iter::repeat_n(' ', spaces));
            column += spaces;
        } else {
            out.push(ch);
            column += ch.width().unwrap_or(0);
        }
    }
    out
}

fn truncate(text: &str, width: usize) -> String {
    if UnicodeWidthStr::width(text) <= width {
        return text.to_string();
    }
    let mut out = String::new();
    for ch in text.chars() {
        if UnicodeWidthStr::width(out.as_str()) + ch.width().unwrap_or(0) + 1 > width {
            break;
        }
        out.push(ch);
    }
    out.push('…');
    out
}

fn paint(text: &str, style: &str, color: bool) -> String {
    if color && !style.is_empty() {
        format!("{style}{text}{RESET}")
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::read::{Affordance, AffordanceSpec, NaturalUnit};

    #[test]
    fn strips_markdown_and_affordances_and_obeys_width() {
        let mut meta = SegmentMeta::new(
            "cairn://p/cairn/issues",
            SegmentKind::Resource,
            NaturalUnit::Record,
        );
        meta.shown_units = 1;
        meta.total_units = Some(20);
        meta.truncated = true;
        let envelope = ReadBatchEnvelope {
            text: "=== cairn://p/cairn/issues [1 of 20 issues] ===\n# Issues\n\n- [cairn-1](cairn://p/cairn/1) a deliberately long title\n\n## Issues\n\n### actions\n- raw agent docs".into(),
            images: Vec::new(), segments: vec![meta], bodies: Some(vec!["# Issues\n\n- [cairn-1](cairn://p/cairn/1) a deliberately long title".into()]),
            affordances: vec![Affordance { kind: SegmentKind::Resource, block: "## Issues\n\n### actions\n- raw agent docs".into(), spec: Some(AffordanceSpec { kind: "issues".into(), name: "Issues".into(), links: Vec::new(), filters: Vec::new(), actions: Vec::new() }) }],
        };
        let rendered = render(&envelope, 32, false);
        assert!(!rendered.contains("###"));
        assert!(!rendered.contains("cairn://p/cairn/1"));
        assert!(
            rendered
                .lines()
                .all(|line| UnicodeWidthStr::width(line) <= 32),
            "{rendered}"
        );
    }

    #[test]
    fn preformatted_segments_preserve_whitespace_and_clip_at_the_edge() {
        let lines = render_body(
            "\tfn main() {\n    let  value = 1;",
            SegmentKind::File,
            16,
            false,
        );
        assert_eq!(lines[0], "        fn main…");
        assert_eq!(lines[1], "    let  value …");
        assert!(lines
            .iter()
            .all(|line| { !line.contains('\t') && UnicodeWidthStr::width(line.as_str()) <= 16 }));

        let grep = render_body("path.rs\n12:alpha   beta", SegmentKind::Grep, 40, false);
        assert_eq!(grep, ["path.rs", "12:alpha   beta"]);
    }

    #[test]
    fn width_provider_prefers_override_then_live_terminal_then_fallback() {
        assert_eq!(width_from(Some(120), || Some(60)), 120);
        assert_eq!(width_from(None, || Some(60)), 60);
        assert_eq!(width_from(Some(20), || Some(72)), 72);
        assert_eq!(width_from(None, || None), 80);
    }
}
