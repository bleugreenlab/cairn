//! The trailing `## References` block a message carries for the URIs it names.
//!
//! A message that cites `cairn://p/cairn/execution/chat/turn/1758` should not
//! need its author to paste the operator's words beside the URI: every paste is
//! a copy that drifts from the one place the words live. Instead the reader is
//! handed one bounded block, appended after the author's own prose, that says
//! what each cited URI is. A chat turn or a comment is quoted verbatim with its
//! author and time, an issue or node is previewed in one line, and a file is
//! named with its line range.
//!
//! This module is the data and the text form of that block, and nothing that
//! needs a database. Which entry a URI resolves to is decided once, by
//! cairn-core's resolver, and both readers consume that one answer: an agent
//! receives [`render_references_block`] appended to its delivered message, and
//! the transcript renders the same [`References`] from a read-only invoke
//! command. Neither reader re-derives an entry, so they cannot disagree.
//!
//! The bounds are part of the contract, because the block rides in prompts
//! under the launch lock: at most [`MAX_REFERENCE_ENTRIES`] entries with a
//! count of the rest, a quotation cut at [`QUOTATION_CHARS`] with the URI that
//! holds the remainder, and no recursion, since an earlier block or a quoted
//! passage is never itself scanned for references.

use serde::{Deserialize, Serialize};

use crate::contract::{contract_for, ResourceKind};

/// The heading that opens the block. It is also how a reader recognizes a
/// block already appended to a text, so it is never scanned or appended twice.
pub const REFERENCES_HEADING: &str = "## References";

/// How many entries one block shows before it counts the rest.
pub const MAX_REFERENCE_ENTRIES: usize = 8;

/// How much of a quoted passage the block carries before it elides the rest and
/// points at the URI that holds it.
pub const QUOTATION_CHARS: usize = 600;

/// What one cited URI means to a reader who has not opened it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ReferenceEntry {
    /// Somebody's words, verbatim: a chat turn, an issue comment, a post
    /// comment. `elided` says `text` stops short of the whole passage.
    Quotation {
        uri: String,
        /// The quoted resource's kind, as its [`ResourceKind`] slug.
        resource: String,
        author: String,
        /// When the words were written, epoch seconds.
        at: Option<i64>,
        text: String,
        elided: bool,
    },
    /// A resource's current state in one line: never its whole body.
    Preview {
        uri: String,
        /// The previewed resource's kind, as its [`ResourceKind`] slug.
        resource: String,
        title: String,
        status: Option<String>,
        line: Option<String>,
        /// When the resource last moved, epoch seconds.
        at: Option<i64>,
    },
    /// A file target: its path and line range, never its content.
    File {
        uri: String,
        path: String,
        lines: Option<String>,
    },
}

impl ReferenceEntry {
    pub fn uri(&self) -> &str {
        match self {
            Self::Quotation { uri, .. } | Self::Preview { uri, .. } | Self::File { uri, .. } => uri,
        }
    }
}

/// One text's references: the entries shown, in first-appearance order, and how
/// many more the text named that the block does not show.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct References {
    pub entries: Vec<ReferenceEntry>,
    pub more: usize,
}

/// `text` without a references block already appended to it. Everything from
/// the block's heading on is block material rather than the author's prose.
pub fn without_references_block(text: &str) -> &str {
    if text.starts_with(REFERENCES_HEADING) {
        return "";
    }
    let marker = format!("\n{REFERENCES_HEADING}\n");
    match text.find(&marker) {
        Some(at) => text[..at].trim_end(),
        None => text,
    }
}

/// The distinct URIs `text` names, in first-appearance order, ignoring any
/// references block already appended to it.
pub fn reference_uris(text: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    crate::uri::scan_reference_uris(without_references_block(text))
        .into_iter()
        .filter(|uri| seen.insert(uri.clone()))
        .collect()
}

/// A passage cut to [`QUOTATION_CHARS`], and whether anything was cut. The cut
/// lands on a word break when one keeps at least half the bound.
pub fn quotation_excerpt(text: &str) -> (String, bool) {
    let text = text.trim();
    if text.chars().count() <= QUOTATION_CHARS {
        return (text.to_string(), false);
    }
    let bound = text
        .char_indices()
        .nth(QUOTATION_CHARS)
        .map_or(text.len(), |(at, _)| at);
    let floor = text
        .char_indices()
        .nth(QUOTATION_CHARS / 2)
        .map_or(0, |(at, _)| at);
    let cut = text[..bound]
        .rfind(char::is_whitespace)
        .filter(|at| *at >= floor)
        .unwrap_or(bound);
    (text[..cut].trim_end().to_string(), true)
}

/// The entry for a `file:` URI: the path and, when the URI names one, its line
/// range. `None` when no path follows the scheme.
///
/// The range is read from the forms a reference is written in: the read
/// window `?offset=N&limit=M` (1-based lines `N+1` to `N+M`, a negative offset
/// being the last `N` lines), and a `path:12` or `path:12-20` suffix.
pub fn file_reference(uri: &str) -> Option<ReferenceEntry> {
    let target = uri.strip_prefix("file:")?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query_value = |key: &str| {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(key)?.strip_prefix('='))
            .and_then(|value| value.parse::<i64>().ok())
    };
    let (path, suffix_lines) = split_line_suffix(path);
    if path.is_empty() {
        return None;
    }
    let lines = match (query_value("offset"), query_value("limit")) {
        (Some(offset), _) if offset < 0 => Some(format!("last {}", -offset)),
        (Some(offset), Some(limit)) if limit > 0 => {
            Some(format!("lines {}–{}", offset + 1, offset + limit))
        }
        (Some(offset), _) => Some(format!("from line {}", offset + 1)),
        (None, Some(limit)) if limit > 0 => Some(format!("lines 1–{limit}")),
        _ => suffix_lines,
    };
    Some(ReferenceEntry::File {
        uri: uri.to_string(),
        path: path.to_string(),
        lines,
    })
}

/// A `path:12` or `path:12-20` line suffix split off its path.
fn split_line_suffix(path: &str) -> (&str, Option<String>) {
    let Some((head, tail)) = path.rsplit_once(':') else {
        return (path, None);
    };
    let (start, end) = tail.split_once('-').unwrap_or((tail, ""));
    let numeric = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !numeric(start) || !(end.is_empty() || numeric(end)) {
        return (path, None);
    }
    let lines = if end.is_empty() {
        format!("line {start}")
    } else {
        format!("lines {start}–{end}")
    };
    (head, Some(lines))
}

/// The block as an agent reads it, or `None` when there is nothing to show.
///
/// Shaped like the affordance block every resource read ends with: a `##`
/// heading, then one list item per entry whose link is the URI it explains.
/// `stamp` renders an epoch-seconds instant in the reader's local time.
pub fn render_references_block(
    references: &References,
    stamp: &dyn Fn(i64) -> Option<String>,
) -> Option<String> {
    if references.entries.is_empty() {
        return None;
    }
    let mut out = format!("{REFERENCES_HEADING}\n\n");
    for entry in &references.entries {
        out.push_str(&render_entry(entry, stamp));
    }
    if references.more > 0 {
        let noun = if references.more == 1 {
            "reference"
        } else {
            "references"
        };
        out.push_str(&format!(
            "- and {} more {noun} not shown\n",
            references.more
        ));
    }
    Some(out.trim_end().to_string())
}

/// The word an entry's link carries for its kind: the kind's [`ResourceContract`]
/// name, the one registry of resource names. A condensed address label is
/// never composed here; the URI beside it already says which resource it is.
///
/// [`ResourceContract`]: crate::contract::ResourceContract
fn kind_word(resource: &str) -> &str {
    ResourceKind::from_slug(resource)
        .and_then(contract_for)
        .map_or(resource, |contract| contract.name)
}

fn render_entry(entry: &ReferenceEntry, stamp: &dyn Fn(i64) -> Option<String>) -> String {
    match entry {
        ReferenceEntry::Quotation {
            uri,
            resource,
            author,
            at,
            text,
            elided,
        } => {
            let when = at
                .and_then(stamp)
                .map(|stamp| format!(", {stamp}"))
                .unwrap_or_default();
            let kind = kind_word(resource);
            let mut out = format!("- [{kind}]({uri}): quoted from {author}{when}\n");
            for line in text.lines() {
                if line.trim().is_empty() {
                    out.push_str("  >\n");
                } else {
                    out.push_str(&format!("  > {line}\n"));
                }
            }
            if *elided {
                out.push_str(&format!("  > … (elided; read {uri} for the rest)\n"));
            }
            out
        }
        ReferenceEntry::Preview {
            uri,
            resource,
            title,
            status,
            line,
            at,
        } => {
            let mut facts = vec![title.clone()];
            facts.extend(status.clone());
            if let Some(moved) = at.and_then(stamp) {
                facts.push(format!("moved {moved}"));
            }
            let kind = kind_word(resource);
            let mut out = format!("- [{kind}]({uri}): {}\n", facts.join(" · "));
            if let Some(line) = line {
                out.push_str(&format!("  {line}\n"));
            }
            out
        }
        ReferenceEntry::File { uri, path, lines } => match lines {
            Some(lines) => format!("- [file]({uri}): {path}, {lines}\n"),
            None => format!("- [file]({uri}): {path}\n"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(at: i64) -> Option<String> {
        Some(format!("T{at}"))
    }

    #[test]
    fn uris_are_distinct_in_first_appearance_order() {
        let text =
            "cairn://p/cairn/2 then file:a.rs and cairn://p/cairn/1, again cairn://p/cairn/2.";
        assert_eq!(
            reference_uris(text),
            vec!["cairn://p/cairn/2", "file:a.rs", "cairn://p/cairn/1"]
        );
    }

    #[test]
    fn an_appended_block_is_never_scanned_again() {
        let text = "see cairn://p/cairn/1\n\n## References\n\n- [issue](cairn://p/cairn/1): x\n  quoted cairn://p/cairn/9";
        assert_eq!(reference_uris(text), vec!["cairn://p/cairn/1"]);
        assert_eq!(without_references_block(text), "see cairn://p/cairn/1");
        assert!(reference_uris("## References\n\n- cairn://p/cairn/9").is_empty());
    }

    #[test]
    fn a_short_passage_is_quoted_whole_and_a_long_one_is_cut_at_a_word() {
        assert_eq!(
            quotation_excerpt("  short words "),
            ("short words".into(), false)
        );
        let long = "word ".repeat(200);
        let (cut, elided) = quotation_excerpt(&long);
        assert!(elided);
        assert!(cut.chars().count() <= QUOTATION_CHARS);
        assert!(cut.ends_with("word"), "{cut}");
        let token = "x".repeat(QUOTATION_CHARS + 50);
        assert_eq!(quotation_excerpt(&token).0.chars().count(), QUOTATION_CHARS);
    }

    #[test]
    fn file_entries_name_the_path_and_range_but_no_content() {
        let entry = |uri: &str| match file_reference(uri) {
            Some(ReferenceEntry::File { path, lines, .. }) => (path, lines),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            entry("file:src/lib.rs?offset=10&limit=5"),
            ("src/lib.rs".into(), Some("lines 11–15".into()))
        );
        assert_eq!(
            entry("file:src/lib.rs:42"),
            ("src/lib.rs".into(), Some("line 42".into()))
        );
        assert_eq!(
            entry("file:src/lib.rs:4-9"),
            ("src/lib.rs".into(), Some("lines 4–9".into()))
        );
        assert_eq!(
            entry("file:a.rs?offset=-20"),
            ("a.rs".into(), Some("last 20".into()))
        );
        assert_eq!(entry("file:docs/a.md"), ("docs/a.md".into(), None));
        assert!(file_reference("file:").is_none());
    }

    #[test]
    fn the_block_quotes_previews_and_counts_the_rest() {
        let references = References {
            entries: vec![
                ReferenceEntry::Quotation {
                    uri: "cairn://p/cairn/execution/chat/turn/3".into(),
                    resource: "node-chat-turn".into(),
                    author: "the operator".into(),
                    at: Some(5),
                    text: "First.\n\nSecond.".into(),
                    elided: true,
                },
                ReferenceEntry::Preview {
                    uri: "cairn://p/cairn/7".into(),
                    resource: "issue".into(),
                    title: "Fix it".into(),
                    status: Some("active".into()),
                    line: Some("Why it matters.".into()),
                    at: Some(9),
                },
                ReferenceEntry::File {
                    uri: "file:a.rs:3".into(),
                    path: "a.rs".into(),
                    lines: Some("line 3".into()),
                },
            ],
            more: 2,
        };
        assert_eq!(
            render_references_block(&references, &stamp).unwrap(),
            "## References\n\n\
             - [Transcript turn](cairn://p/cairn/execution/chat/turn/3): quoted from the operator, T5\n\
             \x20 > First.\n\
             \x20 >\n\
             \x20 > Second.\n\
             \x20 > … (elided; read cairn://p/cairn/execution/chat/turn/3 for the rest)\n\
             - [Issue details](cairn://p/cairn/7): Fix it · active · moved T9\n\
             \x20 Why it matters.\n\
             - [file](file:a.rs:3): a.rs, line 3\n\
             - and 2 more references not shown"
        );
        assert!(render_references_block(&References::default(), &stamp).is_none());
    }

    #[test]
    fn entries_serialize_with_a_kind_tag_for_the_transcript() {
        let entry = ReferenceEntry::File {
            uri: "file:a.rs".into(),
            path: "a.rs".into(),
            lines: None,
        };
        assert_eq!(
            serde_json::to_value(&entry).unwrap(),
            serde_json::json!({"kind": "file", "uri": "file:a.rs", "path": "a.rs", "lines": null})
        );
    }
}
