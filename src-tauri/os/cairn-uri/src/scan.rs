//! Finding the cairn:// URIs a human or an agent wrote into free text.
//!
//! Several paths have to act on the image URIs inside a body of prose: a user
//! message's images are delivered to the model as native image blocks, and a
//! read result's promoted-image trailer is lifted onto segment metadata. Each
//! used to carry its own regex spelling out the URI's shape, so the written form
//! of an image URI was encoded in three places and adding a form meant finding
//! all of them.
//!
//! There is no second grammar here. A candidate is any `cairn://…` run of URI
//! characters; [`super::parse_uri`] — the one parser — decides what it names. A
//! new URI shape therefore reaches every text scanner the moment the parser
//! learns it, workspace-level shapes like `cairn://posts/{id}` included.

use super::parse::parse_uri;
use super::types::CairnResource;

/// One cairn:// URI found in text, with the resource it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriMatch {
    /// The URI exactly as written, so a caller can dedupe or replace it.
    pub uri: String,
    pub resource: CairnResource,
}

/// Stored-image destinations from actual Markdown image syntax, excluding
/// fenced code blocks. This is deliberately narrower than [`scan_stored_images`]:
/// callers that attach native image bytes must not reinterpret URI examples in
/// prose or logs as attachments.
pub fn scan_markdown_stored_images(text: &str) -> Vec<UriMatch> {
    let mut found = Vec::new();
    let mut fence: Option<(char, usize)> = None;

    for line in text.lines() {
        let trimmed = line.trim_start_matches(' ');
        let indent = line.len() - trimmed.len();
        let marker = trimmed.chars().next();
        let marker_len = marker
            .filter(|c| matches!(c, '`' | '~'))
            .map(|c| trimmed.chars().take_while(|next| *next == c).count())
            .unwrap_or_default();
        if indent <= 3 && marker_len >= 3 {
            match fence {
                None => fence = Some((marker.unwrap(), marker_len)),
                Some((open, width)) if marker == Some(open) && marker_len >= width => fence = None,
                _ => {}
            }
            continue;
        }
        if fence.is_some() {
            continue;
        }

        let mut rest = line;
        while let Some(image_start) = rest.find("![") {
            let after_bang = &rest[image_start + 2..];
            let Some(destination_start) = after_bang.find("](") else {
                break;
            };
            let destination = &after_bang[destination_start + 2..];
            let Some(destination_end) = destination.find(')') else {
                break;
            };
            let uri = destination[..destination_end]
                .trim()
                .trim_matches(['<', '>']);
            if let Some(resource) = parse_uri(uri) {
                if matches!(resource, CairnResource::ProjectImage { .. }) {
                    found.push(UriMatch {
                        uri: uri.to_string(),
                        resource,
                    });
                }
            }
            rest = &destination[destination_end + 1..];
        }
    }
    found
}

/// Characters that continue a URI in prose. Whitespace ends one; so do the
/// closing delimiters of the markdown and HTML constructs a URI is embedded in.
fn continues_uri(c: char) -> bool {
    !c.is_whitespace() && !matches!(c, ')' | ']' | '}' | '>' | '"' | '\'' | '`' | '<' | '(')
}

/// Trailing characters that are sentence punctuation rather than part of a URI.
const TRAILING_PUNCTUATION: &[char] = &['.', ',', ';', ':', '!', '?'];

/// Every stored-image URI written into `text`, in textual order, with duplicates
/// preserved (callers that want uniqueness dedupe on [`UriMatch::uri`]).
pub fn scan_stored_images(text: &str) -> Vec<UriMatch> {
    scan_uris(text)
        .into_iter()
        .filter(|found| matches!(found.resource, CairnResource::ProjectImage { .. }))
        .collect()
}

/// Every parseable cairn:// URI written into `text`, in textual order.
pub fn scan_uris(text: &str) -> Vec<UriMatch> {
    let prefix = "cairn://";
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(prefix) {
        let candidate_start = start;
        let tail = &rest[candidate_start..];
        let end = tail
            .char_indices()
            .find(|(_, c)| !continues_uri(*c))
            .map(|(index, _)| index)
            .unwrap_or(tail.len());
        let mut candidate = &tail[..end];
        // Prose ends a URI with a period or comma far more often than a URI
        // legitimately ends with one, so shrink past trailing punctuation until
        // the parser recognizes what is left.
        loop {
            if let Some(resource) = parse_uri(candidate) {
                found.push(UriMatch {
                    uri: candidate.to_string(),
                    resource,
                });
                break;
            }
            match candidate.strip_suffix(TRAILING_PUNCTUATION) {
                Some(shorter) if !shorter.is_empty() => candidate = shorter,
                _ => break,
            }
        }
        rest = &tail[end.max(1)..];
    }
    found
}

/// Every reference a reader could follow out of `text`: each parseable
/// `cairn://` URI and each `file:` target, exactly as written and in textual
/// order, duplicates preserved.
///
/// A `file:` candidate has no parser to consult, so it counts when a path
/// follows the scheme and the scheme starts a word (`profile:` is prose, not a
/// reference). Trailing sentence punctuation is shed from it as from a
/// `cairn://` candidate.
pub fn scan_reference_uris(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut index = 0;
    while index < text.len() {
        let rest = &text[index..];
        let cairn = rest.find("cairn://");
        let file = file_scheme_start(text, index);
        let start = match (cairn, file) {
            (Some(cairn), Some(file)) => cairn.min(file),
            (Some(cairn), None) => cairn,
            (None, Some(file)) => file,
            (None, None) => break,
        };
        let tail = &rest[start..];
        let end = tail
            .char_indices()
            .find(|(_, c)| !continues_uri(*c))
            .map(|(at, _)| at)
            .unwrap_or(tail.len());
        let mut candidate = &tail[..end];
        let is_file = Some(start) == file && Some(start) != cairn;
        // Prose punctuation is shed before the parser is asked, because some
        // segments (a node name) would accept `builder,` as written.
        let shed = candidate.trim_end_matches(TRAILING_PUNCTUATION);
        if !is_file && shed.len() < candidate.len() && parse_uri(shed).is_some() {
            candidate = shed;
        }
        loop {
            let accepted = if is_file {
                candidate.len() > "file:".len() && !candidate.ends_with(TRAILING_PUNCTUATION)
            } else {
                parse_uri(candidate).is_some()
            };
            if accepted {
                found.push(candidate.to_string());
                break;
            }
            match candidate.strip_suffix(TRAILING_PUNCTUATION) {
                Some(shorter) if !shorter.is_empty() => candidate = shorter,
                _ => break,
            }
        }
        index += start + end.max(1);
        while !text.is_char_boundary(index) {
            index += 1;
        }
    }
    found
}

/// The offset, relative to `from`, of the next `file:` that starts a word.
fn file_scheme_start(text: &str, from: usize) -> Option<usize> {
    let mut offset = from;
    while let Some(found) = text[offset..].find("file:") {
        let at = offset + found;
        let starts_word = text[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_' || c == '-' || c == '/'));
        if starts_word {
            return Some(at - from);
        }
        offset = at + "file:".len();
    }
    None
}

#[cfg(test)]
mod tests;
