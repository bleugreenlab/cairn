//! Finding the cairn:// URIs a human or an agent wrote into free text.
//!
//! Several paths have to act on the image URIs inside a body of prose: a user
//! message's images are delivered to the model as native image blocks, and a
//! read result's promoted-image trailer is lifted onto segment metadata. Each
//! used to carry its own regex spelling out the URI's shape, so the written form
//! of an image URI was encoded in three places and adding a form meant finding
//! all of them.
//!
//! There is no second grammar here. A candidate is any `cairn://p/…` run of URI
//! characters; [`super::parse_uri`] — the one parser — decides what it names. A
//! new URI shape therefore reaches every text scanner the moment the parser
//! learns it.

use super::parse::parse_uri;
use super::types::{CairnResource, PROJECT_SCOPE};

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

/// Every parseable cairn:// project URI written into `text`, in textual order.
pub fn scan_uris(text: &str) -> Vec<UriMatch> {
    let prefix = format!("cairn://{PROJECT_SCOPE}/");
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(&prefix) {
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

#[cfg(test)]
mod tests {
    use super::super::types::ImageRef;
    use super::*;

    fn uris(text: &str) -> Vec<String> {
        scan_stored_images(text)
            .into_iter()
            .map(|found| found.uri)
            .collect()
    }

    #[test]
    fn finds_every_written_form_of_a_stored_image() {
        let hash = "a".repeat(64);
        let text = format!(
            "markdown ![Pasted image](cairn://p/CAIRN/3242/images/1) then a bare \
             cairn://p/CAIRN/images/7 and a legacy cairn://p/CAIRN/images/{hash}."
        );
        assert_eq!(
            uris(&text),
            vec![
                "cairn://p/CAIRN/3242/images/1".to_string(),
                "cairn://p/CAIRN/images/7".to_string(),
                format!("cairn://p/CAIRN/images/{hash}"),
            ]
        );
    }

    #[test]
    fn reports_the_parsed_reference_for_each_form() {
        let hash = "b".repeat(64);
        let text = format!(
            "cairn://p/CAIRN/3242/images/2 cairn://p/CAIRN/images/9 cairn://p/CAIRN/images/{hash}"
        );
        let refs: Vec<ImageRef> = scan_stored_images(&text)
            .into_iter()
            .map(|found| match found.resource {
                CairnResource::ProjectImage { reference, .. } => reference,
                other => panic!("expected an image resource, got {other:?}"),
            })
            .collect();
        assert_eq!(
            refs,
            vec![
                ImageRef::Issue {
                    number: 3242,
                    ordinal: 2
                },
                ImageRef::Project { ordinal: 9 },
                ImageRef::Hash(hash),
            ]
        );
    }

    #[test]
    fn ignores_cairn_uris_that_are_not_images() {
        assert!(uris("see cairn://p/CAIRN/3242 and cairn://p/CAIRN/3242/messages").is_empty());
    }

    #[test]
    fn a_repeated_reference_is_reported_once_per_occurrence() {
        let text = "cairn://p/CAIRN/1/images/1 again cairn://p/CAIRN/1/images/1";
        assert_eq!(uris(text).len(), 2);
    }

    #[test]
    fn a_malformed_image_uri_yields_nothing() {
        // A zero ordinal is not a reference, and neither is a truncated hash.
        assert!(uris("cairn://p/CAIRN/images/0 cairn://p/CAIRN/images/abc123").is_empty());
    }

    #[test]
    fn markdown_image_scan_ignores_bare_uris_and_fenced_examples() {
        let uri = "cairn://p/cairn/3242/images/1";
        let text = format!("bare {uri}\n```md\n![example]({uri})\n```\n![attached]({uri})");
        assert_eq!(
            scan_markdown_stored_images(&text)
                .into_iter()
                .map(|found| found.uri)
                .collect::<Vec<_>>(),
            vec![uri]
        );
    }
}
