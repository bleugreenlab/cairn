use super::super::types::{AttachmentRef, AttachmentScopeRef};
use super::*;

#[test]
fn reference_scan_finds_cairn_and_file_targets_in_order() {
    let found = scan_reference_uris(
            "See file:src/lib.rs?offset=10&limit=5, then cairn://p/cairn/42/1/builder, cairn://p/cairn/42. \
             A profile: is prose, (file:docs/a.md) is not, and cairn://nonsense/x is skipped.",
        );
    assert_eq!(
        found,
        vec![
            "file:src/lib.rs?offset=10&limit=5".to_string(),
            "cairn://p/cairn/42/1/builder".to_string(),
            "cairn://p/cairn/42".to_string(),
            "file:docs/a.md".to_string(),
        ]
    );
}

#[test]
fn reference_scan_keeps_duplicates_and_survives_multibyte_text() {
    let found = scan_reference_uris("é cairn://p/cairn/1 — and cairn://p/cairn/1 again file:");
    assert_eq!(found, vec!["cairn://p/cairn/1".to_string(); 2]);
}

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
    let refs: Vec<AttachmentRef> = scan_stored_images(&text)
        .into_iter()
        .map(|found| match found.resource {
            CairnResource::ProjectImage { reference, .. } => reference,
            other => panic!("expected an image resource, got {other:?}"),
        })
        .collect();
    assert_eq!(
        refs,
        vec![
            AttachmentRef::Scoped {
                scope: AttachmentScopeRef::Issue(3242),
                ordinal: 2
            },
            AttachmentRef::Scoped {
                scope: AttachmentScopeRef::Project,
                ordinal: 9
            },
            AttachmentRef::Hash(hash),
        ]
    );
}

#[test]
fn scan_uris_finds_workspace_shapes_and_skips_what_does_not_parse() {
    let found: Vec<CairnResource> = scan_uris(
        "see cairn://posts/7, cairn://posts/7/comments/2 and cairn://p/cairn/42; \
             cairn:// alone is prose, as is cairn://nonsense/here.",
    )
    .into_iter()
    .map(|found| found.resource)
    .collect();
    assert_eq!(
        found,
        vec![
            CairnResource::Post { id: 7 },
            CairnResource::PostComment {
                post_id: 7,
                comment_id: 2
            },
            CairnResource::Issue {
                project: "cairn".into(),
                number: 42
            },
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
