//! Durable promotion of the image blocks a read produced.
//!
//! Reading an image is one act that has to land with two audiences. The agent
//! gets the bytes as a native MCP image content block; the user gets whatever
//! the transcript kept, and a transcript keeps a tool result's *text*, not its
//! content blocks. Without promotion the second audience sees an empty section
//! for an image the agent could describe in detail.
//!
//! So every image a producer emitted is stored content-addressed in the owning
//! project's content store here, and the resulting
//! `cairn://p/{KEY}/{issue}/images/{n}` URI rides back on the block for the composer
//! to render as a markdown reference. This is the same promotion
//! [`crate::durable_content`] performs for messages, issue descriptions, and
//! artifacts, moved to the read seam so the image outlives the worktree, no
//! filesystem path (and therefore no asset-protocol scope) is involved, and no
//! agent has to remember a second deliberate embedding step.
//!
//! Promotion is best-effort, unlike the durable-write path's fail-closed
//! behavior: a read that cannot store its bytes still has an image to hand the
//! agent, and failing the whole read would be a strictly worse outcome. A refusal
//! is reported in the segment body instead of vanishing, because a silently blank
//! image section is the exact failure this module exists to remove.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use base64::Engine;
use cairn_common::read::{AnchoredText, ImageBlock, ReadSegment};

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

type StoreFuture<'a> = Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;

/// The content-addressed sink promoted image bytes go to. Injected so the
/// promotion policy — which blocks are eligible, and how a refusal is reported —
/// is exercised without an orchestrator or a database.
trait ImageStore: Sync {
    fn store<'a>(&'a self, bytes: Vec<u8>) -> StoreFuture<'a>;
}

struct ProjectStore {
    db: Arc<LocalDb>,
    scope: crate::attachments::AttachmentScope,
}

impl ImageStore for ProjectStore {
    fn store<'a>(&'a self, bytes: Vec<u8>) -> StoreFuture<'a> {
        Box::pin(async move {
            // A promoted block is bytes and nothing else — no filename and no
            // identity to attribute it to — so the record carries the neutral
            // name and no provenance rather than an invented one.
            crate::attachments::store_attachment_bytes(
                &self.db,
                &self.scope,
                bytes,
                "image",
                None,
                &crate::attachments::AttachmentProvenance::default(),
            )
            .await
            .map(|stored| stored.uri)
            .map_err(|error| error.to_string())
        })
    }
}

/// Whether any block in one result still needs promoting. Keeps a text-only batch
/// — the overwhelming majority — from paying a run lookup.
fn has_promotable_images(images: &[ImageBlock]) -> bool {
    images
        .iter()
        .any(|image| image.uri.is_none() && !image.data.is_empty())
}

/// Store every read segment's images. A read is not cited here: the read
/// renderer writes each stored image's one `![label](uri)` reference, in the
/// composer that the live batch and archival reconstruction share.
pub(crate) async fn promote_read_images(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    segments: &mut [ReadSegment],
) {
    for segment in segments {
        let mut body = AnchoredText::new(
            std::mem::take(&mut segment.body),
            std::mem::take(&mut segment.images),
        );
        store_images(orch, request, &mut body).await;
        (segment.body, segment.images) = body.into_parts();
    }
}

/// Store every image in a run item's `body` that has no durable address yet,
/// and cite each stored one in the text beside it. Run output has no renderer
/// trailer, so this citation is its one reference.
pub(crate) async fn promote_images(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    body: &mut AnchoredText,
) {
    store_images(orch, request, body).await;
    cite_stored_images(body);
}

/// Store every image in `body` that has no durable address yet, recording its
/// address on the block. A refusal is noted at the end of the text.
async fn store_images(orch: &Orchestrator, request: &McpCallbackRequest, body: &mut AnchoredText) {
    if has_promotable_images(body.images()) {
        match crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await {
            Ok((run, db)) => {
                promote_with(
                    &ProjectStore {
                        db,
                        scope: crate::attachments::AttachmentScope::new(
                            run.project_id,
                            run.project_key,
                        )
                        .in_issue(run.issue_number),
                    },
                    body,
                )
                .await;
            }
            Err(error) => {
                log::debug!("images were not promoted (no routed run): {error}");
            }
        }
    }
}

/// Cite every stored image in `body` that the text does not already cite, as a
/// markdown reference — the only part of an image a transcript keeps. An image
/// placed in the text is cited on its own line immediately before it, so the
/// reference and the bytes travel together. An image after all of the text is
/// cited at the end, which is where its bytes arrive.
pub(crate) fn cite_stored_images(body: &mut AnchoredText) {
    for index in 0..body.images().len() {
        let image = &body.images()[index];
        let Some(uri) = image.uri.as_deref() else {
            continue;
        };
        let citation = format!("![image]({uri})");
        if body.text().contains(&citation) {
            continue;
        }
        match image.at {
            Some(at) => {
                let text = body.text().as_bytes();
                let opens_line = at == 0 || text[at - 1] == b'\n';
                let closes_line = at == text.len() || text[at] == b'\n';
                let line = format!(
                    "{}{citation}{}",
                    if opens_line { "" } else { "\n" },
                    if closes_line { "" } else { "\n" },
                );
                body.insert_before_image(index, &line);
            }
            None => {
                if !body.is_empty() {
                    body.push_str("\n");
                }
                body.push_str(&citation);
            }
        }
    }
}

async fn promote_with(store: &dyn ImageStore, body: &mut AnchoredText) {
    let mut notes: Vec<String> = Vec::new();
    for image in body.images_mut() {
        if image.uri.is_some() || image.data.is_empty() {
            continue;
        }
        let bytes = match base64::engine::general_purpose::STANDARD.decode(&image.data) {
            Ok(bytes) => bytes,
            Err(error) => {
                notes.push(format!("[image not stored: malformed base64: {error}]"));
                continue;
            }
        };
        match store.store(bytes).await {
            Ok(uri) => image.uri = Some(uri),
            Err(error) => notes.push(format!("[image not stored: {error}]")),
        }
    }
    if !notes.is_empty() {
        log::warn!("image promotion failed: {}", notes.join(" "));
        for note in notes {
            if !body.is_empty() {
                body.push_str("\n");
            }
            body.push_str(&note);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::read::{ImageBlock, NaturalUnit, SegmentKind, SegmentMeta};
    use std::sync::Mutex;

    const STORED: &str = "cairn://p/cairn/3242/images/1";

    struct Fake {
        failure: Option<String>,
        stored: Mutex<Vec<Vec<u8>>>,
    }

    impl Fake {
        fn ok() -> Self {
            Self {
                failure: None,
                stored: Mutex::new(Vec::new()),
            }
        }
        fn failing(message: &str) -> Self {
            Self {
                failure: Some(message.to_string()),
                stored: Mutex::new(Vec::new()),
            }
        }
    }

    impl ImageStore for Fake {
        fn store<'a>(&'a self, bytes: Vec<u8>) -> StoreFuture<'a> {
            Box::pin(async move {
                match &self.failure {
                    Some(message) => Err(message.clone()),
                    None => {
                        self.stored.lock().unwrap().push(bytes);
                        Ok(STORED.to_string())
                    }
                }
            })
        }
    }

    fn image_segment(target: &str, data: &str) -> ReadSegment {
        let mut segment = ReadSegment::text(
            String::new(),
            SegmentMeta::new(target, SegmentKind::Image, NaturalUnit::Line),
        );
        segment.images.push(ImageBlock::inline("image/png", data));
        segment
    }

    fn png_base64() -> String {
        base64::engine::general_purpose::STANDARD.encode(b"\x89PNG\r\n\x1a\nfixture")
    }

    /// [`promote_with`] over one segment's body and images, as
    /// [`promote_read_images`] carries them.
    async fn promote_segment(store: &Fake, segment: &mut ReadSegment) {
        let mut body = AnchoredText::new(
            std::mem::take(&mut segment.body),
            std::mem::take(&mut segment.images),
        );
        promote_with(store, &mut body).await;
        (segment.body, segment.images) = body.into_parts();
    }

    #[tokio::test]
    async fn promotes_each_block_and_leaves_the_body_alone() {
        let store = Fake::ok();
        let mut segments = [image_segment("file:plot.png", &png_base64())];
        promote_segment(&store, &mut segments[0]).await;

        assert_eq!(segments[0].images[0].uri.as_deref(), Some(STORED));
        // The reference is composed by the renderer from the block, never spliced
        // into the producer's body.
        assert!(segments[0].body.is_empty());
        assert_eq!(store.stored.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn text_only_batch_needs_no_promotion() {
        let segments = [ReadSegment::text(
            "fn main() {}",
            SegmentMeta::new("file:a.rs", SegmentKind::File, NaturalUnit::Line),
        )];
        assert!(!has_promotable_images(&segments[0].images));
    }

    #[tokio::test]
    async fn an_already_promoted_block_is_not_stored_twice() {
        let mut segments = [image_segment("file:plot.png", &png_base64())];
        segments[0].images[0].uri = Some(STORED.to_string());
        assert!(!has_promotable_images(&segments[0].images));

        let store = Fake::ok();
        promote_segment(&store, &mut segments[0]).await;
        assert!(store.stored.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_storage_refusal_is_reported_in_the_body_not_swallowed() {
        // A blank image section with no explanation is the failure this module
        // exists to remove, so a refusal must say so where the user can read it.
        let store = Fake::failing("image exceeds the 5 MiB limit (6000000 bytes)");
        let mut segments = [image_segment("file:huge.png", &png_base64())];
        promote_segment(&store, &mut segments[0]).await;

        assert!(segments[0].images[0].uri.is_none());
        assert_eq!(
            segments[0].body,
            "[image not stored: image exceeds the 5 MiB limit (6000000 bytes)]"
        );
    }

    #[tokio::test]
    async fn malformed_base64_is_reported_without_reaching_the_store() {
        let store = Fake::ok();
        let mut segments = [image_segment("file:plot.png", "not base64!!")];
        promote_segment(&store, &mut segments[0]).await;

        assert!(store.stored.lock().unwrap().is_empty());
        assert!(segments[0]
            .body
            .starts_with("[image not stored: malformed base64"));
    }

    #[tokio::test]
    async fn a_refusal_note_appends_after_an_existing_body() {
        // A browser screenshot carries a status banner; the note joins it rather
        // than replacing it.
        let store = Fake::failing("unsupported image format");
        let mut segments = [image_segment("cairn:~/browser?screenshot", &png_base64())];
        segments[0].body = "Browser: https://example.com".to_string();
        promote_segment(&store, &mut segments[0]).await;

        assert_eq!(
            segments[0].body,
            "Browser: https://example.com\n[image not stored: unsupported image format]"
        );
    }

    #[tokio::test]
    async fn a_run_outcome_image_is_promoted_and_cited_in_its_body() {
        let store = Fake::ok();
        let mut body = AnchoredText::new(
            "Axon captured the desktop",
            vec![ImageBlock::inline("image/png", png_base64())],
        );

        promote_with(&store, &mut body).await;
        cite_stored_images(&mut body);

        assert_eq!(body.images()[0].uri.as_deref(), Some(STORED));
        // A trailing image is cited at the end, where its bytes arrive.
        assert_eq!(
            body.text(),
            format!("Axon captured the desktop\n![image]({STORED})")
        );
        assert_eq!(body.images()[0].at, None);
    }

    fn stored_at(at: usize) -> ImageBlock {
        ImageBlock::stored("image/png", "b64", STORED).placed_at(at)
    }

    /// The text immediately before the first image block, as delivered.
    fn text_before_the_image(body: &AnchoredText) -> String {
        let blocks = body.blocks();
        let image = blocks
            .iter()
            .position(|block| matches!(block, cairn_common::read::OrderedBlock::Image(_)))
            .expect("an image block");
        match blocks[..image].last() {
            Some(cairn_common::read::OrderedBlock::Text(text)) => text.to_string(),
            other => panic!("expected text before the image, got {other:?}"),
        }
    }

    #[test]
    fn an_anchored_image_is_cited_in_place_immediately_before_it() {
        // `[image, text]` from a server: the citation opens the text, then the
        // bytes, then the description.
        let mut first = AnchoredText::new("its description", vec![stored_at(0)]);
        cite_stored_images(&mut first);
        assert_eq!(first.text(), format!("![image]({STORED})\nits description"));
        assert_eq!(text_before_the_image(&first), format!("![image]({STORED})"));

        // `[text, image, text]`: anchored at the start of the second part.
        let mut between = AnchoredText::new("before\nafter", vec![stored_at(7)]);
        cite_stored_images(&mut between);
        assert_eq!(between.text(), format!("before\n![image]({STORED})\nafter"));
        assert!(text_before_the_image(&between).ends_with(&format!("![image]({STORED})")));
        let at = between.images()[0].at.unwrap();
        assert!(between.text()[at..].starts_with("after"));

        // Anchored mid-line, the citation still gets a line of its own.
        let mut inline = AnchoredText::new("ab", vec![stored_at(1)]);
        cite_stored_images(&mut inline);
        assert_eq!(inline.text(), format!("a\n![image]({STORED})\nb"));
    }

    #[test]
    fn an_existing_citation_is_not_duplicated() {
        let cited = format!("![image]({STORED})\nits description");
        let mut body = AnchoredText::new(cited.clone(), vec![stored_at(0)]);
        cite_stored_images(&mut body);
        assert_eq!(body.text(), cited);

        let mut trailing = AnchoredText::new(
            format!("tree\n![image]({STORED})"),
            vec![ImageBlock::stored("image/png", "b64", STORED)],
        );
        cite_stored_images(&mut trailing);
        assert_eq!(trailing.text(), format!("tree\n![image]({STORED})"));
    }
}
