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
use cairn_common::read::{ImageBlock, ReadSegment};

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
    scope: crate::images::ImageScope,
}

impl ImageStore for ProjectStore {
    fn store<'a>(&'a self, bytes: Vec<u8>) -> StoreFuture<'a> {
        Box::pin(async move {
            crate::images::store_image_bytes(&self.db, &self.scope, bytes)
                .await
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

/// Apply the shared image policy to every read segment.
pub(crate) async fn promote_read_images(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    segments: &mut [ReadSegment],
) {
    for segment in segments {
        promote_images(orch, request, &mut segment.images, &mut segment.body).await;
    }
}

pub(crate) async fn promote_images(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    images: &mut [ImageBlock],
    body: &mut String,
) {
    if has_promotable_images(images) {
        match crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await {
            Ok((run, db)) => {
                promote_with(
                    &ProjectStore {
                        db,
                        scope: crate::images::ImageScope::new(run.project_id, run.project_key)
                            .in_issue(run.issue_number),
                    },
                    images,
                    body,
                )
                .await;
            }
            Err(error) => {
                log::debug!("images were not promoted (no routed run): {error}");
            }
        }
    }

    cite_stored_images(images, body);
}

fn cite_stored_images(images: &[ImageBlock], body: &mut String) {
    let citations: Vec<String> = images
        .iter()
        .filter_map(|image| image.uri.as_deref())
        .map(|uri| format!("![image]({uri})"))
        .collect();
    for citation in citations {
        if body.contains(&citation) {
            continue;
        }
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str(&citation);
    }
}

async fn promote_with(store: &dyn ImageStore, images: &mut [ImageBlock], body: &mut String) {
    let mut notes: Vec<String> = Vec::new();
    for image in images {
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
                body.push('\n');
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

    #[tokio::test]
    async fn promotes_each_block_and_leaves_the_body_alone() {
        let store = Fake::ok();
        let mut segments = [image_segment("file:plot.png", &png_base64())];
        let segment = &mut segments[0];
        promote_with(&store, &mut segment.images, &mut segment.body).await;

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
        let segment = &mut segments[0];
        promote_with(&store, &mut segment.images, &mut segment.body).await;
        assert!(store.stored.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_storage_refusal_is_reported_in_the_body_not_swallowed() {
        // A blank image section with no explanation is the failure this module
        // exists to remove, so a refusal must say so where the user can read it.
        let store = Fake::failing("image exceeds the 5 MiB limit (6000000 bytes)");
        let mut segments = [image_segment("file:huge.png", &png_base64())];
        let segment = &mut segments[0];
        promote_with(&store, &mut segment.images, &mut segment.body).await;

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
        let segment = &mut segments[0];
        promote_with(&store, &mut segment.images, &mut segment.body).await;

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
        let segment = &mut segments[0];
        promote_with(&store, &mut segment.images, &mut segment.body).await;

        assert_eq!(
            segments[0].body,
            "Browser: https://example.com\n[image not stored: unsupported image format]"
        );
    }

    #[tokio::test]
    async fn a_run_outcome_image_is_promoted_and_cited_in_its_body() {
        let store = Fake::ok();
        let mut images = vec![ImageBlock::inline("image/png", png_base64())];
        let mut body = "Axon captured the desktop".to_string();

        promote_with(&store, &mut images, &mut body).await;
        cite_stored_images(&images, &mut body);

        assert_eq!(images[0].uri.as_deref(), Some(STORED));
        assert_eq!(
            body,
            format!("Axon captured the desktop\n![image]({STORED})")
        );
    }
}
