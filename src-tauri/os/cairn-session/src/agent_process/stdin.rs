//! The provider-neutral user message a session sends.
//!
//! The stream-json and app-server serializations of this content, and the
//! stdin writers that carry it, are wire facts and live in
//! `cairn_models::wire::{claude_stream, codex_app_server}`.

pub use cairn_models::wire::MessageContent;
pub use cairn_models::wire::MessageImage;

#[cfg(test)]
pub trait StableImageResolver {
    fn resolve(&self, uri: &str) -> Result<MessageImage, String>;
}

/// Resolve every unique Markdown image reference in textual order. Missing
/// or corrupt durable resources degrade to the unchanged textual reference.
#[cfg(test)]
fn resolve_message_content(
    text: impl Into<String>,
    resolver: &dyn StableImageResolver,
) -> Result<MessageContent, String> {
    let text = text.into();
    let mut seen = std::collections::HashSet::new();
    let mut images = Vec::new();
    for found in cairn_common::uri::scan_markdown_stored_images(&text) {
        if seen.insert(found.uri.clone()) {
            if let Ok(image) = resolver.resolve(&found.uri) {
                images.push(image);
            }
        }
    }
    Ok(MessageContent { text, images })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models::wire::claude_stream::build_message_content;
    use serde_json::Value;

    struct FixtureResolver;

    impl StableImageResolver for FixtureResolver {
        fn resolve(&self, _uri: &str) -> Result<MessageImage, String> {
            Ok(MessageImage {
                mime_type: "image/png".to_string(),
                bytes: b"native-image-payload".to_vec(),
            })
        }
    }

    fn assert_no_text_contains(value: &Value, needle: &str) {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(text)) = map.get("text") {
                    assert!(!text.contains(needle), "encoded image leaked into text");
                }
                map.values()
                    .for_each(|v| assert_no_text_contains(v, needle));
            }
            Value::Array(values) => values
                .iter()
                .for_each(|v| assert_no_text_contains(v, needle)),
            _ => {}
        }
    }

    #[test]
    fn stable_uri_resolves_once_and_serializes_as_native_claude_image() {
        let uri = format!("cairn://p/cairn/images/{}", "a".repeat(64));
        let content =
            resolve_message_content(format!("![one]({uri}) again {uri}"), &FixtureResolver)
                .unwrap();
        assert_eq!(content.images.len(), 1);

        let serialized = build_message_content(&content).unwrap();
        assert_eq!(serialized[1]["type"], "image");
        assert_eq!(serialized[1]["source"]["media_type"], "image/png");
        let encoded = serialized[1]["source"]["data"].as_str().unwrap();
        assert_no_text_contains(&serialized, encoded);
    }

    #[test]
    fn missing_markdown_image_degrades_to_unchanged_text() {
        struct Missing;
        impl StableImageResolver for Missing {
            fn resolve(&self, uri: &str) -> Result<MessageImage, String> {
                Err(format!("missing image: {uri}"))
            }
        }
        let uri = format!("cairn://p/cairn/images/{}", "b".repeat(64));
        let text = format!("![missing]({uri})");
        let content = resolve_message_content(&text, &Missing).unwrap();
        assert_eq!(content.text, text);
        assert!(content.images.is_empty());
    }

    #[test]
    fn bare_and_fenced_image_uris_are_not_resolved() {
        struct Panics;
        impl StableImageResolver for Panics {
            fn resolve(&self, uri: &str) -> Result<MessageImage, String> {
                panic!("prose URI was treated as an attachment: {uri}")
            }
        }
        let uri = "cairn://p/cairn/3242/images/1";
        let text = format!("Mention {uri} in prose.\n```\n{uri}\n```");
        let content = resolve_message_content(&text, &Panics).unwrap();
        assert_eq!(content.text, text);
        assert!(content.images.is_empty());
    }
}
