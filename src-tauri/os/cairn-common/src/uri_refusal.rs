//! What a refused `cairn://` address answers with: the sentence a reader sees
//! and the typed data a script branches on.
//!
//! The classification is `cairn_uri::classify_uri`'s. This module adds the one
//! fact the grammar crate cannot know, which build is answering, because an
//! address this build does not have is most often version skew: a newer
//! client talking to an older runner. Naming the runner's version and build
//! there turns a diagnosis round into one sentence.

use std::sync::OnceLock;

use cairn_uri::{UnknownSegment, UriRefusal};

/// `Cairn <version>, build <content id>` for the running executable, computed
/// once per process.
pub fn runner_identity() -> &'static str {
    static IDENTITY: OnceLock<String> = OnceLock::new();
    IDENTITY.get_or_init(|| {
        let build = crate::build_identity::current_executable_build_id()
            .unwrap_or_else(|_| "unknown".to_string());
        format!("Cairn {}, build {build}", env!("CARGO_PKG_VERSION"))
    })
}

/// The refusal text for `uri`, which the grammar refused for `refusal`.
pub fn refusal_message(uri: &str, refusal: &UriRefusal) -> String {
    message_as(uri, refusal, runner_identity())
}

fn message_as(uri: &str, refusal: &UriRefusal, runner: &str) -> String {
    match refusal {
        UriRefusal::Malformed(fault) => format!("Malformed cairn address `{uri}`: {fault}."),
        UriRefusal::Unknown(unknown) => unknown_message(unknown, runner),
    }
}

fn unknown_message(unknown: &UnknownSegment, runner: &str) -> String {
    let mut text = format!(
        "This runner ({runner}) has no `{}` resource.",
        unknown.address()
    );
    if let Some(nearest) = &unknown.nearest {
        text.push_str(&format!(" Did you mean `{}`?", unknown.address_of(nearest)));
    }
    text.push_str(
        " If the address came from a newer client, this runner may be older than the client; \
         rebuild or update it.",
    );
    if unknown.known.is_empty() {
        text.push_str(&format!(
            " Nothing is addressable beneath `{}`.",
            unknown.parent
        ));
    } else if unknown.is_family() {
        text.push_str(&format!(" Known families: {}.", unknown.known.join(", ")));
    } else {
        text.push_str(&format!(
            " Known under `{}`: {}.",
            unknown.parent,
            unknown.known.join(", ")
        ));
    }
    text
}

/// The refusal as typed data, for a transport that carries structure beside
/// the text (an MCP error's `data`). `refusal` is `malformed` or
/// `unknown_resource`, so a caller can tell skew from a garbled address
/// without reading the sentence.
pub fn refusal_data(refusal: &UriRefusal) -> serde_json::Value {
    match refusal {
        UriRefusal::Malformed(fault) => serde_json::json!({
            "refusal": "malformed",
            "fault": fault.to_string(),
        }),
        UriRefusal::Unknown(unknown) => serde_json::json!({
            "refusal": "unknown_resource",
            "parent": unknown.parent,
            "segment": unknown.segment,
            "known": unknown.known,
            "nearest": unknown.nearest,
            "runner": runner_identity(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_uri::{classify_uri, CairnResource, UriGrammar};

    const RUNNER: &str = "Cairn 0.13.7, build content-v2:33a079e4";

    fn is_dev(resource: &CairnResource) -> bool {
        matches!(
            resource,
            CairnResource::Dev
                | CairnResource::DevDb
                | CairnResource::DevPid
                | CairnResource::DevLaunchTicket
        )
    }

    #[test]
    fn an_unknown_family_names_the_runner_and_the_known_families() {
        let uri = "cairn://dev/launch-ticket";
        let refusal = UriGrammar::without(is_dev).classify(uri).unwrap_err();
        let text = message_as(uri, &refusal, RUNNER);
        assert!(
            text.starts_with(
                "This runner (Cairn 0.13.7, build content-v2:33a079e4) has no `cairn://dev` \
                 resource. If the address came from a newer client, this runner may be older \
                 than the client; rebuild or update it. Known families: "
            ),
            "{text}"
        );
        assert!(text.contains("models"), "{text}");
        assert!(!text.contains("Did you mean"), "{text}");
        assert!(!text.contains("Invalid"), "{text}");
    }

    #[test]
    fn a_near_miss_suggests_the_nearest_family() {
        let uri = "cairn://modles";
        let text = message_as(uri, &classify_uri(uri).unwrap_err(), RUNNER);
        assert!(
            text.contains("has no `cairn://modles` resource. Did you mean `cairn://models`?"),
            "{text}"
        );
    }

    #[test]
    fn an_unknown_sub_resource_lists_its_siblings() {
        let uri = "cairn://dev/launch-ticket";
        let refusal = UriGrammar::without(|r| matches!(r, CairnResource::DevLaunchTicket))
            .classify(uri)
            .unwrap_err();
        let text = message_as(uri, &refusal, RUNNER);
        assert!(
            text.contains("has no `cairn://dev/launch-ticket` resource."),
            "{text}"
        );
        assert!(
            text.ends_with("Known under `cairn://dev`: db, pid."),
            "{text}"
        );
    }

    #[test]
    fn a_garbled_address_says_what_is_wrong_with_it() {
        let uri = "cairn://posts//1";
        assert_eq!(
            message_as(uri, &classify_uri(uri).unwrap_err(), RUNNER),
            "Malformed cairn address `cairn://posts//1`: segment 2 is empty (a doubled or \
             trailing `/`)."
        );
    }

    #[test]
    fn the_typed_data_tells_skew_from_a_garbled_address() {
        let unknown = refusal_data(&classify_uri("cairn://modles").unwrap_err());
        assert_eq!(unknown["refusal"], "unknown_resource");
        assert_eq!(unknown["segment"], "modles");
        assert_eq!(unknown["nearest"], "models");
        assert!(unknown["runner"]
            .as_str()
            .unwrap()
            .starts_with(&format!("Cairn {}, build ", env!("CARGO_PKG_VERSION"))));

        let malformed = refusal_data(&classify_uri("cairn://posts//1").unwrap_err());
        assert_eq!(malformed["refusal"], "malformed");
    }
}
