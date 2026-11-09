//! The cairn:// URI grammar as the frontend calls it (tenet 1435).
//!
//! Three functions cross the boundary, each taking a URI string, so the module
//! needs no JS-object bindings and never deserializes a resource:
//!
//! - [`parse`]: what the URI addresses, after thread delegation, as JSON
//!   `{"kind": <ResourceKind slug>, ...fields}`, the `serde` form of
//!   `cairn_uri::CairnResource`.
//! - [`label`]: its condensed label, `CairnResource::label`.
//! - [`surface`]: the view it opens, as JSON `{"surface": <name>, ...fields}`,
//!   the `serde` form of `cairn_uri::Surface`.
//! - [`canonical`]: the one spelling of what it addresses, `to_uri()` of the
//!   resolved resource, so two spellings of one address compare equal.
//!
//! Each returns `undefined` for an address that names nothing. The frontend
//! builds URIs from typed fields itself, so there is no JSON-to-URI export:
//! deserializing a resource was most of the module's size. `canonical` takes
//! a URI string, not a resource, and `to_uri` is already linked for
//! `Surface::Read`, so it costs the module almost nothing.

use cairn_uri::{delegate_thread_descendant, parse_uri, CairnResource, LabelContext};
use wasm_bindgen::prelude::*;

/// What `uri` addresses, after thread delegation. `None` when it names
/// nothing: unparseable, or a thread sub-path the resolver refuses (a retired
/// surface, or a branch-shaped segment under a thread).
pub fn resolve(uri: &str) -> Option<CairnResource> {
    delegate_thread_descendant(parse_uri(uri)?).ok()
}

/// [`resolve`] as JSON, or `undefined` in JS.
#[wasm_bindgen]
pub fn parse(uri: &str) -> Option<String> {
    resolve(uri)
        .map(|resource| serde_json::to_string(&resource).expect("a resource always serializes"))
}

/// [`label`] without the JS error type, so native callers (the fixture, the
/// tests) run exactly what the module runs. `Err` names a malformed context.
pub fn label_with(uri: &str, ctx_json: Option<&str>) -> Result<Option<String>, String> {
    let ctx: LabelContext = match ctx_json {
        Some(json) => serde_json::from_str(json).map_err(|error| error.to_string())?,
        None => LabelContext::default(),
    };
    Ok(resolve(uri).map(|resource| resource.label(&ctx)))
}

/// The condensed label of `uri`, or `undefined` when it names nothing.
/// `ctx_json` is `{"nodeLabel"?: string, "taskLabel"?: string}`, display
/// names for the job a node-family address names; malformed JSON throws.
#[wasm_bindgen]
pub fn label(uri: &str, ctx_json: Option<String>) -> Result<Option<String>, JsError> {
    label_with(uri, ctx_json.as_deref()).map_err(|error| JsError::new(&error))
}

/// The view `uri` opens, as JSON, or `undefined` when it names nothing.
#[wasm_bindgen]
pub fn surface(uri: &str) -> Option<String> {
    resolve(uri).map(|resource| {
        serde_json::to_string(&resource.surface()).expect("a surface always serializes")
    })
}

/// The canonical URI of what `uri` addresses, or `undefined` when it names
/// nothing. Lowercases the project and drops any query.
#[wasm_bindgen]
pub fn canonical(uri: &str) -> Option<String> {
    resolve(uri).map(|resource| resource.to_uri())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_uri::samples::every_resource_variant;

    /// The JSON tag is the kind's slug, so the frontend keys on the same
    /// names `cairn://help?kind=` and the contract table use.
    #[test]
    fn every_sample_is_tagged_with_its_kind_slug() {
        for sample in every_resource_variant() {
            let json = serde_json::to_value(&sample.resource).unwrap();
            assert_eq!(
                json["kind"],
                sample.resource.kind().slug(),
                "{}",
                sample.resource.to_uri()
            );
        }
    }

    #[test]
    fn every_sample_survives_parse_as_json() {
        for sample in every_resource_variant() {
            let uri = sample.resource.to_uri();
            let json = parse(&uri).unwrap_or_else(|| panic!("{uri} must resolve"));
            let back: CairnResource = serde_json::from_str(&json).unwrap();
            assert_eq!(back, sample.resource, "{uri}");
        }
    }

    #[test]
    fn every_sample_has_a_label_and_a_tagged_surface() {
        for sample in every_resource_variant() {
            let uri = sample.resource.to_uri();
            assert!(label_with(&uri, None).unwrap().is_some(), "{uri}");
            let surface: serde_json::Value = serde_json::from_str(&surface(&uri).unwrap()).unwrap();
            assert!(surface["surface"].is_string(), "{uri}: {surface}");
        }
    }

    #[test]
    fn two_spellings_of_one_address_have_one_canonical_form() {
        assert_eq!(
            canonical("cairn://p/CAIRN/42/1/builder/chat?limit=5").as_deref(),
            Some("cairn://p/cairn/42/1/builder/chat")
        );
        for sample in every_resource_variant() {
            let uri = sample.resource.to_uri();
            assert_eq!(canonical(&uri).as_deref(), Some(uri.as_str()));
        }
    }

    #[test]
    fn a_label_context_takes_the_frontend_option_names() {
        let uri = "cairn://p/CAIRN/42/1/builder/task/review/chat";
        assert_eq!(
            label_with(uri, Some(r#"{"nodeLabel":"Builder","taskLabel":"Review"}"#)).unwrap(),
            Some("cairn/42 / Builder / Review".to_string())
        );
        assert_eq!(
            label_with(uri, Some("{}")).unwrap(),
            Some("cairn/42 / builder / review".to_string())
        );
        assert!(label_with(uri, Some(r#"{"node_label":"Builder"}"#)).is_err());
        assert!(label_with(uri, Some("not json")).is_err());
    }

    #[test]
    fn a_refused_thread_path_resolves_to_nothing() {
        for uri in [
            "cairn://p/cairn/design-review/procedures",
            "cairn://p/cairn/design-review/diff",
            "https://example.com",
        ] {
            assert_eq!(parse(uri), None);
            assert_eq!(label_with(uri, None), Ok(None));
            assert_eq!(surface(uri), None);
            assert_eq!(canonical(uri), None);
        }
    }
}
