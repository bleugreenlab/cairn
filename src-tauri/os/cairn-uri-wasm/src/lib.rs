//! The cairn:// URI grammar as the frontend calls it (tenet 1435).
//!
//! Two functions cross the boundary, both as JSON strings so the module needs
//! no JS-object bindings: [`parse`] resolves a URI through the parser and the
//! thread resolver, and [`to_uri`] renders a resource back. A resource is
//! `{"kind": <ResourceKind slug>, ...fields}`, the `serde` form of
//! `cairn_uri::CairnResource`.

use cairn_uri::{delegate_thread_descendant, parse_uri, CairnResource};
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

/// The canonical URI of a resource given as [`parse`]'s JSON.
#[wasm_bindgen]
pub fn to_uri(json: &str) -> Result<String, JsError> {
    let resource: CairnResource =
        serde_json::from_str(json).map_err(|error| JsError::new(&error.to_string()))?;
    Ok(resource.to_uri())
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
    fn every_sample_survives_parse_and_to_uri_as_json() {
        for sample in every_resource_variant() {
            let uri = sample.resource.to_uri();
            let json = parse(&uri).unwrap_or_else(|| panic!("{uri} must resolve"));
            let back: CairnResource = serde_json::from_str(&json).unwrap();
            assert_eq!(back, sample.resource, "{uri}");
            assert_eq!(to_uri(&json).ok().as_deref(), Some(uri.as_str()));
        }
    }

    #[test]
    fn a_refused_thread_path_resolves_to_nothing() {
        assert_eq!(parse("cairn://p/cairn/design-review/procedures"), None);
        assert_eq!(parse("cairn://p/cairn/design-review/diff"), None);
        assert_eq!(parse("https://example.com"), None);
    }
}
