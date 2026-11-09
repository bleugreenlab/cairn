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
mod tests;
