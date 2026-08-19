//! Tab completion, sourced from what is already on screen.
//!
//! The last render names its own children — an issue collection lists issues, an
//! issue lists its nodes, a node lists its artifacts — so completion is a matter
//! of reading them back out rather than of knowing anything about the graph.
//! [`scan_uris`] is the tested authority for pulling cairn:// URIs out of prose,
//! so this module does no markdown parsing of its own.
//!
//! Verb candidates come from the same affordance spec that defines the commands,
//! which keeps completion honest by construction: it can only offer what the
//! resource actually accepts.

use cairn_common::contract::RESOURCE_CONTRACTS;
use cairn_common::read::AffordanceSpec;
use cairn_common::uri::{scan_uris, ROOT_URI};

use super::actions;

/// Builtins that exist at every location.
const BUILTINS: &[&str] = &["help", "ls", "pwd", "quit", "read", "watch"];

/// Completions for the word being typed at `cwd`.
///
/// A word starting with `:` completes commands; anything else completes places.
pub(crate) fn candidates(
    cwd: &str,
    last_text: &str,
    spec: Option<&AffordanceSpec>,
    word: &str,
) -> Vec<String> {
    let mut out = match word.strip_prefix(':') {
        Some(partial) => verb_candidates(spec, partial),
        None => navigation_candidates(cwd, last_text, spec)
            .into_iter()
            .filter(|candidate| candidate.starts_with(word))
            .collect(),
    };
    out.sort();
    out.dedup();
    out
}

fn verb_candidates(spec: Option<&AffordanceSpec>, partial: &str) -> Vec<String> {
    spec.map(actions::verbs)
        .unwrap_or_default()
        .into_iter()
        .chain(BUILTINS.iter().map(|builtin| builtin.to_string()))
        .filter(|verb| verb.starts_with(partial))
        .map(|verb| format!(":{verb}"))
        .collect()
}

/// Every place reachable from `cwd` by typing, named the way it would be typed.
pub(crate) fn navigation_candidates(
    cwd: &str,
    last_text: &str,
    spec: Option<&AffordanceSpec>,
) -> Vec<String> {
    let mut out: Vec<String> = scan_uris(last_text)
        .into_iter()
        .filter_map(|found| relative_to(cwd, &found.uri))
        .collect();

    // Named collections a resource advertises are children too, and a link is
    // often the only place a collection like `/messages` is written down.
    if let Some(spec) = spec {
        out.extend(
            spec.links
                .iter()
                .filter_map(|link| link.uri.as_deref())
                .filter_map(|uri| relative_to(cwd, uri)),
        );
    }

    // The root has no render of its own to scan for the workspace-level
    // collections, and the contract table already names every one of them.
    if cwd == ROOT_URI {
        out.extend(root_collections());
    }

    out.push("..".to_string());
    out.sort();
    out.dedup();
    out
}

/// The single-segment resources that live directly under the root, derived from
/// the contract table so a new global family completes the day it is declared.
fn root_collections() -> Vec<String> {
    RESOURCE_CONTRACTS
        .iter()
        .filter_map(|contract| contract.uri_template.strip_prefix(ROOT_URI))
        .filter(|segment| !segment.is_empty() && !segment.contains('/') && !segment.contains('{'))
        .map(str::to_string)
        .collect()
}

/// How `uri` would be typed from `cwd`, or `None` when it is not below it.
fn relative_to(cwd: &str, uri: &str) -> Option<String> {
    let rest = if cwd == ROOT_URI {
        uri.strip_prefix(ROOT_URI)?
    } else {
        uri.strip_prefix(cwd)?.strip_prefix('/')?
    };
    (!rest.is_empty()).then(|| rest.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::read::{ActionSpec, LinkSpec};

    fn spec_with(actions: Vec<&str>, links: Vec<(&str, &str)>) -> AffordanceSpec {
        AffordanceSpec {
            kind: "issue".to_string(),
            name: "Issue".to_string(),
            links: links
                .into_iter()
                .map(|(label, uri)| LinkSpec {
                    label: label.to_string(),
                    uri_template: uri.to_string(),
                    uri: Some(uri.to_string()),
                })
                .collect(),
            filters: Vec::new(),
            actions: actions
                .into_iter()
                .map(|label| ActionSpec {
                    label: label.to_string(),
                    mode: "append".to_string(),
                    uri_template: String::new(),
                    uri: None,
                    required: Vec::new(),
                    optional: Vec::new(),
                    example: String::new(),
                    guidance: None,
                })
                .collect(),
        }
    }

    #[test]
    fn children_of_the_last_render_are_offered_as_they_would_be_typed() {
        let rendered = "nodes:\ncairn://p/cairn/4279/1/builder and cairn://p/cairn/4279/1/planner";
        let candidates = candidates("cairn://p/cairn/4279", rendered, None, "1/");
        assert_eq!(candidates, ["1/builder", "1/planner"]);
    }

    #[test]
    fn a_uri_outside_the_cursor_is_not_offered_as_a_child() {
        let rendered = "see also cairn://p/other/9";
        assert!(
            navigation_candidates("cairn://p/cairn/4279", rendered, None)
                .iter()
                .all(|candidate| candidate != "cairn://p/other/9")
        );
    }

    #[test]
    fn advertised_links_complete_even_when_the_body_never_writes_them() {
        let spec = spec_with(
            Vec::new(),
            vec![("messages", "cairn://p/cairn/4279/messages")],
        );
        assert!(
            navigation_candidates("cairn://p/cairn/4279", "", Some(&spec))
                .contains(&"messages".to_string())
        );
    }

    #[test]
    fn the_root_offers_the_workspace_collections_the_contract_declares() {
        let candidates = navigation_candidates(ROOT_URI, "", None);
        // Derived from the contract table, never hand-listed here.
        assert!(candidates.contains(&"posts".to_string()), "{candidates:?}");
        assert!(candidates.contains(&"skills".to_string()), "{candidates:?}");
        assert!(
            candidates.contains(&"projects".to_string()),
            "{candidates:?}"
        );
    }

    #[test]
    fn a_colon_completes_the_commands_this_resource_advertises() {
        let spec = spec_with(vec!["append comment", "patch issue"], Vec::new());
        assert_eq!(
            candidates("cairn://p/cairn/4279", "", Some(&spec), ":a"),
            [":append-comment"]
        );
        // Builtins are always reachable, resource or not.
        assert_eq!(
            candidates("cairn://p/cairn/4279", "", None, ":q"),
            [":quit"]
        );
    }

    #[test]
    fn climbing_is_always_offered() {
        assert!(navigation_candidates("cairn://p/cairn/4279", "", None).contains(&"..".to_string()));
    }
}
