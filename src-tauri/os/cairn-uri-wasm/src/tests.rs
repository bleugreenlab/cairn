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
