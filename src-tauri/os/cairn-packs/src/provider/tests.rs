use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tempfile::TempDir;

use super::http::{self, Inputs, ResponseMapping};
use super::*;

fn bundled_packs() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packs")
}

fn bundled(pack: &str, slot: Slot) -> PackProvider {
    load_pack_provider(&bundled_packs().join(pack), pack, slot)
        .unwrap()
        .unwrap_or_else(|| panic!("{pack} ships no {} provider", slot.as_str()))
}

const CREDENTIAL: &str = "label: K\nkind: secret\nvar: API_KEY\nhosts: [api.example.test]\naccessors: [provider:web-search]\n";

const PROVIDER: &str = r#"label: Example
provides: web-search
credential: api-key
options:
  - { key: count, label: Count, number: { min: 1, max: 20, integer: true }, default: 5 }
adapter:
  http:
    request:
      method: GET
      url: https://api.example.test/search?q=${query|query}&n=${options.count|query}
      headers: { Authorization: "Bearer ${credential}" }
    response:
      results: results
      fields: { title: [title], url: [url], snippet: [body], score: [score] }
"#;

fn pack(credential: &str, provider: &str) -> (TempDir, PathBuf) {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("example");
    std::fs::create_dir_all(root.join(CREDENTIALS_DIR)).unwrap();
    std::fs::create_dir_all(root.join(PROVIDERS_DIR)).unwrap();
    std::fs::write(root.join("credentials/api-key.yaml"), credential).unwrap();
    std::fs::write(root.join("providers/web-search.yaml"), provider).unwrap();
    (temp, root)
}

fn load_error(credential: &str, provider: &str) -> String {
    let (_temp, root) = pack(credential, provider);
    load_pack_providers(&root, "example").unwrap_err()
}

#[test]
fn a_well_formed_declaration_loads() {
    let (_temp, root) = pack(CREDENTIAL, PROVIDER);
    let providers = load_pack_providers(&root, "example").unwrap();
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].provider.slot, Slot::WebSearch);
    assert_eq!(providers[0].credential.var, "API_KEY");
    assert_eq!(
        providers[0].provider.adapter.request.host,
        "api.example.test"
    );
}

#[test]
fn sending_the_credential_to_an_undeclared_host_is_refused_naming_the_key() {
    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("https://api.example.test/", "https://api.evil.test/"),
    );
    assert_eq!(
        error,
        "provider `example/web-search`: would send credential `api-key` (API_KEY) to \
         `api.evil.test`, which is not in its `hosts`"
    );
}

#[test]
fn the_host_must_be_literal_and_every_url_interpolation_names_its_encoding() {
    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("https://api.example.test/", "https://${options.count|raw}/"),
    );
    assert!(error.contains("interpolates into its host"), "{error}");

    let error = load_error(CREDENTIAL, &PROVIDER.replace("${query|query}", "${query}"));
    assert!(error.contains("must name its encoding"), "{error}");

    let error = load_error(CREDENTIAL, &PROVIDER.replace("https://api", "http://api"));
    assert!(error.contains("must start with `https://`"), "{error}");

    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("Bearer ${credential}", "Bearer ${credential|raw}"),
    );
    assert!(error.contains("applies only inside `url`"), "{error}");
}

#[test]
fn only_declared_variables_for_the_slot_may_be_interpolated() {
    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("options.count", "options.depth"),
    );
    assert!(error.contains("undeclared option"), "{error}");

    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("${query|query}", "${target|query}"),
    );
    assert!(
        error.contains("not defined for a web-search provider"),
        "{error}"
    );

    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("${query|query}", "${env|query}"),
    );
    assert!(error.contains("is not a variable"), "{error}");
}

#[test]
fn the_slot_must_be_known_and_match_the_file_name() {
    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("provides: web-search", "provides: web-fetch"),
    );
    assert!(error.contains("must match the file name"), "{error}");

    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("provides: web-search", "provides: images"),
    );
    assert!(error.contains("is not a slot"), "{error}");

    let (_temp, root) = pack(CREDENTIAL, PROVIDER);
    std::fs::write(root.join("providers/images.yaml"), PROVIDER).unwrap();
    let error = load_pack_providers(&root, "example").unwrap_err();
    assert!(error.contains("`images.yaml` is not a slot"), "{error}");
}

#[test]
fn the_credential_must_exist_and_name_the_provider_as_an_accessor() {
    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("credential: api-key", "credential: token"),
    );
    assert!(
        error.contains("credential `token` is not declared"),
        "{error}"
    );

    let error = load_error(
        &CREDENTIAL.replace("provider:web-search", "provider:web-fetch"),
        PROVIDER,
    );
    assert!(
        error.contains("does not list `provider:web-search` in its `accessors`"),
        "{error}"
    );
}

#[test]
fn reserved_forms_parse_and_are_refused_as_not_supported_yet() {
    for (credential, provider) in [
        (
            CREDENTIAL.replace("kind: secret", "kind: oauth"),
            PROVIDER.to_string(),
        ),
        (
            CREDENTIAL.replace("[provider:web-search]", "[provider:web-search, run]"),
            PROVIDER.to_string(),
        ),
        (
            CREDENTIAL.replace(
                "[provider:web-search]",
                "[provider:web-search, tool:scrape]",
            ),
            PROVIDER.to_string(),
        ),
        (
            CREDENTIAL.to_string(),
            PROVIDER.replace("  http:\n", "  tool:\n"),
        ),
        (
            CREDENTIAL.to_string(),
            PROVIDER.replace("  http:\n", "  mcp:\n"),
        ),
    ] {
        let error = load_error(&credential, &provider);
        assert!(error.contains(NOT_SUPPORTED_YET), "{error}");
    }
}

#[test]
fn a_pdf_provider_and_json_markdown_are_not_supported_yet() {
    let cred = "label: K\nkind: secret\nvar: API_KEY\nhosts: [api.example.test]\naccessors: [provider:web-fetch, provider:pdf]\n";
    let fetch = |markdown: &str| {
        format!(
            "label: F\nprovides: web-fetch\ncredential: api-key\nadapter:\n  http:\n    request:\n      method: GET\n      url: https://api.example.test/${{target|raw}}\n    response:\n      markdown: {markdown}\n"
        )
    };
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("example");
    std::fs::create_dir_all(root.join("providers")).unwrap();
    std::fs::create_dir_all(root.join("credentials")).unwrap();
    std::fs::write(root.join("credentials/api-key.yaml"), cred).unwrap();

    std::fs::write(root.join("providers/web-fetch.yaml"), fetch("body")).unwrap();
    let providers = load_pack_providers(&root, "example").unwrap();
    assert_eq!(
        providers[0].provider.adapter.response,
        ResponseMapping::MarkdownBody
    );

    std::fs::write(
        root.join("providers/web-fetch.yaml"),
        fetch("{json: [data.markdown]}"),
    )
    .unwrap();
    let error = load_pack_providers(&root, "example").unwrap_err();
    assert!(error.contains(NOT_SUPPORTED_YET), "{error}");

    std::fs::remove_file(root.join("providers/web-fetch.yaml")).unwrap();
    std::fs::write(
        root.join("providers/pdf.yaml"),
        fetch("body").replace("provides: web-fetch", "provides: pdf"),
    )
    .unwrap();
    let error = load_pack_providers(&root, "example").unwrap_err();
    assert!(error.contains(NOT_SUPPORTED_YET), "{error}");
}

#[test]
fn the_workspace_owner_segment_is_reserved() {
    let (_temp, root) = pack(CREDENTIAL, PROVIDER);
    let error = load_pack_credentials(&root, RESERVED_CREDENTIAL_OWNER).unwrap_err();
    assert!(error.contains("reserved"), "{error}");
}

#[test]
fn option_descriptors_are_validated() {
    let error = load_error(CREDENTIAL, &PROVIDER.replace("default: 5", "default: 50"));
    assert!(error.contains("outside"), "{error}");
    let error = load_error(CREDENTIAL, &PROVIDER.replace("default: 5", "default: 2.5"));
    assert!(error.contains("integer"), "{error}");
}

/// Every bundled pack validates, so a broken declaration cannot ship.
#[test]
fn every_bundled_pack_validates() {
    let mut provider_packs = Vec::new();
    for entry in std::fs::read_dir(bundled_packs()).unwrap() {
        let root = entry.unwrap().path();
        let id = root.file_name().unwrap().to_str().unwrap().to_string();
        let providers = load_pack_providers(&root, &id).unwrap();
        if !providers.is_empty() {
            provider_packs.push(id);
        }
    }
    provider_packs.sort();
    assert_eq!(provider_packs, ["brave", "exa", "jina", "tavily"]);
}

fn yaml_options(value: &Value) -> HashMap<String, serde_yaml::Value> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), serde_yaml::to_value(v).unwrap()))
        .collect()
}

fn goldens() -> Value {
    serde_json::from_str(include_str!("testdata/search_goldens.json")).unwrap()
}

fn search_mapping(provider: &PackProvider) -> &http::SearchMapping {
    match &provider.provider.adapter.response {
        ResponseMapping::Search(mapping) => mapping,
        other => panic!("not a search mapping: {other:?}"),
    }
}

/// The requests the compiled adapters built, captured before the port, are
/// reproduced exactly by the bundled declarations.
#[test]
fn the_bundled_declarations_reproduce_every_captured_request() {
    for case in goldens()["requests"].as_array().unwrap() {
        let pack_id = case["provider"].as_str().unwrap();
        let provider = bundled(pack_id, Slot::WebSearch);
        let options = yaml_options(&case["options"]);
        let prepared = http::prepare(
            &provider.provider,
            &Inputs {
                query: case["query"].as_str(),
                target: None,
                credential: case["key"].as_str().unwrap(),
                options: &options,
            },
        );
        assert_eq!(prepared.method, case["method"], "{pack_id}");
        assert_eq!(prepared.url, case["url"], "{pack_id}");
        let headers: Vec<(String, String)> =
            serde_json::from_value(case["headers"].clone()).unwrap();
        assert_eq!(prepared.headers, headers, "{pack_id}");
        let body = prepared
            .body
            .as_deref()
            .map(|body| serde_json::from_str::<Value>(body).unwrap())
            .unwrap_or(Value::Null);
        assert_eq!(body, case["body"], "{pack_id}");
    }
}

/// The results the compiled parsers produced from each captured fixture are
/// reproduced exactly, title-from-url fallback included.
#[test]
fn the_bundled_declarations_reproduce_every_captured_result_list() {
    for case in goldens()["renders"].as_array().unwrap() {
        let pack_id = case["provider"].as_str().unwrap();
        let provider = bundled(pack_id, Slot::WebSearch);
        let hits = http::map_search(search_mapping(&provider), case["raw"].as_str().unwrap())
            .unwrap_or_else(|e| panic!("{pack_id}: {e}"));
        let got: Vec<Value> = hits
            .iter()
            .map(|hit| {
                serde_json::json!({
                    "title": hit.title, "url": hit.url, "snippet": hit.snippet, "score": hit.score,
                })
            })
            .collect();
        assert_eq!(
            Value::Array(got),
            case["results"],
            "{pack_id}: {}",
            case["raw"]
        );
        assert_eq!(provider.provider.label, case["label"], "{pack_id}");
    }
}

#[test]
fn a_non_json_body_reports_the_parser_error_the_compiled_adapter_did() {
    let provider = bundled("tavily", Slot::WebSearch);
    let error = http::map_search(search_mapping(&provider), "not json").unwrap_err();
    let golden = goldens()["errors"]["non_json"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        golden.contains(&format!("({error})")),
        "{error} vs {golden}"
    );
}

/// A JSON null in a result field is the same as the field being absent, for
/// every provider: responses that failed to parse before now render.
#[test]
fn a_null_field_is_treated_as_missing() {
    let tavily = bundled("tavily", Slot::WebSearch);
    let hits = http::map_search(
        search_mapping(&tavily),
        r#"{"results":[{"title":null,"url":"https://t.test","content":null,"score":null}]}"#,
    )
    .unwrap();
    assert_eq!(hits[0].title, "https://t.test");
    assert_eq!(hits[0].snippet, "");
    assert_eq!(hits[0].score, None);

    let brave = bundled("brave", Slot::WebSearch);
    assert!(http::map_search(search_mapping(&brave), r#"{"web":null}"#)
        .unwrap()
        .is_empty());
    assert!(
        http::map_search(search_mapping(&brave), r#"{"web":{"results":null}}"#)
            .unwrap()
            .is_empty()
    );

    let jina = bundled("jina", Slot::WebSearch);
    let hits = http::map_search(
        search_mapping(&jina),
        r#"{"data":[{"title":"J","url":"https://j.test","description":null,"content":"body"}]}"#,
    )
    .unwrap();
    assert_eq!(hits[0].snippet, "body");
}

#[test]
fn a_field_of_the_wrong_type_is_a_shape_error() {
    let tavily = bundled("tavily", Slot::WebSearch);
    let mapping = search_mapping(&tavily);
    let error = http::map_search(
        mapping,
        r#"{"results":[{"title":3,"url":"https://t.test"}]}"#,
    )
    .unwrap_err();
    assert!(error.contains("`title` of result 1"), "{error}");
    let error = http::map_search(mapping, r#"{"results":5}"#).unwrap_err();
    assert!(error.contains("is not a list"), "{error}");
}

#[test]
fn jina_fetch_sends_the_target_raw_with_the_shared_credential() {
    let fetch = bundled("jina", Slot::WebFetch);
    let search = bundled("jina", Slot::WebSearch);
    assert_eq!(
        fetch.credential, search.credential,
        "one credential, two readers"
    );
    assert_eq!(
        fetch.provider.adapter.response,
        ResponseMapping::MarkdownBody
    );
    let prepared = http::prepare(
        &fetch.provider,
        &Inputs {
            query: None,
            target: Some("https://example.com/a?b=c"),
            credential: "tok",
            options: &HashMap::new(),
        },
    );
    assert_eq!(prepared.method, "GET");
    assert_eq!(prepared.url, "https://r.jina.ai/https://example.com/a?b=c");
    assert_eq!(
        prepared.headers,
        vec![
            ("Authorization".to_string(), "Bearer tok".to_string()),
            ("X-Return-Format".to_string(), "markdown".to_string()),
        ]
    );
    assert_eq!(prepared.body, None);
}

#[test]
fn a_stored_non_integer_for_an_integer_option_falls_back_to_the_default() {
    let brave = bundled("brave", Slot::WebSearch);
    let options = HashMap::from([
        ("count".to_string(), serde_yaml::Value::from(2.5)),
        ("safesearch".to_string(), serde_yaml::Value::from(7)),
    ]);
    let prepared = http::prepare(
        &brave.provider,
        &Inputs {
            query: Some("q"),
            target: None,
            credential: "k",
            options: &options,
        },
    );
    assert!(
        prepared.url.ends_with("&count=10&safesearch=moderate"),
        "{}",
        prepared.url
    );
}
