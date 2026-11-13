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
    assert_eq!(
        providers[0].credential.as_ref().unwrap().var(),
        Some("API_KEY")
    );
    assert_eq!(
        providers[0].provider.http().unwrap().request.host,
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
    ] {
        let error = load_error(&credential, &provider);
        assert!(error.contains(NOT_SUPPORTED_YET), "{error}");
    }
}

#[test]
fn an_http_pdf_provider_is_not_supported_yet() {
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
        providers[0].provider.http().unwrap().response,
        ResponseMapping::MarkdownBody
    );

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
fn the_bundled_bmd_pack_binds_its_fetch_tool_to_the_pdf_slot() {
    let pdf = bundled("bmd", Slot::Pdf);
    let Adapter::Mcp(binding) = &pdf.provider.adapter else {
        panic!("bmd's pdf provider is an MCP binding");
    };
    assert_eq!(
        (binding.server.as_str(), binding.tool.as_str()),
        ("bmd", "fetch")
    );
    let arguments = binding::arguments(
        &pdf.provider,
        binding,
        &Inputs {
            query: None,
            target: Some("https://x.test/a.pdf"),
            credential: "",
            options: &Default::default(),
        },
    );
    assert_eq!(
        arguments,
        serde_json::json!({ "source": "https://x.test/a.pdf" })
    );
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
    assert_eq!(
        provider_packs,
        [
            "bmd",
            "brave",
            "cloudflare",
            "exa",
            "firecrawl",
            "jina",
            "tavily"
        ]
    );
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
    match &provider.provider.http().unwrap().response {
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
            provider.provider.http().unwrap(),
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

/// Jina percent-decodes its path once, so the target is sent fully encoded:
/// escapes, `?`, `&` and `#` in it reach the origin exactly.
#[test]
fn jina_fetch_sends_the_target_encoded_with_the_shared_credential() {
    let fetch = bundled("jina", Slot::WebFetch);
    let search = bundled("jina", Slot::WebSearch);
    assert_eq!(
        fetch.credential, search.credential,
        "one credential, two readers"
    );
    assert_eq!(
        fetch.provider.http().unwrap().response,
        ResponseMapping::MarkdownBody
    );
    let prepare = |target: &str| {
        http::prepare(
            &fetch.provider,
            fetch.provider.http().unwrap(),
            &Inputs {
                query: None,
                target: Some(target),
                credential: "tok",
                options: &HashMap::new(),
            },
        )
    };
    let prepared = prepare("https://httpbingo.org/anything/a%20b?q=a%26b&z=%23x");
    assert_eq!(prepared.method, "GET");
    assert_eq!(
        prepared.url,
        "https://r.jina.ai/https%3A%2F%2Fhttpbingo.org%2Fanything%2Fa%2520b%3Fq%3Da%2526b%26z%3D%2523x"
    );
    assert_eq!(
        prepared.headers,
        vec![
            ("Authorization".to_string(), "Bearer tok".to_string()),
            ("X-Return-Format".to_string(), "markdown".to_string()),
        ]
    );
    assert_eq!(prepared.body, None);
}

fn fetch_goldens() -> Value {
    serde_json::from_str(include_str!("testdata/fetch_goldens.json")).unwrap()
}

/// A golden case with its deliberate changes applied.
fn now(case: &Value) -> Value {
    let mut case = case.clone();
    if let Some(Value::Object(changed)) = case.get("now").cloned() {
        for (key, value) in changed {
            case[key.as_str()] = value;
        }
    }
    case
}

/// The requests the compiled fetch adapters built are reproduced by the
/// bundled declarations, except where a case records a deliberate change.
#[test]
fn the_bundled_fetch_declarations_reproduce_every_captured_request() {
    let goldens = fetch_goldens();
    let target = goldens["target"].as_str().unwrap();
    for case in goldens["requests"].as_array().unwrap() {
        let case = now(case);
        let pack_id = case["provider"].as_str().unwrap();
        let provider = bundled(pack_id, Slot::WebFetch);
        let options = yaml_options(&case["options"]);
        let prepared = http::prepare(
            &provider.provider,
            provider.provider.http().unwrap(),
            &Inputs {
                query: None,
                target: Some(target),
                credential: case["credential"].as_str().unwrap(),
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

/// bmd binds its own pack's `bmd` server's `fetch` tool with `{source}`.
#[test]
fn the_bmd_binding_calls_fetch_with_the_target_as_source() {
    let golden = &fetch_goldens()["bmd"];
    let provider = bundled("bmd", Slot::WebFetch);
    assert_eq!(provider.credential, None);
    let Adapter::Mcp(binding) = &provider.provider.adapter else {
        panic!("bmd is not an MCP binding");
    };
    assert_eq!(binding.server, golden["server"]);
    assert_eq!(binding.tool, golden["tool"]);
    assert_eq!(binding.result, binding::McpResult::Text);
    let arguments = binding::arguments(
        &provider.provider,
        binding,
        &Inputs {
            query: None,
            target: fetch_goldens()["target"].as_str(),
            credential: "",
            options: &HashMap::new(),
        },
    );
    assert_eq!(arguments, golden["arguments"]);
}

const FETCH_CREDENTIAL: &str = "label: K\nkind: secret\nvar: API_KEY\nhosts: [api.example.test]\naccessors: [provider:web-fetch]\n";

/// A pack root holding one fetch provider, a credential, and optionally an
/// `mcp.yaml`.
fn fetch_pack(credential: Option<&str>, provider: &str, mcp: Option<&str>) -> (TempDir, PathBuf) {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("example");
    std::fs::create_dir_all(root.join(PROVIDERS_DIR)).unwrap();
    if let Some(credential) = credential {
        std::fs::create_dir_all(root.join(CREDENTIALS_DIR)).unwrap();
        std::fs::write(root.join("credentials/api-key.yaml"), credential).unwrap();
    }
    if let Some(mcp) = mcp {
        std::fs::write(root.join("mcp.yaml"), mcp).unwrap();
    }
    std::fs::write(root.join("providers/web-fetch.yaml"), provider).unwrap();
    (temp, root)
}

fn json_fetch(response: &str) -> String {
    format!(
        "label: F\nprovides: web-fetch\ncredential: api-key\nadapter:\n  http:\n    request:\n      method: POST\n      url: https://api.example.test/scrape\n      json: {{ url: \"${{target}}\" }}\n    response:\n{response}"
    )
}

fn json_mapping(response: &str) -> Result<http::JsonMarkdown, String> {
    let (_temp, root) = fetch_pack(Some(FETCH_CREDENTIAL), &json_fetch(response), None);
    let providers = load_pack_providers(&root, "example")?;
    match providers[0].provider.http().unwrap().response.clone() {
        ResponseMapping::MarkdownJson(mapping) => Ok(mapping),
        other => panic!("not a JSON mapping: {other:?}"),
    }
}

#[test]
fn json_markdown_takes_the_first_text_path_behind_its_success_gate() {
    use http::MarkdownFailure::*;
    let gated = json_mapping(
        "      success: { path: ok, error: [errors.0.message, error] }\n      markdown: { json: [data.markdown, result] }\n",
    )
    .unwrap();
    let map = |body: &str| http::map_markdown(&gated, body);
    assert_eq!(
        map(r##"{"ok":true,"data":{"markdown":"# A"}}"##),
        Ok("# A".into())
    );
    assert_eq!(
        map(r##"{"ok":true,"data":{"markdown":"  "},"result":"# B"}"##),
        Ok("# B".into()),
        "a blank value is absent"
    );
    assert_eq!(map(r##"{"ok":true,"data":null}"##), Err(Missing));
    assert_eq!(
        map(r##"{"ok":false,"errors":[{"message":"denied"}]}"##),
        Err(Unsuccessful(Some("denied".into())))
    );
    assert_eq!(
        map(r##"{"ok":false,"error":"nope"}"##),
        Err(Unsuccessful(Some("nope".into())))
    );
    assert_eq!(map(r##"{"ok":false}"##), Err(Unsuccessful(None)));
    assert!(
        matches!(map(r##"{"data":{"markdown":"# A"}}"##), Err(Shape(e)) if e.contains("`ok` is not a boolean"))
    );
    assert!(
        matches!(map(r##"{"ok":true,"result":3}"##), Err(Shape(e)) if e.contains("`result` is not text"))
    );
    assert!(matches!(map("not json"), Err(Shape(_))));
    assert_eq!(
        http::error_detail(&gated, r##"{"errors":[{"message":"rate"}]}"##),
        Some("rate".into())
    );
    assert_eq!(http::error_detail(&gated, "<html>"), None);

    let ungated = json_mapping("      markdown: { json: [result] }\n").unwrap();
    assert_eq!(
        http::map_markdown(&ungated, r##"{"result":"# C"}"##),
        Ok("# C".into())
    );
    assert_eq!(http::error_detail(&ungated, r##"{"error":"x"}"##), None);
}

#[test]
fn a_success_gate_needs_a_json_body() {
    let error = json_mapping("      success: { path: ok }\n      markdown: body\n").unwrap_err();
    assert!(
        error.contains("needs `markdown: {json: [paths]}`"),
        "{error}"
    );
    let error = json_mapping("      markdown: { json: [] }\n").unwrap_err();
    assert!(error.contains("at least one path"), "{error}");
    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace(
            "      results: results\n",
            "      results: results\n      success: { path: ok }\n",
        ),
    );
    assert!(
        error.contains("`success` gate applies to a fetch"),
        "{error}"
    );
}

const MCP_YAML: &str = "mcpServers:\n  reader:\n    type: http\n    url: https://reader.example.test/mcp\n    oauth: {}\n";

fn binding(adapter: &str) -> String {
    format!(
        "label: Reader\nprovides: web-fetch\noptions:\n  - {{ key: mode, label: Mode, select: [[a, A], [b, B]], default: a }}\nadapter:\n  mcp:\n{adapter}"
    )
}

const BINDING: &str = "    server: reader\n    tool: fetch\n    arguments: { source: \"${target}\", mode: \"${options.mode}\" }\n    result: text\n";

/// Search has no result contract for a tool's text, so a search provider may
/// not bind one: it would install, show ready and be selectable, then fail.
#[test]
fn an_mcp_binding_is_refused_for_web_search() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("example");
    std::fs::create_dir_all(root.join(PROVIDERS_DIR)).unwrap();
    std::fs::write(root.join("mcp.yaml"), MCP_YAML).unwrap();
    std::fs::write(
        root.join("providers/web-search.yaml"),
        "label: Reader\nprovides: web-search\nadapter:\n  mcp:\n    server: reader\n    tool: search\n    arguments: { query: \"${query}\" }\n    result: text\n",
    )
    .unwrap();
    let error = load_pack_providers(&root, "example").unwrap_err();
    assert!(
        error.contains("adapter `mcp` for `web-search` is not supported yet"),
        "{error}"
    );
}

#[test]
fn an_mcp_binding_names_a_server_in_its_own_pack() {
    let (_temp, root) = fetch_pack(None, &binding(BINDING), Some(MCP_YAML));
    let providers = load_pack_providers(&root, "example").unwrap();
    let Adapter::Mcp(bound) = &providers[0].provider.adapter else {
        panic!("not a binding");
    };
    assert_eq!(bound.server, "reader");
    assert_eq!(providers[0].credential, None);
    let stored = HashMap::from([("mode".to_string(), serde_yaml::Value::from("zz"))]);
    let arguments = binding::arguments(
        &providers[0].provider,
        bound,
        &Inputs {
            query: None,
            target: Some("https://t.test/"),
            credential: "",
            options: &stored,
        },
    );
    assert_eq!(
        arguments,
        serde_json::json!({"source": "https://t.test/", "mode": "a"}),
        "a stored select value outside the choices falls back to the default"
    );

    let refused = |provider: String, mcp: Option<&str>, credential: Option<&str>| {
        let (_temp, root) = fetch_pack(credential, &provider, mcp);
        load_pack_providers(&root, "example").unwrap_err()
    };
    let error = refused(binding(BINDING), None, None);
    assert!(
        error.contains("names server `reader`, which this pack's `mcp.yaml` does not declare"),
        "{error}"
    );
    let error = refused(
        binding(BINDING),
        Some(&MCP_YAML.replace("reader:", "other:")),
        None,
    );
    assert!(error.contains("does not declare"), "{error}");
    let error = refused(
        binding(BINDING).replace(
            "provides: web-fetch\n",
            "provides: web-fetch\ncredential: api-key\n",
        ),
        Some(MCP_YAML),
        Some(FETCH_CREDENTIAL),
    );
    assert!(error.contains("takes no `credential:`"), "{error}");
    let error = refused(
        binding(&BINDING.replace("${options.mode}", "${credential}")),
        Some(MCP_YAML),
        None,
    );
    assert!(
        error.contains("not available to an `mcp` adapter"),
        "{error}"
    );
    let error = refused(
        binding(&BINDING.replace("options.mode", "options.depth")),
        Some(MCP_YAML),
        None,
    );
    assert!(error.contains("undeclared option"), "{error}");
    let error = refused(
        binding(&BINDING.replace("tool: fetch", "tool: \" \"")),
        Some(MCP_YAML),
        None,
    );
    assert!(error.contains("needs a `tool`"), "{error}");
    let error = refused(
        binding(&BINDING.replace("result: text", "result: structured")),
        Some(MCP_YAML),
        None,
    );
    assert!(error.contains(NOT_SUPPORTED_YET), "{error}");
    let error = refused(
        binding(&BINDING.replace("result: text", "result: images")),
        Some(MCP_YAML),
        None,
    );
    assert!(error.contains("unknown `result"), "{error}");
}

const OAUTH_CREDENTIAL: &str = "label: Example login\nkind: oauth\nhosts: [api.example.test]\naccessors: [provider:web-fetch]\noauth:\n  authorize: https://login.example.test/oauth2/auth\n  token: https://login.example.test/oauth2/token\n  clientId: abc123\n  scopes: [read.write]\n  redirect: https://cairn.computer/oauth/example/callback\n";

#[test]
fn an_oauth_credential_declares_its_client_and_redirect() {
    let credential = parse_credential("login", OAUTH_CREDENTIAL).unwrap();
    assert_eq!(credential.var(), None);
    let oauth = credential.oauth().unwrap();
    assert_eq!(oauth.client_id, "abc123");
    assert_eq!(oauth.redirect, OAuthRedirect::Relay("example".into()));
    assert_eq!(
        oauth.redirect.relay_uri().as_deref(),
        Some("https://cairn.computer/oauth/example/callback")
    );
    let loopback = parse_credential(
        "login",
        &OAUTH_CREDENTIAL.replace("https://cairn.computer/oauth/example/callback", "loopback"),
    )
    .unwrap();
    assert_eq!(loopback.oauth().unwrap().redirect, OAuthRedirect::Loopback);

    for (from, to, expect) in [
        (
            "https://login.example.test/oauth2/auth",
            "http://login.example.test/a",
            "must start with `https://`",
        ),
        (
            "https://login.example.test/oauth2/token",
            "https://${options.host}/t",
            "literal lowercase host",
        ),
        ("[read.write]", "[]", "at least one scope"),
        ("[read.write]", "[\"a b\"]", "is not one scope"),
        (
            "clientId: abc123",
            "clientId: \" \"",
            "pre-registered client id",
        ),
        (
            "https://cairn.computer/oauth/example/callback",
            "https://evil.test/cb",
            "must be `loopback` or",
        ),
        (
            "https://cairn.computer/oauth/example/callback",
            "https://cairn.computer/oauth/Ex/callback",
            "must be `loopback` or",
        ),
    ] {
        let error = parse_credential("login", &OAUTH_CREDENTIAL.replace(from, to)).unwrap_err();
        assert!(error.contains(expect), "{to}: {error}");
    }
    let error = parse_credential(
        "login",
        &OAUTH_CREDENTIAL.replace("kind: oauth", "kind: secret"),
    )
    .unwrap_err();
    assert!(error.contains("takes no `oauth` block"), "{error}");
    let error = parse_credential("k", &CREDENTIAL.replace("var: API_KEY\n", "")).unwrap_err();
    assert!(error.contains("needs `var`"), "{error}");
}

/// An access token is a credential like any other: it may only be sent to the
/// hosts it lists.
#[test]
fn an_oauth_token_sent_to_an_undeclared_host_is_refused() {
    let provider = json_fetch("      markdown: { json: [result] }\n")
        .replace("https://api.example.test/", "https://api.evil.test/");
    let (_temp, root) = fetch_pack(Some(OAUTH_CREDENTIAL), &provider, None);
    let error = load_pack_providers(&root, "example").unwrap_err();
    assert!(
        error.contains("would send credential `api-key` (oauth) to `api.evil.test`"),
        "{error}"
    );
}

#[test]
fn two_credentials_in_one_pack_may_not_claim_one_relay() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("example");
    std::fs::create_dir_all(root.join(CREDENTIALS_DIR)).unwrap();
    std::fs::write(root.join("credentials/a.yaml"), OAUTH_CREDENTIAL).unwrap();
    std::fs::write(root.join("credentials/b.yaml"), OAUTH_CREDENTIAL).unwrap();
    let error = load_pack_credentials(&root, "example").unwrap_err();
    assert!(error.contains("relay `example`"), "{error}");
}

#[test]
fn only_a_text_option_or_a_fetched_select_can_be_required() {
    let error = load_error(
        CREDENTIAL,
        &PROVIDER.replace("default: 5 }", "default: 5, required: true }"),
    );
    assert!(
        error.contains("only a `text` option or a `select.from` can be `required`"),
        "{error}"
    );
}

const ACCOUNTS: &str = "  - key: account\n    label: Account\n    required: true\n    select:\n      from:\n        request:\n          method: GET\n          url: https://api.example.test/accounts\n          headers: { Authorization: \"Bearer ${credential}\" }\n        items: result\n        value: id\n        label: name\n";

fn with_accounts(provider: &str, accounts: &str) -> String {
    provider.replace("options:\n", &format!("options:\n{accounts}"))
}

/// A select may take its choices from a request made with the provider's own
/// credential; it is required like a text option and has no default.
#[test]
fn a_select_may_take_its_choices_from_the_providers_credential() {
    let (_temp, root) = pack(CREDENTIAL, &with_accounts(PROVIDER, ACCOUNTS));
    let providers = load_pack_providers(&root, "example").unwrap();
    let account = &providers[0].provider.options[0];
    assert!(account.required);
    let source = account.control.choice_source().expect("a dynamic select");
    assert_eq!(source.request.host, "api.example.test");
    let prepared = http::prepare_choices(source, "tok");
    assert_eq!(
        (prepared.method.as_str(), prepared.url.as_str()),
        ("GET", "https://api.example.test/accounts")
    );
    assert_eq!(
        prepared.headers,
        vec![("Authorization".to_string(), "Bearer tok".to_string())]
    );
    // A label never falls back to the value, an id the operator has no use
    // for: an unnamed item reads `Unnamed`, and a repeated name is numbered.
    let choices = http::map_choices(
        source,
        r#"{"result":[{"id":"b2","name":"beta"},{"id":"a1","name":"Alpha"},{"id":7},{"id":"a0","name":"alpha"}]}"#,
    )
    .unwrap();
    let pairs: Vec<_> = choices
        .iter()
        .map(|c| (c.value.as_str(), c.label.as_str()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("a0", "alpha"),
            ("a1", "Alpha 2"),
            ("b2", "beta"),
            ("7", "Unnamed")
        ]
    );
    assert_eq!(http::map_choices(source, r#"{"result":null}"#), Ok(vec![]));
    assert!(http::map_choices(source, r#"{"result":{}}"#).is_err());
    assert!(http::map_choices(source, r#"{"result":[{"name":"x"}]}"#).is_err());
    assert!(http::map_choices(source, "not json").is_err());
}

/// Each way a fetched-choices request could send the credential somewhere it
/// should not, or be shaped by something other than it, is refused by name.
#[test]
fn a_fetched_select_is_refused_by_name_when_it_would_misuse_the_credential() {
    let no_credential = with_accounts(PROVIDER, ACCOUNTS).replace("credential: api-key\n", "");
    let (_temp, root) = pack(CREDENTIAL, &no_credential);
    let error = load_pack_providers(&root, "example").unwrap_err();
    assert!(
        error.contains("this provider names no `credential:`"),
        "{error}"
    );

    for (from, to, expect) in [
        (
            "https://api.example.test/accounts",
            "https://api.evil.test/accounts",
            "would send credential `api-key` (API_KEY) to `api.evil.test`",
        ),
        (
            "https://api.example.test/accounts",
            "https://${credential|raw}/accounts",
            "interpolates into its host",
        ),
        (
            "https://api.example.test/accounts",
            "https://api.example.test/accounts?q=${options.count|query}",
            "may interpolate only `${credential}`, not `${options.count}`",
        ),
        (
            "Bearer ${credential}",
            "Bearer ${query}",
            "may interpolate only `${credential}`, not `${query}`",
        ),
        (
            "    required: true\n",
            "    required: true\n    default: a1\n",
            "a `select.from` has no `default`",
        ),
    ] {
        let error = load_error(
            CREDENTIAL,
            &with_accounts(PROVIDER, &ACCOUNTS.replace(from, to)),
        );
        assert!(error.contains(expect), "{to}: {error}");
    }
}

/// The shipped Cloudflare fetch provider lists its accounts with the token it
/// already holds, from the host that token may reach.
#[test]
fn the_cloudflare_account_is_chosen_from_the_connected_login() {
    let cloudflare = bundled("cloudflare", Slot::WebFetch);
    let account = cloudflare
        .provider
        .options
        .iter()
        .find(|option| option.key == "accountId")
        .unwrap();
    assert!(account.required);
    let prepared = http::prepare_choices(account.control.choice_source().unwrap(), "cf-tok");
    assert_eq!(
        prepared.url,
        "https://api.cloudflare.com/client/v4/accounts?per_page=50"
    );
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
        brave.provider.http().unwrap(),
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
