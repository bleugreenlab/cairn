//! OpenRouter's Image API request and replies, pinned against its documented
//! shapes (openrouter.ai/docs, Image Generation and Images › Generate an
//! image) and the live discovery records captured on 2026-10-17.

use std::time::Duration;

use serde_json::{json, Map, Value};

use super::discovery::*;
use super::wire::*;
use super::*;
use crate::http::loopback::serve_once;
use crate::sniff::fixtures::{PNG_2X3, SVG_4X4};

/// A slice of the live `images/models` listing.
pub const IMAGE_MODELS: &str = include_str!("../../testdata/openrouter/image_models.json");
/// The endpoint records of the same models, keyed by model id.
pub const ENDPOINTS: &str = include_str!("../../testdata/openrouter/endpoints.json");

fn generation<'a>() -> Generation<'a> {
    Generation {
        model: "black-forest-labs/flux.2-pro",
        prompt: "a lighthouse at dusk",
        n: 1,
        aspect_ratio: None,
        size: None,
        input_references: Vec::new(),
    }
}

fn no_extras() -> Map<String, Value> {
    Map::new()
}

#[test]
fn a_prompt_only_body_sends_only_the_model_and_prompt() {
    assert_eq!(
        build_body(&generation(), &no_extras()).unwrap(),
        json!({"model": "black-forest-labs/flux.2-pro", "prompt": "a lighthouse at dusk"})
    );
}

#[test]
fn aspect_count_references_and_extras_take_their_documented_places() {
    let mut generation = generation();
    generation.n = 3;
    generation.aspect_ratio = Some("2:3".into());
    generation.input_references = vec!["data:image/png;base64,iVBORw==".into()];
    let extras = json!({"quality": "high", "provider": {"only": ["black-forest-labs"]}});
    assert_eq!(
        build_body(&generation, extras.as_object().unwrap()).unwrap(),
        json!({
            "model": "black-forest-labs/flux.2-pro",
            "prompt": "a lighthouse at dusk",
            "n": 3,
            "aspect_ratio": "2:3",
            "input_references": [
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw=="}}
            ],
            "quality": "high",
            "provider": {"only": ["black-forest-labs"]}
        })
    );
    let colliding = json!({"aspect_ratio": "16:9"});
    assert!(matches!(
        build_body(&generation, colliding.as_object().unwrap()),
        Err(ImageWireError::InvalidRequest(_))
    ));
}

#[test]
fn the_documented_replies_decode_with_and_without_their_media_type_and_cost() {
    let png = read(&format!(
        r#"{{"created":1748372400,"data":[{{"b64_json":"{PNG_2X3}","media_type":"image/png"}}],"usage":{{"prompt_tokens":0,"completion_tokens":4175,"total_tokens":4175,"cost":0.04}}}}"#
    ))
    .unwrap();
    assert_eq!(png.data[0].media_type.as_deref(), Some("image/png"));
    assert_eq!(
        png.usage,
        Some(ImagesUsage {
            prompt_tokens: Some(0),
            completion_tokens: Some(4175),
            total_tokens: Some(4175),
            cost: Some(0.04),
        })
    );
    let svg = read(&format!(
        r#"{{"created":1748372400,"data":[{{"b64_json":"{SVG_4X4}","media_type":"image/svg+xml"}}]}}"#
    ))
    .unwrap();
    assert_eq!(svg.data[0].media_type.as_deref(), Some("image/svg+xml"));
    assert_eq!(svg.usage, None);
    // The API reference's own example names no media type and no cost-free
    // usage fields are required.
    let bare = read(&format!(
        r#"{{"created":1,"data":[{{"b64_json":"{PNG_2X3}"}}],"usage":{{"completion_tokens":10}}}}"#
    ))
    .unwrap();
    assert_eq!(bare.data[0].media_type, None);
    assert_eq!(bare.usage.unwrap().cost, None);
}

fn endpoint(base_url: String) -> ImagesEndpoint {
    ImagesEndpoint {
        provider_name: "OpenRouter".into(),
        backend_key: "openrouter".into(),
        base_url,
        headers: vec![
            ("authorization".into(), "Bearer sk-or-test".into()),
            ("content-type".into(), "application/json".into()),
            ("x-openrouter-title".into(), "Cairn".into()),
        ],
    }
}

#[test]
fn the_post_goes_to_the_images_path_with_the_bearer_key() {
    let (base, request) = serve_once(
        "200 OK",
        format!(r#"{{"created":1,"data":[{{"b64_json":"{PNG_2X3}"}}]}}"#),
    );
    let body = build_body(&generation(), &no_extras()).unwrap();
    let reply = post_images(&endpoint(base), &body, Duration::from_secs(5)).unwrap();
    assert_eq!(reply.data.len(), 1);
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        request.head.starts_with("POST /v1/images "),
        "{}",
        request.head
    );
    assert_eq!(
        request.header("authorization").as_deref(),
        Some("Bearer sk-or-test")
    );
    assert_eq!(
        request.header("x-openrouter-title").as_deref(),
        Some("Cairn")
    );
    assert_eq!(
        request.header("content-type").as_deref(),
        Some("application/json")
    );
    assert_eq!(request.json(), body);
}

#[test]
fn documented_error_bodies_keep_their_status_code_and_sentence() {
    for (status, body, code, message) in [
        (
            "400 Bad Request",
            r#"{"error":{"code":400,"message":"Invalid request parameters"}}"#,
            "400",
            "Invalid request parameters",
        ),
        (
            "402 Payment Required",
            r#"{"error":{"code":402,"message":"Insufficient credits. Add more using https://openrouter.ai/credits"}}"#,
            "402",
            "Insufficient credits. Add more using https://openrouter.ai/credits",
        ),
        (
            "502 Bad Gateway",
            r#"{"error":{"code":502,"message":"Provider returned error"}}"#,
            "502",
            "Provider returned error",
        ),
    ] {
        let (base, _) = serve_once(status, body.to_string());
        let error = post_images(&endpoint(base), &json!({}), Duration::from_secs(5)).unwrap_err();
        assert_eq!(
            error,
            ImageWireError::Http {
                status: status[..3].parse().unwrap(),
                code: Some(code.into()),
                message: message.into(),
            }
        );
    }
}

#[test]
fn the_listing_decodes_every_descriptor_type() {
    let models = read_image_models(IMAGE_MODELS).unwrap();
    let gpt = models
        .data
        .iter()
        .find(|model| model.id == "openai/gpt-image-2")
        .unwrap();
    assert_eq!(
        gpt.architecture.as_ref().unwrap().output_modalities,
        vec!["image"]
    );
    assert_eq!(gpt.supported_parameters["n"].max(), Some(10));
    assert!(gpt.supported_parameters["aspect_ratio"]
        .values()
        .contains(&"auto".to_string()));
    let tencent = models
        .data
        .iter()
        .find(|model| model.id == "tencent/hy-image-v3.5-preview")
        .unwrap();
    assert_eq!(tencent.supported_parameters["size"], Descriptor::Boolean);
    let muse = models
        .data
        .iter()
        .find(|model| model.id == "meta/muse-image")
        .unwrap();
    assert!(muse.supported_parameters.is_empty());
    // A descriptor type this crate does not know does not fail the listing.
    let novel: ImageModels = read_image_models(
        r#"{"data":[{"id":"x/y","supported_parameters":{"style":{"type":"palette","colors":3}}}]}"#,
    )
    .unwrap();
    assert_eq!(
        novel.data[0].supported_parameters["style"],
        Descriptor::Other
    );
}

#[test]
fn endpoint_records_decode_pricing_lines_with_their_variants() {
    let all: serde_json::Map<String, Value> = serde_json::from_str(ENDPOINTS).unwrap();
    let records = |id: &str| read_model_endpoints(&all[id].to_string()).unwrap();
    let flux = records("black-forest-labs/flux.2-pro");
    assert_eq!(flux.endpoints[0].provider_slug, "black-forest-labs");
    assert_eq!(
        flux.endpoints[0].pricing,
        vec![PricingLine {
            billable: "output_image".into(),
            unit: "megapixel".into(),
            cost_usd: 0.03,
            variant: None,
        }]
    );
    let gpt = records("openai/gpt-image-2");
    assert_eq!(gpt.endpoints[0].pricing[0].usd_text(), "0.000008");
    assert_eq!(
        gpt.endpoints[0].allowed_passthrough_parameters,
        vec!["moderation"]
    );
    let qwen = records("qwen/qwen-image-2.1-turbo");
    assert!(qwen.endpoints[0]
        .pricing
        .iter()
        .any(|line| line.variant.as_deref() == Some("2k")));
    assert_eq!(records("google/gemini-3.1-flash-image").endpoints.len(), 3);
}

#[test]
fn discovery_reads_the_listing_and_an_endpoints_record_over_http() {
    let (base, request) = serve_once("200 OK", IMAGE_MODELS.to_string());
    let models = list_image_models(&base, Duration::from_secs(5)).unwrap();
    assert_eq!(models.data.len(), 11);
    assert!(request
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .head
        .starts_with("GET /v1/images/models "));

    let all: serde_json::Map<String, Value> = serde_json::from_str(ENDPOINTS).unwrap();
    let (base, request) = serve_once("200 OK", all["openai/gpt-image-2"].to_string());
    let record = model_endpoints(&base, "openai/gpt-image-2", Duration::from_secs(5)).unwrap();
    assert_eq!(record.id, "openai/gpt-image-2");
    assert!(request
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .head
        .starts_with("GET /v1/images/models/openai/gpt-image-2/endpoints "));
}
