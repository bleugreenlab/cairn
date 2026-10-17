//! The OpenAI Images request and replies, pinned against the API reference
//! (developers.openai.com, Images › Create image and Create image edit).

use std::time::Duration;

use serde_json::{json, Map};

use super::wire::*;
use super::*;
use crate::http::loopback::serve_once;
use crate::sniff::fixtures::{bytes, JPEG_4X3, MASK_2X3, PNG_2X3};

fn generation<'a>() -> Generation<'a> {
    Generation {
        model: "gpt-image-2",
        prompt: "a lighthouse at dusk",
        n: 1,
        size: None,
    }
}

fn endpoint(base_url: String) -> ImagesEndpoint {
    ImagesEndpoint {
        provider_name: "Sparks".into(),
        backend_key: "custom:acc_sparks".into(),
        base_url,
        headers: vec![
            ("content-type".into(), "application/json".into()),
            ("authorization".into(), "Bearer sk-test".into()),
        ],
    }
}

#[test]
fn the_generations_body_is_the_documented_request() {
    let mut generation = generation();
    generation.n = 2;
    generation.size = Some("1024x1536".into());
    let extras = json!({"quality": "high", "moderation": "low"});
    assert_eq!(
        generations_body(&generation, extras.as_object().unwrap()).unwrap(),
        json!({
            "model": "gpt-image-2",
            "prompt": "a lighthouse at dusk",
            "n": 2,
            "size": "1024x1536",
            "quality": "high",
            "moderation": "low"
        })
    );
    assert_eq!(
        generations_body(&self::generation(), &Map::new()).unwrap(),
        json!({"model": "gpt-image-2", "prompt": "a lighthouse at dusk"})
    );
    let colliding = json!({"size": "auto"});
    assert!(generations_body(&generation, colliding.as_object().unwrap()).is_err());
}

/// The OpenAI reference's generation reply for a GPT Image model.
fn documented_reply() -> String {
    format!(
        r#"{{
          "created": 1713833628,
          "data": [{{ "b64_json": "{PNG_2X3}" }}],
          "background": "opaque",
          "output_format": "png",
          "quality": "high",
          "size": "1024x1024",
          "usage": {{
            "total_tokens": 100,
            "input_tokens": 50,
            "output_tokens": 50,
            "input_tokens_details": {{ "text_tokens": 10, "image_tokens": 40 }}
          }}
        }}"#
    )
}

#[test]
fn the_documented_reply_decodes_with_its_usage_details() {
    let reply = read(&documented_reply()).unwrap();
    assert_eq!(reply.size.as_deref(), Some("1024x1024"));
    assert_eq!(reply.output_format.as_deref(), Some("png"));
    assert_eq!(reply.data[0].b64_json.as_deref(), Some(PNG_2X3));
    assert_eq!(
        reply.usage,
        Some(ImagesUsage {
            input_tokens: Some(50),
            output_tokens: Some(50),
            total_tokens: Some(100),
            input_tokens_details: Some(InputTokensDetails {
                text_tokens: Some(10),
                image_tokens: Some(40),
            }),
        })
    );
}

#[test]
fn a_dall_e_style_reply_carries_its_revised_prompt_and_may_be_url_only() {
    let reply = read(
        r#"{"created":1,"data":[{"url":"https://files.example/img.png","revised_prompt":"A tall white lighthouse at dusk"}]}"#,
    )
    .unwrap();
    assert_eq!(reply.data[0].b64_json, None);
    assert_eq!(
        reply.data[0].url.as_deref(),
        Some("https://files.example/img.png")
    );
    assert_eq!(
        reply.data[0].revised_prompt.as_deref(),
        Some("A tall white lighthouse at dusk")
    );
    // Token usage without its breakdown, as compatible servers send it.
    let usage = read(r#"{"data":[],"usage":{"input_tokens":12,"output_tokens":4160}}"#)
        .unwrap()
        .usage
        .unwrap();
    assert_eq!(usage.input_tokens_details, None);
    assert_eq!(usage.output_tokens, Some(4160));
}

#[test]
fn the_generation_post_goes_to_its_path_as_json() {
    let (base, request) = serve_once("200 OK", documented_reply());
    let body = generations_body(&generation(), &Map::new()).unwrap();
    post_generations(&endpoint(base), &body, Duration::from_secs(5)).unwrap();
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        request.head.starts_with("POST /v1/images/generations "),
        "{}",
        request.head
    );
    assert_eq!(
        request.header("authorization").as_deref(),
        Some("Bearer sk-test")
    );
    assert_eq!(request.json(), body);
}

/// The multipart parts of `body` under `boundary`: (headers, content).
fn parts(body: &[u8], boundary: &str) -> Vec<(String, Vec<u8>)> {
    let delimiter = format!("--{boundary}").into_bytes();
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = find(rest, &delimiter) {
        rest = &rest[start + delimiter.len()..];
        if rest.starts_with(b"--") {
            break;
        }
        let rest_part = &rest[2..];
        let head_end = find(rest_part, b"\r\n\r\n").unwrap();
        let head = String::from_utf8_lossy(&rest_part[..head_end]).to_string();
        let content_start = head_end + 4;
        let next = find(&rest_part[content_start..], &delimiter).unwrap();
        out.push((
            head,
            rest_part[content_start..content_start + next - 2].to_vec(),
        ));
        rest = &rest_part[content_start + next..];
    }
    out
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[test]
fn an_edit_sends_its_fields_images_in_order_and_mask_as_multipart() {
    let (base, request) = serve_once("200 OK", documented_reply());
    let mut generation = generation();
    generation.n = 2;
    generation.size = Some("1024x1024".into());
    let base_image = ImagePart {
        bytes: bytes(PNG_2X3),
        media_type: "image/png".into(),
    };
    let reference = ImagePart {
        bytes: bytes(JPEG_4X3),
        media_type: "image/jpeg".into(),
    };
    let mask = ImagePart {
        bytes: bytes(MASK_2X3),
        media_type: "image/png".into(),
    };
    let extras = json!({"quality": "low", "input_fidelity": "high"});
    let form = edits_form(
        &generation,
        vec![base_image.clone(), reference.clone()],
        Some(mask.clone()),
        extras.as_object().unwrap(),
    )
    .unwrap();
    post_edits(&endpoint(base), form, Duration::from_secs(5)).unwrap();
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        request.head.starts_with("POST /v1/images/edits "),
        "{}",
        request.head
    );
    let content_type = request.header("content-type").unwrap();
    let boundary = content_type
        .strip_prefix("multipart/form-data; boundary=")
        .unwrap_or_else(|| panic!("not multipart: {content_type}"));
    let parts = parts(&request.body, boundary);
    let named = |head: &str| -> String {
        head.split("name=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .to_string()
    };
    let text_fields: Vec<(String, String)> = parts
        .iter()
        .filter(|(head, _)| !head.contains("filename="))
        .map(|(head, body)| (named(head), String::from_utf8_lossy(body).to_string()))
        .collect();
    for field in [
        ("model", "gpt-image-2"),
        ("prompt", "a lighthouse at dusk"),
        ("n", "2"),
        ("size", "1024x1024"),
        ("quality", "low"),
        ("input_fidelity", "high"),
    ] {
        assert!(
            text_fields.contains(&(field.0.to_string(), field.1.to_string())),
            "{field:?} in {text_fields:?}"
        );
    }
    let files: Vec<&(String, Vec<u8>)> = parts
        .iter()
        .filter(|(head, _)| head.contains("filename="))
        .collect();
    assert_eq!(files.len(), 3);
    assert!(
        files[0]
            .0
            .contains("name=\"image[]\"; filename=\"image-1.png\""),
        "{}",
        files[0].0
    );
    assert!(files[0]
        .0
        .to_lowercase()
        .contains("content-type: image/png"));
    assert_eq!(files[0].1, base_image.bytes);
    assert!(
        files[1]
            .0
            .contains("name=\"image[]\"; filename=\"image-2.jpg\""),
        "{}",
        files[1].0
    );
    assert!(files[1]
        .0
        .to_lowercase()
        .contains("content-type: image/jpeg"));
    assert_eq!(files[1].1, reference.bytes);
    assert!(
        files[2].0.contains("name=\"mask\"; filename=\"mask.png\""),
        "{}",
        files[2].0
    );
    assert_eq!(files[2].1, mask.bytes);
    // The endpoint's JSON content type never reaches a multipart body.
    assert!(!request.head.to_lowercase().contains("application/json"));
}

#[test]
fn a_single_image_edit_names_its_part_image() {
    let form = edits_form(
        &generation(),
        vec![ImagePart {
            bytes: bytes(PNG_2X3),
            media_type: "image/png".into(),
        }],
        None,
        &Map::new(),
    )
    .unwrap();
    let (base, request) = serve_once("200 OK", documented_reply());
    post_edits(&endpoint(base), form, Duration::from_secs(5)).unwrap();
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    let body = String::from_utf8_lossy(&request.body).to_string();
    assert!(
        body.contains("name=\"image\"; filename=\"image-1.png\""),
        "{body}"
    );
    assert!(!body.contains("image[]"));
    assert!(edits_form(&generation(), Vec::new(), None, &Map::new()).is_err());
}

#[test]
fn a_moderation_refusal_keeps_its_code() {
    let (base, _) = serve_once(
        "400 Bad Request",
        r#"{"error":{"message":"Your request was rejected as a result of our safety system.","type":"image_generation_user_error","param":null,"code":"moderation_blocked"}}"#.to_string(),
    );
    let error = post_generations(&endpoint(base), &json!({}), Duration::from_secs(5)).unwrap_err();
    assert_eq!(
        error,
        ImageWireError::Http {
            status: 400,
            code: Some("moderation_blocked".into()),
            message: "Your request was rejected as a result of our safety system.".into(),
        }
    );
}

#[test]
fn an_unanswered_request_times_out_as_a_timeout() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let held = std::thread::spawn(move || {
        let accepted = listener.accept();
        std::thread::sleep(Duration::from_millis(800));
        drop(accepted);
    });
    let error =
        post_generations(&endpoint(base), &json!({}), Duration::from_millis(200)).unwrap_err();
    assert_eq!(error, ImageWireError::Timeout);
    let _ = held.join();
}
