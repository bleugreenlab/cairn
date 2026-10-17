//! The image verb over a scripted transport: admission, lowerings, the wire
//! calls each route builds, reply mapping, usage, errors, and offer
//! selection.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use cairn_models_types::catalog::{
    claude_models, custom_descriptor, descriptor, CustomProvider, DiscoveredModel,
    ProviderModelCatalog, ServedVerb, Verb, VerbKind, Wire,
};
use cairn_models_types::image::{
    EditSupport, ImageCapabilities, Lowering, Orientation, Pixels, Ratio, ReferenceSupport,
    ShapeSupport, Support,
};
use cairn_models_types::usage::{BillingUnit, Charge, Meter};
use serde_json::{json, Value};

use super::openrouter::{capabilities, discovered_model, pricing};
use super::*;
use crate::image_wire::openrouter::discovery::{read_image_models, read_model_endpoints};
use crate::image_wire::{openai as openai_wire, openrouter as openrouter_wire, ImageWireError};
use crate::interface::image::*;
use crate::interface::{Catalog, CatalogProvider, SelectedOffer};

const PNG_2X3: &str = "iVBORw0KGgoAAAANSUhEUgAAAAIAAAADCAIAAAA2iEnWAAAAFElEQVR4nGM8ISfHwMDAxMDAgKAAFEoBCqHkTQMAAAAASUVORK5CYII=";
const MASK_2X3: &str = "iVBORw0KGgoAAAANSUhEUgAAAAIAAAADCAYAAAC56t6BAAAAGUlEQVR4nGNggID/TAwMDP8ZGBgYmKAiDAAhMAID+rl5HQAAAABJRU5ErkJggg==";
const WEBP_1X1: &str = "UklGRhwAAABXRUJQVlA4TA8AAAAvAAAAAAcQ0f/+ByKi/wEA";
const IMAGE_MODELS: &str =
    include_str!("../../../cairn-image-wire/testdata/openrouter/image_models.json");
const ENDPOINTS: &str =
    include_str!("../../../cairn-image-wire/testdata/openrouter/endpoints.json");

fn png() -> ImageInput {
    ImageInput {
        bytes: crate::image_wire::decode_base64(PNG_2X3).unwrap(),
        media_type: "image/png".into(),
    }
}

fn mask() -> ImageInput {
    ImageInput {
        bytes: crate::image_wire::decode_base64(MASK_2X3).unwrap(),
        media_type: "image/png".into(),
    }
}

fn reference(label: Option<&str>) -> Reference {
    Reference {
        image: png(),
        label: label.map(str::to_string),
    }
}

/// An offer shaped like an OpenRouter image model: aspect ratios only, four
/// references, edits as reference 1, no mask.
fn openrouter_shaped() -> ImageCapabilities {
    ImageCapabilities {
        max_count: 4,
        shape: ShapeSupport {
            auto: true,
            pixels: None,
            aspect: vec![Ratio::new(1, 1), Ratio::new(2, 3), Ratio::new(3, 2)],
            orientation: Support::Lowered(Lowering::OrientationAsAspect {
                portrait: Ratio::new(2, 3),
                landscape: Ratio::new(3, 2),
                square: Ratio::new(1, 1),
            }),
        },
        references: Some(ReferenceSupport {
            max: 4,
            labels: Support::Lowered(Lowering::LabelsInPrompt),
        }),
        edit: Some(EditSupport {
            base: Support::Lowered(Lowering::EditAsReference),
            mask: Support::Unsupported,
        }),
    }
}

/// Codex's image generation as cairn/4905 measured it: the provider sizes
/// the image, orientation and labels ride in the prompt, an edit is sent as
/// a reference, and there is no mask.
fn codex() -> ImageCapabilities {
    ImageCapabilities {
        max_count: 4,
        shape: ShapeSupport {
            auto: true,
            pixels: None,
            aspect: Vec::new(),
            orientation: Support::Lowered(Lowering::OrientationInPrompt),
        },
        references: Some(ReferenceSupport {
            max: 5,
            labels: Support::Lowered(Lowering::LabelsInPrompt),
        }),
        edit: Some(EditSupport {
            base: Support::Lowered(Lowering::EditAsReference),
            mask: Support::Unsupported,
        }),
    }
}

fn refused_feature(result: Result<Vec<Lowering>, ImageError>) -> ImageFeature {
    match result {
        Err(ImageError::Unsupported { feature, .. }) => feature,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn each_feature_an_offer_cannot_honour_is_refused_by_name() {
    let mut request = ImageRequest::new("a lighthouse");
    request.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    assert_eq!(
        refused_feature(admit(
            &openrouter_shaped(),
            Wire::OpenRouterImages,
            &request
        )),
        ImageFeature::Mask
    );

    let mut request = ImageRequest::new("a lighthouse");
    request.shape = ImageShape::Pixels(Pixels::new(1024, 1536));
    let error = admit(&openrouter_shaped(), Wire::OpenRouterImages, &request).unwrap_err();
    assert_eq!(
        error,
        ImageError::Unsupported {
            feature: ImageFeature::Pixels,
            detail: "this offer takes no exact size, so 1024x1536 cannot be asked for (it serves ≤4 · refs ≤4 · aspect 1:1|2:3|3:2 · no mask)".into(),
        }
    );

    let mut request = ImageRequest::new("a lighthouse");
    request.references = (0..5).map(|_| reference(None)).collect();
    assert_eq!(
        refused_feature(admit(
            &openrouter_shaped(),
            Wire::OpenRouterImages,
            &request
        )),
        ImageFeature::References
    );
    // The edit base rides as a reference, so it counts toward the limit.
    let mut request = ImageRequest::new("a lighthouse");
    request.references = (0..4).map(|_| reference(None)).collect();
    request.edit = Some(Edit {
        base: png(),
        mask: None,
    });
    let error = admit(&openrouter_shaped(), Wire::OpenRouterImages, &request).unwrap_err();
    assert!(
        error.to_string().contains("the edit base counts as one"),
        "{error}"
    );

    let mut request = ImageRequest::new("a lighthouse");
    request.count = 5;
    assert_eq!(
        refused_feature(admit(
            &openrouter_shaped(),
            Wire::OpenRouterImages,
            &request
        )),
        ImageFeature::Count
    );

    let mut no_orientation = openrouter_shaped();
    no_orientation.shape.orientation = Support::Unsupported;
    let mut request = ImageRequest::new("a lighthouse");
    request.shape = ImageShape::Orientation(Orientation::Portrait);
    assert_eq!(
        refused_feature(admit(&no_orientation, Wire::OpenRouterImages, &request)),
        ImageFeature::Orientation
    );

    let mut request = ImageRequest::new("a lighthouse");
    request.shape = ImageShape::Aspect(Ratio::new(21, 9));
    assert_eq!(
        refused_feature(admit(
            &openrouter_shaped(),
            Wire::OpenRouterImages,
            &request
        )),
        ImageFeature::Aspect
    );

    let mut minimal = ImageRequest::new("a lighthouse");
    minimal.references = vec![reference(None)];
    assert_eq!(
        refused_feature(admit(
            &ImageCapabilities::openai_images_minimal(),
            Wire::OpenAiImages,
            &minimal
        )),
        ImageFeature::References
    );
    minimal.references.clear();
    minimal.edit = Some(Edit {
        base: png(),
        mask: None,
    });
    assert_eq!(
        refused_feature(admit(
            &ImageCapabilities::openai_images_minimal(),
            Wire::OpenAiImages,
            &minimal
        )),
        ImageFeature::Edit
    );

    assert!(matches!(
        admit(
            &openrouter_shaped(),
            Wire::OpenRouterImages,
            &ImageRequest::new("  ")
        ),
        Err(ImageError::InvalidRequest(_))
    ));
}

#[test]
fn a_mask_must_be_a_png_the_size_of_what_it_edits() {
    let capabilities = ImageCapabilities::openai_gpt_image();
    let mut request = ImageRequest::new("add a boat");
    request.edit = Some(Edit {
        base: png(),
        mask: Some(ImageInput {
            bytes: crate::image_wire::decode_base64(WEBP_1X1).unwrap(),
            media_type: "image/webp".into(),
        }),
    });
    assert!(matches!(
        admit(&capabilities, Wire::OpenAiImages, &request),
        Err(ImageError::InvalidRequest(text)) if text.contains("PNG")
    ));
    let mut wrong_size = png();
    wrong_size.bytes = crate::image_wire::decode_base64(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGBgAAAABQABpfZFQAAAAABJRU5ErkJggg==",
    )
    .unwrap();
    request.edit = Some(Edit {
        base: png(),
        mask: Some(wrong_size),
    });
    assert!(matches!(
        admit(&capabilities, Wire::OpenAiImages, &request),
        Err(ImageError::InvalidRequest(text)) if text.contains("must match")
    ));
    request.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    assert_eq!(
        admit(&capabilities, Wire::OpenAiImages, &request).unwrap(),
        Vec::new()
    );
}

#[test]
fn each_lowering_is_recorded_and_composed_only_when_applied() {
    let prompt = "A lighthouse at dusk, \n oil on canvas  ";
    let plain = ImageRequest::new(prompt);
    assert_eq!(
        admit(&openrouter_shaped(), Wire::OpenRouterImages, &plain).unwrap(),
        Vec::new()
    );
    assert_eq!(compose_prompt(prompt, &plain, &[]), prompt);

    let mut labelled = ImageRequest::new("Put the cat in the chair");
    labelled.references = vec![
        reference(Some("the cat")),
        reference(None),
        reference(Some("the chair")),
    ];
    let lowerings = admit(&openrouter_shaped(), Wire::OpenRouterImages, &labelled).unwrap();
    assert_eq!(lowerings, vec![Lowering::LabelsInPrompt]);
    assert_eq!(
        compose_prompt(&labelled.prompt, &labelled, &lowerings),
        "Put the cat in the chair\n\nImage 1: the cat\nImage 3: the chair"
    );

    let mut portrait = ImageRequest::new("card art");
    portrait.shape = ImageShape::Orientation(Orientation::Portrait);
    let lowerings = admit(&openrouter_shaped(), Wire::OpenRouterImages, &portrait).unwrap();
    assert!(matches!(
        lowerings[..],
        [Lowering::OrientationAsAspect { .. }]
    ));
    // An aspect lowering changes the body, not the prompt.
    assert_eq!(
        compose_prompt("card art", &portrait, &lowerings),
        "card art"
    );

    let lowerings = admit(
        &ImageCapabilities::openai_gpt_image(),
        Wire::OpenAiImages,
        &portrait,
    )
    .unwrap();
    assert!(matches!(
        lowerings[..],
        [Lowering::OrientationAsPixels { .. }]
    ));

    let lowerings = admit(&codex(), Wire::CodexAppServer, &portrait).unwrap();
    assert_eq!(lowerings, vec![Lowering::OrientationInPrompt]);
    assert_eq!(
        compose_prompt("card art", &portrait, &lowerings),
        format!(
            "card art\n\n{}",
            orientation_sentence(Orientation::Portrait)
        )
    );
    assert_eq!(
        orientation_sentence(Orientation::Portrait),
        "Compose the image in portrait orientation."
    );

    let mut edit = ImageRequest::new("Make the sky stormy");
    edit.edit = Some(Edit {
        base: png(),
        mask: None,
    });
    edit.references = vec![reference(Some("the storm to copy"))];
    let lowerings = admit(&openrouter_shaped(), Wire::OpenRouterImages, &edit).unwrap();
    assert_eq!(
        lowerings,
        vec![Lowering::EditAsReference, Lowering::LabelsInPrompt]
    );
    assert_eq!(
        compose_prompt(&edit.prompt, &edit, &lowerings),
        format!("Make the sky stormy\n\n{EDIT_SENTENCE}\nImage 2: the storm to copy")
    );
}

#[test]
fn codex_as_measured_admits_portrait_card_art_with_labels_and_refuses_masks_and_sizes() {
    let mut card = ImageRequest::new("Tarot card: The Tower");
    card.shape = ImageShape::Orientation(Orientation::Portrait);
    card.references = vec![
        reference(Some("the art style")),
        reference(Some("the tower")),
    ];
    let lowerings = admit(&codex(), Wire::CodexAppServer, &card).unwrap();
    assert_eq!(
        lowerings,
        vec![Lowering::LabelsInPrompt, Lowering::OrientationInPrompt]
    );
    assert_eq!(
        compose_prompt(&card.prompt, &card, &lowerings),
        "Tarot card: The Tower\n\nImage 1: the art style\nImage 2: the tower\nCompose the image in portrait orientation."
    );

    let mut masked = ImageRequest::new("fix the hand");
    masked.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    assert_eq!(
        refused_feature(admit(&codex(), Wire::CodexAppServer, &masked)),
        ImageFeature::Mask
    );
    let mut sized = ImageRequest::new("card art");
    sized.shape = ImageShape::Pixels(Pixels::new(1024, 1536));
    assert_eq!(
        refused_feature(admit(&codex(), Wire::CodexAppServer, &sized)),
        ImageFeature::Pixels
    );
}

/// Replies with one canned reply and keeps every body it was sent.
struct Scripted {
    reply: Result<ImageReply, ImageWireError>,
    sent: Mutex<Vec<(String, ImageBody)>>,
}

impl Scripted {
    fn replying(reply: Result<ImageReply, ImageWireError>) -> Self {
        Self {
            reply,
            sent: Mutex::new(Vec::new()),
        }
    }

    fn body(&self) -> ImageBody {
        self.sent.lock().unwrap()[0].1.clone()
    }
}

impl ImageTransport for Scripted {
    fn post(&self, post: ImagePost<'_>) -> Result<ImageReply, ImageWireError> {
        self.sent
            .lock()
            .unwrap()
            .push((post.endpoint.base_url.clone(), post.body));
        self.reply.clone()
    }
}

fn openrouter_reply(media_type: Option<&str>, cost: Option<f64>) -> ImageReply {
    ImageReply::OpenRouter(openrouter_wire::wire::ImagesResponse {
        created: Some(1),
        data: vec![openrouter_wire::wire::ImageData {
            b64_json: PNG_2X3.into(),
            media_type: media_type.map(str::to_string),
        }],
        usage: Some(openrouter_wire::wire::ImagesUsage {
            prompt_tokens: Some(12),
            completion_tokens: Some(4175),
            total_tokens: Some(4187),
            cost,
        }),
    })
}

fn openai_reply(data: openai_wire::wire::ImageData, details: bool) -> ImageReply {
    ImageReply::OpenAi(openai_wire::wire::ImagesResponse {
        created: Some(1),
        data: vec![data],
        size: Some("1024x1536".into()),
        output_format: Some("png".into()),
        usage: Some(openai_wire::wire::ImagesUsage {
            input_tokens: Some(50),
            output_tokens: Some(4160),
            total_tokens: Some(4210),
            input_tokens_details: details.then_some(openai_wire::wire::InputTokensDetails {
                text_tokens: Some(10),
                image_tokens: Some(40),
            }),
        }),
    })
}

fn b64(b64_json: &str) -> openai_wire::wire::ImageData {
    openai_wire::wire::ImageData {
        b64_json: Some(b64_json.into()),
        url: None,
        revised_prompt: None,
    }
}

fn openrouter_route() -> ImageRoute {
    openrouter::endpoint("sk-or-test", &crate::openrouter::Attribution::cairn())
}

fn openai_route() -> ImageRoute {
    openai::route_for(
        "Sparks",
        "custom:acc_sparks",
        "http://spark:8080/v1/",
        Some("sk-test"),
        &[],
    )
}

fn call<'a>(
    route: &'a ImageRoute,
    capabilities: &'a ImageCapabilities,
    request: &'a ImageRequest,
) -> ImageCall<'a> {
    ImageCall {
        route,
        model: "some/model",
        capabilities,
        request,
        timeout: Duration::from_secs(5),
    }
}

fn measure(usage: &cairn_models_types::usage::MeteredUsage, meter: Meter) -> Option<u64> {
    usage
        .measures
        .iter()
        .find(|measure| measure.meter == meter)
        .map(|measure| measure.quantity)
}

fn meter(charge: Charge, modality: Option<Modality>, unit: BillingUnit) -> Meter {
    Meter {
        charge,
        modality,
        unit,
    }
}

use cairn_models_types::catalog::Modality;

#[test]
fn an_openrouter_call_sends_lowered_shape_and_references_and_reads_its_usage() {
    let transport = Scripted::replying(Ok(openrouter_reply(None, Some(0.04))));
    let route = openrouter_route();
    let capabilities = openrouter_shaped();
    let mut request = ImageRequest::new("card art");
    request.count = 2;
    request.shape = ImageShape::Orientation(Orientation::Portrait);
    request.edit = Some(Edit {
        base: png(),
        mask: None,
    });
    request.references = vec![reference(Some("style"))];
    request.extras = vec![
        WireExtras {
            wire: Wire::OpenRouterImages,
            fields: json!({"quality": "high"}).as_object().unwrap().clone(),
        },
        WireExtras {
            wire: Wire::OpenAiImages,
            fields: json!({"response_format": "b64_json"})
                .as_object()
                .unwrap()
                .clone(),
        },
    ];
    let result = image(&transport, call(&route, &capabilities, &request)).unwrap();

    let ImageBody::OpenRouter(body) = transport.body() else {
        panic!("an OpenRouter body");
    };
    let data_url = format!("data:image/png;base64,{PNG_2X3}");
    assert_eq!(
        body,
        json!({
            "model": "some/model",
            "prompt": result.provenance.prompt_sent,
            "n": 2,
            "aspect_ratio": "2:3",
            "input_references": [
                {"type": "image_url", "image_url": {"url": data_url}},
                {"type": "image_url", "image_url": {"url": data_url}}
            ],
            "quality": "high"
        })
    );
    assert_eq!(
        result.provenance.prompt_sent,
        format!("card art\n\n{EDIT_SENTENCE}\nImage 2: style")
    );
    assert_eq!(result.provenance.provider, "openrouter");
    assert_eq!(result.provenance.wire, Wire::OpenRouterImages);
    assert_eq!(
        result.provenance.requested,
        ImageShape::Orientation(Orientation::Portrait)
    );
    assert_eq!(
        result.provenance.lowerings[0..2],
        [Lowering::EditAsReference, Lowering::LabelsInPrompt]
    );
    assert!(matches!(
        result.provenance.lowerings[2],
        Lowering::OrientationAsAspect { .. }
    ));

    let first = &result.images[0];
    // No media type in the reply: sniffed from the bytes.
    assert_eq!(first.media_type, "image/png");
    assert_eq!(first.size, Some(Pixels::new(2, 3)));
    assert_eq!(
        measure(
            &result.usage,
            meter(Charge::Output, Some(Modality::Image), BillingUnit::Images)
        ),
        Some(1)
    );
    assert_eq!(
        measure(
            &result.usage,
            meter(Charge::Input, None, BillingUnit::Tokens)
        ),
        Some(12)
    );
    assert_eq!(
        measure(
            &result.usage,
            meter(Charge::Output, Some(Modality::Image), BillingUnit::Tokens)
        ),
        Some(4175)
    );
    assert_eq!(result.usage.cost_usd, Some(0.04));

    // A reply that names its media type is taken at its word.
    let transport = Scripted::replying(Ok(openrouter_reply(Some("image/svg+xml"), None)));
    let plain = ImageRequest::new("vector logo");
    let result = image(&transport, call(&route, &capabilities, &plain)).unwrap();
    assert_eq!(result.images[0].media_type, "image/svg+xml");
    assert_eq!(result.usage.cost_usd, None);
}

#[test]
fn an_openai_call_generates_as_json_and_edits_as_multipart() {
    let route = openai_route();
    let capabilities = ImageCapabilities::openai_gpt_image();
    let mut portrait = ImageRequest::new("card art");
    portrait.shape = ImageShape::Orientation(Orientation::Portrait);
    let transport = Scripted::replying(Ok(openai_reply(b64(PNG_2X3), true)));
    let result = image(&transport, call(&route, &capabilities, &portrait)).unwrap();
    let ImageBody::OpenAiGenerations(body) = transport.body() else {
        panic!("a generations body");
    };
    assert_eq!(
        body,
        json!({"model": "some/model", "prompt": "card art", "size": "1024x1536"})
    );
    assert_eq!(transport.sent.lock().unwrap()[0].0, "http://spark:8080/v1");
    assert_eq!(
        measure(
            &result.usage,
            meter(Charge::Input, Some(Modality::Text), BillingUnit::Tokens)
        ),
        Some(10)
    );
    assert_eq!(
        measure(
            &result.usage,
            meter(Charge::Input, Some(Modality::Image), BillingUnit::Tokens)
        ),
        Some(40)
    );
    assert_eq!(
        measure(
            &result.usage,
            meter(Charge::Output, Some(Modality::Image), BillingUnit::Tokens)
        ),
        Some(4160)
    );
    assert_eq!(result.usage.cost_usd, None);
    assert_eq!(result.images[0].size, Some(Pixels::new(2, 3)));

    let mut edit = ImageRequest::new("add a boat");
    edit.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    edit.references = vec![reference(Some("the boat"))];
    let transport = Scripted::replying(Ok(openai_reply(b64(PNG_2X3), false)));
    let result = image(&transport, call(&route, &capabilities, &edit)).unwrap();
    let ImageBody::OpenAiEdits(form) = transport.body() else {
        panic!("an edits form");
    };
    assert_eq!(form.images.len(), 2);
    assert_eq!(form.images[0].bytes, png().bytes);
    assert_eq!(form.mask.as_ref().unwrap().bytes, mask().bytes);
    assert!(form
        .fields
        .contains(&("prompt".into(), "add a boat\n\nImage 2: the boat".into())));
    assert_eq!(result.provenance.lowerings, vec![Lowering::LabelsInPrompt]);
    // Without the breakdown, input is counted whole.
    assert_eq!(
        measure(
            &result.usage,
            meter(Charge::Input, None, BillingUnit::Tokens)
        ),
        Some(50)
    );
}

#[test]
fn a_url_only_reply_names_the_fix_and_revised_prompts_pass_through() {
    let route = openai_route();
    let capabilities = ImageCapabilities::openai_images_minimal();
    let request = ImageRequest::new("a lighthouse");
    let transport = Scripted::replying(Ok(openai_reply(
        openai_wire::wire::ImageData {
            b64_json: None,
            url: Some("https://files.example/1.png".into()),
            revised_prompt: None,
        },
        false,
    )));
    let error = image(&transport, call(&route, &capabilities, &request)).unwrap_err();
    assert!(
        matches!(&error, ImageError::InvalidResponse(text) if text.contains("response_format: b64_json")),
        "{error:?}"
    );

    let transport = Scripted::replying(Ok(openai_reply(
        openai_wire::wire::ImageData {
            b64_json: Some(PNG_2X3.into()),
            url: None,
            revised_prompt: Some("A tall lighthouse".into()),
        },
        false,
    )));
    let result = image(&transport, call(&route, &capabilities, &request)).unwrap();
    assert_eq!(
        result.images[0].revised_prompt.as_deref(),
        Some("A tall lighthouse")
    );
}

#[test]
fn extras_cannot_set_a_core_key_and_another_wires_extras_are_ignored() {
    let route = openai_route();
    let capabilities = ImageCapabilities::openai_images_minimal();
    let mut request = ImageRequest::new("a lighthouse");
    request.extras = vec![WireExtras {
        wire: Wire::OpenAiImages,
        fields: json!({"n": 4}).as_object().unwrap().clone(),
    }];
    let transport = Scripted::replying(Ok(openai_reply(b64(PNG_2X3), false)));
    assert!(matches!(
        image(&transport, call(&route, &capabilities, &request)),
        Err(ImageError::InvalidRequest(text)) if text.contains("'n'")
    ));
    assert!(transport.sent.lock().unwrap().is_empty());

    request.extras = vec![WireExtras {
        wire: Wire::OpenRouterImages,
        fields: json!({"n": 4, "aspect_ratio": "1:1"})
            .as_object()
            .unwrap()
            .clone(),
    }];
    image(&transport, call(&route, &capabilities, &request)).unwrap();
    let ImageBody::OpenAiGenerations(body) = transport.body() else {
        panic!("a generations body");
    };
    assert_eq!(
        body,
        json!({"model": "some/model", "prompt": "a lighthouse"})
    );
}

#[test]
fn wire_failures_map_onto_what_a_caller_can_act_on() {
    let route = openrouter_route();
    let capabilities = openrouter_shaped();
    let request = ImageRequest::new("a lighthouse");
    let failing = |error: ImageWireError| {
        image(
            &Scripted::replying(Err(error)),
            call(&route, &capabilities, &request),
        )
        .unwrap_err()
    };
    let http = |status: u16, code: Option<&str>, message: &str| ImageWireError::Http {
        status,
        code: code.map(str::to_string),
        message: message.into(),
    };
    assert_eq!(
        failing(http(
            400,
            Some("moderation_blocked"),
            "rejected by the safety system"
        )),
        ImageError::Refused {
            message: "OpenRouter: rejected by the safety system".into()
        }
    );
    assert!(matches!(
        failing(http(400, Some("content_policy_violation"), "no")),
        ImageError::Refused { .. }
    ));
    assert_eq!(
        failing(http(402, Some("402"), "Insufficient credits")),
        ImageError::Quota {
            message: "OpenRouter (HTTP 402): Insufficient credits".into()
        }
    );
    assert!(matches!(
        failing(http(429, None, "Rate limit exceeded")),
        ImageError::Quota { .. }
    ));
    assert_eq!(
        failing(http(400, Some("400"), "Invalid request parameters")),
        ImageError::InvalidRequest(
            "OpenRouter refused it (HTTP 400): Invalid request parameters".into()
        )
    );
    assert_eq!(
        failing(http(502, Some("502"), "Provider returned error")),
        ImageError::Upstream("OpenRouter returned HTTP 502: Provider returned error".into())
    );
    assert_eq!(failing(ImageWireError::Timeout), ImageError::Timeout);
}

fn image_model(id: &str, capabilities: ImageCapabilities, wire: Wire) -> DiscoveredModel {
    let mut model = claude_models().remove(0);
    model.id = id.into();
    model.model = id.into();
    model.display_name = id.into();
    model.context_window = None;
    model.serves = Some(vec![ServedVerb {
        verb: Verb::Image(capabilities),
        wire,
    }]);
    model
}

fn provider(key: &str, models: Vec<DiscoveredModel>) -> CatalogProvider {
    let descriptor = match cairn_models_types::catalog::custom_account_id(key) {
        Some(account) => custom_descriptor(&CustomProvider {
            account_id: account.into(),
            display_name: account.into(),
        }),
        None => descriptor(key).expect("shipped").clone(),
    };
    CatalogProvider {
        descriptor,
        installed: true,
        models: Some(ProviderModelCatalog {
            backend: key.into(),
            models,
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        }),
        tiers: Vec::new(),
        serves: Vec::new(),
        unable: Vec::new(),
    }
}

fn image_catalog() -> Catalog {
    Catalog {
        providers: vec![
            provider(
                "openrouter",
                vec![
                    image_model("bfl/flux", openrouter_shaped(), Wire::OpenRouterImages),
                    image_model(
                        "openai/gpt-image-2",
                        openrouter_shaped(),
                        Wire::OpenRouterImages,
                    ),
                ],
            ),
            provider(
                "custom:acc_openai",
                vec![image_model(
                    "gpt-image-2",
                    ImageCapabilities::openai_gpt_image(),
                    Wire::OpenAiImages,
                )],
            ),
        ],
    }
}

#[test]
fn an_unset_model_takes_the_first_offer_that_admits_the_request() {
    let catalog = image_catalog();
    let (offer, capabilities) = catalog
        .image_offer(None, &ImageRequest::new("a lighthouse"))
        .unwrap();
    assert_eq!(
        (offer.provider.as_str(), offer.model.as_str()),
        ("openrouter", "bfl/flux")
    );
    assert_eq!(capabilities, openrouter_shaped());

    let mut masked = ImageRequest::new("add a boat");
    masked.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    let (offer, capabilities) = catalog.image_offer(Some("  "), &masked).unwrap();
    assert_eq!(offer.provider, "custom:acc_openai");
    assert_eq!(capabilities, ImageCapabilities::openai_gpt_image());

    let mut too_many = ImageRequest::new("many");
    too_many.count = 50;
    let error = catalog.image_offer(None, &too_many).unwrap_err();
    let text = error.to_string();
    assert!(
        text.starts_with(
            "No image-capable model is available: bfl/flux on openrouter: the image request's image count cannot be honoured: it asks for 50 images"
        ),
        "{text}"
    );
    assert!(
        text.contains("; gpt-image-2 on custom:acc_openai: "),
        "{text}"
    );

    // A request no offer could fix is reported as itself.
    assert!(matches!(
        catalog.image_offer(None, &ImageRequest::new(" ")),
        Err(ImageError::InvalidRequest(_))
    ));
}

#[test]
fn a_pinned_model_that_refuses_names_offers_that_would_admit() {
    let catalog = image_catalog();
    let mut masked = ImageRequest::new("add a boat");
    masked.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    let error = catalog.image_offer(Some("bfl/flux"), &masked).unwrap_err();
    let ImageError::NoOffer(refusal) = error else {
        panic!("expected a refusal");
    };
    assert_eq!(refusal.requested.as_deref(), Some("bfl/flux"));
    let detail = refusal.detail.unwrap();
    assert!(
        detail.starts_with(
            "bfl/flux on openrouter: the image request's mask cannot be honoured: this offer takes no mask"
        ),
        "{detail}"
    );
    assert!(
        detail.ends_with("; offers that would: custom:acc_openai/gpt-image-2"),
        "{detail}"
    );

    let unknown = catalog
        .image_offer(Some("not/a-model"), &masked)
        .unwrap_err();
    assert!(matches!(unknown, ImageError::NoOffer(_)));
}

#[test]
fn an_empty_catalog_refuses_naming_the_image_fixes() {
    let catalog = Catalog {
        providers: vec![provider("claude", claude_models())],
    };
    let error = catalog
        .image_offer(None, &ImageRequest::new("a lighthouse"))
        .unwrap_err();
    let text = error.to_string();
    assert!(matches!(error, ImageError::NoOffer(_)));
    assert!(
        text.starts_with("No image-capable model is configured."),
        "{text}"
    );
    assert!(text.contains(IMAGE_FIXES), "{text}");
    let pinned = catalog
        .image_offer(Some("opus"), &ImageRequest::new("a lighthouse"))
        .unwrap_err()
        .to_string();
    assert!(
        pinned.contains("'opus' is offered only by claude, which cannot generate images"),
        "{pinned}"
    );
    // Image offers never stand in for another verb.
    assert!(image_catalog()
        .offers_serving(VerbKind::Complete)
        .is_empty());
    assert_eq!(image_catalog().offers_serving(VerbKind::Image).len(), 3);
    let _: Option<SelectedOffer> = None;
}

fn endpoints(id: &str) -> crate::image_wire::openrouter::discovery::ModelEndpoints {
    let all: serde_json::Map<String, Value> = serde_json::from_str(ENDPOINTS).unwrap();
    read_model_endpoints(&all[id].to_string()).unwrap()
}

fn listed(id: &str) -> crate::image_wire::openrouter::discovery::ImageModel {
    read_image_models(IMAGE_MODELS)
        .unwrap()
        .data
        .into_iter()
        .find(|model| model.id == id)
        .unwrap()
}

#[test]
fn discovery_declares_what_every_endpoint_takes() {
    let gpt = capabilities(&endpoints("openai/gpt-image-2").endpoints).unwrap();
    assert_eq!(gpt.max_count, 10);
    assert_eq!(gpt.references.as_ref().unwrap().max, 16);
    assert!(gpt.shape.auto);
    assert!(gpt.shape.aspect.contains(&Ratio::new(21, 9)));
    assert_eq!(gpt.shape.pixels, None);
    assert_eq!(
        gpt.shape.orientation,
        Support::Lowered(Lowering::OrientationAsAspect {
            portrait: Ratio::new(2, 3),
            landscape: Ratio::new(3, 2),
            square: Ratio::new(1, 1),
        })
    );
    assert_eq!(
        gpt.edit,
        Some(EditSupport {
            base: Support::Lowered(Lowering::EditAsReference),
            mask: Support::Unsupported,
        })
    );

    // Two endpoints that disagree: the model takes only what both do.
    let gemini = endpoints("google/gemini-3-pro-image");
    assert_eq!(gemini.endpoints.len(), 2);
    let gemini = capabilities(&gemini.endpoints).unwrap();
    assert_eq!(gemini.max_count, 1);
    assert!(!gemini.shape.auto);

    let tencent = capabilities(&endpoints("tencent/hy-image-v3.5-preview").endpoints).unwrap();
    assert!(tencent.shape.pixels.is_some());

    // An endpoint with no parameters at all: one image, no references, no
    // aspect, the orientation said in the prompt.
    let mut bare = endpoints("openai/gpt-image-2").endpoints;
    bare[0].supported_parameters.clear();
    let bare = capabilities(&bare).unwrap();
    assert_eq!(bare.max_count, 1);
    assert_eq!(bare.references, None);
    assert_eq!(bare.edit, None);
    assert_eq!(
        bare.shape.orientation,
        Support::Lowered(Lowering::OrientationInPrompt)
    );
    // The live listing carries a model with no endpoints: it serves nothing.
    assert!(endpoints("meta/muse-image").endpoints.is_empty());
    assert_eq!(capabilities(&[]), None);
    let muse = discovered_model(
        &listed("meta/muse-image"),
        Ok(&endpoints("meta/muse-image")),
    );
    assert_eq!(muse.serves, Some(Vec::new()));
}

#[test]
fn discovery_prices_agreed_meters_and_keeps_the_rest_unplaced() {
    let flux = pricing(&endpoints("black-forest-labs/flux.2-pro").endpoints);
    let image = Some(&Modality::Image);
    assert_eq!(
        flux.usd_per(Charge::Output, image, BillingUnit::Megapixels),
        Some("0.03")
    );
    let gpt = pricing(&endpoints("openai/gpt-image-2").endpoints);
    assert_eq!(
        gpt.usd_per(Charge::Input, Some(&Modality::Text), BillingUnit::Tokens),
        Some("0.000005")
    );
    assert_eq!(
        gpt.usd_per(Charge::Output, image, BillingUnit::Tokens),
        Some("0.00003")
    );
    let qwen = pricing(&endpoints("qwen/qwen-image-2.1-turbo").endpoints);
    // Only tiered output prices: none of them lands on a meter.
    assert_eq!(
        qwen.usd_per(Charge::Output, image, BillingUnit::Images),
        None
    );
    assert_eq!(
        qwen.usd_per(Charge::Input, image, BillingUnit::Images),
        Some("0")
    );
    assert_eq!(
        qwen.unplaced
            .get("alibaba/output_image/image/2k")
            .map(String::as_str),
        Some("0.0184")
    );
    // Three endpoints pricing the same meter differently: each one kept.
    let gemini = pricing(&endpoints("google/gemini-3.1-flash-image").endpoints);
    assert_eq!(
        gemini.usd_per(Charge::Output, image, BillingUnit::Tokens),
        None
    );
    assert_eq!(
        gemini
            .unplaced
            .get("google-vertex/us/output_image/token")
            .map(String::as_str),
        Some("0.000066")
    );
    let recraft = pricing(&endpoints("recraft/recraft-v4-styles-vector").endpoints);
    assert_eq!(
        recraft.usd_per(Charge::Input, image, BillingUnit::Requests),
        Some("0.005")
    );
    let riverflow = pricing(&endpoints("sourceful/riverflow-v2-fast").endpoints);
    assert!(riverflow
        .unplaced
        .contains_key("sourceful/input_font/image"));
}

#[test]
fn a_discovered_model_serves_image_over_openrouter_and_a_failed_record_serves_nothing() {
    let flux = discovered_model(
        &listed("black-forest-labs/flux.2-pro"),
        Ok(&endpoints("black-forest-labs/flux.2-pro")),
    );
    let serves = flux.serves.as_ref().unwrap();
    assert_eq!(serves.len(), 1);
    assert_eq!(serves[0].wire, Wire::OpenRouterImages);
    assert_eq!(serves[0].verb.kind(), VerbKind::Image);
    assert_eq!(
        flux.modalities.as_ref().unwrap().output,
        vec![Modality::Image]
    );
    assert!(flux
        .supported_parameters
        .contains(&"aspect_ratio".to_string()));

    let failed = discovered_model(&listed("black-forest-labs/flux.2-pro"), Err("HTTP 500"));
    assert_eq!(failed.serves, Some(Vec::new()));
    assert_eq!(failed.pricing, None);
    let _ = BTreeMap::<String, String>::new();
}

fn references(count: usize) -> Vec<Reference> {
    (0..count).map(|_| reference(None)).collect()
}

#[test]
fn an_edits_base_counts_against_the_input_image_limit_native_or_not() {
    let gpt = ImageCapabilities::openai_gpt_image();
    let mut only_references = ImageRequest::new("a collage");
    only_references.references = references(16);
    assert!(admit(&gpt, Wire::OpenAiImages, &only_references).is_ok());

    let mut edit = ImageRequest::new("add a boat");
    edit.edit = Some(Edit {
        base: png(),
        mask: None,
    });
    edit.references = references(15);
    assert!(admit(&gpt, Wire::OpenAiImages, &edit).is_ok());
    edit.references = references(16);
    let error = admit(&gpt, Wire::OpenAiImages, &edit).unwrap_err();
    assert_eq!(
        refused_feature(Err(error.clone())),
        ImageFeature::References
    );
    assert!(
        error
            .to_string()
            .contains("it sends 17 input images (the edit base counts as one) and this offer takes at most 16"),
        "{error}"
    );

    // An offer that edits but takes no references still takes its base.
    let mut base_only = ImageCapabilities::openai_images_minimal();
    base_only.edit = Some(EditSupport {
        base: Support::Native,
        mask: Support::Unsupported,
    });
    edit.references.clear();
    assert!(admit(&base_only, Wire::OpenAiImages, &edit).is_ok());
    // ...but takes no reference, alone or beside the base.
    edit.references = references(1);
    assert_eq!(
        refused_feature(admit(&base_only, Wire::OpenAiImages, &edit)),
        ImageFeature::References
    );
    let mut standalone = ImageRequest::new("a collage");
    standalone.references = references(1);
    assert_eq!(
        refused_feature(admit(&base_only, Wire::OpenAiImages, &standalone)),
        ImageFeature::References
    );
}

/// Each declaration a wire has no field for, refused before anything is
/// posted rather than sent without the feature.
#[test]
fn a_declaration_the_wire_cannot_carry_is_refused_before_anything_is_sent() {
    let refused = |route: &ImageRoute, capabilities: ImageCapabilities, request: ImageRequest| {
        let transport = Scripted::replying(Ok(openrouter_reply(None, None)));
        let error = image(&transport, call(route, &capabilities, &request)).unwrap_err();
        assert!(
            transport.sent.lock().unwrap().is_empty(),
            "nothing may be posted for {error:?}"
        );
        match error {
            ImageError::Unsupported { feature, .. } => feature,
            other => panic!("expected a refusal, got {other:?}"),
        }
    };
    let openrouter = openrouter_route();
    let openai = openai_route();

    let mut native_labels = openrouter_shaped();
    native_labels.references.as_mut().unwrap().labels = Support::Native;
    let mut labelled = ImageRequest::new("the cat in the chair");
    labelled.references = vec![reference(Some("the cat"))];
    assert_eq!(
        refused(&openrouter, native_labels.clone(), labelled.clone()),
        ImageFeature::Labels
    );
    assert_eq!(
        refused(&openai, native_labels, labelled.clone()),
        ImageFeature::Labels
    );
    let mut wrong_label_lowering = openrouter_shaped();
    wrong_label_lowering.references.as_mut().unwrap().labels =
        Support::Lowered(Lowering::OrientationInPrompt);
    assert_eq!(
        refused(&openrouter, wrong_label_lowering, labelled),
        ImageFeature::Labels
    );

    let mut portrait = ImageRequest::new("card art");
    portrait.shape = ImageShape::Orientation(Orientation::Portrait);
    let mut native_orientation = openrouter_shaped();
    native_orientation.shape.orientation = Support::Native;
    assert_eq!(
        refused(&openrouter, native_orientation, portrait.clone()),
        ImageFeature::Orientation
    );
    // The OpenAI Images API has no aspect ratio to send an orientation as.
    assert_eq!(
        refused(&openai, openrouter_shaped(), portrait),
        ImageFeature::Orientation
    );
    let mut square = ImageRequest::new("a tile");
    square.shape = ImageShape::Aspect(Ratio::new(1, 1));
    assert_eq!(
        refused(&openai, openrouter_shaped(), square),
        ImageFeature::Aspect
    );

    let mut masked = ImageRequest::new("add a boat");
    masked.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    // OpenRouter has no mask, and no native edit, whatever is declared.
    assert_eq!(
        refused(&openrouter, ImageCapabilities::openai_gpt_image(), masked),
        ImageFeature::Edit
    );
    let mut native_mask = openrouter_shaped();
    native_mask.edit.as_mut().unwrap().mask = Support::Native;
    let mut masked = ImageRequest::new("add a boat");
    masked.edit = Some(Edit {
        base: png(),
        mask: Some(mask()),
    });
    assert_eq!(
        refused(&openrouter, native_mask, masked),
        ImageFeature::Mask
    );
}
