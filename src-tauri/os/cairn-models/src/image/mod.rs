//! The facet's image implementation: one [`image`] over either route, through
//! an [`ImageTransport`] so the POST passes one seam.
//!
//! A call runs in order: [`admit`] the request against the offer's declared
//! capabilities, [`compose_prompt`] for the lowerings that admits, build the
//! wire call, post it, and map the reply onto images and metered usage.
//! Nothing is stored: the result carries the bytes and the usage.

mod admit;
pub mod http;
pub mod openai;
pub mod openrouter;
mod prompt;
#[cfg(test)]
mod tests;
mod usage;

pub use admit::{admit, request_problems};
pub use prompt::{compose_prompt, label_line, orientation_sentence, EDIT_SENTENCE};

use cairn_models_types::image::{Lowering, Orientation, Pixels};

use crate::image_wire::{
    data_url, decode_base64, openai as openai_wire, openrouter as openrouter_wire, sniff,
};
use crate::interface::image::{
    GeneratedImage, ImageBody, ImageCall, ImageError, ImagePost, ImageReply, ImageResult,
    ImageRoute, ImageShape, ImageTransport, Provenance,
};

/// Draw what `call.request` asks for on `call.route`.
pub fn image(
    transport: &dyn ImageTransport,
    call: ImageCall<'_>,
) -> Result<ImageResult, ImageError> {
    let request = call.request;
    let wire = call.route.wire();
    let lowerings = admit(call.capabilities, wire, request)?;
    let prompt_sent = compose_prompt(&request.prompt, request, &lowerings);
    let extras = request.extras_for(wire);
    let endpoint = call.route.endpoint();
    let provider = endpoint.provider_name.as_str();
    let shaped = Shaped::from(request.shape, &lowerings);

    let body = match call.route {
        ImageRoute::OpenRouter(_) => {
            // Admission over this wire refused a mask.
            let input_references = request
                .edit
                .iter()
                .map(|edit| &edit.base)
                .chain(request.references.iter().map(|reference| &reference.image))
                .map(|image| data_url(&image.media_type, &image.bytes))
                .collect();
            let generation = openrouter_wire::Generation {
                model: call.model,
                prompt: &prompt_sent,
                n: request.count,
                aspect_ratio: shaped.aspect.map(|ratio| ratio.to_string()),
                size: shaped.pixels.map(|pixels| pixels.to_string()),
                input_references,
            };
            ImageBody::OpenRouter(
                openrouter_wire::build_body(&generation, &extras)
                    .map_err(|error| ImageError::from_wire(provider, error))?,
            )
        }
        ImageRoute::OpenAi(_) => {
            // Admission over this wire refused any aspect ratio, so the
            // shape is a size or nothing.
            let generation = openai_wire::Generation {
                model: call.model,
                prompt: &prompt_sent,
                n: request.count,
                size: shaped.pixels.map(|pixels| pixels.to_string()),
            };
            let images: Vec<openai_wire::ImagePart> = request
                .edit
                .iter()
                .map(|edit| &edit.base)
                .chain(request.references.iter().map(|reference| &reference.image))
                .map(|image| openai_wire::ImagePart {
                    bytes: image.bytes.clone(),
                    media_type: image.media_type.clone(),
                })
                .collect();
            if images.is_empty() {
                ImageBody::OpenAiGenerations(
                    openai_wire::generations_body(&generation, &extras)
                        .map_err(|error| ImageError::from_wire(provider, error))?,
                )
            } else {
                let mask = request
                    .edit
                    .as_ref()
                    .and_then(|edit| edit.mask.as_ref())
                    .map(|mask| openai_wire::ImagePart {
                        bytes: mask.bytes.clone(),
                        media_type: mask.media_type.clone(),
                    });
                ImageBody::OpenAiEdits(
                    openai_wire::edits_form(&generation, images, mask, &extras)
                        .map_err(|error| ImageError::from_wire(provider, error))?,
                )
            }
        }
    };

    let reply = transport
        .post(ImagePost {
            endpoint,
            body,
            timeout: call.timeout,
        })
        .map_err(|error| ImageError::from_wire(provider, error))?;
    let (images, usage) = match reply {
        ImageReply::OpenRouter(reply) => {
            let images = reply
                .data
                .iter()
                .map(|data| generated(&data.b64_json, data.media_type.as_deref(), None, None))
                .collect::<Result<Vec<_>, _>>()?;
            let usage = usage::openrouter(reply.usage.as_ref(), images.len());
            (images, usage)
        }
        ImageReply::OpenAi(reply) => {
            let served = reply
                .size
                .as_deref()
                .and_then(|size| size.parse::<Pixels>().ok());
            let images = reply
                .data
                .iter()
                .map(|data| match &data.b64_json {
                    Some(b64) => generated(b64, None, served, data.revised_prompt.clone()),
                    None => Err(ImageError::InvalidResponse(format!(
                        "{provider} returned {} instead of image bytes; set response_format: b64_json in this call's openai-images extras",
                        if data.url.is_some() { "an image URL" } else { "an empty image" }
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let usage = usage::openai(reply.usage.as_ref(), images.len());
            (images, usage)
        }
    };
    if images.is_empty() {
        return Err(ImageError::InvalidResponse(format!(
            "{provider} returned no images"
        )));
    }
    Ok(ImageResult {
        images,
        provenance: Provenance {
            provider: endpoint.backend_key.clone(),
            model: call.model.to_string(),
            wire,
            prompt_sent,
            lowerings,
            requested: request.shape,
        },
        usage,
    })
}

/// What the request's shape becomes on the wire, after its lowering.
struct Shaped {
    aspect: Option<cairn_models_types::image::Ratio>,
    pixels: Option<Pixels>,
}

impl Shaped {
    fn from(shape: ImageShape, lowerings: &[Lowering]) -> Self {
        let none = Self {
            aspect: None,
            pixels: None,
        };
        match shape {
            ImageShape::Auto => none,
            ImageShape::Pixels(pixels) => Self {
                pixels: Some(pixels),
                ..none
            },
            ImageShape::Aspect(ratio) => Self {
                aspect: Some(ratio),
                ..none
            },
            ImageShape::Orientation(orientation) => {
                for lowering in lowerings {
                    match lowering {
                        Lowering::OrientationAsAspect {
                            portrait,
                            landscape,
                            square,
                        } => {
                            return Self {
                                aspect: Some(*pick(orientation, portrait, landscape, square)),
                                ..none
                            }
                        }
                        Lowering::OrientationAsPixels {
                            portrait,
                            landscape,
                            square,
                        } => {
                            return Self {
                                pixels: Some(*pick(orientation, portrait, landscape, square)),
                                ..none
                            }
                        }
                        _ => {}
                    }
                }
                none
            }
        }
    }
}

fn pick<'a, T>(
    orientation: Orientation,
    portrait: &'a T,
    landscape: &'a T,
    square: &'a T,
) -> &'a T {
    match orientation {
        Orientation::Portrait => portrait,
        Orientation::Landscape => landscape,
        Orientation::Square => square,
    }
}

fn generated(
    b64: &str,
    media_type: Option<&str>,
    served: Option<Pixels>,
    revised_prompt: Option<String>,
) -> Result<GeneratedImage, ImageError> {
    let bytes =
        decode_base64(b64).map_err(|error| ImageError::InvalidResponse(error.to_string()))?;
    let media_type = media_type
        .filter(|media_type| !media_type.is_empty())
        .or_else(|| sniff::media_type(&bytes))
        .unwrap_or("application/octet-stream")
        .to_string();
    let size = sniff::dimensions(&bytes)
        .map(|(width, height)| Pixels::new(width, height))
        .or(served);
    Ok(GeneratedImage {
        bytes,
        media_type,
        size,
        revised_prompt,
    })
}
