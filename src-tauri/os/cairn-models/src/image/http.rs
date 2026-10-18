//! The facet's HTTP image transport: the one caller of the image wires' POSTs.

use crate::image_wire::{openai, openrouter, ImageWireError};
use crate::interface::image::{ImageBody, ImagePost, ImageReply, ImageRoute, ImageTransport};

/// Sends each image call straight to its provider.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpImages;

impl ImageTransport for HttpImages {
    fn post(&self, post: ImagePost<'_>) -> Result<ImageReply, ImageWireError> {
        let ImagePost {
            route,
            body,
            timeout,
        } = post;
        let endpoint = match route {
            ImageRoute::OpenRouter(endpoint) | ImageRoute::OpenAi(endpoint) => endpoint,
            ImageRoute::Codex(_) => {
                return Err(ImageWireError::InvalidRequest(
                    "a Codex image call runs on a Codex app-server, not over HTTP; draw it with CodexImages".to_string(),
                ))
            }
        };
        match body {
            ImageBody::OpenRouter(body) => {
                openrouter::post_images(endpoint, &body, timeout).map(ImageReply::OpenRouter)
            }
            ImageBody::OpenAiGenerations(body) => {
                openai::post_generations(endpoint, &body, timeout).map(ImageReply::OpenAi)
            }
            ImageBody::OpenAiEdits(form) => {
                openai::post_edits(endpoint, form, timeout).map(ImageReply::OpenAi)
            }
            ImageBody::Codex(_) => Err(ImageWireError::InvalidRequest(
                "a Codex image body has no HTTP route".to_string(),
            )),
        }
    }
}
