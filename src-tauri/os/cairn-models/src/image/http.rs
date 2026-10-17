//! The facet's image transport: the one caller of the image wires' POSTs.

use crate::image_wire::{openai, openrouter, ImageWireError};
use crate::interface::image::{ImageBody, ImagePost, ImageReply, ImageTransport};

/// Sends each image call straight to its provider.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpImages;

impl ImageTransport for HttpImages {
    fn post(&self, post: ImagePost<'_>) -> Result<ImageReply, ImageWireError> {
        let ImagePost {
            endpoint,
            body,
            timeout,
        } = post;
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
        }
    }
}
