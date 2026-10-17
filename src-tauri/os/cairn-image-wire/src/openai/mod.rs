//! The OpenAI Images API: `POST {base}/images/generations` (JSON) for a
//! prompt alone, and `POST {base}/images/edits` (multipart) when there are
//! input images, the first of which a mask applies to.
//!
//! One wire, many servers: OpenAI itself, and every OpenAI-compatible image
//! front (LocalAI, vLLM-Omni, SGLang diffusion, the stable-diffusion.cpp
//! server). Nothing here branches on which one it is talking to. Replies
//! carry base64 images without a media type (the caller sniffs it), an
//! optional `revised_prompt`, and token usage; some compatible servers answer
//! with `url` only unless `response_format: b64_json` is sent.

pub mod wire;

#[cfg(test)]
mod tests;

use std::time::Duration;

use reqwest::blocking::multipart::{Form, Part};
use serde_json::{json, Map, Value};

use crate::{http, merge_extras, sniff, ImageWireError, ImagesEndpoint};
use wire::ImagesResponse;

/// The body and form keys the typed request owns.
pub const CORE_KEYS: &[&str] = &["model", "prompt", "n", "size", "image", "image[]", "mask"];

/// What one call asks for, already admitted by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generation<'a> {
    pub model: &'a str,
    pub prompt: &'a str,
    /// Omitted when 1, the API default.
    pub n: u32,
    /// `1024x1536`; `None` leaves the server to pick.
    pub size: Option<String>,
}

impl Generation<'_> {
    fn core(&self) -> Map<String, Value> {
        let mut core = Map::new();
        core.insert("model".into(), json!(self.model));
        core.insert("prompt".into(), json!(self.prompt));
        if self.n > 1 {
            core.insert("n".into(), json!(self.n));
        }
        if let Some(size) = &self.size {
            core.insert("size".into(), json!(size));
        }
        core
    }
}

/// The generations body: `extras` under the typed fields.
pub fn generations_body(
    generation: &Generation<'_>,
    extras: &Map<String, Value>,
) -> Result<Value, ImageWireError> {
    merge_extras(generation.core(), extras, CORE_KEYS).map(Value::Object)
}

/// One image in a multipart form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImagePart {
    pub bytes: Vec<u8>,
    pub media_type: String,
}

/// An edits request before it becomes a multipart body, so it can be
/// inspected: text fields in order, then the images, then the mask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditsForm {
    pub fields: Vec<(String, String)>,
    pub images: Vec<ImagePart>,
    pub mask: Option<ImagePart>,
}

/// The edits form. `images` are sent in order (an edit's base first, since a
/// mask applies to the first image), one as `image` and several as
/// `image[]`, which is how OpenAI's own SDKs send them.
pub fn edits_form(
    generation: &Generation<'_>,
    images: Vec<ImagePart>,
    mask: Option<ImagePart>,
    extras: &Map<String, Value>,
) -> Result<EditsForm, ImageWireError> {
    if images.is_empty() {
        return Err(ImageWireError::InvalidRequest(
            "an edit needs at least one image".into(),
        ));
    }
    let merged = merge_extras(generation.core(), extras, CORE_KEYS)?;
    let fields = merged
        .into_iter()
        .map(|(key, value)| {
            let text = match value {
                Value::String(text) => text,
                other => other.to_string(),
            };
            (key, text)
        })
        .collect();
    Ok(EditsForm {
        fields,
        images,
        mask,
    })
}

impl EditsForm {
    pub fn into_multipart(self) -> Result<Form, ImageWireError> {
        let mut form = Form::new();
        for (key, value) in self.fields {
            form = form.text(key, value);
        }
        let name = if self.images.len() == 1 {
            "image"
        } else {
            "image[]"
        };
        for (index, image) in self.images.into_iter().enumerate() {
            let filename = format!(
                "image-{}.{}",
                index + 1,
                sniff::extension(&image.media_type)
            );
            form = form.part(name, part(image, filename)?);
        }
        if let Some(mask) = self.mask {
            form = form.part("mask", part(mask, "mask.png".to_string())?);
        }
        Ok(form)
    }
}

fn part(image: ImagePart, filename: String) -> Result<Part, ImageWireError> {
    Part::bytes(image.bytes)
        .file_name(filename)
        .mime_str(&image.media_type)
        .map_err(|error| {
            ImageWireError::InvalidRequest(format!(
                "'{}' is not a media type: {error}",
                image.media_type
            ))
        })
}

/// Post a generation and read its reply.
pub fn post_generations(
    endpoint: &ImagesEndpoint,
    body: &Value,
    timeout: Duration,
) -> Result<ImagesResponse, ImageWireError> {
    let request = http::client(timeout)?
        .post(endpoint.url("images/generations"))
        .headers(http::header_map(endpoint)?)
        .json(body);
    read(&http::send(request, &endpoint.provider_name)?)
}

/// Post an edit and read its reply.
pub fn post_edits(
    endpoint: &ImagesEndpoint,
    form: EditsForm,
    timeout: Duration,
) -> Result<ImagesResponse, ImageWireError> {
    let request = http::client(timeout)?
        .post(endpoint.url("images/edits"))
        .headers(http::header_map(endpoint)?)
        .multipart(form.into_multipart()?);
    read(&http::send(request, &endpoint.provider_name)?)
}

/// Decode an Images API reply body.
pub fn read(text: &str) -> Result<ImagesResponse, ImageWireError> {
    http::read(text, "Images API reply")
}
