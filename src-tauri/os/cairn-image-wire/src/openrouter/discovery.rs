//! OpenRouter's image model discovery: the public listing and each model's
//! per-endpoint records. No key is needed for either.
//!
//! The listing's `supported_parameters` is the union across a model's
//! endpoints; an endpoint record's is what that endpoint itself accepts, and
//! providers clamp what they do not. So the listing says what exists and the
//! endpoint records say what can be relied on.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{http, ImageWireError};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageModels {
    #[serde(default)]
    pub data: Vec<ImageModel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageModel {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub created: Option<i64>,
    #[serde(default)]
    pub architecture: Option<Architecture>,
    #[serde(default)]
    pub supported_parameters: BTreeMap<String, Descriptor>,
    #[serde(default)]
    pub supports_streaming: Option<bool>,
    /// The path of this model's endpoint records.
    #[serde(default)]
    pub endpoints: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Architecture {
    #[serde(default)]
    pub input_modalities: Vec<String>,
    #[serde(default)]
    pub output_modalities: Vec<String>,
}

/// What one request field accepts. An absent key is unsupported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Descriptor {
    /// One of these strings.
    Enum {
        #[serde(default)]
        values: Vec<String>,
    },
    /// Any integer in `[min, max]`.
    Range {
        #[serde(default)]
        min: Option<f64>,
        #[serde(default)]
        max: Option<f64>,
    },
    /// Supported.
    Boolean,
    /// A descriptor type this crate does not know, kept rather than failing
    /// the whole listing.
    #[serde(other)]
    Other,
}

impl Descriptor {
    /// A range's maximum as a count; `None` for anything else.
    pub fn max(&self) -> Option<u32> {
        match self {
            Self::Range { max: Some(max), .. } if *max >= 0.0 => Some(*max as u32),
            _ => None,
        }
    }

    pub fn values(&self) -> &[String] {
        match self {
            Self::Enum { values } => values,
            _ => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelEndpoints {
    pub id: String,
    #[serde(default)]
    pub endpoints: Vec<EndpointRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointRecord {
    #[serde(default)]
    pub provider_name: Option<String>,
    /// The key `provider.options` takes for this endpoint's passthroughs.
    pub provider_slug: String,
    /// The slug to pin with `provider.only`; `None` when it cannot be pinned.
    #[serde(default)]
    pub provider_tag: Option<String>,
    #[serde(default)]
    pub supported_parameters: BTreeMap<String, Descriptor>,
    #[serde(default)]
    pub allowed_passthrough_parameters: Vec<String>,
    #[serde(default)]
    pub supports_streaming: Option<bool>,
    #[serde(default)]
    pub pricing: Vec<PricingLine>,
}

/// One billable line: `cost_usd` per `unit` of `billable`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PricingLine {
    /// `output_image`, `input_image`, `input_reference`, `input_text`, ...
    pub billable: String,
    /// `image`, `megapixel`, `token`, `request`, ...
    pub unit: String,
    pub cost_usd: f64,
    /// A tier such as `1k` or `2k`, for resolution-tiered pricing.
    #[serde(default)]
    pub variant: Option<String>,
}

impl PricingLine {
    /// The price as plain decimal text (`8e-06` becomes `0.000008`). Rust
    /// prints the shortest decimal that reads back as the same number, so
    /// the digits are the ones the provider wrote.
    pub fn usd_text(&self) -> String {
        self.cost_usd.to_string()
    }
}

/// The image model listing.
pub fn list_image_models(base_url: &str, timeout: Duration) -> Result<ImageModels, ImageWireError> {
    let url = format!("{}/images/models", base_url.trim_end_matches('/'));
    let text = http::send(http::client(timeout)?.get(url), "OpenRouter")?;
    read_image_models(&text)
}

/// One model's endpoint records.
pub fn model_endpoints(
    base_url: &str,
    model: &str,
    timeout: Duration,
) -> Result<ModelEndpoints, ImageWireError> {
    let url = format!(
        "{}/images/models/{model}/endpoints",
        base_url.trim_end_matches('/')
    );
    let text = http::send(http::client(timeout)?.get(url), "OpenRouter")?;
    read_model_endpoints(&text)
}

pub fn read_image_models(text: &str) -> Result<ImageModels, ImageWireError> {
    http::read(text, "OpenRouter image model listing")
}

pub fn read_model_endpoints(text: &str) -> Result<ModelEndpoints, ImageWireError> {
    http::read(text, "OpenRouter image endpoints record")
}
