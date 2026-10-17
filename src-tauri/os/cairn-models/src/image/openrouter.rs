//! OpenRouter as an image provider: its route, and what its Image API serves
//! as discovery reads it.
//!
//! Discovery reads the public listing, then every model's endpoint records.
//! What a model can do is the intersection across its endpoints: OpenRouter
//! routes a call to any of them and providers clamp a value they do not take,
//! so anything one endpoint lacks is not something the model can be relied
//! on for.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use cairn_models_types::catalog::{
    DiscoveredModel, Modalities, Modality, ModelPricing, Price, ServedVerb, Verb, Wire,
};
use cairn_models_types::image::{
    EditSupport, ImageCapabilities, Lowering, PixelRule, Ratio, ReferenceSupport, ShapeSupport,
    Support,
};
use cairn_models_types::usage::{BillingUnit, Charge, Meter};

use crate::image_wire::openrouter::discovery::{
    list_image_models, model_endpoints, Descriptor, EndpointRecord, ImageModel, ModelEndpoints,
};
use crate::image_wire::openrouter::API_BASE;
use crate::image_wire::ImagesEndpoint;
use crate::interface::image::ImageRoute;
use crate::openrouter::{headers, Attribution};

/// The backend key OpenRouter's offers carry.
pub const BACKEND_KEY: &str = "openrouter";

/// How many endpoint records are read at once.
const DISCOVERY_PARALLELISM: usize = 8;

/// The route to OpenRouter's Image API on `api_key`, attributed to the host.
pub fn endpoint(api_key: &str, attribution: &Attribution) -> ImageRoute {
    ImageRoute::OpenRouter(ImagesEndpoint {
        provider_name: "OpenRouter".into(),
        backend_key: BACKEND_KEY.into(),
        base_url: API_BASE.into(),
        headers: headers(api_key, attribution),
    })
}

/// What one discovery pass found.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageDiscovery {
    pub models: Vec<DiscoveredModel>,
    /// Models whose endpoint records could not be read, each as a sentence.
    /// Those models are still listed, serving nothing.
    pub failures: Vec<String>,
}

/// Discover OpenRouter's image models. Needs no key.
pub fn discover(timeout: Duration) -> Result<ImageDiscovery, String> {
    discover_from(API_BASE, timeout)
}

/// Discover the image models of the OpenRouter API at `base_url`.
pub fn discover_from(base_url: &str, timeout: Duration) -> Result<ImageDiscovery, String> {
    let listing = list_image_models(base_url, timeout)
        .map_err(|error| format!("OpenRouter image model listing failed: {error}"))?;
    let models = listing.data;
    let records: Mutex<Vec<Option<Result<ModelEndpoints, String>>>> =
        Mutex::new(vec![None; models.len()]);
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..DISCOVERY_PARALLELISM.min(models.len()) {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(model) = models.get(index) else {
                    return;
                };
                let record = model_endpoints(base_url, &model.id, timeout)
                    .map_err(|error| error.to_string());
                records.lock().expect("discovery lock")[index] = Some(record);
            });
        }
    });
    let records = records.into_inner().expect("discovery lock");
    let mut failures = Vec::new();
    let models = models
        .iter()
        .zip(records)
        .map(|(model, record)| {
            let record = record.unwrap_or_else(|| Err("never read".to_string()));
            if let Err(error) = &record {
                failures.push(format!("{}: {error}", model.id));
            }
            discovered_model(model, record.as_ref().map_err(String::as_str))
        })
        .collect();
    Ok(ImageDiscovery { models, failures })
}

/// One listed model as the catalog carries it. A model whose endpoint
/// records could not be read, or that has none, serves nothing.
pub fn discovered_model(
    model: &ImageModel,
    record: Result<&ModelEndpoints, &str>,
) -> DiscoveredModel {
    let endpoints = record
        .map(|record| record.endpoints.as_slice())
        .unwrap_or(&[]);
    let serves = capabilities(endpoints)
        .map(|capabilities| ServedVerb {
            verb: Verb::Image(capabilities),
            wire: Wire::OpenRouterImages,
        })
        .into_iter()
        .collect();
    let modalities = model.architecture.as_ref().map(|architecture| Modalities {
        input: architecture
            .input_modalities
            .iter()
            .map(|value| Modality::from(value.as_str()))
            .collect(),
        output: architecture
            .output_modalities
            .iter()
            .map(|value| Modality::from(value.as_str()))
            .collect(),
    });
    DiscoveredModel {
        id: model.id.clone(),
        model: model.id.clone(),
        display_name: model.name.clone().unwrap_or_else(|| model.id.clone()),
        description: model.description.clone(),
        hidden: false,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: Vec::new(),
        context_window: None,
        canonical_slug: None,
        serving_account_ids: Vec::new(),
        pricing: (!endpoints.is_empty()).then(|| pricing(endpoints)),
        supported_parameters: model.supported_parameters.keys().cloned().collect(),
        router: false,
        modalities,
        serves: Some(serves),
    }
}

/// What every one of `endpoints` takes, or `None` when there are none.
pub fn capabilities(endpoints: &[EndpointRecord]) -> Option<ImageCapabilities> {
    let first = endpoints.first()?;
    let parameter =
        |record: &EndpointRecord, key: &str| record.supported_parameters.get(key).cloned();
    let max_count = endpoints
        .iter()
        .map(|record| {
            parameter(record, "n")
                .and_then(|n| n.max())
                .unwrap_or(1)
                .max(1)
        })
        .min()
        .unwrap_or(1);
    let references = endpoints
        .iter()
        .map(|record| {
            parameter(record, "input_references")
                .and_then(|references| references.max())
                .unwrap_or(0)
        })
        .min()
        .unwrap_or(0);
    let aspect_values = |record: &EndpointRecord| -> Vec<String> {
        parameter(record, "aspect_ratio")
            .map(|descriptor| descriptor.values().to_vec())
            .unwrap_or_default()
    };
    let shared: Vec<String> = aspect_values(first)
        .into_iter()
        .filter(|value| {
            endpoints
                .iter()
                .all(|record| aspect_values(record).contains(value))
        })
        .collect();
    let auto = shared.iter().any(|value| value == "auto");
    // Values a `Ratio` cannot hold (`2.35:1`, `9:19.5`) are not offered.
    let aspect: Vec<Ratio> = shared
        .iter()
        .filter_map(|value| value.parse().ok())
        .collect();
    let sized = endpoints
        .iter()
        .all(|record| matches!(parameter(record, "size"), Some(Descriptor::Boolean)));
    let pixels = sized.then_some(PixelRule::Free {
        // OpenRouter normalizes an explicit size for its provider, so the
        // only bound it publishes is its own aspect range, 1:8 to 8:1.
        multiple_of: 1,
        min_edge: 1,
        max_edge: 4096,
        max_ratio: Ratio::new(8, 1),
    });
    let orientation = Support::Lowered(orientation_lowering(&aspect));
    Some(ImageCapabilities {
        max_count,
        shape: ShapeSupport {
            auto,
            pixels,
            aspect,
            orientation,
        },
        references: (references > 0).then_some(ReferenceSupport {
            max: references,
            labels: Support::Lowered(Lowering::LabelsInPrompt),
        }),
        // No mask is documented; an edit rides as reference 1.
        edit: (references > 0).then_some(EditSupport {
            base: Support::Lowered(Lowering::EditAsReference),
            mask: Support::Unsupported,
        }),
    })
}

/// The aspect ratios an orientation is sent as, preferred first.
const PORTRAITS: [Ratio; 3] = [Ratio::new(2, 3), Ratio::new(3, 4), Ratio::new(9, 16)];
const LANDSCAPES: [Ratio; 3] = [Ratio::new(3, 2), Ratio::new(4, 3), Ratio::new(16, 9)];
const SQUARE: Ratio = Ratio::new(1, 1);

/// An orientation as a native aspect ratio when the offer has all three,
/// and otherwise a sentence of the prompt.
fn orientation_lowering(aspect: &[Ratio]) -> Lowering {
    let first = |candidates: &[Ratio]| {
        candidates
            .iter()
            .find(|ratio| aspect.contains(ratio))
            .copied()
    };
    match (
        first(&PORTRAITS),
        first(&LANDSCAPES),
        aspect.contains(&SQUARE),
    ) {
        (Some(portrait), Some(landscape), true) => Lowering::OrientationAsAspect {
            portrait,
            landscape,
            square: SQUARE,
        },
        _ => Lowering::OrientationInPrompt,
    }
}

fn meter(billable: &str, unit: &str) -> Option<Meter> {
    let (charge, modality) = match billable {
        "output_image" => (Charge::Output, Modality::Image),
        "input_image" | "input_reference" => (Charge::Input, Modality::Image),
        "input_text" => (Charge::Input, Modality::Text),
        _ => return None,
    };
    let unit = match unit {
        "image" => BillingUnit::Images,
        "megapixel" => BillingUnit::Megapixels,
        "token" => BillingUnit::Tokens,
        "request" => BillingUnit::Requests,
        _ => return None,
    };
    Some(Meter {
        charge,
        modality: Some(modality),
        unit,
    })
}

/// The model's prices: one per meter where every endpoint agrees. A tiered
/// line (`variant`), a line Cairn cannot place, or a meter the endpoints
/// price differently is kept in `unplaced` as
/// `{provider_slug}/{billable}/{unit}[/{variant}]`.
pub fn pricing(endpoints: &[EndpointRecord]) -> ModelPricing {
    let mut unplaced = BTreeMap::new();
    let mut per_endpoint: Vec<Vec<(Meter, String, String)>> = Vec::new();
    for record in endpoints {
        let mut placed: Vec<(Meter, String, String)> = Vec::new();
        for line in &record.pricing {
            let key = match &line.variant {
                Some(variant) => format!(
                    "{}/{}/{}/{variant}",
                    record.provider_slug, line.billable, line.unit
                ),
                None => format!("{}/{}/{}", record.provider_slug, line.billable, line.unit),
            };
            match (line.variant.is_none(), meter(&line.billable, &line.unit)) {
                (true, Some(meter)) if !placed.iter().any(|(m, _, _)| *m == meter) => {
                    placed.push((meter, line.usd_text(), key));
                }
                _ => {
                    unplaced.insert(key, line.usd_text());
                }
            }
        }
        per_endpoint.push(placed);
    }
    let mut prices = Vec::new();
    let mut meters: Vec<&Meter> = Vec::new();
    for placed in &per_endpoint {
        for (meter, _, _) in placed {
            if !meters.contains(&meter) {
                meters.push(meter);
            }
        }
    }
    for meter in meters {
        let quoted: Vec<Option<&(Meter, String, String)>> = per_endpoint
            .iter()
            .map(|placed| placed.iter().find(|(m, _, _)| m == meter))
            .collect();
        let first = quoted[0].map(|(_, usd, _)| usd);
        let agreed = first.is_some()
            && quoted
                .iter()
                .all(|quote| quote.map(|(_, usd, _)| usd) == first);
        if agreed {
            prices.push(Price {
                meter: meter.clone(),
                usd: first.expect("agreed").clone(),
            });
        } else {
            for (_, usd, key) in quoted.into_iter().flatten() {
                unplaced.insert(key.clone(), usd.clone());
            }
        }
    }
    ModelPricing { prices, unplaced }
}
