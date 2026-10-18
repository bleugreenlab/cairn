//! Admission: whether an offer, over the wire that carries it, can honour a
//! request, and the lowerings it takes to.

use cairn_models_types::catalog::Wire;
use cairn_models_types::image::{ImageCapabilities, Lowering, Ratio, Support};

use crate::image_wire::sniff;
use crate::interface::image::{ImageError, ImageFeature, ImageRequest, ImageShape};

/// What a wire has a field for. A declaration can only be honoured where
/// its wire carries it: there is no native label or orientation field
/// anywhere, so those are only ever lowerings.
struct Carries {
    aspect: bool,
    pixels: bool,
    native_edit: bool,
    mask: bool,
}

fn carries(wire: Wire) -> Carries {
    match wire {
        Wire::OpenRouterImages => Carries {
            aspect: true,
            pixels: true,
            native_edit: false,
            mask: false,
        },
        Wire::OpenAiImages => Carries {
            aspect: false,
            pixels: true,
            native_edit: true,
            mask: true,
        },
        // Codex draws from its prompt and attached images alone; every
        // other wire carries no image request at all.
        Wire::CodexAppServer
        | Wire::OpenAiChatCompletions
        | Wire::AnthropicMessages
        | Wire::OpenAiResponses
        | Wire::OpenAiDecisions
        | Wire::ClaudeStreamJson
        | Wire::ElevenLabsStreamInput
        | Wire::OpenAiSpeech => Carries {
            aspect: false,
            pixels: false,
            native_edit: false,
            mask: false,
        },
    }
}

fn unsupported(
    capabilities: &ImageCapabilities,
    feature: ImageFeature,
    why: impl Into<String>,
) -> ImageError {
    ImageError::Unsupported {
        feature,
        detail: format!("{} (it serves {})", why.into(), capabilities.summary()),
    }
}

fn ratios(ratios: &[Ratio]) -> String {
    ratios
        .iter()
        .map(Ratio::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// What any request must be, whatever offer it goes to.
pub fn request_problems(request: &ImageRequest) -> Result<(), ImageError> {
    if request.prompt.trim().is_empty() {
        return Err(ImageError::InvalidRequest(
            "an image request needs a prompt".into(),
        ));
    }
    if request.count == 0 {
        return Err(ImageError::InvalidRequest(
            "an image request asks for at least one image".into(),
        ));
    }
    if let Some(mask) = request.edit.as_ref().and_then(|edit| edit.mask.as_ref()) {
        if sniff::media_type(&mask.bytes) != Some("image/png") {
            return Err(ImageError::InvalidRequest(
                "a mask must be a PNG with an alpha channel".into(),
            ));
        }
        let base = &request
            .edit
            .as_ref()
            .expect("a mask belongs to an edit")
            .base;
        if let (Some(mask_size), Some(base_size)) = (
            sniff::dimensions(&mask.bytes),
            sniff::dimensions(&base.bytes),
        ) {
            if mask_size != base_size {
                return Err(ImageError::InvalidRequest(format!(
                    "the mask is {}x{} but the image it edits is {}x{}; they must match",
                    mask_size.0, mask_size.1, base_size.0, base_size.1
                )));
            }
        }
    }
    Ok(())
}

/// Check every feature of `request` against `capabilities` as `wire` carries
/// them, in order: count, shape, edit and mask, then input images and
/// labels. Returns the lowerings it takes, in the order they compose: the
/// edit, the labels, then the orientation.
///
/// A declaration the wire cannot carry (a native label, a native
/// orientation, an aspect ratio or a mask on a wire without one, a lowering
/// in a feature it does not lower) is refused like an unsupported feature,
/// never treated as honoured.
pub fn admit(
    capabilities: &ImageCapabilities,
    wire: Wire,
    request: &ImageRequest,
) -> Result<Vec<Lowering>, ImageError> {
    request_problems(request)?;
    let carried = carries(wire);
    if request.count > capabilities.max_count {
        return Err(unsupported(
            capabilities,
            ImageFeature::Count,
            format!(
                "it asks for {} images and this offer returns at most {}",
                request.count, capabilities.max_count
            ),
        ));
    }

    let shape = &capabilities.shape;
    let mut orientation = None;
    match request.shape {
        ImageShape::Auto => {}
        ImageShape::Pixels(size) => match &shape.pixels {
            Some(_) if !carried.pixels => {
                return Err(unsupported(
                    capabilities,
                    ImageFeature::Pixels,
                    format!("the {wire} wire has no size field, so {size} cannot be sent"),
                ))
            }
            None => {
                return Err(unsupported(
                    capabilities,
                    ImageFeature::Pixels,
                    format!("this offer takes no exact size, so {size} cannot be asked for"),
                ))
            }
            Some(rule) => {
                if let Some(why) = rule.refuses(size) {
                    return Err(unsupported(capabilities, ImageFeature::Pixels, why));
                }
            }
        },
        ImageShape::Aspect(ratio) => {
            let why = if !carried.aspect {
                Some(format!(
                    "the {wire} wire has no aspect ratio field, so {ratio} cannot be sent; ask for an exact size or an orientation"
                ))
            } else if shape.aspect.is_empty() {
                Some(format!(
                    "this offer has no aspect ratio parameter, so {ratio} cannot be asked for"
                ))
            } else if !shape.aspect.contains(&ratio) {
                Some(format!(
                    "{ratio} is not one of its aspect ratios ({})",
                    ratios(&shape.aspect)
                ))
            } else {
                None
            };
            if let Some(why) = why {
                return Err(unsupported(capabilities, ImageFeature::Aspect, why));
            }
        }
        ImageShape::Orientation(asked) => {
            let refuse =
                |why: String| Err(unsupported(capabilities, ImageFeature::Orientation, why));
            match &shape.orientation {
                Support::Lowered(lowering @ Lowering::OrientationInPrompt) => {
                    orientation = Some(lowering.clone())
                }
                Support::Lowered(lowering @ Lowering::OrientationAsAspect { .. }) => {
                    if !carried.aspect {
                        return refuse(format!(
                            "it declares an orientation sent as an aspect ratio, which the {wire} wire has no field for"
                        ));
                    }
                    orientation = Some(lowering.clone())
                }
                Support::Lowered(lowering @ Lowering::OrientationAsPixels { .. }) => {
                    if !carried.pixels {
                        return refuse(format!(
                            "it declares an orientation sent as a size, which the {wire} wire has no field for"
                        ));
                    }
                    orientation = Some(lowering.clone())
                }
                Support::Native => {
                    return refuse(
                        "it declares a native orientation, which no image wire carries".into(),
                    )
                }
                Support::Lowered(other) => {
                    return refuse(format!(
                        "it declares {other:?} for an orientation, which does not honour one"
                    ))
                }
                Support::Unsupported => {
                    return refuse(format!("this offer cannot be asked for a {asked} image"))
                }
            }
        }
    }

    let mut edit_lowering = None;
    if let Some(edit) = &request.edit {
        let no_edit = || {
            unsupported(
                capabilities,
                ImageFeature::Edit,
                "this offer does not edit images",
            )
        };
        let Some(support) = &capabilities.edit else {
            return Err(no_edit());
        };
        match &support.base {
            Support::Native if carried.native_edit => {}
            Support::Native => {
                return Err(unsupported(
                    capabilities,
                    ImageFeature::Edit,
                    format!("it declares a native edit, which the {wire} wire has no field for"),
                ))
            }
            Support::Lowered(lowering @ Lowering::EditAsReference) => {
                edit_lowering = Some(lowering.clone())
            }
            Support::Lowered(other) => {
                return Err(unsupported(
                    capabilities,
                    ImageFeature::Edit,
                    format!("it declares {other:?} for an edit, which does not carry one"),
                ))
            }
            Support::Unsupported => return Err(no_edit()),
        }
        if edit.mask.is_some() {
            let why = match &support.mask {
                Support::Native if carried.mask => None,
                Support::Native => Some(format!(
                    "it declares a mask, which the {wire} wire has no field for"
                )),
                Support::Lowered(other) => Some(format!(
                    "it declares {other:?} for a mask, which no lowering can carry"
                )),
                Support::Unsupported => Some(
                    "this offer takes no mask; send the edit without one and say in the prompt what to change"
                        .to_string(),
                ),
            };
            if let Some(why) = why {
                return Err(unsupported(capabilities, ImageFeature::Mask, why));
            }
        }
    }

    // An edit's base is an input image wherever it goes: the first part of
    // an Images edit, or reference 1 on a wire that has only references. So
    // it counts against the same limit the references do.
    let mut label_lowering = None;
    let inputs = request.references.len() + usize::from(request.edit.is_some());
    if !request.references.is_empty() || request.edit.is_some() {
        let max = match &capabilities.references {
            Some(references) => references.max as usize,
            None if !request.references.is_empty() => {
                return Err(unsupported(
                    capabilities,
                    ImageFeature::References,
                    "this offer takes no reference images",
                ))
            }
            // Only an edit is left, and its base was admitted above.
            None => 1,
        };
        if inputs > max {
            let why = match (max, request.edit.is_some()) {
                (0, _) => "this offer takes no reference images".to_string(),
                (_, true) => format!(
                    "it sends {inputs} input images (the edit base counts as one) and this offer takes at most {max}"
                ),
                (_, false) => format!(
                    "it sends {inputs} reference images and this offer takes at most {max}"
                ),
            };
            return Err(unsupported(capabilities, ImageFeature::References, why));
        }
        let labelled = request.references.iter().any(|reference| {
            reference
                .label
                .as_deref()
                .is_some_and(|label| !label.trim().is_empty())
        });
        if labelled {
            let refuse = |why: &str| Err(unsupported(capabilities, ImageFeature::Labels, why));
            match capabilities
                .references
                .as_ref()
                .map(|references| &references.labels)
            {
                Some(Support::Lowered(lowering @ Lowering::LabelsInPrompt)) => {
                    label_lowering = Some(lowering.clone())
                }
                Some(Support::Native) => {
                    return refuse(
                        "it declares native reference labels, which no image wire carries",
                    )
                }
                Some(Support::Lowered(_)) => {
                    return refuse("it declares a lowering for labels that does not carry them")
                }
                Some(Support::Unsupported) | None => {
                    return refuse("this offer cannot be told which reference is which")
                }
            }
        }
    }

    Ok([edit_lowering, label_lowering, orientation]
        .into_iter()
        .flatten()
        .collect())
}
