//! Whether an offer can return the width a request asks for.

use cairn_models_types::embedding::{EmbeddingCapabilities, EmbeddingWidths};

use crate::interface::embed::EmbedError;

/// The `dimensions` to send for `requested` on an offer declaring
/// `capabilities`: `None` for the native width (nothing is sent, so a server
/// that does not know the field still answers), or the width itself. A width
/// the offer does not return is refused here, before any request.
pub(crate) fn admit(
    capabilities: &EmbeddingCapabilities,
    requested: Option<u32>,
) -> Result<Option<u32>, EmbedError> {
    let Some(width) = requested else {
        return Ok(None);
    };
    let Some(native) = capabilities.native_dims else {
        return Err(EmbedError::Unsupported(format!(
            "its native width is not known, so it is only asked for that width, not {width}"
        )));
    };
    if width == native {
        return Ok(None);
    }
    let admitted = match &capabilities.widths {
        EmbeddingWidths::NativeOnly => false,
        EmbeddingWidths::Choices(widths) => widths.contains(&width),
        EmbeddingWidths::UpTo { min } => (*min..=native).contains(&width),
    };
    if admitted {
        return Ok(Some(width));
    }
    let offered = match &capabilities.widths {
        EmbeddingWidths::NativeOnly => format!("only its native {native}"),
        EmbeddingWidths::Choices(widths) => format!(
            "{native} or {}",
            widths
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        EmbeddingWidths::UpTo { min } => format!("any width from {min} to {native}"),
    };
    Err(EmbedError::Unsupported(format!(
        "it returns {offered}, not {width}"
    )))
}
