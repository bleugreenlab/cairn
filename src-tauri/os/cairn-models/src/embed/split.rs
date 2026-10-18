//! Cutting one request into consecutive requests an offer will take.

use std::ops::Range;

use cairn_models_types::embedding::BatchLimits;

use crate::interface::embed::EmbedError;

/// Consecutive ranges of `texts`, each within `limits`, covering every text
/// in order. A single text longer than one request may carry is refused by
/// its index: cutting it would embed something other than what was asked.
pub(super) fn batches(
    texts: &[String],
    limits: &BatchLimits,
) -> Result<Vec<Range<usize>>, EmbedError> {
    let max_texts = limits
        .max_texts
        .map_or(usize::MAX, |max| (max as usize).max(1));
    let max_chars = limits.max_chars.map(|max| max as usize);
    let mut batches = Vec::new();
    let mut start = 0;
    let mut used = 0;
    for (index, text) in texts.iter().enumerate() {
        let chars = text.chars().count();
        if let Some(max) = max_chars.filter(|max| chars > *max) {
            return Err(EmbedError::InvalidRequest(format!(
                "text {index} is {chars} characters, more than the {max} one request to this model may carry; split it before embedding"
            )));
        }
        let fits = index - start < max_texts && max_chars.is_none_or(|max| used + chars <= max);
        if !fits {
            batches.push(start..index);
            start = index;
            used = 0;
        }
        used += chars;
    }
    batches.push(start..texts.len());
    Ok(batches)
}
