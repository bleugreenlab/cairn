//! Prompt composition: the sentences a declared lowering adds.

use cairn_models_types::image::{Lowering, Orientation};

use crate::interface::image::{ImageRequest, ImageShape};

/// Said when an edit's base rides as reference 1.
pub const EDIT_SENTENCE: &str =
    "Image 1 is the image to edit: change only what this prompt asks for and keep everything else exactly as it is.";

/// One labelled reference, numbered in the order the images are sent.
pub fn label_line(number: usize, label: &str) -> String {
    format!("Image {number}: {label}")
}

/// The orientation, said in the prompt.
pub fn orientation_sentence(orientation: Orientation) -> String {
    format!("Compose the image in {orientation} orientation.")
}

/// `prompt` with the sentences `lowerings` call for appended: the edit
/// sentence, the reference labels, then the orientation. A lowering that
/// adds no sentence leaves it alone, and so does no lowering at all: the
/// prompt is then sent byte for byte as written.
pub fn compose_prompt(prompt: &str, request: &ImageRequest, lowerings: &[Lowering]) -> String {
    let mut lines = Vec::new();
    if lowerings.contains(&Lowering::EditAsReference) && request.edit.is_some() {
        lines.push(EDIT_SENTENCE.to_string());
    }
    if lowerings.contains(&Lowering::LabelsInPrompt) {
        // The edit base, sent natively or as a reference, is always image 1.
        let first = 1 + usize::from(request.edit.is_some());
        for (index, reference) in request.references.iter().enumerate() {
            if let Some(label) = reference
                .label
                .as_deref()
                .map(str::trim)
                .filter(|l| !l.is_empty())
            {
                lines.push(label_line(first + index, label));
            }
        }
    }
    if lowerings.contains(&Lowering::OrientationInPrompt) {
        if let ImageShape::Orientation(orientation) = request.shape {
            lines.push(orientation_sentence(orientation));
        }
    }
    if lines.is_empty() {
        return prompt.to_string();
    }
    format!("{prompt}\n\n{}", lines.join("\n"))
}
