//! What an image call consumed, on the facet's meters.
//!
//! Every call counts the images it returned. Token and cost fields are
//! measured only when the provider reported them: an absent meter was not
//! reported, which is not the same as zero.

use cairn_models_types::catalog::Modality;
use cairn_models_types::usage::{BillingUnit, Charge, Measure, Meter, MeteredUsage};

use crate::image_wire::{openai, openrouter};
use crate::wire::TurnUsage;

fn measure(
    charge: Charge,
    modality: Option<Modality>,
    unit: BillingUnit,
    quantity: u64,
) -> Measure {
    Measure {
        meter: Meter {
            charge,
            modality,
            unit,
        },
        quantity,
    }
}

fn images_returned(count: usize) -> Measure {
    measure(
        Charge::Output,
        Some(Modality::Image),
        BillingUnit::Images,
        count as u64,
    )
}

pub fn openrouter(usage: Option<&openrouter::wire::ImagesUsage>, images: usize) -> MeteredUsage {
    let mut measures = vec![images_returned(images)];
    let mut cost_usd = None;
    if let Some(usage) = usage {
        if let Some(prompt) = usage.prompt_tokens {
            measures.push(measure(Charge::Input, None, BillingUnit::Tokens, prompt));
        }
        if let Some(completion) = usage.completion_tokens {
            measures.push(measure(
                Charge::Output,
                Some(Modality::Image),
                BillingUnit::Tokens,
                completion,
            ));
        }
        cost_usd = usage.cost;
    }
    MeteredUsage { measures, cost_usd }
}

pub fn openai(usage: Option<&openai::wire::ImagesUsage>, images: usize) -> MeteredUsage {
    let mut measures = vec![images_returned(images)];
    if let Some(usage) = usage {
        match &usage.input_tokens_details {
            Some(details) => {
                if let Some(text) = details.text_tokens {
                    measures.push(measure(
                        Charge::Input,
                        Some(Modality::Text),
                        BillingUnit::Tokens,
                        text,
                    ));
                }
                if let Some(image) = details.image_tokens {
                    measures.push(measure(
                        Charge::Input,
                        Some(Modality::Image),
                        BillingUnit::Tokens,
                        image,
                    ));
                }
            }
            None => {
                if let Some(input) = usage.input_tokens {
                    measures.push(measure(Charge::Input, None, BillingUnit::Tokens, input));
                }
            }
        }
        if let Some(output) = usage.output_tokens {
            measures.push(measure(
                Charge::Output,
                Some(Modality::Image),
                BillingUnit::Tokens,
                output,
            ));
        }
    }
    // The Images API reports no cost.
    MeteredUsage {
        measures,
        cost_usd: None,
    }
}

/// A Codex image call: the images returned, and every worker turn's tokens
/// summed meter by meter. Those are the worker model's text tokens, not the
/// image model's, which Codex does not report; a subscription has no price.
pub fn codex(turns: &[TurnUsage], images: usize) -> MeteredUsage {
    let mut measures = vec![images_returned(images)];
    for turn in turns {
        for measure in crate::usage::metered(turn).measures {
            match measures
                .iter_mut()
                .find(|summed| summed.meter == measure.meter)
            {
                Some(summed) => summed.quantity += measure.quantity,
                None => measures.push(measure),
            }
        }
    }
    MeteredUsage {
        measures,
        cost_usd: None,
    }
}
