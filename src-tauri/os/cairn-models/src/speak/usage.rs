//! What one utterance consumed, counted on the wire and summed across
//! segments.
//!
//! | Source | Meter |
//! |---|---|
//! | characters the facet sent (every wire) | Input, Characters |
//! | PCM samples received ÷ sample rate (PCM only) | Output, Audio, Seconds |
//! | OpenAI SSE `usage.input_tokens` | Input, Tokens |
//! | OpenAI SSE `usage.output_tokens` | Output, Audio, Tokens |
//!
//! Characters and PCM samples are counted exactly by the facet, which is what
//! lets ElevenLabs (whose socket reports no usage but bills input characters)
//! be accounted at all. Seconds are whole units on the meter, so the exact
//! sample count is rounded up, the way per-second billing rounds.

use cairn_models_types::catalog::Modality;
use cairn_models_types::usage::{BillingUnit, Charge, Measure, Meter, MeteredUsage};

use crate::interface::speak::{AudioEncoding, AudioFormat, ClipUsage};

#[derive(Debug, Default)]
pub(super) struct Tally {
    characters: u64,
    audio_bytes: u64,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

impl Tally {
    /// `text` went to the provider.
    pub(super) fn sent(&mut self, text: &str) {
        self.characters += text.chars().count() as u64;
    }

    /// `bytes` of audio came back.
    pub(super) fn received(&mut self, bytes: usize) {
        self.audio_bytes += bytes as u64;
    }

    /// A clip's provider reported `usage`.
    pub(super) fn reported(&mut self, usage: Option<ClipUsage>) {
        if let Some(usage) = usage {
            *self.input_tokens.get_or_insert(0) += usage.input_tokens;
            *self.output_tokens.get_or_insert(0) += usage.output_tokens;
        }
    }

    pub(super) fn metered(&self, format: &AudioFormat) -> MeteredUsage {
        let audio = |charge, unit| Meter {
            charge,
            modality: Some(Modality::Audio),
            unit,
        };
        let mut measures = vec![Measure {
            meter: Meter {
                charge: Charge::Input,
                modality: None,
                unit: BillingUnit::Characters,
            },
            quantity: self.characters,
        }];
        if let (AudioEncoding::PcmS16le, Some(rate)) = (format.encoding, format.sample_rate) {
            let samples = self.audio_bytes / 2;
            measures.push(Measure {
                meter: audio(Charge::Output, BillingUnit::Seconds),
                quantity: samples.div_ceil(u64::from(rate.max(1))),
            });
        }
        if let Some(tokens) = self.input_tokens {
            measures.push(Measure {
                meter: Meter::tokens(Charge::Input),
                quantity: tokens,
            });
        }
        if let Some(tokens) = self.output_tokens {
            measures.push(Measure {
                meter: audio(Charge::Output, BillingUnit::Tokens),
                quantity: tokens,
            });
        }
        MeteredUsage {
            measures,
            cost_usd: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn characters_always_and_seconds_only_for_pcm() {
        let mut tally = Tally::default();
        tally.sent("héllo ");
        tally.received(48_000);
        let pcm = tally.metered(&AudioFormat::pcm(24_000));
        assert_eq!(
            pcm.quantity(Charge::Input, BillingUnit::Characters),
            Some(6)
        );
        assert_eq!(pcm.quantity(Charge::Output, BillingUnit::Seconds), Some(1));
        assert_eq!(pcm.quantity(Charge::Input, BillingUnit::Tokens), None);
        let mp3 = tally.metered(&AudioFormat::any(AudioEncoding::Mp3));
        assert_eq!(mp3.quantity(Charge::Output, BillingUnit::Seconds), None);
        // A partial second is a whole one on the meter.
        tally.received(2);
        let pcm = tally.metered(&AudioFormat::pcm(24_000));
        assert_eq!(pcm.quantity(Charge::Output, BillingUnit::Seconds), Some(2));
    }

    #[test]
    fn reported_tokens_sum_and_land_on_audio_output() {
        let mut tally = Tally::default();
        tally.reported(Some(ClipUsage {
            input_tokens: 10,
            output_tokens: 100,
        }));
        tally.reported(None);
        tally.reported(Some(ClipUsage {
            input_tokens: 5,
            output_tokens: 50,
        }));
        let usage = tally.metered(&AudioFormat::any(AudioEncoding::Opus));
        assert_eq!(usage.quantity(Charge::Input, BillingUnit::Tokens), Some(15));
        let output_audio = usage
            .measures
            .iter()
            .find(|m| m.meter.charge == Charge::Output && m.meter.unit == BillingUnit::Tokens)
            .unwrap();
        assert_eq!(output_audio.meter.modality, Some(Modality::Audio));
        assert_eq!(output_audio.quantity, 150);
    }
}
