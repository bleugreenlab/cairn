//! What a speech offer can do: the capability payload of [`Verb::Speak`].
//!
//! These are facts about an offer, declared beside the wire that carries it,
//! so a request can be admitted or refused before any connection opens. The
//! facet's `speak` verb negotiates against them and never quietly degrades a
//! request to something the offer does not list.
//!
//! [`Verb::Speak`]: crate::catalog::Verb::Speak

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// How audio bytes are encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AudioEncoding {
    /// Raw signed 16-bit little-endian mono PCM. Chunks concatenate cleanly.
    PcmS16le,
    /// MP3 frames. Chunks concatenate cleanly.
    Mp3,
    /// Ogg Opus. A stream that is assembled from several requests is chained
    /// Ogg: each request is its own complete Ogg stream.
    Opus,
}

impl AudioEncoding {
    /// The codec's short name, as the format string spells it (`pcm`, `mp3`,
    /// `opus`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PcmS16le => "pcm",
            Self::Mp3 => "mp3",
            Self::Opus => "opus",
        }
    }
}

/// One audio format: its encoding, and the sample rate and bitrate where they
/// are fixed.
///
/// PCM always carries its sample rate, since the bytes cannot be played
/// without it. Elsewhere `None` means the provider chooses, and what it chose
/// is reported as served. In a *request*, `None` means any: the first format
/// the offer lists that agrees on every field the request does set is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioFormat {
    pub encoding: AudioEncoding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bitrate_kbps: Option<u32>,
}

impl AudioFormat {
    /// 16-bit PCM at `sample_rate` Hz.
    pub const fn pcm(sample_rate: u32) -> Self {
        Self {
            encoding: AudioEncoding::PcmS16le,
            sample_rate: Some(sample_rate),
            bitrate_kbps: None,
        }
    }

    /// `encoding` with the provider choosing its rate and bitrate.
    pub const fn any(encoding: AudioEncoding) -> Self {
        Self {
            encoding,
            sample_rate: None,
            bitrate_kbps: None,
        }
    }

    /// `encoding` at a fixed sample rate and bitrate.
    pub const fn fixed(encoding: AudioEncoding, sample_rate: u32, bitrate_kbps: u32) -> Self {
        Self {
            encoding,
            sample_rate: Some(sample_rate),
            bitrate_kbps: Some(bitrate_kbps),
        }
    }

    /// Whether this offered format serves `requested`: the same encoding, and
    /// the same value on every field `requested` sets.
    pub fn serves(&self, requested: &AudioFormat) -> bool {
        let agrees = |offered: Option<u32>, asked: Option<u32>| {
            asked.is_none_or(|asked| offered == Some(asked))
        };
        self.encoding == requested.encoding
            && agrees(self.sample_rate, requested.sample_rate)
            && agrees(self.bitrate_kbps, requested.bitrate_kbps)
    }
}

impl fmt::Display for AudioFormat {
    /// `codec[_rate[_kbps]]`: `pcm_24000`, `mp3_44100_128`, `opus`. This is
    /// the spelling ElevenLabs uses on its wire, and it reads plainly for any
    /// provider.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.encoding.as_str())?;
        if let Some(rate) = self.sample_rate {
            write!(f, "_{rate}")?;
            if let Some(kbps) = self.bitrate_kbps {
                write!(f, "_{kbps}")?;
            }
        }
        Ok(())
    }
}

impl FromStr for AudioFormat {
    type Err = String;

    /// The inverse of `Display`. A bitrate needs a sample rate before it,
    /// since the format string is positional.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.trim().split('_');
        let encoding = match parts.next().unwrap_or_default() {
            "pcm" => AudioEncoding::PcmS16le,
            "mp3" => AudioEncoding::Mp3,
            "opus" => AudioEncoding::Opus,
            other => {
                return Err(format!(
                    "unknown audio encoding '{other}' in '{value}'; expected pcm, mp3 or opus"
                ))
            }
        };
        let mut number = |what: &str| -> Result<Option<u32>, String> {
            parts
                .next()
                .map(|part| {
                    part.parse::<u32>()
                        .map_err(|_| format!("'{part}' in '{value}' is not a {what}"))
                })
                .transpose()
        };
        let sample_rate = number("sample rate")?;
        let bitrate_kbps = number("bitrate")?;
        if parts.next().is_some() {
            return Err(format!(
                "'{value}' has too many parts; expected codec[_rate[_kbps]]"
            ));
        }
        Ok(Self {
            encoding,
            sample_rate,
            bitrate_kbps,
        })
    }
}

/// How text reaches the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InputStreaming {
    /// The provider takes text as it arrives over one connection and decides
    /// for itself when it has enough to speak.
    Native,
    /// The provider needs whole input, so the facet cuts the text stream at
    /// sentence and clause boundaries and pipelines one request per segment.
    Segmented,
}

impl InputStreaming {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Segmented => "segmented",
        }
    }
}

/// Delivery control that maps across providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StyleControl {
    /// Nothing beyond voice and speed. Provider-specific expressive knobs are
    /// extras, not style.
    None,
    /// Free-text delivery instructions ("warm, unhurried, a little wry").
    Instructions,
}

/// The speeds an offer accepts, as percentages of normal. Integers, so a
/// capability compares exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeedRange {
    pub min_percent: u16,
    pub max_percent: u16,
}

impl SpeedRange {
    /// Whether `speed` (1.0 = normal) is inside the range.
    pub fn admits(&self, speed: f32) -> bool {
        let percent = speed * 100.0;
        speed.is_finite()
            && percent >= f32::from(self.min_percent) - 1e-3
            && percent <= f32::from(self.max_percent) + 1e-3
    }
}

/// What one speech offer can do.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeechCapabilities {
    pub input: InputStreaming,
    /// The formats it serves; the first is the default.
    pub formats: Vec<AudioFormat>,
    /// The speeds it accepts, or `None` when speed is not settable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<SpeedRange>,
    pub style: StyleControl,
}

impl fmt::Display for SpeechCapabilities {
    /// `segmented · pcm_24000|mp3|opus · speed 25–400% · instructions`, the
    /// compact line a catalog shows.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formats: Vec<String> = self.formats.iter().map(ToString::to_string).collect();
        write!(f, "{} · {}", self.input.as_str(), formats.join("|"))?;
        if let Some(speed) = self.speed {
            write!(f, " · speed {}–{}%", speed.min_percent, speed.max_percent)?;
        }
        if self.style == StyleControl::Instructions {
            f.write_str(" · instructions")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_format_string_round_trips() {
        for text in ["pcm_24000", "mp3_44100_128", "opus_48000_64", "mp3", "opus"] {
            let format: AudioFormat = text.parse().unwrap();
            assert_eq!(format.to_string(), text);
        }
        assert_eq!(
            "pcm_16000".parse::<AudioFormat>().unwrap(),
            AudioFormat::pcm(16_000)
        );
        assert!("wav_44100"
            .parse::<AudioFormat>()
            .unwrap_err()
            .contains("unknown audio encoding"));
        assert!("mp3_x"
            .parse::<AudioFormat>()
            .unwrap_err()
            .contains("sample rate"));
        assert!("mp3_1_2_3"
            .parse::<AudioFormat>()
            .unwrap_err()
            .contains("too many"));
    }

    #[test]
    fn an_unset_request_field_is_served_by_any_value() {
        let offered = AudioFormat::fixed(AudioEncoding::Mp3, 44_100, 128);
        assert!(offered.serves(&AudioFormat::any(AudioEncoding::Mp3)));
        assert!(offered.serves(&"mp3_44100".parse().unwrap()));
        assert!(!offered.serves(&"mp3_44100_192".parse().unwrap()));
        assert!(!offered.serves(&AudioFormat::any(AudioEncoding::Opus)));
        // A provider-chosen rate does not promise any particular one.
        assert!(!AudioFormat::any(AudioEncoding::Mp3).serves(&"mp3_44100".parse().unwrap()));
    }

    #[test]
    fn speed_is_admitted_inclusively_by_percent() {
        let range = SpeedRange {
            min_percent: 70,
            max_percent: 120,
        };
        assert!(range.admits(0.7) && range.admits(1.2) && range.admits(1.0));
        assert!(!range.admits(0.69) && !range.admits(1.21) && !range.admits(f32::NAN));
    }

    #[test]
    fn capabilities_render_as_one_compact_line() {
        let capabilities = SpeechCapabilities {
            input: InputStreaming::Segmented,
            formats: vec![
                AudioFormat::pcm(24_000),
                AudioFormat::any(AudioEncoding::Mp3),
                AudioFormat::any(AudioEncoding::Opus),
            ],
            speed: Some(SpeedRange {
                min_percent: 25,
                max_percent: 400,
            }),
            style: StyleControl::Instructions,
        };
        assert_eq!(
            capabilities.to_string(),
            "segmented · pcm_24000|mp3|opus · speed 25–400% · instructions"
        );
        let json = serde_json::to_value(&capabilities).unwrap();
        assert_eq!(
            json["formats"][0],
            serde_json::json!({"encoding": "pcm-s16le", "sampleRate": 24000})
        );
        assert_eq!(
            serde_json::from_value::<SpeechCapabilities>(json).unwrap(),
            capabilities
        );
    }
}
