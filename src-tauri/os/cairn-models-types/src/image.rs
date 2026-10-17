//! What an image offer can do: the capability type the `image` verb carries.
//!
//! A request feature (an exact size, labelled references, a mask) is either
//! honoured natively, honoured by a declared [`Lowering`] onto something the
//! offer does take, or refused. Nothing is lowered unless the offer declares
//! it, and every lowering applied to a call is recorded with its result, so
//! a caller can always see how its request was carried out.
//!
//! Everything here is `Eq` (ratios are integer pairs, never floats), because
//! a capability rides on [`crate::catalog::Verb`], which is.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// An aspect ratio, `w:h`, as integers. Serialized as `"2:3"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Ratio {
    pub w: u16,
    pub h: u16,
}

impl Ratio {
    pub const fn new(w: u16, h: u16) -> Self {
        Self { w, h }
    }

    pub fn orientation(self) -> Orientation {
        match self.w.cmp(&self.h) {
            std::cmp::Ordering::Less => Orientation::Portrait,
            std::cmp::Ordering::Greater => Orientation::Landscape,
            std::cmp::Ordering::Equal => Orientation::Square,
        }
    }

    /// Whether `width:height` is no more elongated than this ratio, in either
    /// direction. `3:1` admits 3:1 and 1:3 but not 4:1.
    pub fn bounds(self, width: u32, height: u32) -> bool {
        let (long, short) = (width.max(height) as u64, width.min(height) as u64);
        let (limit_long, limit_short) = (self.w.max(self.h) as u64, self.w.min(self.h) as u64);
        long * limit_short <= limit_long * short
    }
}

impl fmt::Display for Ratio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.w, self.h)
    }
}

impl FromStr for Ratio {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (w, h) = value
            .split_once(':')
            .ok_or_else(|| format!("'{value}' is not a ratio like 2:3"))?;
        let side = |text: &str| -> Result<u16, String> {
            text.trim()
                .parse::<u16>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("'{value}' is not a ratio like 2:3"))
        };
        Ok(Self::new(side(w)?, side(h)?))
    }
}

impl TryFrom<String> for Ratio {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Ratio> for String {
    fn from(value: Ratio) -> Self {
        value.to_string()
    }
}

/// An exact size in pixels. Serialized as `"1024x1536"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Pixels {
    pub width: u32,
    pub height: u32,
}

impl Pixels {
    pub const fn new(width: u32, height: u32) -> Self {
        Self { width, height }
    }

    pub fn orientation(self) -> Orientation {
        match self.width.cmp(&self.height) {
            std::cmp::Ordering::Less => Orientation::Portrait,
            std::cmp::Ordering::Greater => Orientation::Landscape,
            std::cmp::Ordering::Equal => Orientation::Square,
        }
    }
}

impl fmt::Display for Pixels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}x{}", self.width, self.height)
    }
}

impl FromStr for Pixels {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (width, height) = value
            .split_once(['x', 'X'])
            .ok_or_else(|| format!("'{value}' is not a size like 1024x1536"))?;
        let side = |text: &str| -> Result<u32, String> {
            text.trim()
                .parse::<u32>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("'{value}' is not a size like 1024x1536"))
        };
        Ok(Self::new(side(width)?, side(height)?))
    }
}

impl TryFrom<String> for Pixels {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Pixels> for String {
    fn from(value: Pixels) -> Self {
        value.to_string()
    }
}

/// Which way an image is long, without saying how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Orientation {
    Portrait,
    Landscape,
    Square,
}

impl Orientation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Portrait => "portrait",
            Self::Landscape => "landscape",
            Self::Square => "square",
        }
    }
}

impl fmt::Display for Orientation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How an offer honours one request feature.
///
/// Every enum here is tagged by a field (`support`, `kind`, `rule`), never
/// externally: these values persist in the YAML credential store inside an
/// internally tagged account, and YAML writes an externally tagged variant as
/// a `!tag` that such a container cannot read back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "support", rename_all = "camelCase")]
pub enum Support {
    Native,
    /// Honoured by rewriting it onto something the offer does take. Only a
    /// lowering declared here is ever applied.
    Lowered(Lowering),
    Unsupported,
}

impl Support {
    pub fn is_supported(&self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

/// A declared rewrite of a request feature onto what an offer takes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Lowering {
    /// Reference labels composed into the prompt as `Image N: {label}` lines.
    LabelsInPrompt,
    /// The orientation said in one sentence of the prompt.
    OrientationInPrompt,
    /// The orientation sent as the offer's native aspect ratio.
    OrientationAsAspect {
        portrait: Ratio,
        landscape: Ratio,
        square: Ratio,
    },
    /// The orientation sent as an exact size.
    OrientationAsPixels {
        portrait: Pixels,
        landscape: Pixels,
        square: Pixels,
    },
    /// The edit base sent as reference 1, with a sentence asking that
    /// everything the prompt does not change is kept.
    EditAsReference,
}

/// Which exact sizes an offer takes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "camelCase")]
pub enum PixelRule {
    /// Only these sizes.
    Sizes { sizes: Vec<Pixels> },
    /// Any size whose edges are multiples of `multiple_of` within
    /// `[min_edge, max_edge]`, no more elongated than `max_ratio`.
    #[serde(rename_all = "camelCase")]
    Free {
        multiple_of: u32,
        min_edge: u32,
        max_edge: u32,
        max_ratio: Ratio,
    },
}

impl PixelRule {
    /// Why `size` is refused, or `None` when it is admitted.
    pub fn refuses(&self, size: Pixels) -> Option<String> {
        match self {
            Self::Sizes { sizes } => (!sizes.contains(&size)).then(|| {
                format!(
                    "{size} is not one of its sizes ({})",
                    sizes
                        .iter()
                        .map(Pixels::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }),
            Self::Free {
                multiple_of,
                min_edge,
                max_edge,
                max_ratio,
            } => {
                let edges = [size.width, size.height];
                if *multiple_of > 1 && edges.iter().any(|edge| edge % multiple_of != 0) {
                    return Some(format!(
                        "{size} has an edge that is not a multiple of {multiple_of}"
                    ));
                }
                if edges.iter().any(|edge| edge < min_edge || edge > max_edge) {
                    return Some(format!(
                        "{size} has an edge outside {min_edge} to {max_edge} pixels"
                    ));
                }
                (!max_ratio.bounds(size.width, size.height))
                    .then(|| format!("{size} is more elongated than {max_ratio}"))
            }
        }
    }
}

impl fmt::Display for PixelRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sizes { sizes } => f.write_str(
                &sizes
                    .iter()
                    .map(Pixels::to_string)
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            Self::Free {
                multiple_of,
                max_edge,
                max_ratio,
                ..
            } => write!(f, "any ×{multiple_of} ≤{max_edge} ≤{max_ratio}"),
        }
    }
}

/// The ways an offer lets a caller shape its output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShapeSupport {
    /// Whether the provider may be left to pick.
    pub auto: bool,
    /// Exact sizes. `None` means an exact request is refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pixels: Option<PixelRule>,
    /// The native aspect parameter's values. Empty means it has none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aspect: Vec<Ratio>,
    /// How an orientation is honoured. There is no native orientation
    /// parameter anywhere, so this is a lowering or unsupported.
    pub orientation: Support,
}

/// How many reference images an offer takes, and whether they can be named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceSupport {
    /// The most input images one call takes, an edit's base included.
    pub max: u32,
    pub labels: Support,
}

/// Whether an offer edits an image, and whether a mask can say where.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EditSupport {
    pub base: Support,
    pub mask: Support,
}

/// What an image offer can do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageCapabilities {
    /// The most images one call returns.
    pub max_count: u32,
    pub shape: ShapeSupport,
    /// `None` means it takes no reference images.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<ReferenceSupport>,
    /// `None` means it does not edit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edit: Option<EditSupport>,
}

/// The three sizes every GPT Image model takes, and the orientation each one
/// is.
const GPT_IMAGE_PORTRAIT: Pixels = Pixels::new(1024, 1536);
const GPT_IMAGE_LANDSCAPE: Pixels = Pixels::new(1536, 1024);
const GPT_IMAGE_SQUARE: Pixels = Pixels::new(1024, 1024);

impl ImageCapabilities {
    /// What any server answering the OpenAI Images contract is assumed to
    /// do when its operator declared nothing: one image per call, sized
    /// automatically or as one of GPT Image's three standard sizes (the same
    /// three an orientation is sent as), and no references or edits.
    /// Compatible servers vary in what else they accept and may clamp a size
    /// they do not take, so nothing wider is assumed; a host that knows its
    /// server declares more. The default for a custom `openai-images`
    /// provider.
    pub fn openai_images_minimal() -> Self {
        Self {
            max_count: 1,
            shape: ShapeSupport {
                auto: true,
                pixels: Some(PixelRule::Sizes {
                    sizes: vec![GPT_IMAGE_SQUARE, GPT_IMAGE_PORTRAIT, GPT_IMAGE_LANDSCAPE],
                }),
                aspect: Vec::new(),
                orientation: Support::Lowered(Self::gpt_image_orientation()),
            },
            references: None,
            edit: None,
        }
    }

    /// OpenAI's GPT Image models over its own Images API, as its reference
    /// documents them (developers.openai.com, Images › Create image and
    /// Create image edit): up to 10 images per call; arbitrary `WIDTHxHEIGHT`
    /// sizes with both edges divisible by 16, a ratio between 1:3 and 3:1, and
    /// at most 3840 on an edge; up to 16 input images on an edit; and a mask,
    /// applied to the first image. Reference labels have no field, so they
    /// ride in the prompt.
    pub fn openai_gpt_image() -> Self {
        Self {
            max_count: 10,
            shape: ShapeSupport {
                auto: true,
                pixels: Some(PixelRule::Free {
                    multiple_of: 16,
                    min_edge: 16,
                    max_edge: 3840,
                    max_ratio: Ratio::new(3, 1),
                }),
                aspect: Vec::new(),
                orientation: Support::Lowered(Self::gpt_image_orientation()),
            },
            references: Some(ReferenceSupport {
                max: 16,
                labels: Support::Lowered(Lowering::LabelsInPrompt),
            }),
            edit: Some(EditSupport {
                base: Support::Native,
                mask: Support::Native,
            }),
        }
    }

    fn gpt_image_orientation() -> Lowering {
        Lowering::OrientationAsPixels {
            portrait: GPT_IMAGE_PORTRAIT,
            landscape: GPT_IMAGE_LANDSCAPE,
            square: GPT_IMAGE_SQUARE,
        }
    }

    /// A compact description: `≤4 · refs ≤16 · aspect 1:1|2:3 · no mask`.
    pub fn summary(&self) -> String {
        let mut parts = vec![format!("≤{}", self.max_count)];
        if let Some(references) = &self.references {
            parts.push(format!("refs ≤{}", references.max));
        }
        if !self.shape.aspect.is_empty() {
            parts.push(format!(
                "aspect {}",
                self.shape
                    .aspect
                    .iter()
                    .map(Ratio::to_string)
                    .collect::<Vec<_>>()
                    .join("|")
            ));
        }
        if let Some(pixels) = &self.shape.pixels {
            parts.push(format!("size {pixels}"));
        }
        match &self.edit {
            None => parts.push("no edit".to_string()),
            Some(edit) if edit.mask.is_supported() => parts.push("mask".to_string()),
            Some(_) => parts.push("no mask".to_string()),
        }
        parts.join(" · ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratios_and_sizes_are_their_plain_strings() {
        assert_eq!(serde_json::to_value(Ratio::new(2, 3)).unwrap(), "2:3");
        assert_eq!(
            serde_json::from_value::<Ratio>("16:9".into()).unwrap(),
            Ratio::new(16, 9)
        );
        assert!(serde_json::from_value::<Ratio>("auto".into()).is_err());
        assert!(serde_json::from_value::<Ratio>("0:1".into()).is_err());
        assert_eq!(
            serde_json::to_value(Pixels::new(1024, 1536)).unwrap(),
            "1024x1536"
        );
        assert_eq!(
            "2048X2048".parse::<Pixels>().unwrap(),
            Pixels::new(2048, 2048)
        );
        assert!("wide".parse::<Pixels>().is_err());
    }

    #[test]
    fn a_ratio_bounds_elongation_either_way() {
        let three = Ratio::new(3, 1);
        assert!(three.bounds(3072, 1024));
        assert!(three.bounds(1024, 3072));
        assert!(!three.bounds(4096, 1024));
        assert_eq!(Ratio::new(2, 3).orientation(), Orientation::Portrait);
        assert_eq!(Pixels::new(10, 10).orientation(), Orientation::Square);
    }

    #[test]
    fn a_free_pixel_rule_names_what_it_refuses() {
        let rule = ImageCapabilities::openai_gpt_image().shape.pixels.unwrap();
        assert_eq!(rule.refuses(Pixels::new(1536, 864)), None);
        assert!(rule
            .refuses(Pixels::new(1000, 1000))
            .unwrap()
            .contains("multiple of 16"));
        assert!(rule
            .refuses(Pixels::new(4096, 2304))
            .unwrap()
            .contains("outside"));
        assert!(rule
            .refuses(Pixels::new(3584, 1024))
            .unwrap()
            .contains("elongated"));
        let fixed = PixelRule::Sizes {
            sizes: vec![Pixels::new(512, 512)],
        };
        assert_eq!(fixed.refuses(Pixels::new(512, 512)), None);
        assert!(fixed.refuses(Pixels::new(256, 256)).is_some());
    }

    #[test]
    fn capabilities_round_trip_with_their_declared_lowerings() {
        for capabilities in [
            ImageCapabilities::openai_images_minimal(),
            ImageCapabilities::openai_gpt_image(),
        ] {
            let json = serde_json::to_value(&capabilities).unwrap();
            assert_eq!(
                serde_json::from_value::<ImageCapabilities>(json).unwrap(),
                capabilities
            );
        }
        let json = serde_json::to_value(ImageCapabilities::openai_gpt_image()).unwrap();
        assert_eq!(
            json["shape"]["orientation"],
            serde_json::json!({
                "support": "lowered",
                "kind": "orientationAsPixels",
                "portrait": "1024x1536",
                "landscape": "1536x1024",
                "square": "1024x1024"
            })
        );
        assert_eq!(
            json["references"]["labels"],
            serde_json::json!({"support": "lowered", "kind": "labelsInPrompt"})
        );
        assert_eq!(
            json["edit"]["mask"],
            serde_json::json!({"support": "native"})
        );
        assert_eq!(
            json["shape"]["pixels"],
            serde_json::json!({"rule": "free", "multipleOf": 16, "minEdge": 16, "maxEdge": 3840, "maxRatio": "3:1"})
        );
        let sizes = PixelRule::Sizes {
            sizes: vec![Pixels::new(512, 512)],
        };
        let json = serde_json::to_value(&sizes).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"rule": "sizes", "sizes": ["512x512"]})
        );
        assert_eq!(serde_json::from_value::<PixelRule>(json).unwrap(), sizes);
    }

    #[test]
    fn the_summary_is_one_compact_line() {
        assert_eq!(
            ImageCapabilities::openai_gpt_image().summary(),
            "≤10 · refs ≤16 · size any ×16 ≤3840 ≤3:1 · mask"
        );
        assert_eq!(
            ImageCapabilities::openai_images_minimal().summary(),
            "≤1 · size 1024x1024|1024x1536|1536x1024 · no edit"
        );
    }

    #[test]
    fn the_minimal_default_takes_only_the_three_standard_sizes() {
        let rule = ImageCapabilities::openai_images_minimal()
            .shape
            .pixels
            .unwrap();
        for size in [
            Pixels::new(1024, 1024),
            Pixels::new(1024, 1536),
            Pixels::new(1536, 1024),
        ] {
            assert_eq!(rule.refuses(size), None, "{size}");
        }
        // A multiple of 16 a compatible server may well clamp.
        assert!(rule.refuses(Pixels::new(2048, 2048)).is_some());
    }
}
