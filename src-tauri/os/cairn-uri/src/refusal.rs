//! Why the parser refused an address.
//!
//! [`crate::parse_uri`] answers yes or no, and a no hides two different facts.
//! An address can be garbled (a bad scheme, an empty segment, a broken percent
//! escape, an id where a number belongs), which is the caller's mistake. Or it
//! can be well formed but name a family or sub-resource this build does not
//! have, which is usually version skew: a newer CLI, SDK, launcher or agent
//! talking to an older runner. [`classify_uri`] tells the two apart, so every
//! caller that refuses an address maps from one classification rather than
//! printing the same words for both.
//!
//! What this build "knows" is not a second list of families. It is derived from
//! [`crate::samples::every_resource_variant`], which is exhaustive over
//! `CairnResource` by construction. Each sample's canonical URI becomes a shape,
//! and each segment of the shape is marked literal or variable by asking the
//! parser itself whether a substitute value still parses there. Adding a family
//! to the grammar therefore adds it to the refusals with no further edit.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::OnceLock;

use crate::percent::decode_component;
use crate::samples::every_resource_variant;
use crate::{parse_uri, CairnResource};

const SCHEME: &str = "cairn://";

/// Stand-ins for a segment's value when testing whether that position takes
/// any value: one shaped like a name or opaque id, one like a positive number.
/// Neither is a literal anywhere in the grammar.
const PROBES: [&str; 2] = ["zz9probe", "7"];

/// Why an address was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UriRefusal {
    /// The address itself is wrong, whatever build reads it.
    Malformed(MalformedUri),
    /// The address is well formed, but this build has nothing by that name.
    Unknown(UnknownSegment),
}

/// What is wrong with a malformed address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MalformedUri {
    /// The address does not start with `cairn://`.
    Scheme,
    /// Nothing follows `cairn://`.
    Empty,
    /// The segment at this 1-based position is empty: a doubled or trailing `/`.
    EmptySegment { position: usize },
    /// A segment holds a `%` that is not an escape of valid UTF-8.
    PercentEscape { segment: String },
    /// The address has a known resource's shape, but a value does not fit its
    /// place, such as an id where a positive number belongs. `segment` names
    /// the value when exactly one position could hold it.
    InvalidValue { segment: Option<String> },
    /// The address stops before it names a resource. `next` lists the names
    /// that can follow when the next segment is a fixed word.
    Incomplete { next: Vec<String> },
}

/// A well-formed address naming something this build does not have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownSegment {
    /// The address up to the unknown segment: `cairn://` for a family, or the
    /// parent resource (`cairn://dev`) for a sub-resource.
    pub parent: String,
    /// The segment this build does not know.
    pub segment: String,
    /// The names this build knows at that level, sorted.
    pub known: Vec<String>,
    /// The known name closest to `segment`, when it is close enough to be a
    /// likely typo.
    pub nearest: Option<String>,
}

impl UnknownSegment {
    /// Whether the unknown segment is a top-level family (`cairn://dev`) rather
    /// than a sub-resource of a known one (`cairn://dev/launch-ticket`).
    pub fn is_family(&self) -> bool {
        self.parent == SCHEME
    }

    /// The address of `name` at the unknown segment's level.
    pub fn address_of(&self, name: &str) -> String {
        if self.is_family() {
            format!("{SCHEME}{name}")
        } else {
            format!("{}/{name}", self.parent)
        }
    }

    /// The address this build does not have, up to the unknown segment.
    pub fn address(&self) -> String {
        self.address_of(&self.segment)
    }
}

impl fmt::Display for MalformedUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scheme => write!(f, "it does not start with `{SCHEME}`"),
            Self::Empty => write!(f, "nothing follows `{SCHEME}`"),
            Self::EmptySegment { position } => {
                write!(f, "segment {position} is empty (a doubled or trailing `/`)")
            }
            Self::PercentEscape { segment } => write!(
                f,
                "`{segment}` has a `%` that is not two hex digits escaping valid UTF-8"
            ),
            Self::InvalidValue {
                segment: Some(segment),
            } => write!(
                f,
                "it has the shape of a known resource, but `{segment}` is not a value that \
                 place accepts (for example, an id that must be a positive number)"
            ),
            Self::InvalidValue { segment: None } => write!(
                f,
                "it has the shape of a known resource, but one of its values does not fit its \
                 place (for example, an id that must be a positive number)"
            ),
            Self::Incomplete { next } if next.is_empty() => {
                write!(f, "it stops before it names a resource")
            }
            Self::Incomplete { next } => write!(
                f,
                "it stops before it names a resource; it continues with one of: {}",
                next.join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Literal(String),
    Variable,
}

/// The set of addresses a build parses, as shapes the refusal is measured
/// against.
pub struct UriGrammar {
    shapes: Vec<Vec<Part>>,
    retired: Option<fn(&CairnResource) -> bool>,
}

impl UriGrammar {
    /// This build's grammar.
    pub fn current() -> &'static UriGrammar {
        static CURRENT: OnceLock<UriGrammar> = OnceLock::new();
        CURRENT.get_or_init(|| UriGrammar::build(None))
    }

    /// This build's grammar less every resource `retired` matches: what a
    /// build that predates those resources parses. It lets a test stand in for
    /// an older runner without building one.
    pub fn without(retired: fn(&CairnResource) -> bool) -> UriGrammar {
        UriGrammar::build(Some(retired))
    }

    fn admits(&self, resource: &CairnResource) -> bool {
        self.retired.is_none_or(|retired| !retired(resource))
    }

    fn parses(&self, uri: &str) -> Option<CairnResource> {
        parse_uri(uri).filter(|resource| self.admits(resource))
    }

    fn build(retired: Option<fn(&CairnResource) -> bool>) -> UriGrammar {
        let mut grammar = UriGrammar {
            shapes: Vec::new(),
            retired,
        };
        let mut shapes: Vec<Vec<Part>> = Vec::new();
        for sample in every_resource_variant() {
            if !grammar.admits(&sample.resource) {
                continue;
            }
            let uri = sample.resource.to_uri();
            let Some(kind) = grammar.parses(&uri).map(|resource| resource.kind()) else {
                continue;
            };
            let Some(path) = uri.strip_prefix(SCHEME) else {
                continue;
            };
            let path = path.split('?').next().unwrap_or_default();
            let segments: Vec<&str> = path.split('/').collect();
            let shape = (0..segments.len())
                .map(|index| {
                    // Variable only if a substitute still reads as the same
                    // kind: `p/x/zz9probe/diff` parses, but as a thread, so
                    // `vcs` in `p/x/vcs/diff` is a fixed word.
                    let takes_any_value = PROBES.iter().any(|probe| {
                        grammar
                            .parses(&substitute(&segments, index, probe))
                            .is_some_and(|resource| resource.kind() == kind)
                    });
                    if takes_any_value {
                        Part::Variable
                    } else {
                        Part::Literal(segments[index].to_string())
                    }
                })
                .collect::<Vec<_>>();
            if !shapes.contains(&shape) {
                shapes.push(shape);
            }
        }
        grammar.shapes = shapes;
        grammar
    }

    /// Parse `uri`, or say why it was refused.
    pub fn classify(&self, uri: &str) -> Result<CairnResource, UriRefusal> {
        self.parses(uri).ok_or_else(|| self.explain(uri))
    }

    fn explain(&self, uri: &str) -> UriRefusal {
        let Some(rest) = uri.strip_prefix(SCHEME) else {
            return UriRefusal::Malformed(MalformedUri::Scheme);
        };
        let path = rest.split('?').next().unwrap_or_default();
        if path.is_empty() {
            return UriRefusal::Malformed(MalformedUri::Empty);
        }
        let segments: Vec<&str> = path.split('/').collect();
        if let Some(index) = segments.iter().position(|segment| segment.is_empty()) {
            return UriRefusal::Malformed(MalformedUri::EmptySegment {
                position: index + 1,
            });
        }
        if let Some(segment) = segments
            .iter()
            .find(|segment| segment.contains('%') && decode_component(segment).is_err())
        {
            return UriRefusal::Malformed(MalformedUri::PercentEscape {
                segment: (*segment).to_string(),
            });
        }

        let mut alive: Vec<&Vec<Part>> = self.shapes.iter().collect();
        for (index, segment) in segments.iter().enumerate() {
            let reaching: Vec<&Vec<Part>> = alive
                .iter()
                .copied()
                .filter(|shape| shape.len() > index)
                .collect();
            // A fixed word wins over a variable at the same place, as the
            // parser's reserved segments do: `p/x/vcs/blam` is an unknown
            // `vcs` operation, not a thread named `vcs`.
            let literal: Vec<&Vec<Part>> = reaching
                .iter()
                .copied()
                .filter(|shape| matches!(&shape[index], Part::Literal(word) if word == segment))
                .collect();
            alive = if literal.is_empty() {
                reaching
                    .iter()
                    .copied()
                    .filter(|shape| shape[index] == Part::Variable)
                    .collect()
            } else {
                literal
            };
            if alive.is_empty() {
                let known = literals_at(&reaching, index);
                let nearest = nearest(segment, &known);
                return UriRefusal::Unknown(UnknownSegment {
                    parent: address(&segments[..index]),
                    segment: (*segment).to_string(),
                    known,
                    nearest,
                });
            }
        }

        let length = segments.len();
        let complete: Vec<&Vec<Part>> = alive
            .iter()
            .copied()
            .filter(|shape| shape.len() == length)
            .collect();
        if complete.is_empty() {
            return UriRefusal::Malformed(MalformedUri::Incomplete {
                next: literals_at(&alive, length),
            });
        }
        // The culprit is the value whose replacement makes the address parse.
        let culprits: BTreeSet<usize> = complete
            .iter()
            .flat_map(|shape| {
                shape
                    .iter()
                    .enumerate()
                    .filter(|(_, part)| **part == Part::Variable)
                    .map(|(index, _)| index)
            })
            .filter(|index| {
                PROBES
                    .iter()
                    .any(|probe| self.parses(&substitute(&segments, *index, probe)).is_some())
            })
            .collect();
        let segment = match culprits.iter().collect::<Vec<_>>().as_slice() {
            [only] => Some(segments[**only].to_string()),
            _ => None,
        };
        UriRefusal::Malformed(MalformedUri::InvalidValue { segment })
    }
}

/// Parse `uri` against this build's grammar, or say why it was refused.
///
/// The sibling of [`crate::parse_uri`] for every caller that reports a refusal.
pub fn classify_uri(uri: &str) -> Result<CairnResource, UriRefusal> {
    UriGrammar::current().classify(uri)
}

fn substitute(segments: &[&str], index: usize, value: &str) -> String {
    let mut probed = segments.to_vec();
    probed[index] = value;
    address(&probed)
}

fn address(segments: &[&str]) -> String {
    format!("{SCHEME}{}", segments.join("/"))
}

fn literals_at(shapes: &[&Vec<Part>], index: usize) -> Vec<String> {
    shapes
        .iter()
        .filter_map(|shape| match shape.get(index) {
            Some(Part::Literal(word)) => Some(word.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The known name a typo most likely meant: the closest by edit distance
/// (a swap of two adjacent letters counting once), within a third of the
/// segment's length and at least one edit.
fn nearest(segment: &str, known: &[String]) -> Option<String> {
    let segment = segment.to_lowercase();
    let allowed = (segment.chars().count() / 3).max(1);
    known
        .iter()
        .map(|name| (edit_distance(&segment, &name.to_lowercase()), name))
        .filter(|(distance, _)| *distance <= allowed)
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, name)| name.clone())
}

/// Optimal-string-alignment distance: Levenshtein plus adjacent transposition.
fn edit_distance(left: &str, right: &str) -> usize {
    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();
    let width = right.len() + 1;
    let mut table = vec![0usize; (left.len() + 1) * width];
    for i in 0..=left.len() {
        table[i * width] = i;
    }
    for (j, cell) in table.iter_mut().enumerate().take(width) {
        *cell = j;
    }
    for i in 1..=left.len() {
        for j in 1..=right.len() {
            let cost = usize::from(left[i - 1] != right[j - 1]);
            let mut best = (table[(i - 1) * width + j] + 1)
                .min(table[i * width + j - 1] + 1)
                .min(table[(i - 1) * width + j - 1] + cost);
            if i > 1 && j > 1 && left[i - 1] == right[j - 2] && left[i - 2] == right[j - 1] {
                best = best.min(table[(i - 2) * width + j - 2] + 1);
            }
            table[i * width + j] = best;
        }
    }
    table[left.len() * width + right.len()]
}

#[cfg(test)]
mod tests;
