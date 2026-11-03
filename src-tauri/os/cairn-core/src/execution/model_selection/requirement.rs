//! The requirement: what a launch needs from its allotment, as a capability
//! and an effort on 0..1.
//!
//! This is the seam the 0-1 task rubric plugs into. Stage 1 ships one
//! resolver, [`TopOfAllotment`]; a rubric or an escalation adds another
//! [`RequirementResolver`] and a [`RequirementSource`] variant, and nothing
//! downstream (bands, constraints, supply, effort) changes shape.

use cairn_config::allotment::ExpandedAllotment;
use serde::{Deserialize, Serialize};

/// How capable a model must be.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
pub enum Capability {
    /// The allotment's highest band.
    Top,
    /// The lowest allotment band whose upper bound reaches `x`, else its top.
    At(f64),
}

/// What decided a requirement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum RequirementSource {
    /// No judgment was made: the launch takes the top of what it may use.
    AllotmentTop,
    /// No selection was made: the job runs on the model it was pinned to.
    Pinned,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Requirement {
    pub capability: Capability,
    /// The position on the chosen model's effort scale, when the requirement
    /// sets one; otherwise the model's ladder entry decides.
    pub effort: Option<f64>,
    pub source: RequirementSource,
    /// One readable clause: why this capability.
    pub note: String,
}

/// Turns a launch into a requirement.
pub trait RequirementResolver {
    fn requirement(&self, allotment: &ExpandedAllotment) -> Requirement;
}

/// Stage 1's resolver: every launch takes the top of its allotment.
pub struct TopOfAllotment;

impl RequirementResolver for TopOfAllotment {
    fn requirement(&self, allotment: &ExpandedAllotment) -> Requirement {
        let names: Vec<String> = allotment.entries.iter().map(ToString::to_string).collect();
        let top = allotment
            .top()
            .map(|band| band.id.clone())
            .unwrap_or_else(|| "top".to_string());
        Requirement {
            capability: Capability::Top,
            effort: None,
            source: RequirementSource::AllotmentTop,
            note: format!("{top} = top of [{}]", names.join(", ")),
        }
    }
}
