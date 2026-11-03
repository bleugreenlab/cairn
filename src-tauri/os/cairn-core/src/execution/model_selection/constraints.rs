//! Constraints between recipe nodes, with the referenced nodes' stored
//! selections in hand. The authored shape,
//! [`cairn_config::allotment::NodeConstraints`], lives with the allotment
//! grammar so a recipe node can carry it.

use super::SelectionRecord;

/// A constraint with the referenced node's stored selection in hand.
#[derive(Debug, Clone, Default)]
pub struct ResolvedConstraints {
    pub same_band_as: Option<(String, SelectionRecord)>,
    pub different_maker_from: Option<(String, SelectionRecord)>,
    /// The provider of the session this launch forks. A forked session can
    /// only continue where it lives, so no other provider's model is a
    /// candidate.
    pub fork_provider: Option<String>,
}
