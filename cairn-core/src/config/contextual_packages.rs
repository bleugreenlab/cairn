use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContextualPackageKind {
    Skill,
    Recipe,
    Agent,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextualPackageRef {
    pub kind: ContextualPackageKind,
    pub id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextualPackagesConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bundles: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    enabled: Vec<ContextualPackageRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled: Vec<ContextualPackageRef>,
    /// Which installed pack supplied each item, read from the workspace install
    /// locks. Never persisted — it is workspace state, not project config — so
    /// it is excluded from serialization and from equality.
    #[serde(skip)]
    owning_packs: BTreeMap<ContextualPackageRef, String>,
}

/// Equality is over the PERSISTED project config only. `owning_packs` is
/// workspace state loaded alongside it, so two configs that say the same thing
/// about a project are equal regardless of which packs happen to be installed.
impl PartialEq for ContextualPackagesConfig {
    fn eq(&self, other: &Self) -> bool {
        self.bundles == other.bundles
            && self.enabled == other.enabled
            && self.disabled == other.disabled
    }
}

impl Eq for ContextualPackagesConfig {}

/// Which installed pack supplied each contextual item, keyed by kind and id.
///
/// Built from the install locks rather than from frontmatter, because a pack id
/// is a fact about where an item came from, not something the item declares
/// about itself.
pub fn pack_ownership_index(
    config_dir: &std::path::Path,
) -> BTreeMap<ContextualPackageRef, String> {
    let mut index = BTreeMap::new();
    for lock in super::pack::installed_packs(config_dir) {
        for item in &lock.items {
            let kind = match item.kind {
                super::pack::PackItemKind::Agent => ContextualPackageKind::Agent,
                super::pack::PackItemKind::Recipe => ContextualPackageKind::Recipe,
                super::pack::PackItemKind::Skill => ContextualPackageKind::Skill,
                // Response templates, workflows, and MCP servers are not part of
                // the contextual-selection vocabulary.
                _ => continue,
            };
            index.insert(
                ContextualPackageRef {
                    kind,
                    id: item.id.clone(),
                },
                lock.id.clone(),
            );
        }
    }
    index
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextualPackageSelection {
    Universal,
    AllBundles,
    SelectedBundle(String),
    ExplicitlyEnabled,
    ExplicitlyDisabled,
    OutsideConsumedBundles,
}

fn normalize_bundle_name(value: &str) -> Result<String, String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.starts_with('-')
        || normalized.ends_with('-')
        || !normalized
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(format!(
            "invalid bundle name `{value}`: expected non-empty kebab-case"
        ));
    }
    Ok(normalized)
}

pub(crate) fn normalize_bundles(values: &mut Vec<String>) -> Result<(), String> {
    let normalized = values
        .iter()
        .map(|value| normalize_bundle_name(value))
        .collect::<Result<BTreeSet<_>, _>>()?;
    *values = normalized.into_iter().collect();
    Ok(())
}

pub(crate) fn load_contextual_packages(
    project_path: Option<&std::path::Path>,
) -> ContextualPackagesConfig {
    let mut config: ContextualPackagesConfig = project_path
        .map(super::project_settings::load_project_settings_read_only)
        .and_then(|settings| settings.contextual_packages)
        .unwrap_or_default();
    // Scoped to the PERSONAL workspace's install locks. A team workspace twin
    // (`<config_dir>/teams/<team>/workspace`) syncs its own packs and writes its
    // own locks, which this does not read — so an item served from a team
    // workspace is selectable by the bundle tags it declares in frontmatter, but
    // not by the id of the pack that installed it there. Resolving that properly
    // means threading the applicable config dir through every selection call
    // site, which is a wider change than making pack ids resolve at all.
    config.owning_packs = pack_ownership_index(&cairn_common::paths::cairn_home());
    config
}

impl ContextualPackagesConfig {
    pub fn normalize(&mut self) -> Result<(), String> {
        if let Some(bundles) = &mut self.bundles {
            normalize_bundles(bundles)?;
        }
        self.enabled.sort();
        self.enabled.dedup();
        self.disabled.sort();
        self.disabled.dedup();
        Ok(())
    }

    /// The bundle names an item can be selected by: what it declares in its own
    /// frontmatter, plus the id of the pack that installed it.
    ///
    /// The pack id is an ADDITIONAL way to reach an already-tagged item, never a
    /// new gate. An item declaring no bundles is universal — that is its own
    /// statement that it applies everywhere — and installing it from a pack does
    /// not change what the item says about itself. Adding the pack id there
    /// would silently re-gate every untagged item in a workspace.
    fn effective_bundles(
        &self,
        package_ref: &ContextualPackageRef,
        declared: &[String],
    ) -> Vec<String> {
        if declared.is_empty() {
            return Vec::new();
        }
        let mut bundles = declared.to_vec();
        if let Some(pack_id) = self.owning_packs.get(package_ref) {
            if !bundles.contains(pack_id) {
                bundles.push(pack_id.clone());
            }
        }
        bundles
    }

    pub(crate) fn selection(
        &self,
        kind: ContextualPackageKind,
        id: &str,
        package_bundles: &[String],
    ) -> ContextualPackageSelection {
        let package_ref = ContextualPackageRef {
            kind,
            id: id.to_string(),
        };
        if self.disabled.contains(&package_ref) {
            return ContextualPackageSelection::ExplicitlyDisabled;
        }
        if self.enabled.contains(&package_ref) {
            return ContextualPackageSelection::ExplicitlyEnabled;
        }
        let package_bundles = self.effective_bundles(&package_ref, package_bundles);
        if package_bundles.is_empty() {
            return ContextualPackageSelection::Universal;
        }
        let Some(consumed) = &self.bundles else {
            return ContextualPackageSelection::AllBundles;
        };
        if let Some(bundle) = package_bundles
            .iter()
            .find(|bundle| consumed.contains(bundle))
        {
            ContextualPackageSelection::SelectedBundle(bundle.clone())
        } else {
            ContextualPackageSelection::OutsideConsumedBundles
        }
    }

    /// Configured bundle names that match neither an installed pack id nor any
    /// bundle tag `declared_tags` carries. Surfaced on the catalog resource as a
    /// warning; a spawn is never failed over one, because a name may legitimately
    /// be waiting on a pack the user has not installed yet.
    pub fn unresolved_bundles(&self, declared_tags: &BTreeSet<String>) -> Vec<String> {
        let Some(consumed) = &self.bundles else {
            return Vec::new();
        };
        let pack_ids: BTreeSet<&String> = self.owning_packs.values().collect();
        consumed
            .iter()
            .filter(|name| !declared_tags.contains(*name) && !pack_ids.contains(name))
            .cloned()
            .collect()
    }

    /// Record which pack installed each item, so pack ids resolve as bundle
    /// names. Loaded automatically by [`load_contextual_packages`].
    pub fn with_owning_packs(
        mut self,
        owning_packs: BTreeMap<ContextualPackageRef, String>,
    ) -> Self {
        self.owning_packs = owning_packs;
        self
    }

    pub(crate) fn is_selected(
        &self,
        kind: ContextualPackageKind,
        id: &str,
        package_bundles: &[String],
    ) -> bool {
        !matches!(
            self.selection(kind, id, package_bundles),
            ContextualPackageSelection::ExplicitlyDisabled
                | ContextualPackageSelection::OutsideConsumedBundles
        )
    }

    pub fn enable(&mut self, package_ref: ContextualPackageRef) {
        self.disabled.retain(|entry| entry != &package_ref);
        if !self.enabled.contains(&package_ref) {
            self.enabled.push(package_ref);
        }
        self.enabled.sort();
    }

    pub fn remove_disabled(&mut self, package_ref: &ContextualPackageRef) {
        self.disabled.retain(|entry| entry != package_ref);
    }

    pub fn disable(&mut self, package_ref: ContextualPackageRef) {
        self.enabled.retain(|entry| entry != &package_ref);
        if !self.disabled.contains(&package_ref) {
            self.disabled.push(package_ref);
        }
        self.disabled.sort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(kind: ContextualPackageKind, id: &str) -> ContextualPackageRef {
        ContextualPackageRef {
            kind,
            id: id.into(),
        }
    }

    #[test]
    fn a_pack_id_resolves_as_a_bundle_name_for_a_tagged_item() {
        let kind = ContextualPackageKind::Skill;
        let owning = BTreeMap::from([(reference(kind, "inspect"), "matlab".to_string())]);
        let mut policy = ContextualPackagesConfig::default().with_owning_packs(owning);

        // The project consumes the PACK, and never heard of the skill's own tag.
        policy.bundles = Some(vec!["matlab".into()]);
        assert_eq!(
            policy.selection(kind, "inspect", &["science".into()]),
            ContextualPackageSelection::SelectedBundle("matlab".into())
        );

        // The item's declared tag still works on its own.
        policy.bundles = Some(vec!["science".into()]);
        assert_eq!(
            policy.selection(kind, "inspect", &["science".into()]),
            ContextualPackageSelection::SelectedBundle("science".into())
        );

        // A pack the project does not consume still excludes the item.
        policy.bundles = Some(vec!["coding".into()]);
        assert_eq!(
            policy.selection(kind, "inspect", &["science".into()]),
            ContextualPackageSelection::OutsideConsumedBundles
        );
    }

    #[test]
    fn an_untagged_item_stays_universal_even_when_a_pack_installed_it() {
        // Installing an item from a pack must not silently re-gate it: declaring
        // no bundles is the item's own statement that it applies everywhere.
        let kind = ContextualPackageKind::Agent;
        let owning = BTreeMap::from([(reference(kind, "helper"), "acme".to_string())]);
        let mut policy = ContextualPackagesConfig::default().with_owning_packs(owning);
        policy.bundles = Some(vec!["coding".into()]);

        assert_eq!(
            policy.selection(kind, "helper", &[]),
            ContextualPackageSelection::Universal
        );
        assert!(policy.is_selected(kind, "helper", &[]));
    }

    #[test]
    fn an_unresolved_bundle_name_is_reported_but_never_fatal() {
        let owning = BTreeMap::from([(
            reference(ContextualPackageKind::Skill, "inspect"),
            "matlab".to_string(),
        )]);
        let mut policy = ContextualPackagesConfig::default().with_owning_packs(owning);
        policy.bundles = Some(vec!["matlab".into(), "coding".into(), "typo-bundle".into()]);

        let declared = BTreeSet::from(["coding".to_string()]);
        assert_eq!(
            policy.unresolved_bundles(&declared),
            vec!["typo-bundle".to_string()]
        );

        // A project that consumes everything reports nothing.
        assert!(ContextualPackagesConfig::default()
            .unresolved_bundles(&declared)
            .is_empty());
    }

    #[test]
    fn compatibility_and_precedence() {
        let kind = ContextualPackageKind::Skill;
        let mut policy = ContextualPackagesConfig::default();
        assert!(policy.is_selected(kind, "tagged", &["coding".into()]));
        assert!(policy.is_selected(kind, "universal", &[]));

        policy.bundles = Some(vec![]);
        assert!(!policy.is_selected(kind, "tagged", &["coding".into()]));
        assert!(policy.is_selected(kind, "universal", &[]));

        policy.enable(reference(kind, "tagged"));
        assert!(policy.is_selected(kind, "tagged", &["coding".into()]));
        policy.disabled.push(reference(kind, "tagged"));
        assert!(!policy.is_selected(kind, "tagged", &["coding".into()]));
    }

    #[test]
    fn normalization_is_deterministic() {
        let mut policy = ContextualPackagesConfig {
            bundles: Some(vec!["Rust".into(), "coding".into(), "rust".into()]),
            enabled: vec![
                reference(ContextualPackageKind::Skill, "b"),
                reference(ContextualPackageKind::Skill, "b"),
            ],
            disabled: vec![],
            owning_packs: BTreeMap::new(),
        };
        policy.normalize().unwrap();
        assert_eq!(policy.bundles, Some(vec!["coding".into(), "rust".into()]));
        assert_eq!(policy.enabled.len(), 1);
        assert!(normalize_bundle_name("bad name").is_err());
    }
}
