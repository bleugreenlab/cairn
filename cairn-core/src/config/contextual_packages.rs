use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextualPackagesConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bundles: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    enabled: Vec<ContextualPackageRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled: Vec<ContextualPackageRef>,
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
    project_path
        .map(super::project_settings::load_project_settings)
        .and_then(|settings| settings.contextual_packages)
        .unwrap_or_default()
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
        };
        policy.normalize().unwrap();
        assert_eq!(policy.bundles, Some(vec!["coding".into(), "rust".into()]));
        assert_eq!(policy.enabled.len(), 1);
        assert!(normalize_bundle_name("bad name").is_err());
    }
}
