//! The pin document: which release of a managed CLI Cairn runs, with which
//! checksums, for which launch contract.
//!
//! `<name>-pin.json` (beside this crate's `Cargo.toml`) is the one place a
//! managed CLI's version is written. This build embeds it; CI republishes it
//! on every merge to `main` as the rolling `<name>-integration-v{N}` release,
//! which [`super::ManagedTool`] follows at runtime, so a compatible bump
//! reaches existing installs without an app release (see `docs/backends.md`,
//! "Managed CLIs").

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::spec::ToolSpec;

/// One release of a managed CLI, as its pin file records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolPin {
    /// The tool's integration this pin was verified against.
    pub integration: u32,
    /// What `<cli> --version` reports, without any product prefix.
    pub version: String,
    /// The publisher's release tag, where the tool has one
    /// ([`ToolSpec::check_tag`] says whether it must).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Why this version was chosen.
    pub reason: String,
    /// Releases a bump must never select: the rollback hold for an upstream
    /// version found broken after it was published. A pin's own `version` is
    /// never in it.
    #[serde(default)]
    pub skip: Vec<String>,
    /// The release's package, by target.
    pub packages: BTreeMap<String, ToolPackage>,
}

/// One target's package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolPackage {
    /// Lowercase hex SHA-256 of the package as downloaded.
    pub sha256: String,
    /// Its size in bytes; a download is cut off past it.
    pub size: u64,
}

impl ToolPin {
    /// Parse and [`validate`](Self::validate) a pin document for `spec`.
    pub fn parse(spec: &ToolSpec, text: &str) -> Result<Self, String> {
        let pin: Self = serde_json::from_str(text)
            .map_err(|error| format!("not a {} pin: {error}", spec.display_name))?;
        pin.validate(spec)?;
        Ok(pin)
    }

    /// Refuse a pin this build cannot install from: another integration, a
    /// version or tag that could steer the package URL anywhere but the
    /// publisher's release for that version, a target missing, or a
    /// malformed digest.
    pub fn validate(&self, spec: &ToolSpec) -> Result<(), String> {
        if self.integration != spec.integration {
            return Err(format!(
                "the pin is for {} integration {}, this build speaks {}",
                spec.display_name, self.integration, spec.integration
            ));
        }
        if !is_well_formed_version(&self.version) {
            return Err(format!("the pin's version {:?} is malformed", self.version));
        }
        if let Some(entry) = self.skip.iter().find(|v| !is_well_formed_version(v)) {
            return Err(format!("the pin's skip entry {entry:?} is malformed"));
        }
        if self.skip.contains(&self.version) {
            return Err(format!(
                "the pin selects {}, which its own skip list holds back",
                self.version
            ));
        }
        (spec.check_tag)(self)?;
        if self.reason.trim().is_empty() {
            return Err("the pin records no reason".to_string());
        }
        for (target, package) in &self.packages {
            if !spec.supported_targets.contains(&target.as_str()) {
                return Err(format!("the pin names an unknown target {target:?}"));
            }
            let digest_ok = package.sha256.len() == 64
                && package
                    .sha256
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c));
            if !digest_ok {
                return Err(format!("the pin's sha256 for {target} is malformed"));
            }
            if package.size == 0 {
                return Err(format!("the pin's size for {target} is zero"));
            }
        }
        if let Some(missing) = spec
            .supported_targets
            .iter()
            .find(|target| !self.packages.contains_key(**target))
        {
            return Err(format!("the pin has no package for {missing}"));
        }
        Ok(())
    }

    pub fn package(&self, target: &str) -> Option<&ToolPackage> {
        self.packages.get(target)
    }
}

/// A version as the publishers tag it: a leading digit, then ASCII letters,
/// digits, dots and dashes, so it can never steer a package URL off the
/// release.
pub fn is_well_formed_version(version: &str) -> bool {
    version.starts_with(|c: char| c.is_ascii_digit())
        && version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::super::{claude::CLAUDE, codex::CODEX};
    use super::*;

    #[test]
    fn every_embedded_pin_speaks_its_integration_and_covers_every_target() {
        for spec in [&CODEX, &CLAUDE] {
            let pin = spec.embedded();
            assert_eq!(pin.integration, spec.integration, "{}", spec.name);
            for target in spec.supported_targets {
                assert!(pin.package(target).is_some(), "{}: no {target}", spec.name);
            }
            if let Some(target) = (spec.host_target)() {
                assert!(spec.supported_targets.contains(&target));
            }
            assert!(!pin.skip.contains(&pin.version));
        }
    }

    #[test]
    fn a_pin_that_could_steer_the_download_is_refused() {
        let mut pin = CODEX.embedded();
        pin.tag = Some("rust-v0.153.3/../../evil".to_string());
        assert!(pin.validate(&CODEX).unwrap_err().contains("tag"));

        let mut pin = CODEX.embedded();
        pin.version = "0.1/../x".to_string();
        pin.tag = Some(format!("rust-v{}", pin.version));
        assert!(pin.validate(&CODEX).unwrap_err().contains("malformed"));

        for spec in [&CODEX, &CLAUDE] {
            let mut pin = spec.embedded();
            pin.integration += 1;
            assert!(pin.validate(spec).unwrap_err().contains("integration"));

            let mut pin = spec.embedded();
            let target = spec.supported_targets[0];
            pin.packages.get_mut(target).unwrap().sha256 = "ABC".into();
            assert!(pin.validate(spec).unwrap_err().contains("sha256"));
        }
    }

    /// Rust and the TypeScript pin tooling agree: a pin covers every target
    /// the tool supports, never a subset.
    #[test]
    fn a_pin_missing_a_target_is_refused() {
        for spec in [&CODEX, &CLAUDE] {
            let mut pin = spec.embedded();
            let dropped = spec.supported_targets[spec.supported_targets.len() - 1];
            pin.packages.remove(dropped);
            assert_eq!(
                pin.validate(spec).unwrap_err(),
                format!("the pin has no package for {dropped}")
            );
        }
    }

    #[test]
    fn codex_requires_its_tag_and_claude_takes_none() {
        let mut pin = CODEX.embedded();
        pin.tag = None;
        assert!(pin.validate(&CODEX).unwrap_err().contains("tag"));

        let pin = CLAUDE.embedded();
        assert_eq!(pin.tag, None);
        let mut tagged = pin.clone();
        tagged.tag = Some(format!("v{}", pin.version));
        assert!(tagged.validate(&CLAUDE).unwrap_err().contains("tag"));
    }

    #[test]
    fn a_pin_may_hold_back_versions_but_never_its_own() {
        let mut pin = CODEX.embedded();
        pin.skip = vec!["0.1.0".into(), "0.2.0-alpha.1".into()];
        pin.validate(&CODEX)
            .expect("well-formed skips other than the version are fine");

        pin.skip.push(pin.version.clone());
        assert!(pin.validate(&CODEX).unwrap_err().contains("skip list"));

        let mut pin = CODEX.embedded();
        pin.skip = vec!["../evil".into()];
        assert!(pin.validate(&CODEX).unwrap_err().contains("skip entry"));
    }

    #[test]
    fn a_pin_without_skip_reads_as_skipping_nothing() {
        let mut value: serde_json::Value = serde_json::from_str(CODEX.embedded_pin).unwrap();
        value.as_object_mut().unwrap().remove("skip");
        let pin = ToolPin::parse(&CODEX, &value.to_string()).expect("skip is optional");
        assert!(pin.skip.is_empty());
    }

    /// Runners on builds older than the managed-tool mechanism parse the
    /// published Codex pin with a required `tag`; a re-serialized Codex pin
    /// keeps it, and a Claude pin never grows one.
    #[test]
    fn the_tag_round_trips_only_where_it_exists() {
        let codex: serde_json::Value = serde_json::to_value(CODEX.embedded()).expect("serializes");
        assert_eq!(
            codex["tag"],
            serde_json::json!(format!("rust-v{}", CODEX.embedded().version))
        );
        let claude: serde_json::Value =
            serde_json::to_value(CLAUDE.embedded()).expect("serializes");
        assert!(claude.get("tag").is_none());
    }
}
