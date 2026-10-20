//! How the store lands work beyond a line merge: which paths a conflict may be
//! settled in structurally (see [`structural_merge`]).
//!
//! The rules come from `.cairn/config.yaml`'s `landing` section AT THE COMMIT
//! BEING LANDED ONTO, never at the branch being landed, so no branch can change
//! the rules it lands by.

pub(crate) mod structural_merge;

use std::path::Path;

use globset::{GlobBuilder, GlobMatcher};

use crate::config::project_settings::{
    landing_from, parse_project_settings, LandingMergeKind, LandingSettings,
};

const CONFIG_PATH: &str = ".cairn/config.yaml";

/// The declared structural merges, compiled for matching.
#[derive(Default)]
pub(crate) struct MergeRules {
    rules: Vec<(Vec<GlobMatcher>, Option<LandingMergeKind>)>,
}

impl MergeRules {
    pub(crate) fn from_settings(settings: &LandingSettings) -> Self {
        let rules = settings
            .merge
            .iter()
            .map(|rule| {
                let globs = rule
                    .paths
                    .iter()
                    .filter_map(|pattern| {
                        GlobBuilder::new(pattern)
                            .literal_separator(true)
                            .build()
                            .map_err(|error| {
                                log::warn!(
                                    "landing: ignoring merge path `{pattern}`, which is not a \
                                     glob: {error}"
                                );
                            })
                            .ok()
                            .map(|glob| glob.compile_matcher())
                    })
                    .collect();
                (globs, rule.merge_kind())
            })
            .collect();
        Self { rules }
    }

    /// The structural merge declared for `path`: the first rule naming it wins,
    /// and a rule of a kind this build cannot perform keeps the line merge.
    pub(crate) fn kind_for(&self, path: &str) -> Option<LandingMergeKind> {
        self.rules
            .iter()
            .find(|(globs, _)| globs.iter().any(|glob| glob.is_match(path)))
            .and_then(|(_, kind)| *kind)
    }

    /// Merge one path's three versions by its declared kind; `None` when no
    /// kind is declared or the merge cannot settle every conflict.
    pub(crate) fn merge(&self, path: &str, base: &str, ours: &str, theirs: &str) -> Option<String> {
        match self.kind_for(path)? {
            LandingMergeKind::OrderedSet if path.ends_with(".rs") => {
                structural_merge::ordered_set_merge(base, ours, theirs)
            }
            LandingMergeKind::OrderedSet => None,
        }
    }
}

impl MergeRules {
    /// [`MergeRules::merge`], kept only when the result parses and the line
    /// merge finds nothing of either side missing from it: merging either side
    /// back into the result changes nothing. Every caller that commits a
    /// structural merge goes through this.
    pub(crate) fn proven_merge(
        &self,
        path: &str,
        base: &str,
        ours: &str,
        theirs: &str,
    ) -> Option<String> {
        let merged = self.merge(path, base, ours, theirs)?;
        if cairn_vcs::parse_gate::parse_failure(path, &merged).is_some() {
            return None;
        }
        let text = |value: &str| crate::jj::FileContent::Text(value.to_string());
        let holds = |side: &str| {
            crate::jj::assess_whole_file_restore(&crate::jj::MergeSides {
                base: text(base),
                ours: text(&merged),
                theirs: text(side),
            }) == crate::jj::RestoreVerdict::Lossless
        };
        (holds(theirs) && holds(ours)).then_some(merged)
    }
}

/// The landing rules at `rev` in the store. A missing, unreadable, or invalid
/// config declares none, so every path keeps the line merge.
pub(crate) fn merge_rules_at(jj: &crate::jj::JjEnv, store: &Path, rev: &str) -> MergeRules {
    let path = [CONFIG_PATH.to_string()];
    let content = crate::jj::paths_present_at(jj, store, rev, &path)
        .and_then(|present| crate::jj::file_at_revision(jj, store, rev, CONFIG_PATH, &present));
    let settings = match content {
        Ok(crate::jj::FileContent::Text(text)) => match parse_project_settings(&text) {
            Ok((settings, _needs_migration)) => landing_from(&settings),
            Err(error) => {
                log::warn!("landing: `{CONFIG_PATH}` at `{rev}` does not parse; no structural merges: {error}");
                LandingSettings::default()
            }
        },
        Ok(_) => LandingSettings::default(),
        Err(error) => {
            log::warn!(
                "landing: could not read `{CONFIG_PATH}` at `{rev}`; no structural merges: {error}"
            );
            LandingSettings::default()
        }
    };
    MergeRules::from_settings(&settings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project_settings::LandingMergeRule;

    #[test]
    fn this_repository_declares_its_registries() {
        let (settings, _) =
            parse_project_settings(include_str!("../../../../../../.cairn/config.yaml")).unwrap();
        let rules = MergeRules::from_settings(&landing_from(&settings));
        for registry in [
            "src-tauri/os/cairn-db/src/storage/migrations.rs",
            "src-tauri/os/cairn-core/src/orchestrator/mod.rs",
            "src-tauri/os/cairn-vcs/src/lib.rs",
        ] {
            assert_eq!(
                rules.kind_for(registry),
                Some(LandingMergeKind::OrderedSet),
                "{registry}"
            );
        }
        assert_eq!(
            rules.kind_for("src-tauri/os/cairn-core/src/orchestrator/base_advance.rs"),
            None
        );
    }

    #[test]
    fn the_first_declared_rule_naming_a_path_decides_it() {
        let rules = MergeRules::from_settings(&LandingSettings {
            merge: vec![
                LandingMergeRule {
                    paths: vec!["src/registry.rs".into(), "src/**/mod.rs".into()],
                    kind: "ordered-set".into(),
                },
                LandingMergeRule {
                    paths: vec!["src/*.rs".into()],
                    kind: "from-the-future".into(),
                },
            ],
        });
        assert_eq!(
            rules.kind_for("src/registry.rs"),
            Some(LandingMergeKind::OrderedSet)
        );
        assert_eq!(
            rules.kind_for("src/a/b/mod.rs"),
            Some(LandingMergeKind::OrderedSet)
        );
        assert_eq!(rules.kind_for("src/other.rs"), None, "unknown kind");
        assert_eq!(
            rules.kind_for("src/a/other.rs"),
            None,
            "`*` stays in a segment"
        );
    }
}
