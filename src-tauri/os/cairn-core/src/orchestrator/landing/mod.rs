//! How the store lands work beyond a line merge: which paths a conflict may be
//! settled in structurally (see [`structural_merge`]), and which sequential
//! identifiers a landing on the default branch binds (see [`bind`]).
//!
//! The rules come from `.cairn/config.yaml`'s `landing` section AT THE COMMIT
//! BEING LANDED ONTO, never at the branch being landed, so no branch can change
//! the rules it lands by.

pub(crate) mod bind;
pub(crate) mod structural_merge;

use std::path::Path;

use globset::{GlobBuilder, GlobMatcher};

use crate::config::project_settings::{
    landing_from, parse_project_settings, IdentifierRule, LandingMergeKind, LandingSettings,
};

const CONFIG_PATH: &str = ".cairn/config.yaml";

/// One declared structural merge, compiled for matching.
struct CompiledRule {
    globs: Vec<GlobMatcher>,
    kind: Option<LandingMergeKind>,
    anchor: Option<String>,
}

/// The declared structural merges, compiled for matching.
#[derive(Default)]
pub(crate) struct MergeRules {
    rules: Vec<CompiledRule>,
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
                CompiledRule {
                    globs,
                    kind: rule.merge_kind(),
                    anchor: rule.anchor.clone(),
                }
            })
            .collect();
        Self { rules }
    }

    fn rule_for(&self, path: &str) -> Option<&CompiledRule> {
        self.rules
            .iter()
            .find(|rule| rule.globs.iter().any(|glob| glob.is_match(path)))
    }

    /// The structural merge declared for `path`: the first rule naming it wins,
    /// and a rule of a kind this build cannot perform keeps the line merge.
    pub(crate) fn kind_for(&self, path: &str) -> Option<LandingMergeKind> {
        self.rule_for(path).and_then(|rule| rule.kind)
    }

    /// Merge one path's three versions by its declared kind; `None` when no
    /// kind is declared or the merge cannot settle every conflict.
    pub(crate) fn merge(&self, path: &str, base: &str, ours: &str, theirs: &str) -> Option<String> {
        let rule = self.rule_for(path)?;
        match rule.kind? {
            LandingMergeKind::OrderedSet if path.ends_with(".rs") => {
                structural_merge::ordered_set_merge(base, ours, theirs)
            }
            LandingMergeKind::OrderedSet => None,
            LandingMergeKind::VersionParagraphs => {
                let anchor = rule.anchor.as_deref()?;
                structural_merge::version_paragraphs_merge(base, ours, theirs, anchor)
            }
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

/// The `landing` section at `rev` in the store. A missing, unreadable, or
/// invalid config declares nothing: every path keeps the line merge and no
/// identifier is bound.
pub(crate) fn landing_settings_at(
    jj: &crate::jj::JjEnv,
    store: &Path,
    rev: &str,
) -> LandingSettings {
    let path = [CONFIG_PATH.to_string()];
    let content = crate::jj::paths_present_at(jj, store, rev, &path)
        .and_then(|present| crate::jj::file_at_revision(jj, store, rev, CONFIG_PATH, &present));
    match content {
        Ok(crate::jj::FileContent::Text(text)) => {
            match parse_project_settings(&text) {
                Ok((settings, _needs_migration)) => landing_from(&settings),
                Err(error) => {
                    log::warn!("landing: `{CONFIG_PATH}` at `{rev}` does not parse; no landing rules: {error}");
                    LandingSettings::default()
                }
            }
        }
        Ok(_) => LandingSettings::default(),
        Err(error) => {
            log::warn!(
                "landing: could not read `{CONFIG_PATH}` at `{rev}`; no landing rules: {error}"
            );
            LandingSettings::default()
        }
    }
}

/// The structural merges declared at `rev`.
pub(crate) fn merge_rules_at(jj: &crate::jj::JjEnv, store: &Path, rev: &str) -> MergeRules {
    MergeRules::from_settings(&landing_settings_at(jj, store, rev))
}

/// The identifier rules a settings section declares, by name, in a stable
/// order: every `numbered-file` rule before every `version-heading` rule, then
/// by name. A kind this build does not know is skipped with a warning; a known
/// kind missing what it needs is an error, because binding nothing where the
/// project asked for a number would land the provisional spelling.
pub(crate) fn identifier_rules(
    settings: &LandingSettings,
) -> Result<Vec<(String, IdentifierRule)>, String> {
    let mut rules = Vec::new();
    for (name, declared) in &settings.identifiers {
        match declared.rule() {
            Ok(Some(rule)) => rules.push((name.clone(), rule)),
            Ok(None) => log::warn!(
                "landing: identifier `{name}` has kind `{}`, which this build cannot bind; skipped",
                declared.kind
            ),
            Err(error) => return Err(format!("landing identifier `{name}`: {error}")),
        }
    }
    rules.sort_by_key(|(name, rule)| {
        (
            matches!(rule, IdentifierRule::VersionHeading { .. }),
            name.clone(),
        )
    });
    Ok(rules)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project_settings::LandingMergeRule;

    #[test]
    fn this_repository_declares_its_registries() {
        let (settings, _) =
            parse_project_settings(include_str!("../../../../../../.cairn/config.yaml")).unwrap();
        let landing = landing_from(&settings);
        let rules = MergeRules::from_settings(&landing);
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
            rules.kind_for("src-tauri/os/cairn-common/src/executor_protocol.rs"),
            Some(LandingMergeKind::VersionParagraphs)
        );
        assert_eq!(
            rules.kind_for("src-tauri/os/cairn-core/src/orchestrator/base_advance.rs"),
            None
        );
        assert_eq!(
            identifier_rules(&landing).unwrap(),
            vec![
                (
                    "migration".to_string(),
                    IdentifierRule::NumberedFile {
                        directory: "src-tauri/turso_migrations".into(),
                        width: 4
                    }
                ),
                (
                    "executor-protocol".to_string(),
                    IdentifierRule::VersionHeading {
                        file: "src-tauri/os/cairn-common/src/executor_protocol.rs".into(),
                        anchor: "EXECUTOR_PROTOCOL_VERSION".into()
                    }
                ),
            ]
        );
    }

    #[test]
    fn the_first_declared_rule_naming_a_path_decides_it() {
        let rules = MergeRules::from_settings(&LandingSettings {
            merge: vec![
                LandingMergeRule {
                    paths: vec!["src/registry.rs".into(), "src/**/mod.rs".into()],
                    kind: "ordered-set".into(),
                    anchor: None,
                },
                LandingMergeRule {
                    paths: vec!["src/*.rs".into()],
                    kind: "from-the-future".into(),
                    anchor: None,
                },
            ],
            ..LandingSettings::default()
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

    #[test]
    fn a_version_paragraphs_rule_without_an_anchor_keeps_the_line_merge() {
        let rules = MergeRules::from_settings(&LandingSettings {
            merge: vec![LandingMergeRule {
                paths: vec!["src/protocol.rs".into()],
                kind: "version-paragraphs".into(),
                anchor: None,
            }],
            ..LandingSettings::default()
        });
        let base = "/// Version 1 a.\npub const V: u32 = 1;\n";
        let ours = "/// Version 1 a.\n///\n/// Version NEXT b.\npub const V: u32 = 1;\n";
        let theirs = "/// Version 1 a.\n///\n/// Version 2 c.\npub const V: u32 = 1;\n";
        assert_eq!(rules.merge("src/protocol.rs", base, ours, theirs), None);
    }
}
