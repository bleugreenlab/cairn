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
    landing_from, parse_project_settings, IdentifierRule, LandingMergeKind, LandingRegenerateRule,
    LandingSettings,
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
            .map(|rule| CompiledRule {
                globs: compile_globs(&rule.paths, "merge"),
                kind: rule.merge_kind(),
                anchor: rule.anchor.clone(),
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

fn compile_globs(patterns: &[String], what: &str) -> Vec<GlobMatcher> {
    patterns
        .iter()
        .filter_map(|pattern| {
            GlobBuilder::new(pattern)
                .literal_separator(true)
                .build()
                .map_err(|error| {
                    log::warn!(
                        "landing: ignoring {what} path `{pattern}`, which is not a glob: {error}"
                    );
                })
                .ok()
                .map(|glob| glob.compile_matcher())
        })
        .collect()
}

/// The declared regenerable outputs (`landing.regenerate`), compiled for
/// matching. A path belongs to the first rule naming it. A pattern that is not
/// a glob covers nothing, which can only leave a conflict standing.
#[derive(Default)]
pub(crate) struct RegenerateRules {
    rules: Vec<(Vec<GlobMatcher>, LandingRegenerateRule)>,
}

impl RegenerateRules {
    pub(crate) fn from_settings(settings: &LandingSettings) -> Self {
        Self {
            rules: settings
                .regenerate
                .iter()
                .map(|rule| (compile_globs(&rule.paths, "regenerate"), rule.clone()))
                .collect(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    fn rule_index(&self, path: &str) -> Option<usize> {
        self.rules
            .iter()
            .position(|(globs, _)| globs.iter().any(|glob| glob.is_match(path)))
    }

    pub(crate) fn covers(&self, path: &str) -> bool {
        self.rule_index(path).is_some()
    }

    /// `paths` grouped under the rule each belongs to, in declaration order;
    /// `None` when any path belongs to no rule.
    pub(crate) fn group<'p>(
        &self,
        paths: impl IntoIterator<Item = &'p str>,
    ) -> Option<Vec<(&LandingRegenerateRule, Vec<String>)>> {
        let mut grouped: Vec<Vec<String>> = vec![Vec::new(); self.rules.len()];
        for path in paths {
            grouped[self.rule_index(path)?].push(path.to_string());
        }
        Some(
            self.rules
                .iter()
                .zip(grouped)
                .filter(|(_, paths)| !paths.is_empty())
                .map(|((_, rule), paths)| (rule, paths))
                .collect(),
        )
    }
}

/// Whether `path` matches one of `globs`, compiled the way the rules are.
pub(crate) fn path_matches(globs: &[String], path: &str) -> bool {
    compile_globs(globs, "regenerate")
        .iter()
        .any(|glob| glob.is_match(path))
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
    fn regenerate_rules_group_paths_by_their_first_rule_and_refuse_strays() {
        let rule = |paths: &[&str], command: &str| LandingRegenerateRule {
            paths: paths.iter().map(|path| path.to_string()).collect(),
            regenerate: command.into(),
            check: format!("{command} --check"),
            executor: None,
            timeout: None,
            lane: None,
        };
        let rules = RegenerateRules::from_settings(&LandingSettings {
            regenerate: vec![
                rule(&["packages/ui/src/uri-wasm/**"], "wasm"),
                rule(&["packages/**", "Cargo.lock"], "other"),
            ],
            ..LandingSettings::default()
        });
        let grouped = rules
            .group([
                "packages/ui/src/uri-wasm/build.json",
                "Cargo.lock",
                "packages/ui/src/uri-wasm/cairn_uri_bg.wasm",
            ])
            .unwrap();
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].0.regenerate, "wasm");
        assert_eq!(
            grouped[0].1,
            [
                "packages/ui/src/uri-wasm/build.json",
                "packages/ui/src/uri-wasm/cairn_uri_bg.wasm"
            ]
        );
        assert_eq!(grouped[1].1, ["Cargo.lock"]);
        assert!(rules.group(["Cargo.lock", "src/lib.rs"]).is_none());
        assert!(!rules.covers("lockfile/Cargo.lock"), "globs are anchored");
    }

    #[test]
    fn this_repository_declares_the_uri_wasm_pair_regenerable() {
        let (settings, _) =
            parse_project_settings(include_str!("../../../../../../.cairn/config.yaml")).unwrap();
        let rules = RegenerateRules::from_settings(&landing_from(&settings));
        let grouped = rules
            .group([
                "packages/ui/src/uri-wasm/build.json",
                "packages/ui/src/uri-wasm/cairn_uri_bg.wasm",
            ])
            .expect("the pair is declared");
        let [(rule, _)] = grouped.as_slice() else {
            panic!("one rule covers the pair")
        };
        assert_eq!(rule.regenerate, "bun run build:uri-wasm");
        assert_eq!(rule.check, "bun run build:uri-wasm --check");
        assert_eq!(rule.lane.as_deref(), Some("uri-wasm"));
        assert!(!rules.covers("src-tauri/os/cairn-uri/src/lib.rs"));
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
