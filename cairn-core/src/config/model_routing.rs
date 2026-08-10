//! The label -> tier binding table (`[project]/.cairn/model-routing.yaml`).
//!
//! A project-scoped, data-only document mapping label predicates to tier
//! references. It is deliberately a document of its own rather than a key inside
//! `config.yaml`: the replay benchmark regenerates it wholesale, and a generator
//! overwriting a dedicated file is far safer than one surgically editing a key
//! beside unrelated user settings.
//!
//! ## The measured constraints this code enforces
//!
//! The numbers behind each of these live in `docs/cairnbench-label-taxonomy.md`,
//! backfilled across a 625-task pool. They are load-bearing, not decoration:
//!
//! 1. **Absence of labels is abstention, not simplicity.** A short issue is
//!    unclassifiable, not simple. Nothing here ever routes an unlabeled or
//!    unmatched issue anywhere but the agent's own authored default -- hence the
//!    deliberate absence of a table-level `default:` key.
//! 2. **Language is nearly unknowable at issue-creation time** (issue-only recall
//!    25-36% for language and surface labels against 97-99% blind). Creation-time
//!    bindings therefore key off `work_type`; language-conditioned refinement is
//!    only legitimate once a plan exists, which is a separate hook.
//! 3. **Four labels are near-universal and separate nothing.** A rule keyed only
//!    on [`NON_DISCRIMINATING_LABELS`] routes ~87% of launches while looking
//!    calibrated, so such a rule is refused at load.
//!
//! Both constants below encode that document's measurements rather than a
//! permanent truth, and must be revisited whenever it is regenerated.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// File name of the binding table inside a project's `.cairn` directory.
pub const MODEL_ROUTING_FILE: &str = "model-routing.yaml";

/// The table shape this build understands. A table omitting `version` (0) is
/// accepted as this one, so a hand-written stub is not a refusal.
const SUPPORTED_VERSION: u32 = 1;

/// Labels carried by ~86-89% of the measured task pool: `backend` 86%,
/// `product-behavior-change` 86%, `live-verification` 89%,
/// `end-to-end-verification` 88%. A rule keyed entirely on these fires on nearly
/// every launch while reading like a calibrated binding, so it is refused at
/// load. Including one *alongside* a discriminating label is fine -- the other
/// label is what does the work.
///
/// Revisit when `docs/cairnbench-label-taxonomy.md` is regenerated.
pub(crate) const NON_DISCRIMINATING_LABELS: &[&str] = &[
    "backend",
    "product-behavior-change",
    "live-verification",
    "end-to-end-verification",
];

/// Labels that may never lower an agent's tier, whether or not the demotion gate
/// is open. `investigation` is over-applied from issue text (precision 32%
/// issue-only, 55% even with a plan in hand): a request to "look into X" reads as
/// investigation even when the work is a fix. Unlike the general
/// [`ModelRoutingTable::allow_demotion`] gate -- which exists to be opened once
/// replay data justifies it -- this bar is permanent, because better capability
/// data does not repair a classification that was wrong about the task.
pub(crate) const NEVER_DEMOTES: &[&str] = &["investigation"];

/// The binding table as authored on disk.
///
/// An absent file is an empty table (the correct permanent behavior for a
/// project that never adopts routing); a malformed one is an `Err` that refuses
/// the launch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelRoutingTable {
    #[serde(default)]
    pub version: u32,
    /// Which calibration produced these rules, recorded into every execution's
    /// routing provenance so a past decision can be traced to its evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    /// While false, a rule that would resolve an agent BELOW its authored tier is
    /// refused at resolve time and recorded as such. The replay phase flips this
    /// deliberately, together with the generation string that justifies it.
    #[serde(default)]
    pub allow_demotion: bool,
    /// Ordered. First matching rule wins per agent; later rules do not apply.
    #[serde(default)]
    pub rules: Vec<RoutingRule>,
}

/// One label predicate bound to one tier reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RoutingRule {
    /// Unique; this is the provenance handle that appears on the execution.
    /// Defaulted at parse time only so a missing id is reported by
    /// [`ModelRoutingTable::validate`] with the rule's position and tier, which
    /// a bare serde "missing field" cannot say.
    #[serde(default)]
    pub id: String,
    pub when: RulePredicate,
    /// Agent ids this rule applies to. Omitted means every agent in the graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<Vec<String>>,
    /// An ordinary tier ref, qualified (`codex/lg`) or not (`lg`).
    pub tier: String,
    /// Why this binding exists and the evidence behind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub because: Option<String>,
}

/// A rule's label predicate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RulePredicate {
    /// Every listed label must be present. Required and non-empty.
    #[serde(default)]
    pub all: Vec<String>,
    /// At least one listed label must be present, when the list is non-empty.
    #[serde(default)]
    pub any: Vec<String>,
    /// None of the listed labels may be present.
    #[serde(default)]
    pub none: Vec<String>,
}

/// The labels an issue carries, indexed for matching.
///
/// A rule token is compared case-insensitively against each present label's slug
/// `id` and its display `name`, mirroring the resolution order `find_label_ref`
/// uses when a label reference is attached. A token naming a label the workspace
/// does not have simply never matches, which is harmless and correct -- so the
/// table is deliberately not validated against the workspace vocabulary.
#[derive(Debug, Clone, Default)]
pub struct IssueLabels {
    /// (slug id, display name) for each present label.
    entries: Vec<(String, String)>,
}

impl IssueLabels {
    pub fn new(entries: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            entries: entries.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every present label's slug, sorted. This is what provenance records.
    pub fn slugs(&self) -> Vec<String> {
        let mut slugs: Vec<String> = self.entries.iter().map(|(id, _)| id.clone()).collect();
        slugs.sort();
        slugs.dedup();
        slugs
    }

    /// The slug of the label `token` names, if the issue carries it.
    fn resolve(&self, token: &str) -> Option<&str> {
        let token = token.trim();
        self.entries
            .iter()
            .find(|(id, name)| id.eq_ignore_ascii_case(token) || name.eq_ignore_ascii_case(token))
            .map(|(id, _)| id.as_str())
    }

    fn has(&self, token: &str) -> bool {
        self.resolve(token).is_some()
    }
}

impl RulePredicate {
    /// The slugs that made this predicate match, or `None` when it does not.
    ///
    /// A match returns the labels responsible for it (the `all` set plus the
    /// `any` labels actually present) so the recorded decision can say which
    /// labels routed the launch rather than just which rule did.
    pub fn matches(&self, labels: &IssueLabels) -> Option<Vec<String>> {
        if self.none.iter().any(|token| labels.has(token)) {
            return None;
        }
        let mut matched = Vec::new();
        for token in &self.all {
            matched.push(labels.resolve(token)?.to_string());
        }
        if !self.any.is_empty() {
            let hits: Vec<String> = self
                .any
                .iter()
                .filter_map(|token| labels.resolve(token))
                .map(str::to_string)
                .collect();
            if hits.is_empty() {
                return None;
            }
            matched.extend(hits);
        }
        matched.sort();
        matched.dedup();
        Some(matched)
    }

    /// Whether this predicate fires *because* one of [`NEVER_DEMOTES`] is
    /// present. `none` is excluded on purpose: asserting a label's absence is not
    /// routing on its presence.
    pub(crate) fn cites_never_demote(&self) -> Option<&str> {
        self.all
            .iter()
            .chain(self.any.iter())
            .find(|token| {
                NEVER_DEMOTES
                    .iter()
                    .any(|bar| bar.eq_ignore_ascii_case(token.trim()))
            })
            .map(String::as_str)
    }
}

impl RoutingRule {
    /// Whether this rule applies to `agent_id`. An omitted `agents` list means
    /// every agent in the graph.
    pub(crate) fn applies_to(&self, agent_id: &str) -> bool {
        match &self.agents {
            None => true,
            Some(agents) => agents
                .iter()
                .any(|id| id.trim().eq_ignore_ascii_case(agent_id)),
        }
    }
}

impl ModelRoutingTable {
    /// Refuse a table that would route dishonestly.
    ///
    /// Every one of these is a refusal rather than a warning: a table is a set of
    /// claims about which model a task needs, and a claim that is silently
    /// dropped or silently over-broad is worse than no table at all.
    pub fn validate(&self) -> Result<(), String> {
        // A future generation that changes what these keys mean must not be read
        // through this generation's semantics. Unknown keys are already refused;
        // this catches the harder case where the shape is the same and the
        // meaning is not.
        if self.version != 0 && self.version != SUPPORTED_VERSION {
            return Err(format!(
                "unsupported table version {} (this build reads version {SUPPORTED_VERSION})",
                self.version
            ));
        }

        let mut seen: Vec<&str> = Vec::new();
        for (index, rule) in self.rules.iter().enumerate() {
            let id = rule.id.trim();
            if id.is_empty() {
                return Err(format!(
                    "rule #{} (tier '{}') has no id; every rule needs a unique id, which is how a routing decision names the rule that made it",
                    index + 1,
                    rule.tier
                ));
            }
            if seen.iter().any(|other| other.eq_ignore_ascii_case(id)) {
                return Err(format!(
                    "duplicate rule id '{id}'; a routing decision names the rule that made it, so ids must be unique"
                ));
            }
            seen.push(id);

            if rule.tier.trim().is_empty() {
                return Err(format!(
                    "rule '{id}' has no tier; a rule with nothing to bind to is not a rule"
                ));
            }

            let required: Vec<&String> = rule
                .when
                .all
                .iter()
                .filter(|token| !token.trim().is_empty())
                .collect();
            if required.is_empty() {
                return Err(format!(
                    "rule '{id}' has an empty `when.all`; a rule with no required label matches every issue, which is a default written in the wrong file"
                ));
            }

            let keys: Vec<&String> = required
                .into_iter()
                .chain(rule.when.any.iter().filter(|t| !t.trim().is_empty()))
                .collect();
            if keys.iter().all(|token| {
                NON_DISCRIMINATING_LABELS
                    .iter()
                    .any(|common| common.eq_ignore_ascii_case(token.trim()))
            }) {
                return Err(format!(
                    "rule '{id}' is keyed only on near-universal labels ({}); those appear on 86-89% of the measured task pool, so this rule would route almost every launch while reading like a calibrated binding. Pair one with a discriminating label, or drop it. See docs/cairnbench-label-taxonomy.md",
                    NON_DISCRIMINATING_LABELS.join(", ")
                ));
            }
        }
        Ok(())
    }
}

/// Load a project's binding table.
///
/// Absent is an empty table. Malformed is an `Err` that refuses the launch,
/// following the resolver's existing loud-by-design stance: a table nobody can
/// parse must never degrade into "route everything to the default" in silence,
/// because that is indistinguishable from a table that says so on purpose.
pub fn load_model_routing(project_path: &Path) -> Result<ModelRoutingTable, String> {
    let path = project_path.join(".cairn").join(MODEL_ROUTING_FILE);
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ModelRoutingTable::default())
        }
        Err(error) => {
            return Err(format!(
                "Failed to read model routing table {}: {error}",
                path.display()
            ))
        }
    };
    let table: ModelRoutingTable = serde_yaml::from_str(&contents)
        .map_err(|error| format!("Invalid model routing table {}: {error}", path.display()))?;
    table
        .validate()
        .map_err(|error| format!("Invalid model routing table {}: {error}", path.display()))?;
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> IssueLabels {
        IssueLabels::new(
            pairs
                .iter()
                .map(|(id, name)| (id.to_string(), name.to_string())),
        )
    }

    fn parse(yaml: &str) -> Result<ModelRoutingTable, String> {
        let table: ModelRoutingTable =
            serde_yaml::from_str(yaml).map_err(|error| error.to_string())?;
        table.validate()?;
        Ok(table)
    }

    /// The table this repository actually ships must round-trip and must assert
    /// nothing: generation zero is the mechanism proven end to end with zero
    /// capability claims behind it.
    #[test]
    fn shipped_generation_zero_table_loads_and_binds_nothing() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("crate lives at src-tauri/os/cairn-core");
        let table = load_model_routing(repo_root).expect("shipped table must load");
        assert_eq!(table.version, 1);
        assert!(table.rules.is_empty(), "generation zero binds nothing");
        assert!(!table.allow_demotion, "the demotion gate ships closed");
        let generation = table.generation.expect("generation string is recorded");
        assert!(generation.contains("gen-0"), "generation: {generation}");
    }

    #[test]
    fn a_future_table_version_is_refused_rather_than_reinterpreted() {
        let error = parse("version: 2\nrules: []\n").expect_err("a newer shape is not this shape");
        assert!(error.contains("version 2"), "{error}");
    }

    #[test]
    fn absent_file_is_an_empty_table_not_an_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let table = load_model_routing(dir.path()).expect("absent table is not an error");
        assert!(table.rules.is_empty());
        assert!(table.generation.is_none());
    }

    #[test]
    fn malformed_file_refuses_with_the_path() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join(".cairn")).unwrap();
        std::fs::write(
            dir.path().join(".cairn").join(MODEL_ROUTING_FILE),
            "rules: [this is not a rule]\n",
        )
        .unwrap();
        let error = load_model_routing(dir.path()).expect_err("malformed table refuses");
        assert!(error.contains(MODEL_ROUTING_FILE), "{error}");
    }

    #[test]
    fn unknown_key_is_refused_rather_than_ignored() {
        let error = parse(
            "rules:\n  - id: a\n    when:\n      all: [migration]\n    teir: lg\n    tier: lg\n",
        )
        .expect_err("a typo'd key must not be silently dropped");
        assert!(error.contains("teir"), "{error}");
    }

    #[test]
    fn missing_rule_id_names_its_position_and_tier() {
        let error = parse("rules:\n  - when:\n      all: [migration]\n    tier: lg\n")
            .expect_err("a rule with no id is refused");
        assert!(error.contains("rule #1"), "{error}");
        assert!(error.contains("lg"), "{error}");
    }

    #[test]
    fn duplicate_rule_id_is_refused_by_id() {
        let error = parse(
            "rules:\n  - id: dupe\n    when:\n      all: [migration]\n    tier: lg\n  - id: dupe\n    when:\n      all: [bug-fix]\n    tier: md\n",
        )
        .expect_err("ids are the provenance handle and must be unique");
        assert!(error.contains("dupe"), "{error}");
    }

    #[test]
    fn empty_when_all_is_refused_by_id() {
        let error =
            parse("rules:\n  - id: catch-all\n    when:\n      any: [migration]\n    tier: lg\n")
                .expect_err("a rule with no required label is a default in the wrong file");
        assert!(error.contains("catch-all"), "{error}");
        assert!(error.contains("when.all"), "{error}");
    }

    #[test]
    fn rule_keyed_only_on_near_universal_labels_is_refused() {
        let error = parse(
            "rules:\n  - id: everything\n    when:\n      all: [backend]\n      any: [live-verification, end-to-end-verification]\n    tier: lg\n",
        )
        .expect_err("near-universal labels separate nothing");
        assert!(error.contains("everything"), "{error}");
        assert!(error.contains("cairnbench-label-taxonomy"), "{error}");
    }

    /// A near-universal label riding alongside a discriminating one is fine --
    /// the discriminating label is what does the work.
    #[test]
    fn near_universal_label_beside_a_discriminating_one_loads() {
        let table = parse("rules:\n  - id: migration-builder\n    when:\n      all: [migration, backend]\n    tier: lg\n")
            .expect("migration does the discriminating");
        assert_eq!(table.rules.len(), 1);
    }

    #[test]
    fn all_requires_every_token_and_any_requires_one() {
        let predicate = RulePredicate {
            all: vec!["migration".into()],
            any: vec!["rust".into(), "sql".into()],
            none: Vec::new(),
        };
        assert!(predicate
            .matches(&labels(&[("migration", "Migration")]))
            .is_none());
        let matched = predicate
            .matches(&labels(&[("migration", "Migration"), ("sql", "SQL")]))
            .expect("all present, one of any present");
        assert_eq!(matched, vec!["migration".to_string(), "sql".to_string()]);
        assert!(
            predicate.matches(&labels(&[("rust", "Rust")])).is_none(),
            "a missing `all` token defeats the rule"
        );
    }

    #[test]
    fn none_excludes() {
        let predicate = RulePredicate {
            all: vec!["migration".into()],
            any: Vec::new(),
            none: vec!["documentation".into()],
        };
        assert!(predicate
            .matches(&labels(&[("migration", "Migration")]))
            .is_some());
        assert!(predicate
            .matches(&labels(&[
                ("migration", "Migration"),
                ("documentation", "Documentation")
            ]))
            .is_none());
    }

    /// Matching mirrors `find_label_ref`: a token resolves against either the
    /// slug id or the display name, case-insensitively, so prose and slug
    /// spellings of the same label bind to the same rule.
    #[test]
    fn matching_is_case_insensitive_across_id_and_name() {
        let present = labels(&[("product-behavior-change", "Product Behavior Change")]);
        for token in [
            "product-behavior-change",
            "Product-Behavior-Change",
            "Product Behavior Change",
            "product behavior change",
        ] {
            let predicate = RulePredicate {
                all: vec![token.to_string()],
                ..Default::default()
            };
            let matched = predicate
                .matches(&present)
                .unwrap_or_else(|| panic!("token {token} should match"));
            assert_eq!(matched, vec!["product-behavior-change".to_string()]);
        }
    }

    #[test]
    fn token_naming_an_unknown_label_simply_never_matches() {
        let predicate = RulePredicate {
            all: vec!["not-a-real-label".into()],
            ..Default::default()
        };
        assert!(predicate
            .matches(&labels(&[("migration", "Migration")]))
            .is_none());
    }

    #[test]
    fn agents_scoping_defaults_to_every_agent() {
        let unscoped = RoutingRule {
            id: "a".into(),
            when: RulePredicate::default(),
            agents: None,
            tier: "lg".into(),
            because: None,
        };
        assert!(unscoped.applies_to("builder"));
        assert!(unscoped.applies_to("review"));
        let scoped = RoutingRule {
            agents: Some(vec!["builder".into()]),
            ..unscoped
        };
        assert!(scoped.applies_to("builder"));
        assert!(!scoped.applies_to("review"));
    }

    #[test]
    fn never_demote_labels_are_detected_in_all_and_any_but_not_none() {
        let cited = RulePredicate {
            all: vec!["investigation".into()],
            ..Default::default()
        };
        assert_eq!(cited.cites_never_demote(), Some("investigation"));
        let excluded = RulePredicate {
            all: vec!["migration".into()],
            none: vec!["investigation".into()],
            ..Default::default()
        };
        assert_eq!(excluded.cites_never_demote(), None);
    }
}
