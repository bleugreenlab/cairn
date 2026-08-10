//! The per-agent launch resolution plan.
//!
//! One execution launch consults one plan. It answers a single question, per
//! agent: what tier should resolution start from? The answer composes three
//! inputs whose precedence is fixed and enforced here rather than emerging from
//! the order later layers happen to apply in:
//!
//! 1. An explicit per-launch choice (a composer pin, or a launch delta carrying
//!    a selection) -- the table is not consulted at all.
//! 2. The first rule in the project's binding table that matches the issue's
//!    labels and is scoped to this agent.
//! 3. The agent config's own authored tier.
//!
//! The execution-wide backend override stays orthogonal at every level. When a
//! backend override and a label tier are both present they compose into a
//! qualified tier ref (`codex/lg`), which the resolver already splits back into
//! backend plus tier -- qualified refs are first-class grammar, not a workaround.
//!
//! Every consultation records why, so the frozen snapshot can answer "why this
//! model" long after the config that decided it has changed.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::config::model_routing::{IssueLabels, ModelRoutingTable, RoutingRule};
use crate::config::presets::{
    parse_tier_ref, resolve_selection_with_provenance, LaunchSelectionOverride, PresetsConfig,
    DEFAULT_TIER,
};
use crate::models::{
    AgentSnapshot, Model, ModelRouting, ModelRoutingDecision, ModelRoutingSource, ModelSelection,
};

/// One agent's resolution input plus the record of how it was decided.
pub struct AgentRouting {
    /// What to hand [`crate::config::presets::resolve_agent_snapshot`].
    pub selection: Option<LaunchSelectionOverride>,
    pub decision: ModelRoutingDecision,
}

/// The resolution plan for one launch.
pub struct LaunchSelectionPlan {
    /// The execution-wide backend override, orthogonal to every tier decision.
    backend: Option<String>,
    table: ModelRoutingTable,
    labels: IssueLabels,
    /// Agents a human already chose a model for. The table is not consulted for
    /// these, and nothing here can move them.
    pinned: HashSet<String>,
    /// Configured tier names in ascending capability order, for ranking a rule's
    /// tier against an agent's authored one.
    tiers: Vec<String>,
    decisions: RefCell<BTreeMap<String, ModelRoutingDecision>>,
}

impl LaunchSelectionPlan {
    pub fn new(
        backend: Option<String>,
        table: ModelRoutingTable,
        labels: IssueLabels,
        pinned: HashSet<String>,
        presets: &PresetsConfig,
    ) -> Self {
        Self {
            backend,
            table,
            labels,
            pinned,
            tiers: presets.tiers.clone(),
            decisions: RefCell::new(BTreeMap::new()),
        }
    }

    /// A plan with nothing to route: a manual execution has no issue, so it has
    /// no labels, and its behavior is exactly what it was before routing existed.
    pub fn empty(backend: Option<String>) -> Self {
        Self {
            backend,
            table: ModelRoutingTable::default(),
            labels: IssueLabels::default(),
            pinned: HashSet::new(),
            tiers: Vec::new(),
            decisions: RefCell::new(BTreeMap::new()),
        }
    }

    /// Resolve one agent, recording the decision.
    ///
    /// `authored_tier` is the agent config's own tier, which the demotion gate
    /// compares against -- the comparison is per-agent precisely because "below"
    /// only means something relative to what the agent would otherwise have run.
    pub fn for_agent(&self, agent_id: &str, authored_tier: Option<&str>) -> AgentRouting {
        let decision = self.decide(agent_id, authored_tier);
        let selection = match (decision.source, &decision.tier) {
            (ModelRoutingSource::LabelBinding, Some(tier)) => {
                Some(LaunchSelectionOverride::Tier(tier.clone()))
            }
            _ => self.backend.clone().map(LaunchSelectionOverride::Backend),
        };
        self.decisions
            .borrow_mut()
            .entry(agent_id.to_string())
            .or_insert_with(|| decision.clone());
        AgentRouting {
            selection,
            decision,
        }
    }

    fn decide(&self, agent_id: &str, authored_tier: Option<&str>) -> ModelRoutingDecision {
        if self.pinned.contains(agent_id) {
            return ModelRoutingDecision {
                source: ModelRoutingSource::ExplicitLaunch,
                rule: None,
                matched_labels: Vec::new(),
                tier: None,
                note: "pinned at launch".to_string(),
            };
        }

        let Some((rule, matched)) = self.first_match(agent_id) else {
            let note = if self.labels.is_empty() {
                "no labels on issue"
            } else {
                "no rule matched"
            };
            return ModelRoutingDecision {
                source: ModelRoutingSource::AgentDefault,
                rule: None,
                matched_labels: Vec::new(),
                tier: None,
                note: note.to_string(),
            };
        };

        if let Some(refusal) = self.demotion_refusal(rule, authored_tier) {
            return ModelRoutingDecision {
                source: ModelRoutingSource::DemotionRefused,
                rule: Some(rule.id.clone()),
                matched_labels: matched,
                tier: Some(rule.tier.clone()),
                note: refusal,
            };
        }

        let mut note = format!("rule '{}' matched {}", rule.id, matched.join(", "));
        if let Some(because) = rule
            .because
            .as_deref()
            .map(str::trim)
            .filter(|b| !b.is_empty())
        {
            note.push_str(" -- ");
            note.push_str(because);
        }
        ModelRoutingDecision {
            source: ModelRoutingSource::LabelBinding,
            rule: Some(rule.id.clone()),
            matched_labels: matched,
            tier: Some(self.compose(&rule.tier)),
            note,
        }
    }

    /// The first rule scoped to this agent whose predicate the issue satisfies.
    /// Order is the table author's explicit precedence; later rules do not merge.
    fn first_match(&self, agent_id: &str) -> Option<(&RoutingRule, Vec<String>)> {
        self.table.rules.iter().find_map(|rule| {
            if !rule.applies_to(agent_id) {
                return None;
            }
            rule.when
                .matches(&self.labels)
                .map(|matched| (rule, matched))
        })
    }

    /// Why this rule must not be applied to an agent authored at `authored_tier`,
    /// or `None` when it may be.
    fn demotion_refusal(&self, rule: &RoutingRule, authored_tier: Option<&str>) -> Option<String> {
        let authored = authored_tier
            .map(|tier| parse_tier_ref(tier).1)
            .filter(|tier| !tier.is_empty())
            .unwrap_or(DEFAULT_TIER);
        let target = parse_tier_ref(&rule.tier).1;
        if target.eq_ignore_ascii_case(authored) {
            return None;
        }

        let rank = |tier: &str| self.tiers.iter().position(|known| known == tier);
        let (Some(authored_rank), Some(target_rank)) = (rank(authored), rank(target)) else {
            // An unrankable pair cannot be shown to be a promotion, and the
            // standing answer to anything unverifiable is to leave the agent
            // where its config put it.
            return Some(format!(
                "rule '{}' names tier '{}', which cannot be ranked against this agent's '{}'; an unverifiable comparison is not applied",
                rule.id, rule.tier, authored
            ));
        };
        if target_rank >= authored_rank {
            return None;
        }

        if let Some(label) = rule.when.cites_never_demote() {
            return Some(format!(
                "rule '{}' would lower this agent from '{authored}' to '{target}' on '{label}', which never demotes: it is over-applied from issue text (precision 32% issue-only, 55% even with a plan)",
                rule.id
            ));
        }
        if !self.table.allow_demotion {
            return Some(format!(
                "rule '{}' would lower this agent from '{authored}' to '{target}'; the demotion gate is closed, so the agent's own tier stands",
                rule.id
            ));
        }
        None
    }

    /// Fold the execution-wide backend override into a tier ref. The override is
    /// the more explicit input, so it replaces a qualifier the rule carried.
    fn compose(&self, tier: &str) -> String {
        match &self.backend {
            Some(backend) => format!("{backend}/{}", parse_tier_ref(tier).1),
            None => tier.to_string(),
        }
    }

    /// Note whether a human's pin kept the model the table would have suggested.
    ///
    /// Every launch through the composer pins every agent, because the model the
    /// user saw when they pressed Start must be the model that runs. Left alone,
    /// that would record every UI launch as an undifferentiated `explicitLaunch`
    /// and lose the one signal calibration actually wants: whether the operator
    /// kept the routed model or corrected it.
    pub fn annotate_pin(
        &self,
        agent_id: &str,
        authored_tier: Option<&str>,
        pinned: &ModelSelection,
        preferred_backend: Option<&str>,
        presets: &PresetsConfig,
    ) {
        let Some((rule, matched)) = self.first_match(agent_id) else {
            return;
        };
        if self.demotion_refusal(rule, authored_tier).is_some() {
            return;
        }
        let routed_tier = self.compose(&rule.tier);
        let Ok(routed) =
            resolve_selection_with_provenance(Some(&routed_tier), None, preferred_backend, presets)
        else {
            return;
        };
        let same = routed.selection.backend == pinned.backend
            && routed.selection.model.as_str() == pinned.model.as_str();
        let mut decisions = self.decisions.borrow_mut();
        let Some(decision) = decisions.get_mut(agent_id) else {
            return;
        };
        if decision.source != ModelRoutingSource::ExplicitLaunch {
            return;
        }
        decision.rule = Some(rule.id.clone());
        decision.matched_labels = matched;
        decision.tier = Some(routed_tier.clone());
        decision.note = if same {
            format!(
                "pinned at launch -- matches the routed suggestion (rule '{}')",
                rule.id
            )
        } else {
            format!(
                "pinned at launch -- differs from the routed suggestion (rule '{}' would have used '{routed_tier}')",
                rule.id
            )
        };
    }

    /// Cover every agent the frozen snapshot actually carries.
    ///
    /// Resolution reaches most agents through [`Self::for_agent`], but a launch
    /// may also insert an agent the resolver never saw -- a composer sending a
    /// concrete snapshot for a node it added. Walking the finished agent map
    /// makes "provenance covers exactly what this execution runs" true by
    /// construction rather than by every call site remembering to record.
    pub fn record_frozen(&self, agents: &HashMap<String, AgentSnapshot>, presets: &PresetsConfig) {
        for (agent_id, agent) in agents {
            let authored = agent.tier.as_ref().map(Model::as_str);
            self.for_agent(agent_id, authored);
            if let Some(selection) = &agent.selection {
                self.annotate_pin(
                    agent_id,
                    authored,
                    selection,
                    agent.backend_preference.as_deref(),
                    presets,
                );
            }
        }
    }

    /// The durable record of everything this plan decided.
    pub fn into_provenance(self) -> ModelRouting {
        ModelRouting {
            labels: self.labels.slugs(),
            generation: self.table.generation,
            rule_count: self.table.rules.len(),
            decisions: self.decisions.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::agents::FileAgent;
    use crate::config::presets::{default_presets_config, resolve_agent_snapshot};

    fn presets() -> PresetsConfig {
        default_presets_config(Some(31999))
    }

    fn table(yaml: &str) -> ModelRoutingTable {
        let table: ModelRoutingTable = serde_yaml::from_str(yaml).expect("test table parses");
        table.validate().expect("test table validates");
        table
    }

    fn labels(slugs: &[&str]) -> IssueLabels {
        IssueLabels::new(
            slugs
                .iter()
                .map(|slug| (slug.to_string(), slug.to_string())),
        )
    }

    fn plan(table: ModelRoutingTable, labels: IssueLabels) -> LaunchSelectionPlan {
        LaunchSelectionPlan::new(None, table, labels, HashSet::new(), &presets())
    }

    fn file_agent(id: &str, tier: Option<&str>) -> FileAgent {
        FileAgent {
            id: id.to_string(),
            name: id.to_string(),
            description: String::new(),
            prompt: "work".to_string(),
            tools: Vec::new(),
            tier: tier.map(Model::new),
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            icon: None,
            bundles: Vec::new(),
            is_project_scoped: true,
            file_path: std::path::PathBuf::new(),
        }
    }

    const MIGRATION_BUILDER: &str = "rules:\n  - id: migration-builder\n    when:\n      all: [migration]\n    agents: [builder]\n    tier: lg\n";

    /// The abstention answer is the agent's own tier, and the record says which
    /// kind of abstention it was: an issue nobody classified reads differently
    /// from an issue the table simply had nothing to say about.
    #[test]
    fn no_labels_falls_through_to_the_agent_default() {
        let plan = plan(table(MIGRATION_BUILDER), IssueLabels::default());
        let routing = plan.for_agent("builder", Some("md"));
        assert!(routing.selection.is_none());
        assert_eq!(routing.decision.source, ModelRoutingSource::AgentDefault);
        assert_eq!(routing.decision.note, "no labels on issue");
    }

    #[test]
    fn labels_with_no_matching_rule_falls_through_to_the_agent_default() {
        let plan = plan(table(MIGRATION_BUILDER), labels(&["bug-fix", "rust"]));
        let routing = plan.for_agent("builder", Some("md"));
        assert!(routing.selection.is_none());
        assert_eq!(routing.decision.source, ModelRoutingSource::AgentDefault);
        assert_eq!(routing.decision.note, "no rule matched");
    }

    #[test]
    fn a_rule_scoped_to_one_agent_leaves_the_others_alone() {
        let plan = plan(table(MIGRATION_BUILDER), labels(&["migration"]));
        let builder = plan.for_agent("builder", Some("md"));
        assert!(matches!(
            builder.selection,
            Some(LaunchSelectionOverride::Tier(ref tier)) if tier == "lg"
        ));
        assert_eq!(builder.decision.source, ModelRoutingSource::LabelBinding);
        assert_eq!(builder.decision.rule.as_deref(), Some("migration-builder"));
        assert_eq!(
            builder.decision.matched_labels,
            vec!["migration".to_string()]
        );

        let review = plan.for_agent("review", Some("md"));
        assert!(review.selection.is_none());
        assert_eq!(review.decision.source, ModelRoutingSource::AgentDefault);
    }

    /// Order is the table author's explicit precedence; a later rule does not
    /// merge with or override an earlier match.
    #[test]
    fn first_matching_rule_wins() {
        let plan = plan(
            table(
                "rules:\n  - id: first\n    when:\n      all: [migration]\n    tier: lg\n  - id: second\n    when:\n      all: [migration]\n    tier: sm\n",
            ),
            labels(&["migration"]),
        );
        let routing = plan.for_agent("builder", Some("md"));
        assert_eq!(routing.decision.rule.as_deref(), Some("first"));
        let provenance = plan.into_provenance();
        assert_eq!(
            provenance.decisions["builder"].rule.as_deref(),
            Some("first")
        );
    }

    /// A backend override and a label tier are orthogonal inputs that compose
    /// into a qualified tier ref, which the resolver already understands.
    #[test]
    fn backend_override_and_label_tier_compose_into_a_qualified_ref() {
        let presets = presets();
        let plan = LaunchSelectionPlan::new(
            Some("codex".to_string()),
            table(MIGRATION_BUILDER),
            labels(&["migration"]),
            HashSet::new(),
            &presets,
        );
        let routing = plan.for_agent("builder", Some("md"));
        assert!(matches!(
            routing.selection,
            Some(LaunchSelectionOverride::Tier(ref tier)) if tier == "codex/lg"
        ));
        let snapshot = resolve_agent_snapshot(
            &file_agent("builder", Some("md")),
            routing.selection.as_ref(),
            &presets,
        )
        .expect("a qualified tier ref resolves");
        let selection = snapshot.selection.expect("resolved selection");
        assert_eq!(selection.backend, "codex");
        assert_eq!(selection.model.as_str(), "gpt-5.6-sol");
    }

    /// An agent a human chose a model for is not routed at all, and the backend
    /// override still reaches it -- that override is orthogonal to the tier.
    #[test]
    fn a_pinned_agent_is_never_routed() {
        let plan = LaunchSelectionPlan::new(
            Some("codex".to_string()),
            table(MIGRATION_BUILDER),
            labels(&["migration"]),
            HashSet::from(["builder".to_string()]),
            &presets(),
        );
        let routing = plan.for_agent("builder", Some("md"));
        assert!(matches!(
            routing.selection,
            Some(LaunchSelectionOverride::Backend(ref backend)) if backend == "codex"
        ));
        assert_eq!(routing.decision.source, ModelRoutingSource::ExplicitLaunch);
        assert!(routing.decision.rule.is_none());
    }

    #[test]
    fn a_demoting_rule_is_refused_while_the_gate_is_closed() {
        let plan = plan(
            table(
                "rules:\n  - id: cheap-docs\n    when:\n      all: [documentation]\n    tier: sm\n",
            ),
            labels(&["documentation"]),
        );
        let routing = plan.for_agent("builder", Some("lg"));
        assert!(routing.selection.is_none(), "the agent keeps its own tier");
        assert_eq!(routing.decision.source, ModelRoutingSource::DemotionRefused);
        assert_eq!(routing.decision.rule.as_deref(), Some("cheap-docs"));
        assert!(
            routing.decision.note.contains("gate is closed"),
            "{}",
            routing.decision.note
        );
    }

    #[test]
    fn the_same_rule_applies_once_the_gate_is_open() {
        let plan = plan(
            table("allowDemotion: true\nrules:\n  - id: cheap-docs\n    when:\n      all: [documentation]\n    tier: sm\n"),
            labels(&["documentation"]),
        );
        let routing = plan.for_agent("builder", Some("lg"));
        assert!(matches!(
            routing.selection,
            Some(LaunchSelectionOverride::Tier(ref tier)) if tier == "sm"
        ));
        assert_eq!(routing.decision.source, ModelRoutingSource::LabelBinding);
    }

    /// `investigation` is over-applied from issue text, and no amount of
    /// capability data repairs a label that was wrong about the task -- so its
    /// bar outlives the general gate.
    #[test]
    fn investigation_never_demotes_even_with_the_gate_open() {
        let plan = plan(
            table("allowDemotion: true\nrules:\n  - id: cheap-investigation\n    when:\n      all: [investigation]\n    tier: sm\n"),
            labels(&["investigation"]),
        );
        let routing = plan.for_agent("builder", Some("lg"));
        assert!(routing.selection.is_none());
        assert_eq!(routing.decision.source, ModelRoutingSource::DemotionRefused);
        assert!(
            routing.decision.note.contains("never demotes"),
            "{}",
            routing.decision.note
        );
    }

    /// Promoting the same agent the rule would otherwise demote is unaffected by
    /// the gate: the gate is about lowering a tier, not about routing at all.
    #[test]
    fn a_promoting_rule_applies_with_the_gate_closed() {
        let plan = plan(table(MIGRATION_BUILDER), labels(&["migration"]));
        let routing = plan.for_agent("builder", Some("sm"));
        assert!(matches!(
            routing.selection,
            Some(LaunchSelectionOverride::Tier(ref tier)) if tier == "lg"
        ));
    }

    /// An agent whose authored selection is a concrete model cannot be ranked
    /// against a tier, and an unverifiable comparison defaults up.
    #[test]
    fn an_unrankable_authored_tier_is_left_alone() {
        let plan = plan(
            table("rules:\n  - id: docs\n    when:\n      all: [documentation]\n    tier: sm\n"),
            labels(&["documentation"]),
        );
        let routing = plan.for_agent("builder", Some("some-custom-model"));
        assert!(routing.selection.is_none());
        assert_eq!(routing.decision.source, ModelRoutingSource::DemotionRefused);
        assert!(
            routing.decision.note.contains("cannot be ranked"),
            "{}",
            routing.decision.note
        );
    }

    #[test]
    fn provenance_carries_the_labels_generation_and_rule_count() {
        let plan = plan(
            table(&format!("generation: gen-test\n{MIGRATION_BUILDER}")),
            labels(&["rust", "migration"]),
        );
        plan.for_agent("builder", Some("md"));
        let provenance = plan.into_provenance();
        assert_eq!(
            provenance.labels,
            vec!["migration".to_string(), "rust".to_string()],
            "labels are recorded sorted"
        );
        assert_eq!(provenance.generation.as_deref(), Some("gen-test"));
        assert_eq!(provenance.rule_count, 1);
        assert_eq!(provenance.decisions.len(), 1);
    }

    /// An empty plan is the pre-routing behavior exactly: it decides nothing and
    /// records that it asserted nothing.
    #[test]
    fn an_empty_plan_routes_nothing_and_claims_nothing() {
        let plan = LaunchSelectionPlan::empty(None);
        let routing = plan.for_agent("builder", Some("md"));
        assert!(routing.selection.is_none());
        let provenance = plan.into_provenance();
        assert_eq!(provenance.rule_count, 0);
        assert!(provenance.labels.is_empty());
        assert!(provenance.generation.is_none());
    }

    /// The composer pins every agent on every UI launch, so without this the
    /// record could not tell "the operator kept the routed model" from "the
    /// operator corrected it" -- which is exactly the acceptance signal the
    /// calibration loop needs.
    #[test]
    fn a_pin_records_whether_it_matches_the_routed_suggestion() {
        let presets = presets();
        let plan = LaunchSelectionPlan::new(
            None,
            table(MIGRATION_BUILDER),
            labels(&["migration"]),
            HashSet::from(["builder".to_string()]),
            &presets,
        );
        plan.for_agent("builder", Some("md"));

        let routed = resolve_agent_snapshot(&file_agent("builder", Some("lg")), None, &presets)
            .unwrap()
            .selection
            .unwrap();
        plan.annotate_pin("builder", Some("md"), &routed, None, &presets);
        let kept = plan.into_provenance();
        assert_eq!(
            kept.decisions["builder"].source,
            ModelRoutingSource::ExplicitLaunch
        );
        assert!(
            kept.decisions["builder"]
                .note
                .contains("matches the routed suggestion"),
            "{}",
            kept.decisions["builder"].note
        );

        let plan = LaunchSelectionPlan::new(
            None,
            table(MIGRATION_BUILDER),
            labels(&["migration"]),
            HashSet::from(["builder".to_string()]),
            &presets,
        );
        plan.for_agent("builder", Some("md"));
        let corrected = resolve_agent_snapshot(&file_agent("builder", Some("sm")), None, &presets)
            .unwrap()
            .selection
            .unwrap();
        plan.annotate_pin("builder", Some("md"), &corrected, None, &presets);
        let changed = plan.into_provenance();
        assert!(
            changed.decisions["builder"]
                .note
                .contains("differs from the routed suggestion"),
            "{}",
            changed.decisions["builder"].note
        );
    }
}
