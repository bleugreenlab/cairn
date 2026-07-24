//! Single-source resource contract table.
//!
//! Every Cairn resource is described once here: its URI template, read-side
//! description and query projections, and its supported `write` mutations
//! (mode + required/optional payload keys + a copy-paste example). Every other
//! surface — the MCP `write` dispatcher's gate, the affordance blocks rendered
//! on read, and the advertised resource templates — is a projection of this
//! table.
//!
//! Adding a mutable resource is two edits: a table entry here and a dispatch
//! arm in `cairn-core`. The `contract_mutations_all_have_dispatch_arms` parity
//! test in `cairn-core` (`resources/mutations/dispatch.rs`) pins the two
//! together so neither can drift without failing CI — it must live there, not
//! here, because `cairn-common` cannot see `CairnResource` or the dispatcher.

mod globals;
mod issues;
mod jobs;
mod nodes;
mod projects;
mod registry;
mod specs;
mod tasks;
mod types;
mod workspace;

pub use specs::BROWSER_ACTIONS;
pub use types::*;

/// Look up the contract for a kind.
pub fn contract_for(kind: ResourceKind) -> Option<&'static ResourceContract> {
    RESOURCE_CONTRACTS
        .iter()
        .find(|contract| contract.kind == kind)
}

/// Look up the mutation spec for a `(kind, mode)` pair. `None` means the
/// dispatcher must reject the mutation.
pub fn mutation_spec(kind: ResourceKind, mode: ChangeMode) -> Option<&'static MutationSpec> {
    contract_for(kind).and_then(|contract| contract.mutation(mode))
}

// ============================================================================
// The table
// ============================================================================

pub const RESOURCE_CONTRACTS: &[ResourceContract] = &[
    globals::DB_CONTRACT,
    globals::DEV_CONTRACT,
    globals::DEV_DB_CONTRACT,
    globals::DEV_PID_CONTRACT,
    globals::LOGS_CONTRACT,
    globals::MCP_CONTRACT,
    globals::HELP_CONTRACT,
    globals::WEB_SEARCH_CONTRACT,
    projects::PROJECT_CONTRACT,
    projects::SETTINGS_CONTRACT,
    projects::PROJECTS_CONTRACT,
    projects::PROJECT_SETTINGS_CONTRACT,
    projects::PROJECT_ISSUES_CONTRACT,
    projects::PROJECT_MESSAGES_CONTRACT,
    projects::PROJECT_TERMINAL_CONTRACT,
    projects::PROJECT_BROWSER_CONTRACT,
    projects::PROJECT_BROWSER_NETWORK_REQUEST_CONTRACT,
    issues::ISSUE_CONTRACT,
    issues::CHANGED_CONTRACT,
    issues::ISSUE_EXECUTIONS_CONTRACT,
    issues::ISSUE_EXECUTION_CONTRACT,
    issues::ISSUE_MESSAGES_CONTRACT,
    issues::ISSUE_COMMENTS_CONTRACT,
    issues::ISSUE_COMMENT_CONTRACT,
    nodes::NODE_CONTRACT,
    nodes::NODE_MESSAGES_CONTRACT,
    nodes::NODE_PROGRESS_CONTRACT,
    nodes::NODE_CHAT_CONTRACT,
    nodes::NODE_CHAT_RAW_CONTRACT,
    nodes::NODE_CHAT_TURN_CONTRACT,
    nodes::NODE_CHAT_EVENT_CONTRACT,
    nodes::NODE_ARTIFACT_CONTRACT,
    nodes::NODE_DIFF_CONTRACT,
    nodes::NODE_TERMINAL_CONTRACT,
    nodes::NODE_REPL_CONTRACT,
    nodes::NODE_BROWSER_CONTRACT,
    nodes::NODE_BROWSER_NETWORK_REQUEST_CONTRACT,
    tasks::TASK_TERMINAL_CONTRACT,
    tasks::TASK_BROWSER_CONTRACT,
    tasks::TASK_BROWSER_NETWORK_REQUEST_CONTRACT,
    tasks::TASK_CONTRACT,
    tasks::TASK_MESSAGES_CONTRACT,
    tasks::TASK_CHAT_CONTRACT,
    tasks::TASK_CHAT_RAW_CONTRACT,
    tasks::TASK_CHAT_TURN_CONTRACT,
    tasks::TASK_CHAT_EVENT_CONTRACT,
    tasks::TASK_ARTIFACT_CONTRACT,
    jobs::JOB_TODOS_CONTRACT,
    jobs::NODE_CHECKS_CONTRACT,
    jobs::TASK_CHECKS_CONTRACT,
    jobs::NODE_WAKES_CONTRACT,
    jobs::NODE_TASKS_CONTRACT,
    jobs::NODE_CALLS_CONTRACT,
    jobs::NODE_QUESTIONS_CONTRACT,
    jobs::NODE_QUESTION_CONTRACT,
    jobs::NODE_PERMISSIONS_CONTRACT,
    jobs::NODE_PERMISSION_CONTRACT,
    jobs::TASK_PERMISSIONS_CONTRACT,
    jobs::TASK_PERMISSION_CONTRACT,
    workspace::BUG_CONTRACT,
    workspace::SKILLS_CONTRACT,
    workspace::SKILL_CONTRACT,
    workspace::PROJECT_SKILLS_CONTRACT,
    workspace::PROJECT_SKILL_CONTRACT,
    workspace::PROJECT_REFERENCES_CONTRACT,
    workspace::PROJECT_REFERENCE_CONTRACT,
    workspace::LABELS_CONTRACT,
    workspace::LABEL_CONTRACT,
    workspace::NODE_SYMBOLS_CONTRACT,
    workspace::PROJECT_SYMBOLS_CONTRACT,
    workspace::NODE_MEMORIES_CONTRACT,
    workspace::NODE_MEMORY_CONTRACT,
    registry::RECIPES_CONTRACT,
    registry::RECIPE_CONTRACT,
    registry::PROJECT_RECIPES_CONTRACT,
    registry::PROJECT_RECIPE_CONTRACT,
    registry::WORKFLOWS_CONTRACT,
    registry::WORKFLOW_CONTRACT,
    registry::PROJECT_WORKFLOWS_CONTRACT,
    registry::PROJECT_WORKFLOW_CONTRACT,
    registry::AGENTS_CONTRACT,
    registry::AGENT_CONTRACT,
    registry::PROJECT_AGENTS_CONTRACT,
    registry::PROJECT_AGENT_CONTRACT,
    registry::ACTIONS_CONTRACT,
    registry::ACTION_CONTRACT,
    registry::PROJECT_ACTIONS_CONTRACT,
    registry::PROJECT_ACTION_CONTRACT,
];

#[cfg(test)]
mod tests {
    use super::specs::SUBAGENT_TYPE;
    use super::*;

    #[test]
    fn every_kind_has_exactly_one_contract() {
        for kind in ResourceKind::ALL {
            let matches = RESOURCE_CONTRACTS
                .iter()
                .filter(|c| c.kind == *kind)
                .count();
            assert_eq!(matches, 1, "kind {:?} must have exactly one contract", kind);
        }
        assert_eq!(RESOURCE_CONTRACTS.len(), ResourceKind::ALL.len());
    }

    /// Every required key a mutation advertises must appear (by canonical name or
    /// an alias) in its copy-paste `example`, so copying the example verbatim
    /// produces a write that clears the required-key gate. Guards against the
    /// affordance example drifting from the schema it documents (CAIRN #170).
    #[test]
    fn every_mutation_example_names_its_required_keys() {
        for contract in RESOURCE_CONTRACTS {
            for spec in contract.mutations {
                for req in spec.required {
                    let present = spec.example.contains(req.key)
                        || req.aliases.iter().any(|a| spec.example.contains(a));
                    assert!(
                        present,
                        "{:?} {:?} example must name required key `{}`: {}",
                        contract.kind, spec.mode, req.key, spec.example
                    );
                }
            }
        }
    }

    /// Spot-check the inverse for a static-schema resource: every payload key the
    /// label-create example uses is a declared key. This is the property the
    /// schema-aware artifact affordance enforces dynamically (CAIRN #170); here
    /// it is pinned for a representative contract whose keys are fully static.
    #[test]
    fn label_create_example_keys_are_all_declared() {
        let spec = mutation_spec(ResourceKind::Labels, ChangeMode::Create)
            .expect("labels must support create");
        let declared: Vec<&str> = spec
            .required
            .iter()
            .chain(spec.optional)
            .flat_map(|k| std::iter::once(k.key).chain(k.aliases.iter().copied()))
            .collect();
        // Pull the keys out of the single `payload:{...}` object in the example.
        let payload = spec
            .example
            .split_once("payload:{")
            .and_then(|(_, rest)| rest.split_once('}'))
            .map(|(inner, _)| inner)
            .expect("label create example must contain a payload object");
        for field in payload.split(',') {
            let key = field.split(':').next().unwrap_or("").trim();
            assert!(
                declared.contains(&key),
                "label create example key `{key}` is not a declared key: {}",
                spec.example
            );
        }
    }

    #[test]
    fn mutation_spec_lookup_matches_table() {
        assert!(mutation_spec(ResourceKind::ProjectIssues, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::ProjectIssues, ChangeMode::Create).is_none());
        assert!(mutation_spec(ResourceKind::Issue, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::Issue, ChangeMode::Delete).is_some());
        assert!(mutation_spec(ResourceKind::Node, ChangeMode::Append).is_none());
        assert!(mutation_spec(ResourceKind::NodeMessages, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::NodeChat, ChangeMode::Append).is_none());
        assert!(mutation_spec(ResourceKind::Task, ChangeMode::Append).is_none());
        assert!(mutation_spec(ResourceKind::TaskMessages, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::JobTodos, ChangeMode::Replace).is_some());
        assert!(mutation_spec(ResourceKind::IssueExecutions, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::IssueExecutions, ChangeMode::Delete).is_none());
        assert!(mutation_spec(ResourceKind::IssueExecution, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::IssueExecution, ChangeMode::Delete).is_none());
        assert!(mutation_spec(ResourceKind::IssueExecution, ChangeMode::Append).is_none());
    }

    #[test]
    fn settings_family_mutation_matrix() {
        // Settings is patch-only (read-only sections live inside it).
        assert!(mutation_spec(ResourceKind::Settings, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::Settings, ChangeMode::Create).is_none());
        assert!(mutation_spec(ResourceKind::Settings, ChangeMode::Delete).is_none());
        // Projects collection creates; the single Project gains patch (no delete).
        assert!(mutation_spec(ResourceKind::Projects, ChangeMode::Create).is_some());
        assert!(mutation_spec(ResourceKind::Project, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::Project, ChangeMode::Delete).is_none());
        // Project settings is patch-only.
        assert!(mutation_spec(ResourceKind::ProjectSettings, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::ProjectSettings, ChangeMode::Create).is_none());
    }

    #[test]
    fn key_spec_satisfied_by_alias() {
        assert!(SUBAGENT_TYPE.satisfied_by(["subagent_type"].into_iter()));
        assert!(SUBAGENT_TYPE.satisfied_by(["subagentType"].into_iter()));
        assert!(!SUBAGENT_TYPE.satisfied_by(["prompt"].into_iter()));
    }
}
