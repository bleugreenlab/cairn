//! Resolve an execution's branch target into recipe topology, once, at launch.
//!
//! The branch target is one property of the execution — where its work lands —
//! and every downstream derivation reads the snapshot, not the recipe file. So
//! the whole difference between "mint a branch and ship a PR" and "work on the
//! base branch" is a transform over the snapshot graph: agent nodes lose their
//! branch, and terminal PR nodes are pruned. The standing-node behaviour then
//! falls out of the existing topology derivation (`is_long_running_node`) with
//! no special-casing anywhere else.

use std::collections::HashSet;

use crate::models::{
    AgentGitConfig, AgentNodeConfig, BranchMode, BranchTarget, ExecutionSnapshot, RecipeNodeType,
};

/// Transform `snapshot` in place for `target`, which must be one the recipe
/// declares (`branchTargets` in the recipe file).
///
/// `New` is the identity: agent nodes keep their authored branch mode and the
/// terminal PR node ships the branch they mint. `Base` rewrites every agent node
/// to [`BranchMode::None`] and drops every `pr` node together with its edges —
/// legal only because a recipe may declare `base` only when each of its PR nodes
/// is control-terminal (enforced by `RecipeFile::validate`).
pub fn apply_branch_target(
    snapshot: &mut ExecutionSnapshot,
    target: BranchTarget,
    declared: &[BranchTarget],
) -> Result<(), String> {
    if !declared.contains(&target) {
        let supported = declared
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "Recipe '{}' does not support branch target '{target}' (supported: {supported})",
            snapshot.recipe.name
        ));
    }

    if target == BranchTarget::Base {
        for node in &mut snapshot.recipe.nodes {
            if node.node_type != RecipeNodeType::Agent {
                continue;
            }
            let agent_config = node.agent_config.get_or_insert(AgentNodeConfig {
                agent_config_id: None,
                output_schema: None,
                git_config: None,
            });
            agent_config
                .git_config
                .get_or_insert_with(AgentGitConfig::default)
                .branch_mode = BranchMode::None;
        }

        let pruned: HashSet<String> = snapshot
            .recipe
            .nodes
            .iter()
            .filter(|node| node.node_type == RecipeNodeType::Pr)
            .map(|node| node.id.clone())
            .collect();
        snapshot
            .recipe
            .nodes
            .retain(|node| !pruned.contains(&node.id));
        snapshot.recipe.edges.retain(|edge| {
            !pruned.contains(&edge.source_node_id) && !pruned.contains(&edge.target_node_id)
        });
    }

    snapshot.branch_target = target;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        ExecutionSnapshot, RecipeFile, RecipeSnapshot, TriggerContext, TriggerType,
    };
    use std::collections::HashMap;

    const COORDINATOR_YAML: &str = include_str!("../../../../recipes/coordinator.yaml");

    fn snapshot_from_yaml(yaml: &str) -> (ExecutionSnapshot, Vec<BranchTarget>) {
        let recipe = RecipeFile::from_yaml(yaml)
            .expect("bundled recipe parses")
            .into_recipe(Some("default".to_string()), None);
        let declared = recipe.branch_targets.clone();
        let snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: recipe.id.clone(),
                name: recipe.name.clone(),
                description: recipe.description.clone(),
                trigger: recipe.trigger.clone(),
                nodes: recipe.nodes.clone(),
                edges: recipe.edges.clone(),
            },
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: None,
                project_id: "p".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        );
        (snapshot, declared)
    }

    /// The coordinator agent node's id. `into_recipe` reassigns node ids to fresh
    /// UUIDs, so nodes are keyed by their agent config instead.
    fn coordinator_node_id(snapshot: &ExecutionSnapshot) -> String {
        snapshot
            .recipe
            .nodes
            .iter()
            .find(|node| {
                node.agent_config
                    .as_ref()
                    .and_then(|c| c.agent_config_id.as_deref())
                    == Some("coordinator")
            })
            .expect("coordinator agent node")
            .id
            .clone()
    }

    #[test]
    fn new_target_is_the_identity_transform() {
        let (mut snapshot, declared) = snapshot_from_yaml(COORDINATOR_YAML);
        let before = snapshot.recipe.clone();
        apply_branch_target(&mut snapshot, BranchTarget::New, &declared).unwrap();

        assert_eq!(snapshot.branch_target, BranchTarget::New);
        assert_eq!(
            serde_json::to_value(&snapshot.recipe).unwrap(),
            serde_json::to_value(&before).unwrap(),
            "the new target leaves the graph untouched"
        );
        // Untransformed, the coordinator ships a PR: it is not long-running.
        let node_id = coordinator_node_id(&snapshot);
        assert!(!crate::execution::jobs::is_long_running_node(
            &snapshot, &node_id, false
        ));
    }

    #[test]
    fn base_target_drops_branches_and_prunes_the_pr_node() {
        let (mut snapshot, declared) = snapshot_from_yaml(COORDINATOR_YAML);
        apply_branch_target(&mut snapshot, BranchTarget::Base, &declared).unwrap();

        assert_eq!(snapshot.branch_target, BranchTarget::Base);
        assert!(
            !snapshot
                .recipe
                .nodes
                .iter()
                .any(|node| node.node_type == RecipeNodeType::Pr),
            "every PR node is pruned"
        );

        let node_ids: HashSet<&str> = snapshot
            .recipe
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect();
        for edge in &snapshot.recipe.edges {
            assert!(
                node_ids.contains(edge.source_node_id.as_str())
                    && node_ids.contains(edge.target_node_id.as_str()),
                "pruning left a dangling edge: {edge:?}"
            );
        }

        for node in &snapshot.recipe.nodes {
            if node.node_type != RecipeNodeType::Agent {
                continue;
            }
            assert_eq!(
                node.agent_config
                    .as_ref()
                    .and_then(|c| c.git_config.as_ref())
                    .map(|g| g.branch_mode.clone()),
                Some(BranchMode::None),
                "agent node '{}' stays on the base branch",
                node.name
            );
        }

        // The whole point: with no terminal action node and a contract-less
        // control-terminal coordinator, the standing-mission behaviour falls out
        // of the existing topology derivation.
        let node_id = coordinator_node_id(&snapshot);
        assert!(crate::execution::jobs::is_long_running_node(
            &snapshot, &node_id, false
        ));

        // The living board survives: context-self edges are never touched.
        assert!(snapshot
            .recipe
            .nodes
            .iter()
            .any(|node| node.node_type == RecipeNodeType::Artifact));
    }

    #[test]
    fn an_undeclared_target_is_rejected() {
        let (mut snapshot, _) = snapshot_from_yaml(COORDINATOR_YAML);
        let error = apply_branch_target(&mut snapshot, BranchTarget::Base, &[BranchTarget::New])
            .expect_err("a recipe that declares only `new` refuses `base`");
        assert!(
            error.contains("does not support branch target 'base'"),
            "{error}"
        );
        // A rejected target leaves the graph exactly as it was.
        assert!(snapshot
            .recipe
            .nodes
            .iter()
            .any(|node| node.node_type == RecipeNodeType::Pr));
        assert_eq!(snapshot.branch_target, BranchTarget::New);
    }
}
