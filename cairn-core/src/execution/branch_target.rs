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
    AgentGitConfig, AgentNodeConfig, BranchMode, BranchTarget, ExecutionSnapshot, Recipe,
    RecipeNode, RecipeNodeType,
};

/// The target an execution takes when its launch named none: the recipe's FIRST
/// declared target.
///
/// `branchTargets` is ordered by intent — a recipe's first entry is the posture
/// it is written for — so a recipe that declares only `base` (a thread, which
/// owns no branch and ships no PR) is launchable through every path that omits
/// the target: the scheduler, an `executions` append with no `branch` key, a
/// trigger. Reading the default off the recipe rather than off the global
/// [`BranchTarget::default`] changes nothing for the recipes that keep the
/// implicit `[new]`, which is every recipe whose file omits `branchTargets`.
fn default_target(declared: &[BranchTarget]) -> BranchTarget {
    declared.first().cloned().unwrap_or_default()
}

/// Transform `snapshot` in place for `requested`, which must be one the recipe
/// declares (`branchTargets` in the recipe file). `None` — a launch that named no
/// target — resolves to [`default_target`] here rather than at the call site, so
/// no launch path can answer "which target" differently from any other.
///
/// `New` is the identity: agent nodes keep their authored branch mode and the
/// terminal PR node ships the branch they mint. `Base` rewrites every agent node
/// to [`BranchMode::None`] and drops every `pr` node together with its edges —
/// legal only because a recipe may declare `base` only when each of its PR nodes
/// is control-terminal (enforced by `RecipeFile::validate`).
pub fn apply_branch_target(
    snapshot: &mut ExecutionSnapshot,
    requested: Option<BranchTarget>,
    declared: &[BranchTarget],
) -> Result<(), String> {
    let target = requested.unwrap_or_else(|| default_target(declared));
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

    transform_for_target(
        &mut snapshot.recipe.nodes,
        &mut snapshot.recipe.edges,
        target,
    );
    snapshot.branch_target = target;
    Ok(())
}

/// The graph rewrite itself, over bare nodes and edges so the same transform
/// serves a snapshot being launched and a recipe being inspected before one
/// exists. `New` is the identity; `Base` is described on [`apply_branch_target`].
fn transform_for_target(
    nodes: &mut Vec<RecipeNode>,
    edges: &mut Vec<crate::models::RecipeEdge>,
    target: BranchTarget,
) {
    if target != BranchTarget::Base {
        return;
    }
    for node in nodes.iter_mut() {
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

    let pruned: HashSet<String> = nodes
        .iter()
        .filter(|node| node.node_type == RecipeNodeType::Pr)
        .map(|node| node.id.clone())
        .collect();
    nodes.retain(|node| !pruned.contains(&node.id));
    edges.retain(|edge| {
        !pruned.contains(&edge.source_node_id) && !pruned.contains(&edge.target_node_id)
    });
}

/// Why a graph that has already been resolved for its branch target cannot run
/// on a thread, or `None` when it can.
///
/// A thread owns no branch and never opens a pull request — that is the whole
/// content of the kind. So the question is not "is this issue a thread" but
/// "would this execution produce the state the kind forbids", asked of the
/// topology that will actually run. Both are true of a graph resolved to `base`
/// (every agent node lands on [`BranchMode::None`] and every `pr` node is
/// pruned), which is why a thread can run a base-target recipe at all.
pub fn thread_incompatibility(nodes: &[RecipeNode]) -> Option<&'static str> {
    if nodes
        .iter()
        .any(|node| node.node_type == RecipeNodeType::Pr)
    {
        return Some("it would open a pull request");
    }
    if nodes.iter().any(|node| {
        node.node_type == RecipeNodeType::Agent
            && node
                .agent_config
                .as_ref()
                .and_then(|config| config.git_config.as_ref())
                .map(|git| git.branch_mode.clone())
                .unwrap_or_default()
                != BranchMode::None
    }) {
        return Some("it would mint a branch");
    }
    None
}

/// [`thread_incompatibility`] for a recipe that has not been snapshotted yet:
/// resolve the target an unnamed launch would take, apply the same transform,
/// and ask the same question. The create-and-start door refuses before its
/// insert and the execution door refuses after the snapshot exists, but there is
/// one rule between them rather than one per door.
pub fn recipe_thread_incompatibility(recipe: &Recipe) -> Option<&'static str> {
    let mut nodes = recipe.nodes.clone();
    let mut edges = recipe.edges.clone();
    transform_for_target(
        &mut nodes,
        &mut edges,
        default_target(&recipe.branch_targets),
    );
    thread_incompatibility(&nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        ExecutionSnapshot, RecipeFile, RecipeSnapshot, TriggerContext, TriggerType,
    };
    use std::collections::HashMap;

    const COORDINATOR_YAML: &str = include_str!("../../../../recipes/coordinator.yaml");
    const THREAD_YAML: &str = include_str!("../../../../recipes/thread.yaml");
    const BUILD_YAML: &str = include_str!("../../../../recipes/build.yaml");

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
        apply_branch_target(&mut snapshot, Some(BranchTarget::New), &declared).unwrap();

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
        apply_branch_target(&mut snapshot, Some(BranchTarget::Base), &declared).unwrap();

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

    /// A launch that names no target takes the recipe's OWN first declared one,
    /// resolved inside the transform so every launch path — the composer, the
    /// scheduler, a trigger, an `executions` append with no `branch` key —
    /// answers it identically. Both directions are pinned: the recipes that keep
    /// the implicit `[new]` are unaffected, which is what makes the change safe,
    /// and a recipe written for one posture gets that posture instead of being
    /// refused.
    #[test]
    fn an_unnamed_launch_takes_the_recipes_own_default() {
        assert_eq!(default_target(&[]), BranchTarget::New);
        assert_eq!(
            default_target(&[BranchTarget::New, BranchTarget::Base]),
            BranchTarget::New
        );

        // A recipe whose file omits `branchTargets` is untouched: unnamed still
        // means `new`, and `new` is still the identity transform.
        let (mut build, build_declared) = snapshot_from_yaml(BUILD_YAML);
        assert_eq!(build_declared, crate::models::default_branch_targets());
        apply_branch_target(&mut build, None, &build_declared).unwrap();
        assert_eq!(build.branch_target, BranchTarget::New);
        assert!(
            build
                .recipe
                .nodes
                .iter()
                .any(|node| node.node_type == RecipeNodeType::Pr),
            "the identity transform leaves build's PR node in place"
        );

        // A base-only recipe resolves to `base` instead of being refused.
        let (mut thread, thread_declared) = snapshot_from_yaml(THREAD_YAML);
        apply_branch_target(&mut thread, None, &thread_declared).unwrap();
        assert_eq!(thread.branch_target, BranchTarget::Base);
    }

    /// A thread owns no branch, and its recipe says so by declaring `base`
    /// alone: the default resolution lands there, and `new` — which would mint a
    /// branch and hand the thread an ending — is refused.
    #[test]
    fn the_thread_recipe_is_base_only() {
        let (mut snapshot, declared) = snapshot_from_yaml(THREAD_YAML);
        assert_eq!(declared, vec![BranchTarget::Base]);

        apply_branch_target(&mut snapshot, Some(BranchTarget::New), &declared)
            .expect_err("a thread may not mint a branch");
        apply_branch_target(&mut snapshot, None, &declared).unwrap();
        assert_eq!(snapshot.branch_target, BranchTarget::Base);

        let thread = snapshot
            .recipe
            .nodes
            .iter()
            .find(|node| node.node_type == RecipeNodeType::Agent)
            .expect("the thread agent node");
        assert_eq!(
            thread
                .agent_config
                .as_ref()
                .and_then(|c| c.git_config.as_ref())
                .map(|g| g.branch_mode.clone()),
            Some(BranchMode::None)
        );
        // The arc survives the transform: context-self edges are never touched.
        assert!(snapshot
            .recipe
            .nodes
            .iter()
            .any(|node| node.node_type == RecipeNodeType::Artifact));
    }

    /// The thread rule, asked of the graph rather than of the issue. Pinned over
    /// the recipes Cairn ships so a recipe edit that would quietly let a thread
    /// mint a branch fails here, and so the two doors that ask it (the execution
    /// start and the create-and-start dispatch) keep answering identically.
    #[test]
    fn only_a_branchless_graph_may_run_on_a_thread() {
        let recipe = |yaml| {
            RecipeFile::from_yaml(yaml)
                .expect("bundled recipe parses")
                .into_recipe(Some("default".to_string()), None)
        };

        // The thread recipe resolves to `base`, so it mints nothing and ships
        // nothing: the one bundled recipe a thread may run.
        assert_eq!(recipe_thread_incompatibility(&recipe(THREAD_YAML)), None);

        // Every shipping recipe is refused, naming what it would produce rather
        // than restating the kind.
        assert_eq!(
            recipe_thread_incompatibility(&recipe(BUILD_YAML)),
            Some("it would open a pull request")
        );
        assert_eq!(
            recipe_thread_incompatibility(&recipe(COORDINATOR_YAML)),
            Some("it would open a pull request"),
            "the coordinator declares [new, base] and an unnamed launch takes `new`"
        );

        // A graph with no PR node but an authored branch is refused for the
        // other reason — the branch alone is disqualifying, since owning one is
        // what makes a thing terminal.
        let mut branching = recipe(THREAD_YAML);
        for node in &mut branching.nodes {
            if node.node_type == RecipeNodeType::Agent {
                node.agent_config.as_mut().unwrap().git_config = Some(AgentGitConfig {
                    branch_mode: BranchMode::Isolate,
                    require_parent_head: false,
                });
            }
        }
        assert_eq!(
            thread_incompatibility(&branching.nodes),
            Some("it would mint a branch")
        );

        // And the transform is what makes a shipping recipe eligible: the same
        // coordinator, resolved to `base`, passes the same rule.
        let (mut standing, declared) = snapshot_from_yaml(COORDINATOR_YAML);
        assert!(thread_incompatibility(&standing.recipe.nodes).is_some());
        apply_branch_target(&mut standing, Some(BranchTarget::Base), &declared).unwrap();
        assert_eq!(thread_incompatibility(&standing.recipe.nodes), None);
    }

    #[test]
    fn an_undeclared_target_is_rejected() {
        let (mut snapshot, _) = snapshot_from_yaml(COORDINATOR_YAML);
        let error = apply_branch_target(
            &mut snapshot,
            Some(BranchTarget::Base),
            &[BranchTarget::New],
        )
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
