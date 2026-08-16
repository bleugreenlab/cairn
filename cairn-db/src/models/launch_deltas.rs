//! Ad-hoc launch deltas: the agent-side grammar for adjusting a recipe as it
//! starts, without editing the recipe everyone else runs.
//!
//! The UI composer already customizes a launch by sending a whole replacement
//! graph ([`SnapshotOverrides`]). That is the right grammar for a direct
//! manipulation editor and the wrong one for an agent: a caller that has to
//! author nodes and edges to say "skip review" will sooner or later author a
//! malformed graph. So an agent says what it wants *changed* and this module
//! compiles that into the same [`SnapshotOverrides`] the composer produces, and
//! hands it to the same start path. There is one snapshot-freezing engine; this
//! is a second door into it.
//!
//! Three orthogonal layers, applied in this order:
//!
//! 1. `without` — graph surgery. Named nodes are removed and their edges spliced
//!    through, so a predecessor reconnects to the removed node's successors.
//! 2. `nodes` — role rebinding. A node's agent reference is repointed and its
//!    snapshot is re-resolved from the named agent config exactly as a fresh
//!    launch resolves it. No prompt authoring at launch, ever.
//! 3. `agents` — snapshot-field merges on top of whatever resolution produced,
//!    through the same [`merge_agent_patch`] the post-create snapshot patch uses.
//!
//! ## Addressing a node
//!
//! A recipe's node ids are minted fresh on every load (`RecipeFile::into_recipe`
//! assigns a new UUID per node), so the `builder-1` in a recipe *file* is not a
//! name anything can hold onto. The durable handles are the node's **name** and,
//! for an agent node, the **agent config id** it references — both of which a
//! caller can read off `cairn://recipes/{id}`. A token is matched against the
//! runtime id, then the name, then the agent id, and must land on exactly one
//! node; nothing is silently skipped, because a `without` that quietly matched
//! nothing would run the review the caller thought it had removed.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use super::recipe::{AgentNodeConfig, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType};
use super::snapshot::{
    merge_agent_patch, selection_inputs, AgentSnapshot, ExecutionSnapshot, RecipeSnapshot,
    SnapshotOverrides,
};

/// How a launch customizes the snapshot it freezes. The two grammars are
/// alternatives, not layers: the composer sends a finished graph, an agent sends
/// a delta. Making that a single parameter means no caller can send both and no
/// code has to decide which wins.
pub enum LaunchCustomization {
    /// A replacement graph and agent map, as the UI launch composer sends it.
    Snapshot(SnapshotOverrides),
    /// An agent caller's delta, compiled against the resolved recipe.
    Deltas(LaunchDeltas),
}

impl LaunchCustomization {
    /// The agents this launch chose a model for explicitly.
    ///
    /// Label routing is the input of last resort before an agent's own authored
    /// tier, so it must never contest a human's choice. Rather than letting that
    /// precedence emerge from the order the layers happen to apply in, the launch
    /// path asks here once, up front, and routes only the agents nobody pinned.
    ///
    /// The two grammars express a pin differently. The composer sends a whole
    /// resolved `AgentSnapshot`, where the pin is the concrete `selection` (its
    /// `tier` is the agent config's own pre-fill, carried along rather than
    /// chosen). A delta sends a field patch, where the pin is `selection` or any
    /// of the authored inputs a selection is resolved from -- the same
    /// `SELECTION_INPUTS` that decide when a merge invalidates a frozen
    /// selection, so "what counts as choosing a model" is defined once.
    pub fn pinned_agent_ids(&self) -> HashSet<String> {
        match self {
            LaunchCustomization::Snapshot(overrides) => overrides
                .agents
                .iter()
                .flatten()
                .filter(|(_, agent)| agent.selection.is_some())
                .map(|(id, _)| id.clone())
                .collect(),
            LaunchCustomization::Deltas(deltas) => deltas
                .agents
                .iter()
                .filter(|merge| {
                    merge.patch.contains_key("selection")
                        || selection_inputs()
                            .iter()
                            .any(|key| merge.patch.contains_key(*key))
                })
                .map(|merge| merge.agent_id.clone())
                .collect(),
        }
    }
}

/// The delta keys, in application order.
const DELTA_KEYS: &[&str] = &["without", "nodes", "agents"];

/// Agent-snapshot fields a launch merge may carry. Deliberately a closed list:
/// an unrecognized key is a typo that would otherwise be dropped in silence, and
/// a caller who writes `{model: "opus"}` needs to be told the field is `tier`
/// rather than watch the launch ignore it.
const AGENT_PATCH_KEYS: &[&str] = &[
    "prompt",
    "tier",
    "backend",
    "selection",
    "tools",
    "disallowedTools",
    "skills",
    "extras",
    "description",
];

/// A launch-time delta over a resolved recipe.
#[derive(Debug, Clone, Default)]
pub struct LaunchDeltas {
    without: Vec<String>,
    nodes: Vec<NodeRebind>,
    agents: Vec<AgentMerge>,
}

/// One node's agent reference repointed at a different agent config.
#[derive(Debug, Clone)]
struct NodeRebind {
    token: String,
    agent: String,
}

/// One agent's snapshot-field merge.
#[derive(Debug, Clone)]
struct AgentMerge {
    agent_id: String,
    patch: Map<String, Value>,
}

impl LaunchDeltas {
    /// Parse the `overrides` object off a launch payload. Every refusal here
    /// happens at write time, before an execution row or any job exists.
    pub fn parse(value: &Value) -> Result<Self, String> {
        let object = value.as_object().ok_or_else(|| {
            format!(
                "overrides must be an object with any of {}",
                quoted_list(DELTA_KEYS)
            )
        })?;
        if let Some(unknown) = object
            .keys()
            .find(|key| !DELTA_KEYS.contains(&key.as_str()))
        {
            return Err(format!(
                "overrides.{unknown} is not a launch override key; accepted keys are {}",
                quoted_list(DELTA_KEYS)
            ));
        }

        let mut deltas = LaunchDeltas::default();

        if let Some(value) = present(object.get("without")) {
            let items = value
                .as_array()
                .ok_or("overrides.without must be an array of node names")?;
            for item in items {
                let token = item
                    .as_str()
                    .map(str::trim)
                    .filter(|token| !token.is_empty())
                    .ok_or("overrides.without entries must be non-empty node names")?;
                deltas.without.push(token.to_string());
            }
        }

        if let Some(value) = present(object.get("nodes")) {
            let entries = value.as_object().ok_or(
                "overrides.nodes must be an object of nodeName -> {agent}, e.g. {\"builder\": {\"agent\": \"coordinator\"}}",
            )?;
            for (token, rebind) in entries {
                let rebind = rebind.as_object().ok_or_else(|| {
                    format!("overrides.nodes.{token} must be an object with an `agent` key")
                })?;
                if let Some(unknown) = rebind.keys().find(|key| key.as_str() != "agent") {
                    return Err(format!(
                        "overrides.nodes.{token}.{unknown} is not supported; a node rebind takes only `agent`. To change an agent's settings use overrides.agents"
                    ));
                }
                let agent = rebind
                    .get("agent")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|agent| !agent.is_empty())
                    .ok_or_else(|| {
                        format!(
                            "overrides.nodes.{token}.agent is required and must be an agent config id"
                        )
                    })?;
                deltas.nodes.push(NodeRebind {
                    token: token.trim().to_string(),
                    agent: agent.to_string(),
                });
            }
        }

        if let Some(value) = present(object.get("agents")) {
            let entries = value.as_object().ok_or(
                "overrides.agents must be an object of agentId -> snapshot fields, e.g. {\"build\": {\"tier\": \"opus\"}}",
            )?;
            for (agent_id, patch) in entries {
                let patch = patch.as_object().ok_or_else(|| {
                    format!("overrides.agents.{agent_id} must be an object of snapshot fields")
                })?;
                validate_agent_patch(agent_id, patch)?;
                deltas.agents.push(AgentMerge {
                    agent_id: agent_id.trim().to_string(),
                    patch: patch.clone(),
                });
            }
        }

        Ok(deltas)
    }

    /// Whether this delta asks for nothing, in which case the launch is an
    /// ordinary one and never touches the override path at all.
    pub fn is_empty(&self) -> bool {
        self.without.is_empty() && self.nodes.is_empty() && self.agents.is_empty()
    }

    /// Compile against the recipe as it resolved for this launch, producing the
    /// same [`SnapshotOverrides`] the composer would have sent.
    ///
    /// `resolve_agent` loads and resolves an agent config by id exactly as a
    /// fresh launch does — it is the whole content of a role rebind, and passing
    /// it in keeps this compiler a pure function of the graph plus the delta.
    pub fn compile(
        &self,
        snapshot: &ExecutionSnapshot,
        resolve_agent: &dyn Fn(&str) -> Result<Option<AgentSnapshot>, String>,
    ) -> Result<SnapshotOverrides, String> {
        let mut nodes = snapshot.recipe.nodes.clone();
        let mut edges = snapshot.recipe.edges.clone();
        let mut agents = snapshot.agents.clone();

        self.apply_without(&mut nodes, &mut edges)?;
        self.apply_rebinds(&mut nodes, &mut agents, resolve_agent)?;
        self.apply_agent_merges(&nodes, &mut agents)?;

        Ok(SnapshotOverrides {
            recipe: Some(RecipeSnapshot {
                id: snapshot.recipe.id.clone(),
                name: snapshot.recipe.name.clone(),
                description: snapshot.recipe.description.clone(),
                trigger: snapshot.recipe.trigger.clone(),
                nodes,
                edges,
            }),
            agents: Some(agents),
        })
    }

    /// Remove the named nodes, splicing each one's edges through so the graph
    /// stays connected, and refuse the removals that would leave a graph that
    /// cannot run.
    fn apply_without(
        &self,
        nodes: &mut Vec<RecipeNode>,
        edges: &mut Vec<RecipeEdge>,
    ) -> Result<(), String> {
        if self.without.is_empty() {
            return Ok(());
        }

        let mut removing = HashSet::new();
        for token in &self.without {
            removing.insert(resolve_node_id(nodes, token, "overrides.without")?);
        }
        cascade_dependents(nodes, edges, &mut removing);

        // Refuse before touching the graph, so a rejected launch leaves the
        // caller's recipe exactly as it found it.
        if let Some(trigger) = nodes
            .iter()
            .find(|node| node.node_type == RecipeNodeType::Trigger && removing.contains(&node.id))
        {
            return Err(format!(
                "overrides.without cannot remove the trigger node '{}': it is where the execution starts",
                trigger.name
            ));
        }
        if !nodes
            .iter()
            .any(|node| node.node_type == RecipeNodeType::Agent && !removing.contains(&node.id))
        {
            return Err(
                "overrides.without would remove every agent node, leaving an execution with nothing to run"
                    .to_string(),
            );
        }

        let reachable_before = control_reachable(nodes, edges);
        for node_id in ordered_removals(nodes, &removing) {
            splice_out(nodes, edges, &node_id);
        }
        let reachable_after = control_reachable(nodes, edges);
        if let Some(stranded) = nodes
            .iter()
            .find(|node| reachable_before.contains(&node.id) && !reachable_after.contains(&node.id))
        {
            return Err(format!(
                "overrides.without would disconnect '{}' from the trigger: the removal leaves no control path to it",
                stranded.name
            ));
        }

        Ok(())
    }

    /// Repoint each named node at a different agent config and resolve that
    /// agent's snapshot the way a fresh launch would.
    fn apply_rebinds(
        &self,
        nodes: &mut [RecipeNode],
        agents: &mut HashMap<String, AgentSnapshot>,
        resolve_agent: &dyn Fn(&str) -> Result<Option<AgentSnapshot>, String>,
    ) -> Result<(), String> {
        for rebind in &self.nodes {
            let node_id = resolve_node_id(nodes, &rebind.token, "overrides.nodes")?;
            let node = nodes
                .iter_mut()
                .find(|node| node.id == node_id)
                .expect("resolve_node_id returns an id from this slice");
            if node.node_type != RecipeNodeType::Agent {
                return Err(format!(
                    "overrides.nodes cannot rebind '{}': it is a {} node, and only agent nodes reference an agent",
                    node.name, node.node_type
                ));
            }
            let resolved = resolve_agent(&rebind.agent)?.ok_or_else(|| {
                format!(
                    "overrides.nodes.{}: unknown agent '{}'; list the available agents with cairn://agents",
                    rebind.token, rebind.agent
                )
            })?;
            node.agent_config
                .get_or_insert(AgentNodeConfig {
                    agent_config_id: None,
                    output_schema: None,
                    git_config: None,
                })
                .agent_config_id = Some(rebind.agent.clone());
            agents.insert(rebind.agent.clone(), resolved);
        }
        Ok(())
    }

    /// Merge snapshot fields over the agents the surviving graph actually runs.
    fn apply_agent_merges(
        &self,
        nodes: &[RecipeNode],
        agents: &mut HashMap<String, AgentSnapshot>,
    ) -> Result<(), String> {
        if self.agents.is_empty() {
            return Ok(());
        }
        let in_graph = graph_agent_ids(nodes);
        for merge in &self.agents {
            if !in_graph.contains(&merge.agent_id) {
                let mut available: Vec<&str> = in_graph.iter().map(String::as_str).collect();
                available.sort_unstable();
                return Err(format!(
                    "overrides.agents.{}: this launch runs no such agent; it runs {}",
                    merge.agent_id,
                    quoted_list(&available)
                ));
            }
            let merged = merge_agent_patch(agents.get(&merge.agent_id), &merge.patch)
                .map_err(|error| format!("overrides.agents.{}: {error}", merge.agent_id))?;
            agents.insert(merge.agent_id.clone(), merged);
        }
        Ok(())
    }
}

/// Reject a launch agent patch that names a field this door does not grant.
///
/// `fence` is refused outright, and that is a deliberate answer rather than an
/// inherited one. The post-create snapshot patch permits a cross-execution fence
/// edit, so reusing its rule verbatim would have let any parent hand a child a
/// wider sandbox than the child's own config authorizes — a permission grant
/// smuggled in through an ergonomics feature. A fence is set in the agent's
/// durable config or by a human in the launch composer; the delta grammar exists
/// to skip a review step and pick a model, not to widen a boundary.
fn validate_agent_patch(agent_id: &str, patch: &Map<String, Value>) -> Result<(), String> {
    if patch.contains_key("fence") {
        return Err(format!(
            "overrides.agents.{agent_id}.fence is not settable at launch: a fence is a permission boundary, set in the agent's config or by a human in the launch composer"
        ));
    }
    if let Some(unknown) = patch
        .keys()
        .find(|key| !AGENT_PATCH_KEYS.contains(&key.as_str()))
    {
        let hint = match unknown.as_str() {
            "model" => " (the model tier is `tier`; a concrete backend+model pair is `selection`)",
            "agent" => " (to change which agent a node runs, use overrides.nodes)",
            _ => "",
        };
        return Err(format!(
            "overrides.agents.{agent_id}.{unknown} is not an agent snapshot field{hint}; accepted fields are {}",
            quoted_list(AGENT_PATCH_KEYS)
        ));
    }
    Ok(())
}

/// `None` for both an absent key and an explicit `null`, so a caller that always
/// sends the object shape can null out a layer it does not want.
fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

fn quoted_list<S: AsRef<str>>(items: &[S]) -> String {
    items
        .iter()
        .map(|item| format!("`{}`", item.as_ref()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The agent config id an agent node references, if it has one.
fn node_agent_id(node: &RecipeNode) -> Option<&str> {
    node.agent_config
        .as_ref()
        .and_then(|config| config.agent_config_id.as_deref())
}

fn graph_agent_ids(nodes: &[RecipeNode]) -> HashSet<String> {
    nodes
        .iter()
        .filter(|node| node.node_type == RecipeNodeType::Agent)
        .filter_map(|node| node_agent_id(node).map(str::to_string))
        .collect()
}

/// One tier of [`resolve_node_id`]'s match cascade.
type NodeMatcher<'a> = Box<dyn Fn(&RecipeNode) -> bool + 'a>;

/// Match a caller's token against the runtime node id, then the node name, then
/// the referenced agent config id — each tier case-insensitively and only if the
/// previous one matched nothing, so an exact id always beats a coincidental name.
fn resolve_node_id(nodes: &[RecipeNode], token: &str, key: &str) -> Result<String, String> {
    let tiers: [NodeMatcher; 3] = [
        Box::new(|node: &RecipeNode| node.id == token),
        Box::new(|node: &RecipeNode| node.name.eq_ignore_ascii_case(token)),
        Box::new(|node: &RecipeNode| {
            node_agent_id(node).is_some_and(|agent| agent.eq_ignore_ascii_case(token))
        }),
    ];
    for matches in tiers {
        let found: Vec<&RecipeNode> = nodes.iter().filter(|node| matches(node)).collect();
        match found.len() {
            0 => continue,
            1 => return Ok(found[0].id.clone()),
            _ => {
                return Err(format!(
                    "{key}: '{token}' matches {} nodes in this recipe; address one of them by its node id",
                    found.len()
                ))
            }
        }
    }
    Err(format!(
        "{key}: this recipe has no node '{token}'. Its nodes are {}",
        describe_nodes(nodes)
    ))
}

/// The addressable handles for every node, for a refusal that tells the caller
/// what it could have written instead.
fn describe_nodes(nodes: &[RecipeNode]) -> String {
    nodes
        .iter()
        .map(|node| match node_agent_id(node) {
            Some(agent) => format!(
                "`{}` ({} node running `{agent}`)",
                node.name, node.node_type
            ),
            None => format!("`{}` ({} node)", node.name, node.node_type),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Grow the removal set with the nodes that have no meaning once their
/// principals are gone: slot nodes docked to a removed parent, and artifact
/// nodes every one of whose producers is being removed. An artifact node is a
/// typed schema on a producer's context edge — kept behind, it would sit in the
/// graph waiting on output that can no longer arrive.
fn cascade_dependents(nodes: &[RecipeNode], edges: &[RecipeEdge], removing: &mut HashSet<String>) {
    loop {
        let mut grew = false;
        for node in nodes {
            if removing.contains(&node.id) {
                continue;
            }
            let orphaned_slot = node
                .parent_id
                .as_ref()
                .is_some_and(|parent| removing.contains(parent));
            let orphaned_artifact = node.node_type == RecipeNodeType::Artifact && {
                let mut producers = edges
                    .iter()
                    .filter(|edge| edge.target_node_id == node.id)
                    .map(|edge| &edge.source_node_id)
                    .peekable();
                producers.peek().is_some() && producers.all(|source| removing.contains(source))
            };
            if orphaned_slot || orphaned_artifact {
                removing.insert(node.id.clone());
                grew = true;
            }
        }
        if !grew {
            return;
        }
    }
}

/// Removals in the graph's own node order, so the same delta produces the same
/// graph on every launch regardless of hash iteration order.
fn ordered_removals(nodes: &[RecipeNode], removing: &HashSet<String>) -> Vec<String> {
    nodes
        .iter()
        .filter(|node| removing.contains(&node.id))
        .map(|node| node.id.clone())
        .collect()
}

/// Drop one node and reconnect around it: every incoming edge is joined to every
/// outgoing edge of the same type, keeping the predecessor's source handle and
/// the successor's target handle. A node with no successors simply takes its
/// inbound edges with it, which is how a terminal node (a `pr`) comes off.
fn splice_out(nodes: &mut Vec<RecipeNode>, edges: &mut Vec<RecipeEdge>, node_id: &str) {
    let incoming: Vec<RecipeEdge> = edges
        .iter()
        .filter(|edge| edge.target_node_id == node_id)
        .cloned()
        .collect();
    let outgoing: Vec<RecipeEdge> = edges
        .iter()
        .filter(|edge| edge.source_node_id == node_id)
        .cloned()
        .collect();
    edges.retain(|edge| edge.source_node_id != node_id && edge.target_node_id != node_id);

    for into in &incoming {
        for out in &outgoing {
            if into.edge_type != out.edge_type || into.source_node_id == out.target_node_id {
                continue;
            }
            let spliced = RecipeEdge {
                id: uuid::Uuid::new_v4().to_string(),
                edge_type: into.edge_type.clone(),
                source_node_id: into.source_node_id.clone(),
                source_handle: into.source_handle.clone(),
                target_node_id: out.target_node_id.clone(),
                target_handle: out.target_handle.clone(),
            };
            if edges.iter().any(|edge| same_wire(edge, &spliced)) {
                continue;
            }
            edges.push(spliced);
        }
    }

    nodes.retain(|node| node.id != node_id);
}

/// Two edges connect the same ports. Edge ids are minted per load, so identity
/// is the wire, not the id.
fn same_wire(left: &RecipeEdge, right: &RecipeEdge) -> bool {
    left.edge_type == right.edge_type
        && left.source_node_id == right.source_node_id
        && left.source_handle == right.source_handle
        && left.target_node_id == right.target_node_id
        && left.target_handle == right.target_handle
}

/// The nodes a control path reaches from the trigger. Comparing this before and
/// after the surgery is what "would this disconnect the graph" means in
/// practice: a node that the trigger could reach and now cannot would be minted
/// as a job that never becomes ready.
fn control_reachable(nodes: &[RecipeNode], edges: &[RecipeEdge]) -> HashSet<String> {
    let Some(trigger) = nodes
        .iter()
        .find(|node| node.node_type == RecipeNodeType::Trigger)
    else {
        return HashSet::new();
    };
    let mut seen = HashSet::from([trigger.id.clone()]);
    let mut frontier = vec![trigger.id.clone()];
    while let Some(current) = frontier.pop() {
        for edge in edges {
            if edge.edge_type != RecipeEdgeType::Control || edge.source_node_id != current {
                continue;
            }
            if seen.insert(edge.target_node_id.clone()) {
                frontier.push(edge.target_node_id.clone());
            }
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Model, ModelSelection, RecipeFile, TriggerContext, TriggerType};
    use serde_json::json;

    const BUILD_YAML: &str = include_str!("../../../../packs/core/recipes/build.yaml");
    const PLANBUILD_YAML: &str = include_str!("../../../../packs/core/recipes/planbuild.yaml");
    const COORDINATOR_YAML: &str = include_str!("../../../../packs/core/recipes/coordinator.yaml");

    fn agent_snapshot(id: &str, model: &str) -> AgentSnapshot {
        AgentSnapshot {
            edited_at: None,
            id: id.to_string(),
            name: id.to_string(),
            description: String::new(),
            prompt: format!("prompt for {id}"),
            tools: vec![],
            tier: Some(Model::new(model)),
            backend_preference: None,
            selection: Some(ModelSelection::new("claude", Model::new(model))),
            disallowed_tools: None,
            skills: None,
            fence: Some(crate::models::Fence::Ask),
            sandbox: None,
            on_escape: None,
            extras: None,
            model: None,
            resolved_backend: None,
        }
    }

    /// A snapshot as a launch would have just built it: the recipe resolved from
    /// its file (fresh node ids and all) plus a resolved snapshot for every agent
    /// the graph references.
    fn snapshot_from_yaml(yaml: &str) -> ExecutionSnapshot {
        let recipe = RecipeFile::from_yaml(yaml)
            .expect("bundled recipe parses")
            .into_recipe(Some("default".to_string()), None);
        let agents = recipe
            .nodes
            .iter()
            .filter_map(node_agent_id)
            .map(|id| (id.to_string(), agent_snapshot(id, Model::SONNET)))
            .collect();
        ExecutionSnapshot::new(
            RecipeSnapshot {
                id: recipe.id.clone(),
                name: recipe.name.clone(),
                description: recipe.description.clone(),
                trigger: recipe.trigger.clone(),
                nodes: recipe.nodes.clone(),
                edges: recipe.edges.clone(),
            },
            agents,
            std::collections::HashMap::new(),
            TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "proj-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
    }

    /// Stands in for the config-backed resolver the real launch passes: it knows
    /// `coordinator` and nothing else, so an unknown agent id is exercised too.
    fn resolver(agent_id: &str) -> Result<Option<AgentSnapshot>, String> {
        Ok(match agent_id {
            "coordinator" => Some(agent_snapshot("coordinator", Model::OPUS)),
            _ => None,
        })
    }

    fn compile(snapshot: &ExecutionSnapshot, deltas: Value) -> Result<SnapshotOverrides, String> {
        LaunchDeltas::parse(&deltas)?.compile(snapshot, &resolver)
    }

    fn recipe_of(overrides: &SnapshotOverrides) -> &RecipeSnapshot {
        overrides
            .recipe
            .as_ref()
            .expect("compile always sets recipe")
    }

    fn node_named<'a>(recipe: &'a RecipeSnapshot, name: &str) -> Option<&'a RecipeNode> {
        recipe
            .nodes
            .iter()
            .find(|node| node.name.eq_ignore_ascii_case(name))
    }

    fn has_edge(recipe: &RecipeSnapshot, from: &str, to: &str, edge_type: RecipeEdgeType) -> bool {
        let (Some(source), Some(target)) = (node_named(recipe, from), node_named(recipe, to))
        else {
            return false;
        };
        recipe.edges.iter().any(|edge| {
            edge.edge_type == edge_type
                && edge.source_node_id == source.id
                && edge.target_node_id == target.id
        })
    }

    /// Every graph this compiler emits has to satisfy what the DAG assumes: no
    /// edge may name a node that is gone, and no edge may be duplicated.
    fn assert_graph_is_sound(recipe: &RecipeSnapshot) {
        let ids: HashSet<&str> = recipe.nodes.iter().map(|node| node.id.as_str()).collect();
        for edge in &recipe.edges {
            assert!(
                ids.contains(edge.source_node_id.as_str())
                    && ids.contains(edge.target_node_id.as_str()),
                "dangling edge left behind: {edge:?}"
            );
        }
        for (index, edge) in recipe.edges.iter().enumerate() {
            assert!(
                !recipe.edges[index + 1..]
                    .iter()
                    .any(|other| same_wire(edge, other)),
                "duplicate wire: {edge:?}"
            );
        }
    }

    // ---------------------------------------------------------------
    // `without` — the operator's everyday case
    // ---------------------------------------------------------------

    /// "I often find myself taking review off tiny fixes when launching."
    #[test]
    fn without_drops_a_terminal_review_node() {
        let snapshot = snapshot_from_yaml(PLANBUILD_YAML);
        let overrides = compile(&snapshot, json!({"without": ["review"]})).unwrap();
        let recipe = recipe_of(&overrides);

        assert!(node_named(recipe, "Review").is_none());
        assert_eq!(recipe.nodes.len(), snapshot.recipe.nodes.len() - 1);
        // Everything the review hung off is untouched: the PR still ships.
        assert!(node_named(recipe, "PR").is_some());
        assert!(has_edge(recipe, "Builder", "PR", RecipeEdgeType::Control));
        assert_graph_is_sound(recipe);
    }

    /// Removing a node from the middle rejoins its predecessor to its successor,
    /// keeping the predecessor's source port and the successor's target port.
    /// Dropping `pr` also drops the `pr-1@open` handle it was reached through,
    /// which is exactly right: that port belonged to the node that left.
    #[test]
    fn without_splices_a_middle_node_through() {
        let snapshot = snapshot_from_yaml(PLANBUILD_YAML);
        let overrides = compile(&snapshot, json!({"without": ["pr"]})).unwrap();
        let recipe = recipe_of(&overrides);

        assert!(node_named(recipe, "PR").is_none());
        assert!(
            has_edge(recipe, "Builder", "Review", RecipeEdgeType::Control),
            "the builder now reaches review directly"
        );
        let spliced = recipe
            .edges
            .iter()
            .find(|edge| {
                edge.source_node_id == node_named(recipe, "Builder").unwrap().id
                    && edge.target_node_id == node_named(recipe, "Review").unwrap().id
            })
            .unwrap();
        assert_eq!(spliced.source_handle, "control-out");
        assert_eq!(spliced.target_handle, "control-in");
        assert_graph_is_sound(recipe);
    }

    /// An artifact node is a typed schema on its producer's context edge. Drop
    /// the producer and the artifact goes too, rather than being left waiting on
    /// output that can no longer arrive — and the splice runs to completion, so
    /// the trigger feeds the builder on both edge types.
    #[test]
    fn without_cascades_to_an_orphaned_artifact() {
        let snapshot = snapshot_from_yaml(PLANBUILD_YAML);
        let overrides = compile(&snapshot, json!({"without": ["planner"]})).unwrap();
        let recipe = recipe_of(&overrides);

        assert!(node_named(recipe, "Planner").is_none());
        assert!(
            node_named(recipe, "Plan").is_none(),
            "the plan artifact has no producer left"
        );
        assert!(has_edge(
            recipe,
            "Trigger",
            "Builder",
            RecipeEdgeType::Control
        ));
        assert!(has_edge(
            recipe,
            "Trigger",
            "Builder",
            RecipeEdgeType::Context
        ));
        assert_graph_is_sound(recipe);
    }

    #[test]
    fn without_accepts_a_node_by_the_agent_it_runs() {
        let snapshot = snapshot_from_yaml(PLANBUILD_YAML);
        // `pr-review` is the agent id; `Review` is the node name. Both address
        // the same node, because a recipe-file node id does not survive loading.
        let by_agent = compile(&snapshot, json!({"without": ["pr-review"]})).unwrap();
        let by_name = compile(&snapshot, json!({"without": ["Review"]})).unwrap();
        assert_eq!(
            recipe_of(&by_agent).nodes.len(),
            recipe_of(&by_name).nodes.len()
        );
        assert!(node_named(recipe_of(&by_agent), "Review").is_none());
    }

    /// The refusal that matters most: a token that matches nothing must not be
    /// skipped, or the caller launches the review it believed it had removed.
    #[test]
    fn without_refuses_a_node_the_recipe_does_not_have() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let error = compile(&snapshot, json!({"without": ["review"]})).unwrap_err();
        assert!(error.contains("no node 'review'"), "{error}");
        // The refusal lists what could have been written instead.
        assert!(error.contains("Builder"), "{error}");
        assert!(error.contains("build"), "{error}");
    }

    #[test]
    fn without_refuses_the_trigger() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let error = compile(&snapshot, json!({"without": ["trigger"]})).unwrap_err();
        assert!(error.contains("cannot remove the trigger"), "{error}");
    }

    #[test]
    fn without_refuses_removing_the_last_agent() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let error = compile(&snapshot, json!({"without": ["builder"]})).unwrap_err();
        assert!(error.contains("every agent node"), "{error}");
    }

    /// Removing several nodes at once is order-independent and still splices the
    /// whole chain: planbuild reduced to trigger → builder → pr.
    #[test]
    fn without_removes_several_nodes_at_once() {
        let snapshot = snapshot_from_yaml(PLANBUILD_YAML);
        let forward = compile(&snapshot, json!({"without": ["planner", "review"]})).unwrap();
        let reversed = compile(&snapshot, json!({"without": ["review", "planner"]})).unwrap();

        let recipe = recipe_of(&forward);
        assert!(node_named(recipe, "Planner").is_none());
        assert!(node_named(recipe, "Review").is_none());
        assert!(node_named(recipe, "Plan").is_none());
        assert!(has_edge(
            recipe,
            "Trigger",
            "Builder",
            RecipeEdgeType::Control
        ));
        assert!(has_edge(recipe, "Builder", "PR", RecipeEdgeType::Control));
        assert_graph_is_sound(recipe);

        let names = |overrides: &SnapshotOverrides| {
            let mut names: Vec<String> = recipe_of(overrides)
                .nodes
                .iter()
                .map(|node| node.name.clone())
                .collect();
            names.sort();
            names
        };
        assert_eq!(names(&forward), names(&reversed));
    }

    /// The coordinator declares both branch targets and carries a living board on
    /// a context-self edge, so it is the recipe most likely to be mangled by
    /// careless surgery. Dropping its PR node leaves the board alone.
    #[test]
    fn without_drops_the_pr_node_of_a_multi_target_recipe() {
        let snapshot = snapshot_from_yaml(COORDINATOR_YAML);
        let overrides = compile(&snapshot, json!({"without": ["pr"]})).unwrap();
        let recipe = recipe_of(&overrides);

        assert!(!recipe
            .nodes
            .iter()
            .any(|node| node.node_type == RecipeNodeType::Pr));
        assert!(
            recipe
                .nodes
                .iter()
                .any(|node| node.node_type == RecipeNodeType::Artifact),
            "the living board survives"
        );
        assert_graph_is_sound(recipe);
    }

    // ---------------------------------------------------------------
    // `nodes` — role rebinding
    // ---------------------------------------------------------------

    /// The operator's other everyday case: promote a node's role without
    /// authoring a single line of that role's prompt.
    #[test]
    fn nodes_rebinds_a_role_and_reresolves_its_snapshot() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let overrides = compile(
            &snapshot,
            json!({"nodes": {"builder": {"agent": "coordinator"}}}),
        )
        .unwrap();
        let recipe = recipe_of(&overrides);

        assert_eq!(
            node_agent_id(node_named(recipe, "Builder").unwrap()),
            Some("coordinator")
        );
        let agents = overrides.agents.as_ref().unwrap();
        let coordinator = agents.get("coordinator").expect("resolved from config");
        // Resolved from the agent config, not hand-authored at launch.
        assert_eq!(coordinator.prompt, "prompt for coordinator");
        assert_eq!(
            coordinator.selection.as_ref().unwrap().model.as_str(),
            "opus"
        );
    }

    #[test]
    fn nodes_refuses_an_unknown_agent() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let error = compile(
            &snapshot,
            json!({"nodes": {"builder": {"agent": "no-such-agent"}}}),
        )
        .unwrap_err();
        assert!(error.contains("unknown agent 'no-such-agent'"), "{error}");
    }

    #[test]
    fn nodes_refuses_rebinding_a_node_that_runs_no_agent() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let error = compile(
            &snapshot,
            json!({"nodes": {"pr": {"agent": "coordinator"}}}),
        )
        .unwrap_err();
        assert!(
            error.contains("only agent nodes reference an agent"),
            "{error}"
        );
    }

    // ---------------------------------------------------------------
    // `agents` — snapshot-field merges
    // ---------------------------------------------------------------

    /// A merge that moves the authored tier must drop the frozen selection, or
    /// the runtime would keep reading the pair it resolved before the patch and
    /// the caller would silently get sonnet after asking for opus.
    #[test]
    fn agents_merge_of_a_tier_invalidates_the_frozen_selection() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let overrides = compile(&snapshot, json!({"agents": {"build": {"tier": "opus"}}})).unwrap();
        let build = &overrides.agents.as_ref().unwrap()["build"];

        assert_eq!(build.tier.as_ref().unwrap().as_str(), "opus");
        assert!(
            build.selection.is_none(),
            "a stale selection must not survive a tier change"
        );
    }

    /// A merge that touches nothing selection-related keeps the resolved pair, so
    /// an ordinary prompt tweak does not send the agent back through resolution.
    #[test]
    fn agents_merge_of_a_prompt_keeps_the_resolved_selection() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let overrides = compile(
            &snapshot,
            json!({"agents": {"build": {"prompt": "be brief"}}}),
        )
        .unwrap();
        let build = &overrides.agents.as_ref().unwrap()["build"];

        assert_eq!(build.prompt, "be brief");
        assert_eq!(build.selection.as_ref().unwrap().model.as_str(), "sonnet");
        // Untouched fields survive the merge.
        assert_eq!(build.id, "build");
    }

    /// The layers compose in order: a rebind resolves the new role, and a merge
    /// lands on top of what that resolution produced.
    #[test]
    fn agents_merge_lands_on_top_of_a_rebind() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let overrides = compile(
            &snapshot,
            json!({
                "nodes": {"builder": {"agent": "coordinator"}},
                "agents": {"coordinator": {"prompt": "drive the wave"}}
            }),
        )
        .unwrap();
        let coordinator = &overrides.agents.as_ref().unwrap()["coordinator"];
        assert_eq!(coordinator.prompt, "drive the wave");
    }

    /// Configuring an agent this launch no longer runs is a mistake worth
    /// naming: it is what "remove review, then set review's model" looks like.
    #[test]
    fn agents_refuses_an_agent_the_launch_does_not_run() {
        let snapshot = snapshot_from_yaml(PLANBUILD_YAML);
        let error = compile(
            &snapshot,
            json!({"without": ["review"], "agents": {"pr-review": {"tier": "opus"}}}),
        )
        .unwrap_err();
        assert!(error.contains("runs no such agent"), "{error}");
        assert!(error.contains("`build`"), "{error}");
    }

    /// A fence is a permission boundary, and this door does not grant one.
    #[test]
    fn agents_refuses_a_fence() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let error =
            compile(&snapshot, json!({"agents": {"build": {"fence": "allow"}}})).unwrap_err();
        assert!(error.contains("not settable at launch"), "{error}");
        assert!(error.contains("permission boundary"), "{error}");
    }

    /// A field that would be dropped in silence is refused instead, and the
    /// refusal names the field the caller meant.
    #[test]
    fn agents_refuses_a_field_that_would_be_ignored() {
        let snapshot = snapshot_from_yaml(BUILD_YAML);
        let error =
            compile(&snapshot, json!({"agents": {"build": {"model": "opus"}}})).unwrap_err();
        assert!(error.contains("the model tier is `tier`"), "{error}");
    }

    // ---------------------------------------------------------------
    // Parsing
    // ---------------------------------------------------------------

    #[test]
    fn parse_refuses_an_unknown_delta_key() {
        let error = LaunchDeltas::parse(&json!({"witout": ["review"]})).unwrap_err();
        assert!(error.contains("is not a launch override key"), "{error}");
        assert!(error.contains("`without`"), "{error}");
    }

    #[test]
    fn parse_refuses_malformed_layers() {
        assert!(LaunchDeltas::parse(&json!(["review"]))
            .unwrap_err()
            .contains("must be an object"));
        assert!(LaunchDeltas::parse(&json!({"without": "review"}))
            .unwrap_err()
            .contains("must be an array"));
        assert!(LaunchDeltas::parse(&json!({"without": [""]}))
            .unwrap_err()
            .contains("non-empty"));
        assert!(LaunchDeltas::parse(&json!({"nodes": {"builder": {}}}))
            .unwrap_err()
            .contains("agent is required"));
        assert!(LaunchDeltas::parse(
            &json!({"nodes": {"builder": {"agent": "x", "tier": "opus"}}})
        )
        .unwrap_err()
        .contains("a node rebind takes only `agent`"));
    }

    /// An absent or explicitly null layer is not an error, and an empty delta is
    /// simply an ordinary launch.
    #[test]
    fn parse_treats_absent_and_null_layers_as_nothing_asked() {
        assert!(LaunchDeltas::parse(&json!({})).unwrap().is_empty());
        assert!(
            LaunchDeltas::parse(&json!({"without": null, "agents": null}))
                .unwrap()
                .is_empty()
        );
        assert!(!LaunchDeltas::parse(&json!({"without": ["review"]}))
            .unwrap()
            .is_empty());
    }

    /// An empty delta compiles to the graph it was handed, so the override path
    /// cannot change a launch that asked for no change.
    #[test]
    fn an_empty_delta_is_the_identity() {
        let snapshot = snapshot_from_yaml(PLANBUILD_YAML);
        let overrides = compile(&snapshot, json!({})).unwrap();
        assert_eq!(
            serde_json::to_value(recipe_of(&overrides)).unwrap(),
            serde_json::to_value(&snapshot.recipe).unwrap()
        );
    }
}

#[cfg(test)]
mod pinned_agent_tests {
    use super::*;
    use crate::models::{Model, ModelSelection};
    use serde_json::json;

    fn agent(id: &str, selection: Option<ModelSelection>) -> AgentSnapshot {
        AgentSnapshot {
            edited_at: None,
            id: id.to_string(),
            name: id.to_string(),
            description: String::new(),
            prompt: String::new(),
            tools: vec![],
            // The composer sends the agent config's own tier as a pre-fill on
            // every row, chosen or not, which is exactly why a pin is read off
            // `selection` rather than off this.
            tier: Some(Model::new("md")),
            backend_preference: None,
            selection,
            disallowed_tools: None,
            skills: None,
            fence: None,
            sandbox: None,
            on_escape: None,
            extras: None,
            model: None,
            resolved_backend: None,
        }
    }

    #[test]
    fn composer_snapshot_pins_exactly_the_agents_carrying_a_selection() {
        let overrides = SnapshotOverrides {
            recipe: None,
            agents: Some(HashMap::from([
                (
                    "builder".to_string(),
                    agent(
                        "builder",
                        Some(ModelSelection::new("claude", Model::new("opus"))),
                    ),
                ),
                ("review".to_string(), agent("review", None)),
            ])),
        };
        let pinned = LaunchCustomization::Snapshot(overrides).pinned_agent_ids();
        assert_eq!(pinned, HashSet::from(["builder".to_string()]));
    }

    #[test]
    fn a_delta_pins_on_selection_or_any_authored_input() {
        for key in ["selection", "tier", "backend"] {
            let deltas = LaunchDeltas::parse(&json!({
                "agents": {"builder": {key: if key == "selection" {
                    json!({"backend": "claude", "model": "opus"})
                } else {
                    json!("lg")
                }}}
            }))
            .unwrap_or_else(|error| panic!("delta with {key} parses: {error}"));
            assert_eq!(
                LaunchCustomization::Deltas(deltas).pinned_agent_ids(),
                HashSet::from(["builder".to_string()]),
                "a delta carrying {key} is an explicit model choice"
            );
        }
    }

    /// A delta that changes something other than the model leaves that agent
    /// routable -- "skip review" is not a statement about which model runs.
    #[test]
    fn a_delta_touching_no_model_input_pins_nothing() {
        let deltas = LaunchDeltas::parse(&json!({
            "agents": {"builder": {"prompt": "do the thing"}}
        }))
        .expect("prompt-only delta parses");
        assert!(LaunchCustomization::Deltas(deltas)
            .pinned_agent_ids()
            .is_empty());
    }
}
