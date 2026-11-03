//! Read-time migration of stored execution snapshots into their current form.
//!
//! Pre-resolve-early snapshots stored a flat `model`/`resolvedBackend` per agent
//! plus a frozen `presets` matrix and the agent's authored `tier`; the runtime
//! reads a single atomic `selection` and `extras`. Migrating on read folds the
//! legacy shape forward exactly once so every load site sees the current
//! representation. All workflow snapshot loads use this migration path.

use crate::models::allotment::AllotmentEntry;
use crate::models::{DelegatedWorkPacket, ExecutionSnapshot, Fence, ModelSelection, Preset};
use cairn_models_types::catalog as providers;
use serde_json::Value;

/// Deserialize a stored snapshot JSON string, applying resolve-early migration.
///
/// The single entry point for turning stored snapshot JSON into a runtime
/// `ExecutionSnapshot`: `serde_json::from_str` followed by [`migrate_on_read`],
/// exactly the semantics the old `ExecutionSnapshot::from_json` carried.
pub fn load(json: &str) -> Result<ExecutionSnapshot, String> {
    let mut value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;
    migrate_ai_conditions(&mut value);
    migrate_frozen_presets(&mut value);
    let mut snapshot: ExecutionSnapshot = serde_json::from_value(value)
        .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;
    migrate_on_read(&mut snapshot);
    // A frozen recipe drawn in the old editor leaves its conditions by
    // `port-<name>` handles, which the engine would never route.
    crate::models::normalize_legacy_port_handles(
        &snapshot.recipe.nodes,
        &mut snapshot.recipe.edges,
    );
    Ok(snapshot)
}

/// Rewrite a frozen `ai` condition node into the decision node it always
/// was: one `choice` question whose options are its ports.
///
/// Done on the JSON because the typed node no longer has the fields an `ai`
/// condition carried; deserializing first would drop the question and leave a
/// programmatic condition with no expression.
fn migrate_ai_conditions(snapshot: &mut serde_json::Value) {
    use crate::models::decision::DecisionNode;
    use serde_json::Value;

    let Some(nodes) = snapshot
        .pointer_mut("/recipe/nodes")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for node in nodes {
        if node.get("nodeType").and_then(Value::as_str) != Some("condition") {
            continue;
        }
        let Some(config) = node.get("conditionConfig") else {
            continue;
        };
        if !crate::models::is_legacy_ai_condition(
            config.get("conditionType").and_then(Value::as_str),
        ) {
            continue;
        }
        let text = |key: &str| config.get(key).and_then(Value::as_str).map(str::to_string);
        let ports: Vec<String> = config
            .get("ports")
            .and_then(Value::as_array)
            .map(|ports| {
                ports
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        // The condition named a tier; a bare tier name is the pool it became.
        let models = text("model")
            .filter(|model| !model.trim().is_empty() && !model.contains('/'))
            .map(|pool| vec![AllotmentEntry::Pool(pool)]);
        let decision = DecisionNode::port_choice(
            text("question").as_deref().unwrap_or_default(),
            &ports,
            models,
        );
        if let Some(object) = node.as_object_mut() {
            object.remove("conditionConfig");
            object.insert("nodeType".into(), Value::from("decision"));
            object.insert(
                "decisionConfig".into(),
                serde_json::to_value(decision).unwrap_or(Value::Null),
            );
        }
    }
}

/// Fold a packet's legacy `sandbox`/`onEscape` pair into its `fence`.
pub fn migrate_packet_on_read(packet: &mut DelegatedWorkPacket) {
    if packet.ownership.fence.is_none()
        && (packet.ownership.sandbox.is_some() || packet.ownership.on_escape.is_some())
    {
        packet.ownership.fence = Some(Fence::from_legacy(
            packet.ownership.sandbox,
            packet.ownership.on_escape,
        ));
    }
    packet.ownership.sandbox = None;
    packet.ownership.on_escape = None;
}

/// Recover, from a frozen `presets` matrix, what each agent's authored `tier`
/// resolved to when the snapshot was written: its `extras`, and the backend a
/// flat legacy `model` cannot name by itself. Then drop the matrix.
///
/// The frozen shape predates per-tier defaults: every tier resolved through
/// its single `activeBackend`, unless the tier was qualified (`codex/lg`).
fn migrate_frozen_presets(snapshot: &mut Value) {
    let Some(presets) = snapshot
        .as_object_mut()
        .and_then(|object| object.remove("presets"))
        .filter(|presets| !presets.is_null())
    else {
        return;
    };
    let active = presets.get("activeBackend").and_then(Value::as_str);
    let Some(agents) = snapshot.get_mut("agents").and_then(Value::as_object_mut) else {
        return;
    };
    for agent in agents.values_mut() {
        let Some(agent) = agent.as_object_mut() else {
            continue;
        };
        let Some(tier) = agent.get("tier").and_then(Value::as_str) else {
            continue;
        };
        let (backend, tier) = match tier.split_once('/') {
            Some((backend, tier)) => (backend, tier),
            None => match active {
                Some(active) => (active, tier),
                None => continue,
            },
        };
        let Some(preset) = presets
            .pointer(&format!("/backends/{backend}/{tier}"))
            .and_then(|preset| serde_json::from_value::<Preset>(preset.clone()).ok())
        else {
            continue;
        };
        let backend = backend.to_string();
        if !agent.contains_key("extras") {
            if let Ok(extras) = serde_json::to_value(preset.to_extras()) {
                agent.insert("extras".into(), extras);
            }
        }
        let model_names_its_backend = agent
            .get("model")
            .and_then(Value::as_str)
            .is_some_and(|model| providers::backend_for_model(model).is_some());
        if !agent.contains_key("selection")
            && !agent.contains_key("resolvedBackend")
            && !model_names_its_backend
        {
            agent.insert("resolvedBackend".into(), Value::String(backend));
        }
    }
}

/// Fold legacy per-agent flat `model`/`resolved_backend` into the atomic
/// `selection`, once: the backend prefers the stored (or recovered)
/// `resolved_backend`, else derives from the model, else Claude. Clears the
/// legacy fields afterward so the atomic field is the single representation
/// going forward.
fn migrate_on_read(snapshot: &mut ExecutionSnapshot) {
    for packet in &mut snapshot.inline_delegated_packets {
        migrate_packet_on_read(packet);
    }

    for agent in snapshot.agents.values_mut() {
        if agent.fence.is_none() && (agent.sandbox.is_some() || agent.on_escape.is_some()) {
            agent.fence = Some(Fence::from_legacy(agent.sandbox, agent.on_escape));
        }
        agent.sandbox = None;
        agent.on_escape = None;

        if agent.selection.is_none() {
            if let Some(model) = agent.model.clone() {
                let backend = agent
                    .resolved_backend
                    .clone()
                    .or_else(|| providers::backend_for_model(model.as_str()).map(str::to_string))
                    .unwrap_or_else(|| "claude".to_string());
                agent.selection = Some(ModelSelection { backend, model });
            }
        }
        agent.model = None;
        agent.resolved_backend = None;
    }
}

#[cfg(test)]
mod tests {
    use super::load;

    /// Pre-resolve-early snapshot (flat model/resolvedBackend + frozen presets,
    /// no `selection`) migrates on read into a concrete atomic selection,
    /// recovers `extras` from the frozen presets, and re-serializes with a
    /// nested `selection` and no `presets`.
    #[test]
    fn migrate_on_read_builds_selection_from_legacy_fields() {
        let json = r#"{
            "recipe": {
                "id": "r-1", "name": "R", "description": null,
                "trigger": "manual", "nodes": [], "edges": []
            },
            "agents": {
                "build": {
                    "id": "build", "name": "Build", "description": "",
                    "prompt": "p", "tools": [],
                    "tier": "md",
                    "model": "sonnet",
                    "resolvedBackend": "claude"
                }
            },
            "skills": {},
            "triggerContext": {"issueId": null, "projectId": "p-1", "triggerType": "manual"},
            "presets": {
                "activeBackend": "claude",
                "tiers": ["sm", "md", "lg"],
                "backends": {
                    "claude": {
                        "md": {"model": "sonnet", "options": {"reasoningEffort": "high"}}
                    }
                }
            },
            "createdAt": 1
        }"#;

        let snapshot = load(json).unwrap();
        let agent = snapshot.agents.get("build").unwrap();
        let selection = agent.selection.as_ref().expect("selection migrated");
        assert_eq!(selection.backend, "claude");
        assert_eq!(selection.model.as_str(), "sonnet");
        assert_eq!(
            agent.extras.as_ref().unwrap().reasoning_effort.as_deref(),
            Some("high")
        );
        assert!(agent.model.is_none());
        assert!(agent.resolved_backend.is_none());
        assert!(
            agent.models.is_empty(),
            "the frozen tier is not an allotment"
        );

        // Re-serialize: nested selection present, no presets, no flat model.
        let reserialized = snapshot.to_json().unwrap();
        assert!(reserialized.contains("\"selection\""));
        assert!(!reserialized.contains("\"presets\""));
        assert!(!reserialized.contains("resolvedBackend"));
        assert!(!reserialized.contains("\"tier\""));
    }

    /// A flat model no backend recognizes takes the backend its frozen tier
    /// resolved through, qualified or by the single active backend.
    #[test]
    fn a_frozen_tier_names_the_backend_a_custom_model_cannot() {
        let json = r#"{
            "recipe": {"id": "r-1", "name": "R", "description": null,
                       "trigger": "manual", "nodes": [], "edges": []},
            "agents": {
                "plan": {"id": "plan", "name": "P", "description": "", "prompt": "p",
                         "tools": [], "tier": "lg", "model": "my-local-model"},
                "build": {"id": "build", "name": "B", "description": "", "prompt": "p",
                          "tools": [], "tier": "codex/md", "model": "my-codex-model"}
            },
            "skills": {},
            "triggerContext": {"issueId": null, "projectId": "p-1", "triggerType": "manual"},
            "presets": {
                "activeBackend": "ollama",
                "tiers": ["sm", "md", "lg"],
                "backends": {
                    "ollama": {"lg": {"model": "my-local-model"}},
                    "codex": {"md": {"model": "my-codex-model", "options": {"reasoningEffort": "low"}}}
                }
            },
            "createdAt": 1
        }"#;
        let snapshot = load(json).unwrap();
        let plan = snapshot.agents["plan"].selection.as_ref().unwrap();
        assert_eq!(plan.backend, "ollama");
        let build = &snapshot.agents["build"];
        assert_eq!(build.selection.as_ref().unwrap().backend, "codex");
        assert_eq!(
            build.extras.as_ref().unwrap().reasoning_effort.as_deref(),
            Some("low")
        );
    }

    /// An ancient snapshot with `presets: null` still loads; backend is derived
    /// from the model when no frozen active backend is available.
    #[test]
    fn migrate_on_read_without_presets_derives_backend_from_model() {
        let json = r#"{
            "recipe": {
                "id": "r-1", "name": "R", "description": null,
                "trigger": "manual", "nodes": [], "edges": []
            },
            "agents": {
                "build": {
                    "id": "build", "name": "Build", "description": "",
                    "prompt": "p", "tools": [],
                    "model": "gpt-5.3-codex"
                }
            },
            "skills": {},
            "triggerContext": {"issueId": null, "projectId": "p-1", "triggerType": "manual"},
            "createdAt": 1
        }"#;

        let snapshot = load(json).unwrap();
        let selection = snapshot
            .agents
            .get("build")
            .unwrap()
            .selection
            .as_ref()
            .unwrap();
        assert_eq!(selection.backend, "codex");
        assert_eq!(selection.model.as_str(), "gpt-5.3-codex");
    }

    #[test]
    fn a_frozen_ai_condition_routes_by_its_ports_after_migration() {
        let json = r#"{
            "recipe": {
                "id": "r-1", "name": "R", "description": null, "trigger": "manual",
                "nodes": [{
                    "id": "db", "nodeType": "condition", "name": "Touches the DB?",
                    "position": {"x": 0, "y": 0},
                    "conditionConfig": {
                        "conditionType": "ai", "question": "Does it touch the DB?",
                        "ports": ["yes", "no"], "onError": "block"
                    }
                }],
                "edges": [
                    {"id": "e1", "edgeType": "control", "sourceNodeId": "db",
                     "sourceHandle": "port-yes", "targetNodeId": "a", "targetHandle": "control-in"},
                    {"id": "e2", "edgeType": "control", "sourceNodeId": "db",
                     "sourceHandle": "no", "targetNodeId": "b", "targetHandle": "control-in"}
                ]
            },
            "agents": {},
            "skills": {},
            "triggerContext": {"issueId": null, "projectId": "p-1", "triggerType": "manual"},
            "createdAt": 1
        }"#;

        let snapshot = load(json).unwrap();
        let node = &snapshot.recipe.nodes[0];
        assert_eq!(node.node_type, crate::models::RecipeNodeType::Decision);
        assert_eq!(node.routing_ports(), vec!["yes", "no"]);
        let handles: Vec<&str> = snapshot
            .recipe
            .edges
            .iter()
            .map(|edge| edge.source_handle.as_str())
            .collect();
        assert_eq!(handles, vec!["yes", "no"]);
    }
}
