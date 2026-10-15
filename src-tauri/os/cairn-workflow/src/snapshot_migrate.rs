//! Read-time migration of stored execution snapshots into their resolved,
//! atomic runtime form.
//!
//! Pre-resolve-early snapshots stored a flat `model`/`resolvedBackend` per agent
//! plus a frozen `presets` matrix; the runtime reads a single atomic
//! `selection` and `extras`. Migrating on read folds the legacy shape forward
//! exactly once so every load site sees the resolved representation.
//!
//! The workflow engine resolves stored snapshots using `cairn-config` provider
//! and preset rules. All workflow snapshot loads use this migration path.

use crate::models::{ExecutionSnapshot, Fence, Model, ModelSelection};
use cairn_config::presets::{resolve_selection_with_provenance, PresetsConfig};
use cairn_llm_types::catalog as providers;

/// Deserialize a stored snapshot JSON string, applying resolve-early migration.
///
/// The single entry point for turning stored snapshot JSON into a runtime
/// `ExecutionSnapshot`: `serde_json::from_str` followed by [`migrate_on_read`],
/// exactly the semantics the old `ExecutionSnapshot::from_json` carried.
pub fn load(json: &str) -> Result<ExecutionSnapshot, String> {
    let mut value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;
    migrate_ai_conditions(&mut value);
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
        let decision = DecisionNode::port_choice(
            text("question").as_deref().unwrap_or_default(),
            &ports,
            text("model"),
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

/// Fold legacy per-agent flat `model`/`resolved_backend` (and frozen `presets`)
/// into the atomic `selection` + `extras` representation, once.
///
/// Recovers exactly what the runtime used to recompute: the backend prefers the
/// stored `resolved_backend`, else derives from the model, else asks the frozen
/// preset matrix which backend served THIS agent's authored tier; extras come
/// from that same frozen resolution. Clears the legacy fields and the frozen
/// presets afterward so the atomic field is the single representation going
/// forward.
///
/// The last-resort backend is a per-agent question, not a config-global one. The
/// legacy shape's single `activeBackend` was only ever the right answer because
/// every tier resolved through it; rehydrating the frozen matrix (see
/// `PresetsConfig::from(&SnapshotPresets)`) restates that as a per-tier default,
/// and resolving the agent's own tier against it gives the same answer wherever
/// the old fallback was correct and a better one where the tier was pinned
/// elsewhere.
fn migrate_on_read(snapshot: &mut ExecutionSnapshot) {
    let frozen = snapshot.presets.as_ref().map(PresetsConfig::from);
    for packet in &mut snapshot.delegated_packets {
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

    for agent in snapshot.agents.values_mut() {
        if agent.fence.is_none() && (agent.sandbox.is_some() || agent.on_escape.is_some()) {
            agent.fence = Some(Fence::from_legacy(agent.sandbox, agent.on_escape));
        }
        agent.sandbox = None;
        agent.on_escape = None;

        // What the frozen matrix says this agent's authored tier resolved to.
        // Computed once and consulted by both the backend fallback and extras.
        let frozen_resolution = frozen.as_ref().and_then(|presets| {
            resolve_selection_with_provenance(
                agent.tier.as_ref().map(Model::as_str),
                agent.backend_preference.as_deref(),
                None,
                presets,
            )
            .ok()
        });

        if agent.selection.is_none() {
            if let Some(model) = agent.model.clone() {
                let backend = agent
                    .resolved_backend
                    .clone()
                    .or_else(|| providers::backend_for_model(model.as_str()).map(str::to_string))
                    .or_else(|| {
                        frozen_resolution
                            .as_ref()
                            .map(|resolved| resolved.selection.backend.clone())
                    })
                    .unwrap_or_else(|| "claude".to_string());
                agent.selection = Some(ModelSelection { backend, model });
            }
        }
        if agent.extras.is_none() {
            if let Some(resolved) = frozen_resolution {
                agent.extras = Some(resolved.extras);
            }
        }
        agent.model = None;
        agent.resolved_backend = None;
    }
    snapshot.presets = None;
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
        assert!(snapshot.presets.is_none());
        assert!(agent.model.is_none());
        assert!(agent.resolved_backend.is_none());

        // Re-serialize: nested selection present, no presets, no flat model.
        let reserialized = snapshot.to_json().unwrap();
        assert!(reserialized.contains("\"selection\""));
        assert!(!reserialized.contains("\"presets\""));
        assert!(!reserialized.contains("resolvedBackend"));
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
