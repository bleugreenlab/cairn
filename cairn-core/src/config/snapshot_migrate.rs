//! Read-time migration of stored execution snapshots into their resolved,
//! atomic runtime form.
//!
//! Pre-resolve-early snapshots stored a flat `model`/`resolvedBackend` per agent
//! plus a frozen `presets` matrix; the runtime reads a single atomic
//! `selection` and `extras`. Migrating on read folds the legacy shape forward
//! exactly once so every load site sees the resolved representation.
//!
//! This lives in `config` rather than `models` because the resolution depends on
//! [`crate::config::presets`] and [`crate::backends`]; homing it here keeps
//! `models` free of any upward edge. **All** stored-snapshot deserialization must
//! go through [`load`].

use crate::backends;
use crate::config::presets::{resolve_selection_with_provenance, PresetsConfig};
use crate::models::{ExecutionSnapshot, Fence, Model, ModelSelection};

/// Deserialize a stored snapshot JSON string, applying resolve-early migration.
///
/// The single entry point for turning stored snapshot JSON into a runtime
/// `ExecutionSnapshot`: `serde_json::from_str` followed by [`migrate_on_read`],
/// exactly the semantics the old `ExecutionSnapshot::from_json` carried.
pub fn load(json: &str) -> Result<ExecutionSnapshot, String> {
    let mut snapshot: ExecutionSnapshot =
        serde_json::from_str(json).map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;
    migrate_on_read(&mut snapshot);
    Ok(snapshot)
}

/// Fold legacy per-agent flat `model`/`resolved_backend` (and frozen `presets`)
/// into the atomic `selection` + `extras` representation, once.
///
/// Recovers exactly what the runtime used to recompute: the backend prefers the
/// stored `resolved_backend`, else derives from the model, else the frozen
/// active backend; extras are recovered from the frozen preset matrix when
/// present. Clears the legacy fields and the frozen presets afterward so the
/// atomic field is the single representation going forward.
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

        if agent.selection.is_none() {
            if let Some(model) = agent.model.clone() {
                let backend = agent
                    .resolved_backend
                    .clone()
                    .or_else(|| backends::backend_for_model(model.as_str()).map(str::to_string))
                    .or_else(|| frozen.as_ref().map(|p| p.active_backend.clone()))
                    .unwrap_or_else(|| "claude".to_string());
                agent.selection = Some(ModelSelection { backend, model });
            }
        }
        if agent.extras.is_none() {
            if let Some(presets) = frozen.as_ref() {
                if let Ok(resolved) = resolve_selection_with_provenance(
                    agent.tier.as_ref().map(Model::as_str),
                    agent.backend_preference.as_deref(),
                    None,
                    presets,
                ) {
                    agent.extras = Some(resolved.extras);
                }
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
}
