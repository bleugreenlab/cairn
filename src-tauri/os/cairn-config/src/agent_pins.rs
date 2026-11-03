//! Per-agent pins layered over agent files (`~/.cairn/agent-pins.yaml`).
//!
//! A pack owns an agent file only while its bytes are exactly what the pack
//! last wrote, so any edit -- even a one-line `backend:` choice -- would fork
//! the whole prompt and freeze it at that vintage. The keys a user sets to make
//! a shipped agent theirs without changing what it *is* live here instead:
//! the file stays pack-owned and keeps updating, and the loader applies these
//! over it. This is a fixed key set layered over the file, not a merge.
//!
//! The document is workspace-scoped: only `~/.cairn/agents/` holds pack-owned
//! agents, so project-scoped agents never read it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::allotment::AllotmentEntry;

/// File name of the pin document inside the workspace config home.
pub const AGENT_PINS_FILE: &str = "agent-pins.yaml";

/// The user's choices for one agent. An absent key is no pin: the agent file's
/// own value applies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentPin {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models: Option<Vec<AllotmentEntry>>,
    /// A tier pin written before `models` existed. Read so the document still
    /// parses (it denies unknown keys) and so the pin carries over as the
    /// pool of that name; the next save writes it as `models`.
    #[serde(default, skip_serializing)]
    pub tier: Option<String>,
    /// A provider pin written before the backend preference retired. Read so
    /// the document still parses; it pins nothing and the next save drops it.
    #[serde(default, skip_serializing)]
    pub backend: Option<String>,
}

impl AgentPin {
    pub fn is_empty(&self) -> bool {
        self.icon.is_none() && self.models.is_none()
    }

    /// Fold the legacy keys forward: a tier pin becomes the pool of that
    /// name, and a backend pin is dropped.
    fn migrated(mut self) -> Self {
        let tier = self.tier.take();
        self.backend = None;
        if self.models.is_none() {
            self.models = tier
                .as_deref()
                .map(str::trim)
                .filter(|tier| !tier.is_empty() && !tier.contains('/'))
                .map(|tier| vec![AllotmentEntry::Pool(tier.to_string())]);
        }
        self
    }

    /// Overlay these pins on an agent loaded from its file.
    pub fn apply(&self, agent: &mut crate::agents::FileAgent) {
        if let Some(icon) = &self.icon {
            agent.icon = Some(icon.clone());
        }
        if let Some(models) = &self.models {
            agent.models = models.clone();
        }
    }
}

/// The whole document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentPins {
    #[serde(default)]
    pub agents: BTreeMap<String, AgentPin>,
}

impl AgentPins {
    pub fn get(&self, agent_id: &str) -> Option<&AgentPin> {
        self.agents.get(agent_id)
    }

    /// Record `pin` for `agent_id`, dropping the entry when it pins nothing.
    pub fn set(&mut self, agent_id: &str, pin: AgentPin) {
        if pin.is_empty() {
            self.agents.remove(agent_id);
        } else {
            self.agents.insert(agent_id.to_string(), pin);
        }
    }
}

pub fn pins_path(config_dir: &Path) -> PathBuf {
    config_dir.join(AGENT_PINS_FILE)
}

/// Read the pin document. An absent file pins nothing; a malformed one is
/// logged and pins nothing rather than making every agent unloadable.
pub fn load_pins(config_dir: &Path) -> AgentPins {
    let path = pins_path(config_dir);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return AgentPins::default();
    };
    match serde_yaml::from_str::<Option<AgentPins>>(&text) {
        Ok(pins) => AgentPins {
            agents: pins
                .unwrap_or_default()
                .agents
                .into_iter()
                .map(|(id, pin)| (id, pin.migrated()))
                .filter(|(_, pin)| !pin.is_empty())
                .collect(),
        },
        Err(error) => {
            log::warn!("Ignoring unreadable agent pins at {path:?}: {error}");
            AgentPins::default()
        }
    }
}

/// Write the pin document. An emptied document is written empty rather than
/// deleted, so the commit that follows always has a path to stage.
pub fn save_pins(config_dir: &Path, pins: &AgentPins) -> Result<PathBuf, String> {
    let path = pins_path(config_dir);
    let body = serde_yaml::to_string(pins)
        .map_err(|error| format!("Failed to serialize agent pins: {error}"))?;
    let text = format!(
        "# Settings you chose for agents a pack ships. They apply over the agent file,\n\
         # so the pack keeps updating it. Edit an agent in Cairn to change them.\n{body}"
    );
    std::fs::write(&path, text).map_err(|error| format!("Failed to write {path:?}: {error}"))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_round_trip_and_empty_entries_disappear() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(load_pins(temp.path()), AgentPins::default());

        let mut pins = AgentPins::default();
        pins.set(
            "build",
            AgentPin {
                icon: Some("hammer".into()),
                models: Some(vec![AllotmentEntry::Pool("lg".into())]),
                ..AgentPin::default()
            },
        );
        pins.set("explore", AgentPin::default());
        save_pins(temp.path(), &pins).unwrap();

        let read = load_pins(temp.path());
        assert_eq!(read, pins);
        assert!(read.get("explore").is_none());
        assert_eq!(
            read.get("build").unwrap().models,
            Some(vec![AllotmentEntry::Pool("lg".into())])
        );
    }

    /// A document written while pins named a tier and a backend still parses:
    /// the icon survives, the tier carries over as its pool, and the backend
    /// pins nothing.
    #[test]
    fn a_tier_pin_carries_over_and_a_backend_pin_is_dropped() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            pins_path(temp.path()),
            "agents:\n  build:\n    backend: codex\n    icon: hammer\n    tier: lg\n  explore:\n    backend: codex\n",
        )
        .unwrap();
        let pins = load_pins(temp.path());
        let build = pins.get("build").unwrap();
        assert_eq!(build.icon.as_deref(), Some("hammer"));
        assert_eq!(build.models, Some(vec![AllotmentEntry::Pool("lg".into())]));
        assert!(pins.get("explore").is_none());

        save_pins(temp.path(), &pins).unwrap();
        let written = std::fs::read_to_string(pins_path(temp.path())).unwrap();
        assert!(!written.contains("backend"), "{written}");
        assert!(!written.contains("tier"), "{written}");
        assert_eq!(load_pins(temp.path()), pins);
    }

    #[test]
    fn a_malformed_document_pins_nothing() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(pins_path(temp.path()), "agents: [not, a, map]\n").unwrap();
        assert_eq!(load_pins(temp.path()), AgentPins::default());
    }
}
