//! Validated, versioned keybind customization persistence.

use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

const CURRENT_VERSION: u32 = 2;
const MAX_SEQUENCE_LENGTH: usize = 4;
const REMOVED_ACTION_IDS: &[&str] = &["chat.newSession", "dialog.toggleBacklog"];

#[derive(Debug)]
pub(crate) struct ActionMetadata {
    pub id: &'static str,
    pub contexts: &'static [&'static str],
    pub default_sequence: &'static [&'static str],
    pub alternative_sequences: &'static [&'static [&'static str]],
}

fn normalize_key(key: &str) -> String {
    if key == " " {
        return key.to_string();
    }
    let lower = key.to_lowercase();
    GENERATED_KEY_ALIASES
        .iter()
        .find_map(|(alias, canonical)| (*alias == lower).then(|| (*canonical).to_string()))
        .unwrap_or_else(|| {
            if key.chars().count() == 1 {
                lower
            } else {
                key.to_string()
            }
        })
}

fn sequence_signature(sequence: &KeySequence) -> Vec<String> {
    sequence
        .iter()
        .map(|stroke| {
            let modifiers = stroke
                .modifiers
                .iter()
                .map(|modifier| modifier.normalized_name())
                .collect::<Vec<_>>()
                .join("+");
            format!("{}:{}", modifiers, stroke.key.to_lowercase())
        })
        .collect()
}

fn sequences_collide(left: &[String], right: &[String]) -> bool {
    left == right
        || (left.len() < right.len() && right.starts_with(left))
        || (right.len() < left.len() && left.starts_with(right))
}
include!(concat!(env!("OUT_DIR"), "/keybind_actions.rs"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Modifier {
    Meta,
    Ctrl,
    Shift,
    Alt,
}

impl Modifier {
    fn rank(self) -> u8 {
        match self {
            Self::Meta => 0,
            Self::Ctrl => 1,
            Self::Alt => 2,
            Self::Shift => 3,
        }
    }
    pub(crate) fn normalized_name(self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Ctrl => "ctrl",
            Self::Alt => "alt",
            Self::Shift => "shift",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyStroke {
    pub(crate) key: String,
    #[serde(default)]
    pub(crate) modifiers: Vec<Modifier>,
}

pub type KeySequence = Vec<KeyStroke>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeybindCustomization {
    pub(crate) action: String,
    /// An empty sequence disables the action.
    #[serde(default)]
    pub(crate) sequence: KeySequence,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeybindsFile {
    version: u32,
    #[serde(default)]
    pub(crate) keybinds: Vec<KeybindCustomization>,
}

impl Default for KeybindsFile {
    fn default() -> Self {
        Self {
            version: CURRENT_VERSION,
            keybinds: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoadDiagnostic {
    pub action: String,
    pub reason: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VersionProbe {
    #[serde(default = "legacy_version")]
    version: u32,
}
fn legacy_version() -> u32 {
    1
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct V1File {
    #[allow(dead_code)]
    #[serde(default = "legacy_version")]
    version: u32,
    #[serde(default)]
    keybinds: Vec<V1Customization>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct V1Customization {
    action: String,
    key: String,
    #[serde(default)]
    modifiers: Vec<Modifier>,
    #[serde(default)]
    description: Option<String>,
}

impl KeybindsFile {
    pub(crate) fn set_keybind(
        &mut self,
        action: &str,
        sequence: KeySequence,
    ) -> Result<(), String> {
        let mut candidate = self.clone();
        candidate.keybinds.retain(|item| item.action != action);
        candidate.keybinds.push(KeybindCustomization {
            action: action.to_string(),
            sequence,
            description: None,
        });
        candidate.normalize_and_validate()?;
        *self = candidate;
        Ok(())
    }

    pub(crate) fn remove_keybind(&mut self, action: &str) -> Result<(), String> {
        ensure_known_action(action)?;
        let mut candidate = self.clone();
        candidate.keybinds.retain(|item| item.action != action);
        candidate.normalize_and_validate()?;
        *self = candidate;
        Ok(())
    }

    pub(crate) fn reset(&mut self) {
        self.keybinds.clear();
        self.version = CURRENT_VERSION;
    }

    pub(crate) fn normalize_and_validate(&mut self) -> Result<(), String> {
        if self.version != CURRENT_VERSION {
            return Err(format!(
                "unsupported keybinds version {}; expected {CURRENT_VERSION}",
                self.version
            ));
        }
        let mut actions = HashSet::new();
        for item in &mut self.keybinds {
            ensure_known_action(&item.action)?;
            if !actions.insert(item.action.as_str()) {
                return Err(format!(
                    "duplicate keybind override for action `{}`",
                    item.action
                ));
            }
            normalize_sequence(&item.action, &mut item.sequence)?;
        }
        let effective = GENERATED_ACTIONS
            .iter()
            .map(|action| {
                let customization = self.keybinds.iter().find(|item| item.action == action.id);
                let disabled = customization.is_some_and(|item| item.sequence.is_empty());
                let primary = customization
                    .map(|item| sequence_signature(&item.sequence))
                    .unwrap_or_else(|| {
                        action
                            .default_sequence
                            .iter()
                            .map(|stroke| (*stroke).to_string())
                            .collect()
                    });
                let mut sequences = if disabled { Vec::new() } else { vec![primary] };
                if !disabled {
                    sequences.extend(action.alternative_sequences.iter().map(|sequence| {
                        sequence
                            .iter()
                            .map(|stroke| (*stroke).to_string())
                            .collect()
                    }));
                }
                (action, sequences)
            })
            .collect::<Vec<_>>();
        for left in 0..effective.len() {
            for right in left + 1..effective.len() {
                if contexts_overlap(effective[left].0.id, effective[right].0.id)
                    && effective[left].1.iter().any(|left_sequence| {
                        effective[right]
                            .1
                            .iter()
                            .any(|right_sequence| sequences_collide(left_sequence, right_sequence))
                    })
                {
                    return Err(format!(
                        "key sequence for `{}` conflicts with `{}` in overlapping contexts",
                        effective[left].0.id, effective[right].0.id
                    ));
                }
            }
        }
        Ok(())
    }
}

fn ensure_known_action(action: &str) -> Result<(), String> {
    if REMOVED_ACTION_IDS.contains(&action) {
        return Err(format!(
            "action `{action}` was removed and is no longer editable"
        ));
    }
    if GENERATED_ACTIONS.iter().any(|known| known.id == action) {
        Ok(())
    } else {
        Err(format!("unknown editable action `{action}`"))
    }
}

fn normalize_sequence(action: &str, sequence: &mut KeySequence) -> Result<(), String> {
    if sequence.len() > MAX_SEQUENCE_LENGTH {
        return Err(format!(
            "key sequence for `{action}` exceeds the maximum of {MAX_SEQUENCE_LENGTH} strokes"
        ));
    }
    for (index, stroke) in sequence.iter_mut().enumerate() {
        stroke.key = if stroke.key == " " {
            stroke.key.clone()
        } else {
            stroke.key.trim().to_string()
        };
        if stroke.key.is_empty() {
            return Err(format!(
                "stroke {} for `{action}` has an empty key; use an empty sequence to disable it",
                index + 1
            ));
        }
        stroke.key = normalize_key(&stroke.key);
        let original_len = stroke.modifiers.len();
        let unique = stroke.modifiers.iter().copied().collect::<HashSet<_>>();
        if unique.len() != original_len {
            return Err(format!(
                "stroke {} for `{action}` contains duplicate modifiers",
                index + 1
            ));
        }
        stroke.modifiers.sort_by_key(|modifier| modifier.rank());
    }
    Ok(())
}

fn contexts_overlap(left: &str, right: &str) -> bool {
    let metadata = |id| {
        GENERATED_ACTIONS
            .iter()
            .find(|entry| entry.id == id)
            .unwrap()
    };
    let left = metadata(left).contexts;
    let right = metadata(right).contexts;
    left.iter().any(|context| right.contains(context))
}

fn get_keybinds_path(config_dir: &Path) -> PathBuf {
    config_dir.join("keybinds.json")
}

pub(crate) fn load_keybinds(config_dir: &Path) -> KeybindsFile {
    match load_keybinds_with_diagnostics(config_dir) {
        Ok((file, diagnostics)) => {
            for diagnostic in diagnostics {
                log::warn!(
                    "Pruned keybind `{}`: {}",
                    diagnostic.action,
                    diagnostic.reason
                );
            }
            file
        }
        Err(error) => {
            log::warn!("Using default keybinds: {error}");
            KeybindsFile::default()
        }
    }
}

fn load_keybinds_with_diagnostics(
    config_dir: &Path,
) -> Result<(KeybindsFile, Vec<LoadDiagnostic>), String> {
    let path = get_keybinds_path(config_dir);
    if !path.exists() {
        return Ok((KeybindsFile::default(), Vec::new()));
    }
    let content =
        std::fs::read_to_string(&path).map_err(|e| format!("failed to read keybinds file: {e}"))?;
    let value: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| format!("failed to parse keybinds file: {e}"))?;
    let version = serde_json::from_value::<VersionProbe>(value.clone())
        .map_err(|e| format!("invalid keybinds header: {e}"))?
        .version;
    let (mut file, migrated) = match version {
        1 => {
            let legacy: V1File = serde_json::from_value(value)
                .map_err(|e| format!("invalid v1 keybinds file: {e}"))?;
            (
                KeybindsFile {
                    version: CURRENT_VERSION,
                    keybinds: legacy
                        .keybinds
                        .into_iter()
                        .map(|item| KeybindCustomization {
                            action: item.action,
                            sequence: if item.key.is_empty() {
                                vec![]
                            } else {
                                vec![KeyStroke {
                                    key: item.key,
                                    modifiers: item.modifiers,
                                }]
                            },
                            description: item.description,
                        })
                        .collect(),
                },
                true,
            )
        }
        CURRENT_VERSION => (
            serde_json::from_value(value).map_err(|e| format!("invalid v2 keybinds file: {e}"))?,
            false,
        ),
        other => {
            return Err(format!(
                "unsupported keybinds version {other}; expected {CURRENT_VERSION}"
            ))
        }
    };
    let mut diagnostics = Vec::new();
    file.keybinds.retain(|item| {
        let reason = if REMOVED_ACTION_IDS.contains(&item.action.as_str()) {
            Some("action was removed".to_string())
        } else if !GENERATED_ACTIONS
            .iter()
            .any(|known| known.id == item.action)
        {
            Some("action is unknown".to_string())
        } else {
            None
        };
        if let Some(reason) = reason {
            diagnostics.push(LoadDiagnostic {
                action: item.action.clone(),
                reason,
            });
            false
        } else {
            true
        }
    });
    file.normalize_and_validate()?;
    if migrated || !diagnostics.is_empty() {
        save_keybinds(config_dir, &file)?;
    }
    Ok((file, diagnostics))
}

pub(crate) fn save_keybinds(config_dir: &Path, file: &KeybindsFile) -> Result<(), String> {
    let mut normalized = file.clone();
    normalized.normalize_and_validate()?;
    let path = get_keybinds_path(config_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create config directory: {e}"))?;
    }
    let content = serde_json::to_string_pretty(&normalized)
        .map_err(|e| format!("failed to serialize keybinds: {e}"))?;
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, content)
        .map_err(|e| format!("failed to write temporary keybinds file: {e}"))?;
    std::fs::rename(&temporary, &path)
        .map_err(|e| format!("failed to replace keybinds file atomically: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn stroke(key: &str, modifiers: Vec<Modifier>) -> KeyStroke {
        KeyStroke {
            key: key.into(),
            modifiers,
        }
    }
    #[test]
    fn normalizes_modifier_order() {
        let mut file = KeybindsFile::default();
        file.set_keybind(
            "issue.create",
            vec![stroke(" c ", vec![Modifier::Shift, Modifier::Meta])],
        )
        .unwrap();
        assert_eq!(
            file.keybinds[0].sequence[0],
            stroke("c", vec![Modifier::Meta, Modifier::Shift])
        );
    }
    #[test]
    fn set_is_atomic_on_invalid_input() {
        let mut file = KeybindsFile::default();
        let before = file.clone();
        assert!(file
            .set_keybind("unknown.action", vec![stroke("x", vec![])])
            .is_err());
        assert_eq!(file, before);
    }
    #[test]
    fn rejects_duplicate_modifiers() {
        let mut file = KeybindsFile::default();
        assert!(file
            .set_keybind(
                "issue.create",
                vec![stroke("c", vec![Modifier::Meta, Modifier::Meta])]
            )
            .unwrap_err()
            .contains("duplicate modifiers"));
    }
    #[test]
    fn rejects_empty_stroke_but_accepts_disabled_sequence() {
        let mut file = KeybindsFile::default();
        file.set_keybind("issue.create", vec![stroke(" ", vec![])])
            .unwrap();
        file.set_keybind("issue.create", vec![]).unwrap();
    }
    #[test]
    fn rejects_conflicts_in_overlapping_contexts() {
        let mut file = KeybindsFile::default();
        file.set_keybind("issue.create", vec![stroke("x", vec![])])
            .unwrap();
        assert!(file
            .set_keybind("palette.open", vec![stroke("x", vec![])])
            .unwrap_err()
            .contains("conflicts"));
    }
    #[test]
    fn rejects_override_conflicting_with_default() {
        let mut file = KeybindsFile::default();
        assert!(file
            .set_keybind("sidebar.previousIssue", vec![stroke("k", vec![])])
            .unwrap_err()
            .contains("sidebar.nextIssue"));
    }
    #[test]
    fn matches_frontend_key_alias_and_case_normalization() {
        for (input, expected) in [
            ("up", "ArrowUp"),
            ("ARROWDOWN", "ArrowDown"),
            ("Left", "ArrowLeft"),
            ("right", "ArrowRight"),
            ("esc", "Escape"),
            ("ESCAPE", "Escape"),
            ("return", "Enter"),
            ("ENTER", "Enter"),
            ("space", " "),
            ("SPACEBAR", " "),
            ("backspace", "Backspace"),
            ("TAB", "Tab"),
            ("A", "a"),
            ("PageUp", "PageUp"),
        ] {
            assert_eq!(normalize_key(input), expected, "{input}");
        }
    }
    #[test]
    fn rejects_alias_override_conflicting_with_canonical_default() {
        let mut file = KeybindsFile::default();
        let error = file
            .set_keybind("sidebar.previousIssue", vec![stroke("down", vec![])])
            .unwrap_err();
        assert!(error.contains("workspace.focusPaneDown"), "{error}");
    }
    #[test]
    fn persists_frontend_canonical_keys() {
        let temp = tempfile::tempdir().unwrap();
        let mut file = KeybindsFile::default();
        for (action, key) in [
            ("issue.create", "RETURN"),
            ("palette.open", "Esc"),
            ("view.toggleSidebar", "SPACEBAR"),
        ] {
            file.set_keybind(action, vec![stroke(key, vec![Modifier::Meta])])
                .unwrap();
        }
        save_keybinds(temp.path(), &file).unwrap();
        let persisted: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(get_keybinds_path(temp.path())).unwrap())
                .unwrap();
        let keys = persisted["keybinds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["sequence"][0]["key"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["Enter", "Escape", " "]);
    }
    #[test]
    fn rejects_override_conflicting_with_alternative() {
        let mut file = KeybindsFile::default();
        assert!(file
            .set_keybind(
                "execution.cycleRecipe",
                vec![stroke("Enter", vec![Modifier::Ctrl])]
            )
            .unwrap_err()
            .contains("execution.start"));
    }
    #[test]
    fn rejects_override_that_prefixes_default_sequence() {
        let mut file = KeybindsFile::default();
        assert!(file
            .set_keybind("issue.create", vec![stroke("g", vec![])])
            .unwrap_err()
            .contains("go."));
    }
    #[test]
    fn rejects_duplicate_actions_on_bulk_save() {
        let temp = tempfile::tempdir().unwrap();
        let item = KeybindCustomization {
            action: "issue.create".into(),
            sequence: vec![stroke("x", vec![])],
            description: None,
        };
        let file = KeybindsFile {
            version: 2,
            keybinds: vec![item.clone(), item],
        };
        assert!(save_keybinds(temp.path(), &file)
            .unwrap_err()
            .contains("duplicate"));
        assert!(!get_keybinds_path(temp.path()).exists());
    }
    #[test]
    fn migrates_v1_prunes_unknown_and_writes_v2() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(get_keybinds_path(temp.path()),r#"{"version":1,"keybinds":[{"action":"issue.create","key":" n ","modifiers":["shift","meta"]},{"action":"chat.newSession","key":"n"},{"action":"gone.action","key":"g"}]}"#).unwrap();
        let (file, diagnostics) = load_keybinds_with_diagnostics(temp.path()).unwrap();
        assert_eq!(file.version, 2);
        assert_eq!(file.keybinds.len(), 1);
        assert_eq!(diagnostics.len(), 2);
        let persisted = std::fs::read_to_string(get_keybinds_path(temp.path())).unwrap();
        assert!(persisted.contains("\"sequence\""));
        let persisted: serde_json::Value = serde_json::from_str(&persisted).unwrap();
        assert!(persisted["keybinds"][0].get("key").is_none());
    }
    #[test]
    fn invalid_v2_falls_back_without_partial_application() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(get_keybinds_path(temp.path()),r#"{"version":2,"keybinds":[{"action":"issue.create","sequence":[{"key":"x"}]},{"action":"palette.open","sequence":[{"key":"x"}]}]}"#).unwrap();
        assert_eq!(load_keybinds(temp.path()), KeybindsFile::default());
    }
    #[test]
    fn rejects_unsupported_version() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            get_keybinds_path(temp.path()),
            r#"{"version":3,"keybinds":[]}"#,
        )
        .unwrap();
        assert!(load_keybinds_with_diagnostics(temp.path())
            .unwrap_err()
            .contains("unsupported"));
    }
}
