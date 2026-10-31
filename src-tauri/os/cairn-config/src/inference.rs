//! `inference:` in `settings.yaml`: how this workspace reaches the services
//! that decide what its inference machines run.

use std::collections::{BTreeMap, BTreeSet};

use cairn_models_types::catalog::{Modality, VerbKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceSettings {
    /// The read-only link to bginf's control service. Absent means Cairn does
    /// not mirror bginf.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bginf: Option<BginfLinkSettings>,
    /// The operator's word on what each recipe's model serves and takes, by
    /// recipe id. It lies over the recipe record wherever the record is read,
    /// so it survives every re-import of a mirrored record, which is why it
    /// lives here rather than on the record (bginf's profiles never say).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub recipes: BTreeMap<String, RecipeDeclaration>,
}

/// What one recipe's model serves and takes, as the operator declares it.
/// Each part that is present replaces the record's own; an absent part
/// leaves the record's word (or its silence) alone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecipeDeclaration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verbs: Option<BTreeSet<VerbKind>>,
    /// What the model takes in, in OpenRouter's modality names (`text`,
    /// `image`, `audio`, ...). Declared together with `output_modalities`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_modalities: Option<Vec<Modality>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_modalities: Option<Vec<Modality>>,
}

impl RecipeDeclaration {
    fn validate(&self, recipe: &str) -> Result<(), String> {
        let at = format!("inference.recipes.{recipe}");
        if recipe.trim().is_empty() {
            return Err("inference.recipes entries must name a recipe id".into());
        }
        if self.verbs.as_ref().is_some_and(BTreeSet::is_empty) {
            return Err(format!("{at}.verbs names no verb"));
        }
        match (&self.input_modalities, &self.output_modalities) {
            (None, None) if self.verbs.is_none() => Err(format!("{at} declares nothing")),
            (None, None) => Ok(()),
            (Some(input), Some(output)) if !input.is_empty() && !output.is_empty() => Ok(()),
            (Some(_), Some(_)) => Err(format!("{at} declares an empty modality list")),
            _ => Err(format!(
                "{at} declares inputModalities and outputModalities together, or neither"
            )),
        }
    }
}

/// Where bginf's control service listens, and how its node names map to
/// Cairn machines. Its token is never here: it is a `bginf` account in the
/// encrypted credential store. Retires with bginf.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BginfLinkSettings {
    /// The control service's base URL, `http://<host>:<port>`.
    pub url: String,
    /// bginf node name to Cairn machine name; a node absent here keeps its name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub machines: BTreeMap<String, String>,
}

impl BginfLinkSettings {
    pub fn validate(&self) -> Result<(), String> {
        let rest = self
            .url
            .strip_prefix("http://")
            .or_else(|| self.url.strip_prefix("https://"))
            .ok_or_else(|| format!("inference.bginf.url {:?} must be an http(s) URL", self.url))?;
        if rest.trim_end_matches('/').is_empty() {
            return Err(format!("inference.bginf.url {:?} has no host", self.url));
        }
        let mut targets = BTreeSet::new();
        for (node, machine) in &self.machines {
            if node.trim().is_empty() || machine.trim().is_empty() {
                return Err(
                    "inference.bginf.machines entries must name a node and a machine".into(),
                );
            }
            if !targets.insert(machine) {
                return Err(format!(
                    "inference.bginf.machines maps two nodes to machine {machine}"
                ));
            }
        }
        Ok(())
    }
}

impl InferenceSettings {
    pub fn validate(&self) -> Result<(), String> {
        self.bginf
            .as_ref()
            .map_or(Ok(()), BginfLinkSettings::validate)?;
        self.recipes
            .iter()
            .try_for_each(|(recipe, declared)| declared.validate(recipe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(url: &str, machines: &[(&str, &str)]) -> BginfLinkSettings {
        BginfLinkSettings {
            url: url.into(),
            machines: machines
                .iter()
                .map(|(n, m)| (n.to_string(), m.to_string()))
                .collect(),
        }
    }

    #[test]
    fn a_link_needs_an_http_url_and_an_invertible_machine_map() {
        assert!(link("http://ctl.example:4100", &[("halo", "fedora")])
            .validate()
            .is_ok());
        assert!(link("ctl.example:4100", &[]).validate().is_err());
        assert!(link("http://", &[]).validate().is_err());
        assert!(link("http://ctl.example", &[("a", "m"), ("b", "m")])
            .validate()
            .is_err());
    }

    #[test]
    fn unknown_keys_are_refused() {
        let parsed: Result<InferenceSettings, _> =
            serde_json::from_value(serde_json::json!({"bginf": {"url": "http://x", "token": "t"}}));
        assert!(parsed.is_err());
        let parsed: Result<InferenceSettings, _> = serde_json::from_value(
            serde_json::json!({"recipes": {"r": {"verbs": ["session"], "contextLength": 4}}}),
        );
        assert!(parsed.is_err());
    }

    #[test]
    fn a_recipe_declaration_parses_and_says_something_whole() {
        let parsed: InferenceSettings = serde_json::from_value(serde_json::json!({
            "recipes": {"dsv4f-tp2": {
                "verbs": ["complete", "session", "decide"],
                "inputModalities": ["text", "image"],
                "outputModalities": ["text"]
            }}
        }))
        .unwrap();
        parsed.validate().unwrap();
        let declared = &parsed.recipes["dsv4f-tp2"];
        assert_eq!(
            declared.verbs,
            Some(BTreeSet::from([
                VerbKind::Complete,
                VerbKind::Session,
                VerbKind::Decide
            ]))
        );
        assert_eq!(
            declared.input_modalities,
            Some(vec![Modality::Text, Modality::Image])
        );

        let refused = |declared: serde_json::Value| {
            let parsed: InferenceSettings =
                serde_json::from_value(serde_json::json!({"recipes": {"r": declared}})).unwrap();
            parsed.validate().unwrap_err()
        };
        assert!(refused(serde_json::json!({})).contains("declares nothing"));
        assert!(refused(serde_json::json!({"verbs": []})).contains("names no verb"));
        assert!(refused(serde_json::json!({"inputModalities": ["text"]})).contains("together"));
        assert!(refused(serde_json::json!({
            "inputModalities": [], "outputModalities": ["text"]
        }))
        .contains("empty"));
        let verbs_only: InferenceSettings = serde_json::from_value(serde_json::json!({
            "recipes": {"r": {"verbs": ["embed"]}}
        }))
        .unwrap();
        assert!(verbs_only.validate().is_ok());
    }
}
