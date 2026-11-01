//! `inference:` in `settings.yaml`: the operator's word on what each
//! inference profile's model serves and takes.

use std::collections::{BTreeMap, BTreeSet};

use cairn_models_types::catalog::{Modality, VerbKind};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceSettings {
    /// The operator's word on what each profile's model serves and takes, by
    /// profile id. It lies over the profile record wherever the record is read.
    /// A file written before inference recipes were renamed profiles names the
    /// section `recipes`; it is read as this one and written back as
    /// `profiles`, since refusing it would fail the whole settings file.
    #[serde(default, alias = "recipes", skip_serializing_if = "BTreeMap::is_empty")]
    pub profiles: BTreeMap<String, InferenceProfileDeclaration>,
    /// Whether the file still names the retired bginf link (`bginf:`).
    /// Accepted and dropped, as `fleet`'s retired `tunnelPort` is: refusing
    /// it would fail the whole settings file under `deny_unknown_fields`, and
    /// every section read from it would fall back to its default. Nothing
    /// reads the link, and the next write of the section drops it.
    #[serde(
        default,
        rename = "bginf",
        skip_serializing,
        deserialize_with = "present"
    )]
    pub retired_bginf_link: bool,
}

fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    serde::de::IgnoredAny::deserialize(deserializer).map(|_| true)
}

/// What one profile's model serves and takes, as the operator declares it.
/// Each part that is present replaces the record's own; an absent part
/// leaves the record's word (or its silence) alone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceProfileDeclaration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verbs: Option<BTreeSet<VerbKind>>,
    /// What the model takes in, in OpenRouter's modality names (`text`,
    /// `image`, `audio`, ...). Declared together with `output_modalities`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_modalities: Option<Vec<Modality>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_modalities: Option<Vec<Modality>>,
}

impl InferenceProfileDeclaration {
    fn validate(&self, profile: &str) -> Result<(), String> {
        let at = format!("inference.profiles.{profile}");
        if profile.trim().is_empty() {
            return Err("inference.profiles entries must name a profile id".into());
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

impl InferenceSettings {
    pub fn validate(&self) -> Result<(), String> {
        self.profiles
            .iter()
            .try_for_each(|(profile, declared)| declared.validate(profile))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_retired_link_is_accepted_and_never_written() {
        let parsed: InferenceSettings = serde_yaml::from_str(
            "bginf: {url: 'http://10.20.20.16:4100', machines: {halo: fedora}}\nprofiles: {r: {verbs: [session]}}\n",
        )
        .unwrap();
        assert!(parsed.retired_bginf_link);
        assert!(parsed.profiles.contains_key("r"));
        let written = serde_yaml::to_string(&parsed).unwrap();
        assert!(!written.contains("bginf"), "{written}");
        let reread: InferenceSettings = serde_yaml::from_str(&written).unwrap();
        assert!(!reread.retired_bginf_link);
        assert_eq!(reread.profiles, parsed.profiles);
    }

    #[test]
    fn declarations_written_as_recipes_load_and_are_written_as_profiles() {
        let parsed: InferenceSettings =
            serde_yaml::from_str("recipes: {dsv4f-tp2: {verbs: [session]}}\n").unwrap();
        assert!(parsed.profiles.contains_key("dsv4f-tp2"));
        let written = serde_yaml::to_string(&parsed).unwrap();
        assert!(written.starts_with("profiles:"), "{written}");
        assert!(!written.contains("recipes"), "{written}");
    }

    #[test]
    fn unknown_keys_are_refused() {
        let parsed: Result<InferenceSettings, _> =
            serde_json::from_value(serde_json::json!({"url": "http://x"}));
        assert!(parsed.is_err());
        let parsed: Result<InferenceSettings, _> = serde_json::from_value(
            serde_json::json!({"profiles": {"r": {"verbs": ["session"], "contextLength": 4}}}),
        );
        assert!(parsed.is_err());
    }

    #[test]
    fn a_profile_declaration_parses_and_says_something_whole() {
        let parsed: InferenceSettings = serde_json::from_value(serde_json::json!({
            "profiles": {"dsv4f-tp2": {
                "verbs": ["complete", "session", "decide"],
                "inputModalities": ["text", "image"],
                "outputModalities": ["text"]
            }}
        }))
        .unwrap();
        parsed.validate().unwrap();
        let declared = &parsed.profiles["dsv4f-tp2"];
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
                serde_json::from_value(serde_json::json!({"profiles": {"r": declared}})).unwrap();
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
            "profiles": {"r": {"verbs": ["embed"]}}
        }))
        .unwrap();
        assert!(verbs_only.validate().is_ok());
    }
}
