//! Shared provider-option descriptors for the typed web-service catalogs.
//!
//! Web fetch ([`super::web_fetch`]), web search ([`super::web_search`]), and PDF
//! ([`super::pdf`]) all present per-provider options to the settings UI through
//! one model: a [`ProviderOption`] carries a key, a label, and an
//! [`OptionControl`] (select / number / bool) that drives how the UI renders and
//! how [`validate_options`] checks a submitted value. Keeping this in one module
//! means the three services validate and render options identically, and the
//! frontend mirrors a single `OptionControl` type.

use serde::Serialize;
use std::collections::HashMap;

/// One choice in a `Select` option control.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Choice {
    pub value: String,
    pub label: String,
}

/// The control type for a provider option, driving how the settings UI renders
/// and validates it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum OptionControl {
    Select {
        choices: Vec<Choice>,
        default: String,
    },
    Number {
        min: f64,
        max: f64,
        default: f64,
    },
    Bool {
        default: bool,
    },
}

/// A single configurable option for a provider, surfaced to the settings UI.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOption {
    pub key: String,
    pub label: String,
    pub control: OptionControl,
}

impl ProviderOption {
    /// A single-choice dropdown option.
    pub fn select(key: &str, label: &str, choices: &[(&str, &str)], default: &str) -> Self {
        ProviderOption {
            key: key.to_string(),
            label: label.to_string(),
            control: OptionControl::Select {
                choices: choices
                    .iter()
                    .map(|(value, label)| Choice {
                        value: (*value).to_string(),
                        label: (*label).to_string(),
                    })
                    .collect(),
                default: default.to_string(),
            },
        }
    }

    /// A bounded numeric option.
    pub fn number(key: &str, label: &str, min: f64, max: f64, default: f64) -> Self {
        ProviderOption {
            key: key.to_string(),
            label: label.to_string(),
            control: OptionControl::Number { min, max, default },
        }
    }

    /// A boolean toggle option.
    pub fn bool(key: &str, label: &str, default: bool) -> Self {
        ProviderOption {
            key: key.to_string(),
            label: label.to_string(),
            control: OptionControl::Bool { default },
        }
    }
}

/// Validate submitted options against a provider's descriptor. Unknown keys and
/// type/range/choice mismatches are rejected so only well-formed options reach
/// `settings.yaml`. `context` names the service in error messages (e.g.
/// "Jina web fetch").
pub fn validate_options(
    descriptors: &[ProviderOption],
    options: &HashMap<String, serde_yaml::Value>,
    context: &str,
) -> Result<(), String> {
    for (key, value) in options {
        let opt = descriptors
            .iter()
            .find(|o| &o.key == key)
            .ok_or_else(|| format!("Unknown option `{key}` for {context}"))?;
        match &opt.control {
            OptionControl::Select { choices, .. } => {
                let s = value
                    .as_str()
                    .ok_or_else(|| format!("Option `{key}` must be a string"))?;
                if !choices.iter().any(|c| c.value == s) {
                    let allowed = choices
                        .iter()
                        .map(|c| c.value.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(format!("Option `{key}` must be one of: {allowed}"));
                }
            }
            OptionControl::Number { min, max, .. } => {
                let n = value
                    .as_f64()
                    .or_else(|| value.as_i64().map(|i| i as f64))
                    .or_else(|| value.as_u64().map(|u| u as f64))
                    .ok_or_else(|| format!("Option `{key}` must be a number"))?;
                if n < *min || n > *max {
                    return Err(format!("Option `{key}` must be between {min} and {max}"));
                }
            }
            OptionControl::Bool { .. } => {
                value
                    .as_bool()
                    .ok_or_else(|| format!("Option `{key}` must be a boolean"))?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, serde_yaml::Value)]) -> HashMap<String, serde_yaml::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn validate_rejects_unknown_and_bad_values() {
        let descriptors = vec![
            ProviderOption::select("mode", "Mode", &[("a", "A"), ("b", "B")], "a"),
            ProviderOption::number("count", "Count", 1.0, 10.0, 5.0),
            ProviderOption::bool("flag", "Flag", true),
        ];
        assert!(validate_options(&descriptors, &opts(&[("nope", 1.into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("mode", "c".into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("count", 99.into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("flag", "yes".into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("mode", "b".into())]), "x").is_ok());
        assert!(validate_options(&descriptors, &opts(&[("count", 7.into())]), "x").is_ok());
        assert!(validate_options(&descriptors, &opts(&[("flag", true.into())]), "x").is_ok());
    }
}
