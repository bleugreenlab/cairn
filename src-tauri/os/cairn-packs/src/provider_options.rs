//! Provider-option descriptors, shared by every web-service slot.
//!
//! A [`ProviderOption`] carries a key, a label, and an [`OptionControl`] (text /
//! select / number / bool) that drives how the settings UI renders it and how
//! [`validate_options`] checks a submitted value. Pack-declared providers
//! deserialize their options into this model, and the compiled fetch and PDF
//! providers construct it directly, so every provider validates and renders
//! options identically and the frontend mirrors a single `OptionControl` type.

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
    Text {
        default: String,
        placeholder: String,
    },
    Select {
        choices: Vec<Choice>,
        default: String,
    },
    Number {
        min: f64,
        max: f64,
        default: f64,
        /// Only whole numbers are accepted, and the value reaches a request as an
        /// integer rather than a float.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        integer: bool,
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
    /// A free-text option.
    pub fn text(key: &str, label: &str, default: &str, placeholder: &str) -> Self {
        ProviderOption {
            key: key.to_string(),
            label: label.to_string(),
            control: OptionControl::Text {
                default: default.to_string(),
                placeholder: placeholder.to_string(),
            },
        }
    }

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
            control: OptionControl::Number {
                min,
                max,
                default,
                integer: false,
            },
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
/// "Firecrawl web fetch").
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
            OptionControl::Text { .. } => {
                let text = value
                    .as_str()
                    .ok_or_else(|| format!("Option `{key}` must be a string"))?;
                if text != text.trim() {
                    return Err(format!(
                        "Option `{key}` must not have leading or trailing whitespace"
                    ));
                }
            }
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
            OptionControl::Number {
                min, max, integer, ..
            } => {
                let n = value
                    .as_f64()
                    .or_else(|| value.as_i64().map(|i| i as f64))
                    .or_else(|| value.as_u64().map(|u| u as f64))
                    .ok_or_else(|| format!("Option `{key}` must be a number"))?;
                if n < *min || n > *max {
                    return Err(format!("Option `{key}` must be between {min} and {max}"));
                }
                if *integer && value.as_i64().is_none() && value.as_u64().is_none() {
                    return Err(format!("Option `{key}` must be a whole number"));
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
            ProviderOption::text("account", "Account", "", "Account ID"),
            ProviderOption::select("mode", "Mode", &[("a", "A"), ("b", "B")], "a"),
            ProviderOption::number("count", "Count", 1.0, 10.0, 5.0),
            ProviderOption::bool("flag", "Flag", true),
        ];
        assert!(validate_options(&descriptors, &opts(&[("nope", 1.into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("mode", "c".into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("count", 99.into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("flag", "yes".into())]), "x").is_err());
        assert!(validate_options(&descriptors, &opts(&[("account", 1.into())]), "x").is_err());
        assert!(
            validate_options(&descriptors, &opts(&[("account", " padded ".into())]), "x").is_err()
        );
        assert!(validate_options(&descriptors, &opts(&[("mode", "b".into())]), "x").is_ok());
        assert!(validate_options(&descriptors, &opts(&[("count", 7.into())]), "x").is_ok());
        assert!(validate_options(&descriptors, &opts(&[("count", 7.5.into())]), "x").is_ok());
        assert!(validate_options(&descriptors, &opts(&[("flag", true.into())]), "x").is_ok());
        assert!(
            validate_options(&descriptors, &opts(&[("account", "abc123".into())]), "x").is_ok()
        );
    }

    #[test]
    fn an_integer_option_refuses_a_fraction() {
        let descriptors = vec![ProviderOption {
            key: "count".into(),
            label: "Count".into(),
            control: OptionControl::Number {
                min: 1.0,
                max: 10.0,
                default: 5.0,
                integer: true,
            },
        }];
        let error =
            validate_options(&descriptors, &opts(&[("count", 2.5.into())]), "x").unwrap_err();
        assert!(error.contains("whole number"), "{error}");
        assert!(validate_options(&descriptors, &opts(&[("count", 3.into())]), "x").is_ok());
    }
}
