//! Provider-option descriptors, shared by every web-service slot.
//!
//! A [`ProviderOption`] carries a key, a label, and an [`OptionControl`] (text /
//! select / number / bool, or a select whose choices a request fetches with
//! the provider's credential) that drives how the settings UI renders it and
//! how [`validate_options`] checks a submitted value. Pack-declared providers
//! deserialize their options into this model, and the compiled fetch and PDF
//! providers construct it directly, so every provider validates and renders
//! options identically and the frontend mirrors a single `OptionControl` type.

use serde::{Serialize, Serializer};
use std::collections::HashMap;

use crate::provider::http::ChoiceSource;

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
    /// A select whose choices are what a request made with the provider's
    /// credential returns. It has no default. The UI sees `{type: "select",
    /// dynamic: true}` and asks the runner for the choices; the request itself
    /// is never mirrored.
    #[serde(rename = "select")]
    DynamicSelect {
        #[serde(rename = "dynamic", serialize_with = "dynamic_marker")]
        from: Box<ChoiceSource>,
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

fn dynamic_marker<S: Serializer>(_: &ChoiceSource, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bool(true)
}

impl OptionControl {
    /// The fetched-choices source, for a dynamic select.
    pub fn choice_source(&self) -> Option<&ChoiceSource> {
        match self {
            OptionControl::DynamicSelect { from } => Some(from),
            _ => None,
        }
    }
}

/// A single configurable option for a provider, surfaced to the settings UI.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOption {
    pub key: String,
    pub label: String,
    pub control: OptionControl,
    /// A text or dynamic-select option the provider cannot run without: while
    /// it is blank the provider needs setup rather than sending a request that
    /// cannot work.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub required: bool,
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
            required: false,
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
            required: false,
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
            required: false,
        }
    }

    /// A boolean toggle option.
    pub fn bool(key: &str, label: &str, default: bool) -> Self {
        ProviderOption {
            key: key.to_string(),
            label: label.to_string(),
            control: OptionControl::Bool { default },
            required: false,
        }
    }
}

/// The first required option whose effective value (the stored text, else the
/// default; a dynamic select has none) is blank.
pub fn missing_required<'a>(
    descriptors: &'a [ProviderOption],
    options: &HashMap<String, serde_yaml::Value>,
) -> Option<&'a ProviderOption> {
    descriptors.iter().find(|option| {
        let default = match &option.control {
            OptionControl::Text { default, .. } => default.as_str(),
            OptionControl::DynamicSelect { .. } => "",
            _ => return false,
        };
        option.required
            && options
                .get(&option.key)
                .and_then(serde_yaml::Value::as_str)
                .unwrap_or(default)
                .trim()
                .is_empty()
    })
}

/// The stored options a settings form shows and resubmits. A static select
/// value that is no longer one of the declared choices is left out, so the form
/// shows the default the request already falls back to, and resubmitting the
/// form along with an unrelated edit does not send a value [`validate_options`]
/// refuses. A dynamic select's value is always kept: whether it is still a
/// choice is only knowable over the network.
pub fn usable_options(
    descriptors: &[ProviderOption],
    options: &HashMap<String, serde_yaml::Value>,
) -> HashMap<String, serde_yaml::Value> {
    let retired = |key: &str, value: &serde_yaml::Value| {
        descriptors.iter().any(|option| {
            option.key == key
                && matches!(&option.control, OptionControl::Select { choices, .. }
                    if !value.as_str().is_some_and(|v| choices.iter().any(|c| c.value == v)))
        })
    };
    options
        .iter()
        .filter(|(key, value)| !retired(key, value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
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
            OptionControl::DynamicSelect { .. } => {
                let s = value
                    .as_str()
                    .ok_or_else(|| format!("Option `{key}` must be a string"))?;
                if s.trim().is_empty() || s != s.trim() {
                    return Err(format!(
                        "Option `{key}` must be one of the choices, not blank or padded"
                    ));
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
            required: false,
        }];
        let error =
            validate_options(&descriptors, &opts(&[("count", 2.5.into())]), "x").unwrap_err();
        assert!(error.contains("whole number"), "{error}");
        assert!(validate_options(&descriptors, &opts(&[("count", 3.into())]), "x").is_ok());
    }

    /// A select value the declaration no longer offers is dropped from what a
    /// form resubmits, and everything else, unknown keys included, is kept
    /// for validation to judge.
    #[test]
    fn a_retired_select_value_is_left_out_of_what_a_form_resubmits() {
        let descriptors = vec![
            ProviderOption::select("waitUntil", "Wait until", &[("a", "A"), ("b", "B")], "a"),
            ProviderOption::text("accountId", "Account ID", "", ""),
        ];
        let stored = opts(&[
            ("waitUntil", serde_yaml::Value::from("load")),
            ("accountId", serde_yaml::Value::from("acct")),
        ]);
        let usable = usable_options(&descriptors, &stored);
        assert_eq!(
            usable,
            opts(&[("accountId", serde_yaml::Value::from("acct"))])
        );
        let mut resubmitted = usable.clone();
        resubmitted.insert("waitUntil".into(), serde_yaml::Value::from("a"));
        assert!(validate_options(&descriptors, &resubmitted, "t").is_ok());
        let kept = opts(&[("waitUntil", serde_yaml::Value::from("b"))]);
        assert_eq!(usable_options(&descriptors, &kept), kept);
    }

    fn dynamic(key: &str) -> ProviderOption {
        let credential = crate::provider::parse_credential(
            "login",
            "label: L\nkind: secret\nvar: K\nhosts: [api.example.test]\naccessors: [provider:web-fetch]\n",
        )
        .unwrap();
        let from = crate::provider::http::parse_choice_source(
            crate::provider::Slot::WebFetch,
            &credential,
            serde_yaml::from_str(
                "{ request: { method: GET, url: \"https://api.example.test/accounts\" }, items: result, value: id, label: name }",
            )
            .unwrap(),
        )
        .unwrap();
        ProviderOption {
            key: key.into(),
            label: "Account".into(),
            control: OptionControl::DynamicSelect {
                from: Box::new(from),
            },
            required: true,
        }
    }

    /// A fetched-choices select keeps whatever it stored, is missing while
    /// blank, and accepts any trimmed non-blank value, since only the network
    /// knows the choices.
    #[test]
    fn a_dynamic_select_keeps_its_value_and_is_missing_while_blank() {
        let descriptors = vec![dynamic("accountId")];
        let stored = opts(&[("accountId", "acct-9".into())]);
        assert_eq!(usable_options(&descriptors, &stored), stored);
        assert!(validate_options(&descriptors, &stored, "t").is_ok());
        for bad in ["", "  ", " acct "] {
            assert!(
                validate_options(&descriptors, &opts(&[("accountId", bad.into())]), "t").is_err(),
                "{bad:?}"
            );
        }
        assert_eq!(
            missing_required(&descriptors, &HashMap::new()).map(|o| o.key.as_str()),
            Some("accountId")
        );
        assert_eq!(missing_required(&descriptors, &stored), None);
        let json = serde_json::to_value(&descriptors[0]).unwrap();
        assert_eq!(
            json["control"],
            serde_json::json!({ "type": "select", "dynamic": true })
        );
        assert_eq!(json["required"], true);
    }

    #[test]
    fn a_required_text_option_is_missing_while_blank() {
        let mut account = ProviderOption::text("account", "Account ID", "", "id");
        account.required = true;
        let descriptors = vec![account, ProviderOption::text("note", "Note", "", "")];
        let missing = |pairs: &[(&str, serde_yaml::Value)]| {
            missing_required(&descriptors, &opts(pairs)).map(|option| option.label.clone())
        };
        assert_eq!(missing(&[]).as_deref(), Some("Account ID"));
        assert_eq!(
            missing(&[("account", "  ".into())]).as_deref(),
            Some("Account ID")
        );
        assert_eq!(missing(&[("account", "acc-1".into())]), None);
    }
}
