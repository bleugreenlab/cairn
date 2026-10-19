//! `inference:` in `settings.yaml`: how this workspace reaches the services
//! that decide what its inference machines run.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceSettings {
    /// The read-only link to bginf's control service. Absent means Cairn does
    /// not mirror bginf.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bginf: Option<BginfLinkSettings>,
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
            .map_or(Ok(()), BginfLinkSettings::validate)
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
    }
}
