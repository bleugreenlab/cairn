//! The shapes of bginf ctl's `GET /profiles` and `GET /state` responses.
//! Unknown fields are tolerated, and every field bginf may omit is optional.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// One bginf profile, as `profiles/*.yaml` holds it and `/profiles` returns it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Profile {
    pub id: Option<String>,
    pub model: Option<String>,
    pub served_model: Option<String>,
    /// Free text describing the engine.
    pub engine: Option<String>,
    pub nodes: Vec<String>,
    pub head: Option<String>,
    pub start: Option<String>,
    pub stop: Option<String>,
    pub health: Option<String>,
    pub api_base: Option<String>,
    pub catalog_id: Option<String>,
    pub aliases: Vec<String>,
    pub notes: Option<String>,
}

/// `GET /profiles`: every profile by id.
pub type Profiles = BTreeMap<String, Profile>;

/// `GET /state`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct State {
    pub pools: BTreeMap<String, PoolState>,
    /// Only nodes with an outlet ctl manages appear here.
    pub power: BTreeMap<String, PowerState>,
    /// Each node's last report, keyed by bginf node name.
    pub nodes: BTreeMap<String, NodeReport>,
}

/// `/state pools.<pool>`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PoolState {
    /// The resolved profile id; `off` when nothing runs.
    pub desired: String,
    pub reason: String,
    pub pin: Option<String>,
    pub default: Option<String>,
    pub running: Option<Running>,
    pub converged: bool,
    pub nodes: Vec<String>,
}

/// What a pool's nodes are running: one profile id, or several when they
/// disagree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Running {
    One(String),
    Many(Vec<String>),
}

impl fmt::Display for Running {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::One(id) => f.write_str(id),
            Self::Many(ids) => f.write_str(&ids.join(", ")),
        }
    }
}

/// `/state power.<node>`, for a node whose outlet ctl manages.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PowerState {
    /// `on` or `off`: what ctl wants, which is what its resolver uses.
    pub desired: String,
    pub phase: Option<String>,
    pub outlet: Option<String>,
    pub draw_w: Option<f64>,
}

/// `/state nodes.<node>`: the node agent's last report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeReport {
    /// The profile answering its health URL on this node, if any.
    pub running: Option<String>,
    pub healthy: Option<bool>,
    pub stale: bool,
    pub age_s: Option<i64>,
    pub converging: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_is_null_a_string_or_a_list() {
        let pools: BTreeMap<String, PoolState> = serde_json::from_value(serde_json::json!({
            "a": {"desired": "off", "reason": "default", "running": null, "converged": true, "nodes": []},
            "b": {"desired": "x", "reason": "pin", "running": "x", "converged": true, "nodes": ["n"]},
            "c": {"desired": "x", "reason": "pin", "running": ["x", "y"], "converged": false,
                  "nodes": ["n", "m"], "future_field": 1}
        }))
        .unwrap();
        assert_eq!(pools["a"].running, None);
        assert_eq!(pools["b"].running, Some(Running::One("x".into())));
        assert_eq!(
            pools["c"].running,
            Some(Running::Many(vec!["x".into(), "y".into()]))
        );
    }
}
