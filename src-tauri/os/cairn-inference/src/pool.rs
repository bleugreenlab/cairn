//! Pools: fixed sets of machines that resolve one desired state together.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::deployment::DeploymentId;
use crate::machine::MachineName;

crate::string_id!(
    /// A pool's id, unique within the inventory.
    PoolId
);

/// A set of deployments on disjoint machines of one pool.
pub type DeploymentSet = BTreeSet<DeploymentId>;

/// Who owns a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecordSource {
    /// Mirrored from bginf and read-only: every re-import replaces it.
    Bginf,
    /// Cairn's own record.
    Cairn,
}

impl RecordSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bginf => "bginf",
            Self::Cairn => "cairn",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bginf" => Some(Self::Bginf),
            "cairn" => Some(Self::Cairn),
            _ => None,
        }
    }
}

/// A fixed set of machines whose desired state resolves together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    pub id: PoolId,
    pub source: RecordSource,
    pub machines: Vec<PoolMachine>,
    /// The standing desired set. Unset is the empty set: nothing runs.
    #[serde(default)]
    pub default: DeploymentSet,
    /// `None` is no pin. `Some` of the empty set is pinned to nothing
    /// (bginf's `off` pin), which fits whatever is powered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<DeploymentSet>,
}

/// One machine of a pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMachine {
    pub name: MachineName,
    /// The machine's network address, held once here because executors don't
    /// carry a LAN address and the Sparks aren't executors. `None` is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

impl Pool {
    pub fn has_machine(&self, machine: &MachineName) -> bool {
        self.machines.iter().any(|m| &m.name == machine)
    }

    pub fn address_of(&self, machine: &MachineName) -> Option<&str> {
        self.machines
            .iter()
            .find(|m| &m.name == machine)
            .and_then(|m| m.address.as_deref())
    }
}
