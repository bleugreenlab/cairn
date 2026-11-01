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

/// Who owns a record. A record that names no source is Cairn's.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum RecordSource {
    /// Mirrored from bginf before it retired and not adopted yet: read-only
    /// until `Inventory::adopt` makes it Cairn's own, keeping its id.
    Bginf,
    /// Cairn's own record, which the operator writes.
    #[default]
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
    #[serde(default)]
    pub source: RecordSource,
    pub machines: Vec<PoolMachine>,
    /// The standing desired set. Unset is the empty set: nothing runs.
    #[serde(default)]
    pub default: DeploymentSet,
    /// `None` is no pin. `Some` of the empty set is pinned to nothing, which
    /// fits whatever is powered. The pool controller has its own pin, with a
    /// holder, on its control record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<DeploymentSet>,
}

/// One machine of a pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMachine {
    pub name: MachineName,
    /// The machine's network address, held once here because executors don't
    /// carry a LAN address. `None` is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// The machine's address on the pool's fast link (the Sparks' ConnectX
    /// cable), which a profile spanning machines rendezvouses over. `None` is
    /// unknown: the operator writes it until link detection reports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
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

    pub fn link_of(&self, machine: &MachineName) -> Option<&str> {
        self.machines
            .iter()
            .find(|m| &m.name == machine)
            .and_then(|m| m.link.as_deref())
    }
}
