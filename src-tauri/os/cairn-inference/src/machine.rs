//! Machines, named the way Cairn names executors, and where to reach them.

use serde::{Deserialize, Serialize};

use crate::record::RecordSource;

crate::string_id!(
    /// A machine by its Cairn executor name, whether or not that executor is
    /// enrolled yet: the Sparks are `bginf-sp1` and `bginf-sp2`, and the halo
    /// box is `fedora`.
    MachineName
);

/// Where one machine's deployments answer: its network address, held once
/// here because executors don't carry a LAN address. A deployment's API base
/// and health URL are its head's address plus the deployment's port and paths.
///
/// Which machines form a pool is not recorded: placement detects it from the
/// links the executors report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineRecord {
    pub name: MachineName,
    #[serde(default)]
    pub source: RecordSource,
    pub address: String,
}
