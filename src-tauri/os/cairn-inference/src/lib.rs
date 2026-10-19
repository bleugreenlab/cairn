//! The cairn-inference facet: which model each inference machine should run.
//!
//! The records ([`recipe`], [`deployment`], [`pool`]) describe how a model is
//! run, where it is placed, and which machines share one desired state; the
//! [`inventory`] holds them together and validates them as a whole.
//! [`resolve`] is the pin-over-default resolver that turns a pool and its
//! powered machines into the deployments it should be running.
//!
//! The [`bginf`] module mirrors the operator's existing bginf control service
//! into these records and compares Cairn's resolution against bginf's own. It
//! is read-only by construction and retires with bginf.
//!
//! The crate owns no storage: the host persists the inventory. It depends only
//! on the models facet's vocabulary, so it is hostable outside Cairn.
pub mod bginf;
pub mod deployment;
pub mod inventory;
pub mod machine;
pub mod pool;
pub mod recipe;
pub mod resolve;

#[cfg(test)]
pub(crate) mod fixture;

pub use deployment::{Deployment, DeploymentId};
pub use inventory::{
    ImportReport, Inventory, KindReport, UpdatedRecord, ValidationError, YamlError,
};
pub use machine::MachineName;
pub use pool::{DeploymentSet, Pool, PoolId, PoolMachine, RecordSource};
pub use recipe::{
    Accelerator, AcceleratorFamily, AcceleratorRuntime, Engine, Interconnect, MemoryPerMachine,
    ModelRef, Recipe, RecipeId, Requirements, SystemRamBytes, VramBytes, WeightFile,
};
pub use resolve::{
    converged, loaded, resolve, Blocked, MachineObservation, Reason, Resolution, Slot, SLOTS,
};

/// A string identifier newtype: transparent in serde, ordered, displayable.
macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }
    };
}
pub(crate) use string_id;
