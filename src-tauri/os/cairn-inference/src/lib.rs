//! The cairn-inference facet: which model each inference machine should run.
//!
//! The records ([`recipe`], [`deployment`], [`pool`]) describe how a model is
//! run, where it is placed, and which machines share one desired state; the
//! [`inventory`] holds them together and validates them as a whole.
//! [`resolve`] is the pin-over-default resolver that turns a pool and its
//! powered machines into the deployments it should be running. A pool under
//! Cairn's own control is decided instead by the [`pool_control`] module's pool
//! controller, which loads on request, unloads for waiting builds and loads
//! the default when the pool is idle, and writes the result as desired state.
//! [`served`] is what Cairn offers from its deployments: every deployed
//! recipe under its served name and aliases, loaded or not, with what each
//! model takes and gives and the context its engine can actually hold.
//!
//! [`control`] is the act side: a deployment's phase as the orch holds it, the
//! transitions start and stop requests and the head's reports move it through,
//! the machines a phase reserves, and whether a gang fits.
//!
//! Every record is Cairn's own and the operator writes it, as a whole
//! [`Inventory::from_yaml`] document or records [`Inventory::upsert`]ed into
//! one. Records mirrored from bginf before it retired stay read-only until
//! [`Inventory::adopt`] makes them Cairn's, keeping their ids.
//!
//! The crate owns no storage: the host persists the inventory. It depends only
//! on the models facet's vocabulary, so it is hostable outside Cairn.
pub mod control;
pub mod deployment;
pub mod inventory;
pub mod machine;
pub mod pool;
pub mod pool_control;
pub mod recipe;
pub mod resolve;
pub mod served;

#[cfg(test)]
pub(crate) mod fixture;

pub use deployment::{Deployment, DeploymentId};
pub use inventory::{
    ChangeReport, Inventory, KindReport, UpdatedRecord, ValidationError, YamlError,
};
pub use machine::MachineName;
pub use pool::{DeploymentSet, Pool, PoolId, PoolMachine, RecordSource};
pub use recipe::{
    Accelerator, AcceleratorFamily, AcceleratorRuntime, Engine, Interconnect, MemoryPerMachine,
    ModelRef, Recipe, RecipeId, Requirements, SystemRamBytes, VramBytes, WeightFile,
};
pub use resolve::{resolve, Blocked, Reason, Resolution, Slot, SLOTS};
pub use served::{served_models, EngineMeasure, ServedModel};

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
