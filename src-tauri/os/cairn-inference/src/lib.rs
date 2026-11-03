//! The cairn-inference facet: how each model is run and where.
//!
//! The records ([`profile`], [`deployment`], [`machine`]) describe how a model
//! is run, which machines a deployment of it occupies, and, for a machine that
//! is not an enrolled executor, where it answers; the [`inventory`] holds them
//! together and validates them as a whole. [`Addresses`] is the one answer to
//! where a machine is reached: its record, else what the fleet observes of the
//! enrolled executor's connection and pool link.
//! Which model runs when is not decided here: execution's placement
//! holds a running model as a resident on a pool of machines it detects, and
//! acquires, evicts and warms it like any other work (tenets 1336, 1337).
//! [`served`] is what Cairn offers from its deployments: every deployed
//! model under its real name, loaded or not, with what each model takes and
//! gives and the context its engine can actually hold.
//!
//! [`metrics`] reads what a running engine says about itself in its
//! Prometheus metrics, vLLM's or llama.cpp's, into rates and latencies.
//!
//! [`control`] is the act side: a deployment's phase as the orch holds it, and
//! the transitions start and stop requests and the head's reports move it
//! through.
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
pub mod fit;
pub mod hf;
pub mod import;
pub mod inventory;
pub mod machine;
pub mod metrics;
pub mod model;
pub mod profile;
pub mod record;
pub mod render;
pub mod served;
pub mod verify;

#[cfg(test)]
pub(crate) mod fixture;

pub use deployment::{Deployment, DeploymentId};
pub use inventory::{
    ChangeReport, Inventory, KindReport, UpdatedRecord, ValidationError, YamlError,
};
pub use machine::{
    unknown_address, Address, AddressSource, Addresses, MachineName, MachineRecord, Network,
};
pub use model::{Model, ModelFacts};
pub use profile::{
    Accelerator, AcceleratorFamily, AcceleratorRuntime, CitedSource, Container, Engine,
    HostOptions, InferenceProfile, InferenceProfileId, Interconnect, MemoryPerMachine, ModelRef,
    Mount, MountSource, OperatorAnswer, ProfileOrigin, Requirements, Server, SourcePin,
    SystemRamBytes, VramBytes, WeightFile,
};
pub use record::RecordSource;
pub use served::{
    deployments_serving, profile_verbs, served_models, EngineMeasure, ServedDeployment, ServedModel,
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
