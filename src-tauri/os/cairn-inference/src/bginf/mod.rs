//! The bginf mirror: import bginf's configuration into Cairn's records, and
//! compare Cairn's resolution against what bginf's control service reports.
//!
//! Read-only by construction: [`client::BginfClient`] can only send GET
//! requests, and nothing here starts, stops or powers anything. The whole
//! module is deleted when bginf retires.
pub mod client;
pub mod compare;
pub mod files;
pub mod import;
pub mod wire;

pub use client::{BginfClient, ClientError};
pub use compare::{compare, Difference, PoolComparison, Verdict};
pub use files::{BginfFiles, FilesError, NodeEntry};
pub use import::{import, Imported, MachineMap, PoolSlots, Topology};
pub use wire::{NodeReport, PoolState, PowerState, Profile, Profiles, Running, State};
